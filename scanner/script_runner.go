// Flow Batch Scan
//
// Copyright Flow Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scanner

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go/module/component"
	"github.com/onflow/flow-go/module/irrecoverable"
	"github.com/rs/zerolog"

	"github.com/onflow/flow-batch-scan/client"
)

// DefaultScriptRunnerMaxConcurrentScripts is the maximum number of scripts that can be running concurrently
// at any given time. If this is more than the rate limit, some scripts will be just waiting.
// As long as they don't wait too long, this is not a problem.
const DefaultScriptRunnerMaxConcurrentScripts = 20

type ScriptRunnerConfig struct {
	Script []byte

	MaxConcurrentScripts int
	HandleScriptError    func(AddressBatch, error) ScriptErrorAction
}

func DefaultScriptRunnerConfig() ScriptRunnerConfig {
	return ScriptRunnerConfig{
		Script: []byte(defaultScript),

		MaxConcurrentScripts: DefaultScriptRunnerMaxConcurrentScripts,
		HandleScriptError:    DefaultHandleScriptError,
	}
}

type ScriptRunner struct {
	component.Component

	ScriptRunnerConfig

	client          client.Client
	resultProcessor *ScriptResultProcessor

	batchChan chan AddressBatch
	limitChan chan struct{}
	log       zerolog.Logger
}

func NewScriptRunner(
	logger zerolog.Logger,
	client client.Client,
	resultProcessor *ScriptResultProcessor,
	config ScriptRunnerConfig,
) *ScriptRunner {
	r := &ScriptRunner{
		ScriptRunnerConfig: config,

		client:          client,
		resultProcessor: resultProcessor,

		batchChan: make(chan AddressBatch, 10),
		limitChan: make(chan struct{}, config.MaxConcurrentScripts),
		log:       logger.With().Str("component", "script_runner").Logger(),
	}

	r.Component = component.NewComponentManagerBuilder().
		AddWorker(r.processWorker).
		Build()

	return r
}

// Submit queues an address batch for script execution.
func (r *ScriptRunner) Submit(batch AddressBatch) {
	r.batchChan <- batch
}

func (r *ScriptRunner) processWorker(ctx irrecoverable.SignalerContext, ready component.ReadyFunc) {
	ready()

	for {
		select {
		case <-ctx.Done():
			return
		case input, ok := <-r.batchChan:
			if !ok {
				return
			}
			r.handleBatch(ctx, input)
		}
	}
}

func (r *ScriptRunner) handleBatch(ctx irrecoverable.SignalerContext, input AddressBatch) {
	if !input.IsValid() {
		return
	}
	if len(input.Addresses) == 0 {
		input.DoneHandling()
		return
	}

	r.limitChan <- struct{}{}
	go func() {
		defer func() { <-r.limitChan }()

		result, err := r.executeScript(ctx, input)

		if err == nil {
			r.resultProcessor.Submit(ProcessedAddressBatch{
				AddressBatch: input,
				Result:       result,
			})
			return
		}

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			ctx.Throw(err)
			return
		}

		r.log.
			Warn().
			Err(err).
			Msg("failed to run script")

		action := r.HandleScriptError(input, err)

		switch action := action.(type) {
		case ScriptErrorActionRetry:
			// retry the same batch
			r.log.
				Info().
				Msg("retrying")
			go func() {
				r.handleBatch(ctx, input)
			}()
			return
		case ScriptErrorActionSplit:
			// split the batch and run each half
			// this reduces computation usage,
			// and might also find any errors that are caused by
			// a single account having problems
			if len(input.Addresses) != 1 {
				r.log.
					Info().
					Int("addresses", len(input.Addresses)).
					Msg("retrying by splitting")
				left, right := input.Split()
				go func() {
					r.handleBatch(ctx, left)
					r.handleBatch(ctx, right)
				}()
				return
			}
			r.log.Info().Msg("cannot split, only one address left")
			// error out
		case ScriptErrorActionExclude:
			// exclude the problematic addresses and retry
			addresses := action.Addresses
			r.log.
				Info().
				Strs("addresses", func() []string {
					s := make([]string, len(addresses))
					for i, a := range addresses {
						s[i] = a.String()
					}
					return s
				}()).
				Msg("retrying by excluding")
			for _, address := range addresses {
				input.ExcludeAddress(address)
			}
			go func() {
				r.handleBatch(ctx, input)
			}()
			return
		case ScriptErrorActionNone:
		// nothing, just continue and error out
		case ScriptErrorActionUnhandled:
		// nothing, just continue and error out
		default:
			r.log.
				Warn().
				Interface("action", action).
				Msg("unknown script error action")
		}

		r.log.Warn().
			Msg("unable to handle error running script")
		ctx.Throw(err)
	}()
}

var accountFrozenRegex = regexp.MustCompile(`\[Error Code: 1204] account (?P<address>\w{16}) is frozen`)

// executeScript retries running the cadence script until we get a successful response back,
// returning an array of Balance pairs, along with a boolean representing whether we can continue
// or are finished processing.
func (r *ScriptRunner) executeScript(
	ctx context.Context,
	input AddressBatch,
) (result cadence.Value, err error) {
	arguments := convertAddressesToArguments(input.Addresses)
	r.log.
		Debug().
		Uint64("block_height", input.BlockHeight).
		Int("num_addresses", len(input.Addresses)).
		Msgf("executing script")

	return r.client.ExecuteScriptAtBlockHeight(
		ctx,
		input.BlockHeight,
		r.Script,
		arguments,
	)
}

// convertAddressesToArguments generates an array of cadence.Value from an array of flow.Address
func convertAddressesToArguments(addresses []flow.Address) []cadence.Value {
	var accounts []cadence.Value
	for _, address := range addresses {
		accounts = append(accounts, cadence.Address(address))
	}
	return []cadence.Value{cadence.NewArray(accounts)}
}

type ScriptErrorAction interface {
	isScriptErrorAction()
}

type ScriptErrorActionRetry struct{}

var _ ScriptErrorAction = ScriptErrorActionRetry{}

func (s ScriptErrorActionRetry) isScriptErrorAction() {}

type ScriptErrorActionNone struct{}

var _ ScriptErrorAction = ScriptErrorActionNone{}

func (s ScriptErrorActionNone) isScriptErrorAction() {}

type ScriptErrorActionUnhandled struct{}

var _ ScriptErrorAction = ScriptErrorActionUnhandled{}

func (s ScriptErrorActionUnhandled) isScriptErrorAction() {}

type ScriptErrorActionSplit struct{}

var _ ScriptErrorAction = ScriptErrorActionSplit{}

func (s ScriptErrorActionSplit) isScriptErrorAction() {}

type ScriptErrorActionExclude struct {
	Addresses []flow.Address
}

var _ ScriptErrorAction = ScriptErrorActionExclude{}

func (s ScriptErrorActionExclude) isScriptErrorAction() {}

func DefaultHandleScriptError(_ AddressBatch, err error) ScriptErrorAction {
	if errors.Is(err, context.Canceled) {
		return ScriptErrorActionNone{}
	}

	if strings.Contains(err.Error(), "state commitment not found") {
		return ScriptErrorActionNone{}
	}
	if strings.Contains(err.Error(), "reference block for this script is not set to a recent block") {
		return ScriptErrorActionNone{}
	}

	// If the account is frozen, we can skip it
	if strings.Contains(err.Error(), "[Error Code: 1204]") {
		addressIndex := accountFrozenRegex.SubexpIndex("address")
		match := accountFrozenRegex.FindStringSubmatch(err.Error())
		if match != nil {
			address := flow.HexToAddress(match[addressIndex])

			return ScriptErrorActionExclude{
				Addresses: []flow.Address{address},
			}
		}
	}

	return ScriptErrorActionUnhandled{}
}
