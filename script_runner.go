// Copyright 2023 Dapper Labs, Inc.
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
	"github.com/rs/zerolog"

	"github.com/onflow/flow-batch-scan/client"
)

// ScriptRunnerMaxConcurrentScripts is the maximum number of scripts that can be running concurrently
// at any given time. If this is more than the rate limit, some scripts will be just waiting.
// As long as they don't wait too long, this is not a problem.
const ScriptRunnerMaxConcurrentScripts = 100

type ScriptRunner struct {
	*ComponentBase

	client     client.Client
	scriptCode []byte

	addressBatchChan <-chan AddressBatch
	resultsChan      chan<- ProcessedAddressBatch

	limitChan chan struct{}
}

var _ Component = (*ScriptRunner)(nil)

func NewScriptRunner(
	ctx context.Context,
	client client.Client,
	scriptCode []byte,
	addressBatchChan <-chan AddressBatch,
	resultsChan chan<- ProcessedAddressBatch,
	logger zerolog.Logger,
) *ScriptRunner {
	r := &ScriptRunner{
		ComponentBase: NewComponent("script_runner", logger),

		client:           client,
		scriptCode:       scriptCode,
		addressBatchChan: addressBatchChan,
		resultsChan:      resultsChan,

		limitChan: make(chan struct{}, ScriptRunnerMaxConcurrentScripts),
	}

	go r.start(ctx)
	r.StartupDone()
	return r
}

func (r *ScriptRunner) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			r.Finish(ctx.Err())
			return
		case input, ok := <-r.addressBatchChan:
			if !ok {
				r.Finish(nil)
				return
			}
			if !input.IsValid() {
				return
			}
			r.handleBatch(ctx, input)
		}
	}
}

func (r *ScriptRunner) handleBatch(ctx context.Context, input AddressBatch) {
	r.limitChan <- struct{}{}
	go func() {
		defer func() { <-r.limitChan }()

		result, err := r.retryScriptUntilSuccess(ctx, input)
		if err != nil {
			r.Logger.Error().
				Err(err).
				Msg("error running script")
			r.Finish(err)
			return
		}

		r.resultsChan <- ProcessedAddressBatch{
			AddressBatch: input,
			Result:       result,
		}
	}()
}

var accountFrozenRegex = regexp.MustCompile(`\[Error Code: 1204] account (?P<address>\w{16}) is frozen`)

// retryScriptUntilSuccess retries running the cadence script until we get a successful response back,
// returning an array of Balance pairs, along with a boolean representing whether we can continue
// or are finished processing.
func (r *ScriptRunner) retryScriptUntilSuccess(
	ctx context.Context,
	input AddressBatch,
) (result cadence.Value, err error) {
	arguments := convertAddressesToArguments(input.Addresses)
	for {
		r.Logger.Debug().Msgf("executing script")

		result, err = r.client.ExecuteScriptAtBlockHeight(
			ctx,
			input.BlockHeight,
			r.scriptCode,
			arguments,
		)
		if err == nil {
			break
		}
		// Context cancelled
		if errors.Is(err, context.Canceled) {
			return nil, err
		}

		// If the account is frozen, we can skip it
		if strings.Contains(err.Error(), "[Error Code: 1204]") {
			addressIndex := accountFrozenRegex.SubexpIndex("address")
			match := accountFrozenRegex.FindStringSubmatch(err.Error())
			if match != nil {
				address := flow.HexToAddress(match[addressIndex])
				r.Logger.
					Info().
					Str("address", address.Hex()).
					Msg("Excluding frozen address")
				input.ExcludeAddress(address)
				arguments = convertAddressesToArguments(input.Addresses)
				continue
			}
		}

		// non retryable error
		if strings.Contains(err.Error(), "state commitment not found") {
			return nil, err
		}

		r.Logger.Warn().Msgf("received unknown error, retrying: %s", err.Error())
	}

	return result, nil
}

// convertAddressesToArguments generates an array of cadence.Value from an array of flow.Address
func convertAddressesToArguments(addresses []flow.Address) []cadence.Value {
	var accounts []cadence.Value
	for _, address := range addresses {
		accounts = append(accounts, cadence.Address(address))
	}
	return []cadence.Value{cadence.NewArray(accounts)}
}
