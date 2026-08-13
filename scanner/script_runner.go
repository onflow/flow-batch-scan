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
	"fmt"
	"regexp"
	"strings"
	"time"

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

// DefaultPrunedStateFatalHeightGap is the default minimum number of blocks the
// latest scanned height must be ahead of an unservable batch's height for the
// batch to be rescheduled instead of treated as fatal.
const DefaultPrunedStateFatalHeightGap = 10

type ScriptRunnerConfig struct {
	Script []byte

	MaxConcurrentScripts int
	HandleScriptError    func(AddressBatch, error) ScriptErrorAction

	// LatestScannedHeight returns the latest block height known to have servable
	// state (typically the latest incrementally scanned height).
	// Before a batch is resubmitted (retry, split, or exclude), it is moved to
	// this height if it is higher than the batch's current height, so retries
	// never re-execute against state that may have been pruned.
	// For ScriptErrorActionRetryAtLatestHeight, the resubmission is deferred
	// until this advances past the batch's height (a new scanned block).
	// May be nil: resubmissions then keep the batch's original height.
	LatestScannedHeight func() (uint64, bool)

	// PrunedStateFatalHeightGap is the minimum number of blocks the latest
	// scanned height must be ahead of a batch's block height for
	// ScriptErrorActionRetryAtLatestHeight to reschedule it. If the gap is
	// smaller (or no height has been scanned yet), the error is fatal
	// (ctx.Throw) instead: state that close to the tip should never be
	// pruned, so waiting for a new block cannot be expected to help.
	// Zero disables the fatal check.
	// Only evaluated when LatestScannedHeight is non-nil.
	PrunedStateFatalHeightGap uint64
}

func DefaultScriptRunnerConfig() ScriptRunnerConfig {
	return ScriptRunnerConfig{
		Script: []byte(defaultScript),

		MaxConcurrentScripts:      DefaultScriptRunnerMaxConcurrentScripts,
		HandleScriptError:         DefaultHandleScriptError,
		PrunedStateFatalHeightGap: DefaultPrunedStateFatalHeightGap,
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
				r.handleBatch(ctx, r.atLatestHeight(input))
			}()
			return
		case ScriptErrorActionRetryAtLatestHeight:
			// The batch's reference block is no longer servable (e.g. its state was
			// pruned on the execution nodes). Splitting is pointless — every half
			// would fail the same way — and retrying at the same height can never
			// succeed, so resubmit the whole batch once the latest scanned height
			// advances to a new block.
			//
			// Exception: if the batch's height is within PrunedStateFatalHeightGap
			// of the latest scanned height, state that close to the tip should
			// never be unservable, so the error is fatal instead of reschedulable.
			if r.LatestScannedHeight != nil && r.PrunedStateFatalHeightGap > 0 {
				if latest, ok := r.LatestScannedHeight(); !ok ||
					latest < input.BlockHeight ||
					latest-input.BlockHeight < r.PrunedStateFatalHeightGap {
					ctx.Throw(fmt.Errorf(
						"batch at height %d is not servable while latest scanned height is %d (fatal gap < %d): %w",
						input.BlockHeight, latest, r.PrunedStateFatalHeightGap, err))
					return
				}
			}
			r.log.
				Info().
				Uint64("block_height", input.BlockHeight).
				Msg("reference block not servable, waiting for a new scanned block to retry")
			go func() {
				ticker := time.NewTicker(newScannedHeightPollInterval)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
					}
					if r.LatestScannedHeight == nil {
						// no way to detect a new block: retry at the same height
						r.handleBatch(ctx, input)
						return
					}
					if h, ok := r.LatestScannedHeight(); ok && h > input.BlockHeight {
						r.handleBatch(ctx, input.WithBlockHeight(h))
						return
					}
				}
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
				input = r.atLatestHeight(input)
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
				r.handleBatch(ctx, r.atLatestHeight(input))
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

// atLatestHeight moves the batch to the latest scanned height if that is
// higher than the batch's current height, so a resubmitted batch never
// re-executes against state that may have been pruned. Returns the batch
// unchanged when LatestScannedHeight is nil or has no higher height.
func (r *ScriptRunner) atLatestHeight(input AddressBatch) AddressBatch {
	if r.LatestScannedHeight == nil {
		return input
	}
	if h, ok := r.LatestScannedHeight(); ok && h > input.BlockHeight {
		return input.WithBlockHeight(h)
	}
	return input
}

var accountFrozenRegex = regexp.MustCompile(`\[Error Code: 1204] account (?P<address>\w{16}) is frozen`)

// newScannedHeightPollInterval is how often the script runner checks whether
// the latest scanned height has advanced while waiting to resubmit a batch via
// ScriptErrorActionRetryAtLatestHeight. There is no retry cap: a long-running
// scan should ride out a multi-hour execution-node state-availability outage
// rather than crash.
// It is a variable so tests can stub it.
var newScannedHeightPollInterval = time.Second

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

// ScriptErrorActionRetryAtLatestHeight resubmits the whole batch at the latest
// scanned block height (see ScriptRunnerConfig.LatestScannedHeight), after a
// backoff. Use it for errors where the batch's reference block can never
// succeed again, e.g. "execution state is pruned".
// If the batch's height is within ScriptRunnerConfig.PrunedStateFatalHeightGap
// of the latest scanned height, the error is treated as fatal instead.
type ScriptErrorActionRetryAtLatestHeight struct{}

var _ ScriptErrorAction = ScriptErrorActionRetryAtLatestHeight{}

func (s ScriptErrorActionRetryAtLatestHeight) isScriptErrorAction() {}

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

	// The execution nodes no longer serve state for the batch's reference
	// block; splitting or retrying at the same height cannot succeed.
	if strings.Contains(err.Error(), "execution state is pruned") {
		return ScriptErrorActionRetryAtLatestHeight{}
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
