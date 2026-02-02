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
	_ "embed"
	"sync"

	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go/module/component"
	"github.com/onflow/flow-go/module/irrecoverable"
	"github.com/rs/zerolog"

	"github.com/onflow/flow-batch-scan/candidates"
	"github.com/onflow/flow-batch-scan/client"
	"github.com/onflow/flow-batch-scan/scanner/event"
)

const DefaultIncrementalScannerBlockLag = 5

// DefaultIncrementalScannerMaxBlockGap is the maximum number of blocks that can scanned by the incremental scanner.
// If the gap is larger than this, the incremental scanner will request a full scan.
const DefaultIncrementalScannerMaxBlockGap = 100

type IncrementalScannerConfig struct {
	CandidateScanners []candidates.CandidateScanner

	// IncrementalScannerMaxBlockGap is the maximum number of blocks that can scanned by the incremental scanner.
	// If the gap is larger than this, the incremental scanner will skip ahead and request a full scan.
	IncrementalScannerMaxBlockGap uint64
}

func DefaultIncrementalScannerConfig() IncrementalScannerConfig {
	return IncrementalScannerConfig{
		CandidateScanners:             []candidates.CandidateScanner{},
		IncrementalScannerMaxBlockGap: DefaultIncrementalScannerMaxBlockGap,
	}
}

type IncrementalScanner struct {
	component.Component
	IncrementalScannerConfig

	client       client.Client
	scriptRunner *ScriptRunner

	fullScanEmitter event.Emitter[FullScanRequest]

	batchSize   int
	latestBlock uint64

	blockSubscription *event.Subscription[SealedBlock]

	latestScanned *LatestIncrementallyScanned
	reporter      StatusReporter
	log           zerolog.Logger
}

func NewIncrementalScanner(
	logger zerolog.Logger,
	client client.Client,
	scriptRunner *ScriptRunner,
	fullScanEmitter event.Emitter[FullScanRequest],
	blockEmitter event.EmitterChannel[SealedBlock],
	batchSize int,
	config IncrementalScannerConfig,
	reporter StatusReporter,
	latestScanned *LatestIncrementallyScanned,
) *IncrementalScanner {
	r := &IncrementalScanner{
		client:                   client,
		scriptRunner:             scriptRunner,
		fullScanEmitter:          fullScanEmitter,
		batchSize:                batchSize,
		IncrementalScannerConfig: config,
		latestScanned:            latestScanned,
		blockSubscription:        blockEmitter.Subscribe(100),

		reporter: reporter,
		log:      logger.With().Str("component", "incremental_scanner").Logger(),
	}

	r.Component = component.NewComponentManagerBuilder().
		AddWorker(r.scanWorker).
		Build()

	return r
}

func (r *IncrementalScanner) scanWorker(ctx irrecoverable.SignalerContext, ready component.ReadyFunc) {
	defer r.blockSubscription.Unsubscribe()
	ready()

	for {
		select {
		case <-ctx.Done():
			return
		case block, ok := <-r.blockSubscription.Channel():
			if !ok {
				return
			}
			err := r.handleNewBlock(ctx, block.Height)
			if err != nil {
				ctx.Throw(err)
				return
			}
		}
	}
}

func (r *IncrementalScanner) handleNewBlock(ctx context.Context, height uint64) error {
	// If this is the first block we've seen, trigger a full scan to establish baseline
	if r.latestBlock == 0 {
		r.log.Info().
			Uint64("current_block", height).
			Msg("first block received, triggering initial full scan")
		r.latestBlock = height
		// Reset the scanned height to create a clean discontinuity
		r.latestScanned.ResetScanned(r.latestBlock)
		// TODO (future): cancel any outstanding incremental scans to not do unnecessary work
		r.fullScanEmitter.Emit(ctx, FullScanRequest{BlockHeight: r.latestBlock})
		return nil
	}

	// Check if the gap is too large
	if r.latestBlock > 0 && height-r.latestBlock > r.IncrementalScannerMaxBlockGap {
		r.log.Info().
			Uint64("latest_block", r.latestBlock).
			Uint64("current_block", height).
			Uint64("diff", height-r.latestBlock).
			Msg("skipping blocks and requesting full scan")
		r.latestBlock = height
		// Reset the scanned height to create a clean discontinuity
		r.latestScanned.ResetScanned(r.latestBlock)
		// TODO (future): cancel any outstanding incremental scans to not do unnecessary work
		r.fullScanEmitter.Emit(ctx, FullScanRequest{BlockHeight: r.latestBlock})
		return nil
	}

	r.log.Info().
		Uint64("block_height", height).
		Msg("processing block")

	err := r.scanBlock(ctx, height)
	r.latestBlock = height
	return err
}

// scanBlock scans a block for any candidates for which a script should be run.
// TODO: Allow multiple blocks to be scanned at once, but they should still commit findings
// sequentially.
func (r *IncrementalScanner) scanBlock(ctx context.Context, blockHeight uint64) error {
	candidatesResult := r.runBlockCandidateScanners(ctx, blockHeight)
	if candidatesResult.Err() != nil {
		return candidatesResult.Err()
	}

	addresses := make([]flow.Address, 0, len(candidatesResult.Addresses))
	for address := range candidatesResult.Addresses {
		addresses = append(addresses, address)
	}

	if len(addresses) == 0 {
		r.latestScanned.SetScanned(blockHeight)
		return nil
	}

	r.log.
		Info().
		Int("count", len(addresses)).
		Uint64("block_height", blockHeight).
		Msg("Found candidates in block range.")

	wg := sync.WaitGroup{}
	for i := 0; i < len(addresses); i += r.batchSize {
		startIndex := i
		endIndex := min(i+r.batchSize, len(addresses))
		wg.Add(1)
		r.scriptRunner.Submit(NewAddressBatch(
			addresses[startIndex:endIndex],
			blockHeight,
			func() {
				wg.Done()
			},
			nil,
		))
	}

	go func() {
		wg.Wait()
		r.latestScanned.SetScanned(blockHeight)
	}()

	return nil
}

func (r *IncrementalScanner) runBlockCandidateScanners(ctx context.Context, blockHeight uint64) candidates.CandidatesResult {
	results := make(chan candidates.CandidatesResult, len(r.CandidateScanners))
	defer close(results)

	for _, scanner := range r.CandidateScanners {
		go func(scanner candidates.CandidateScanner) {
			results <- scanner.Scan(ctx, r.client, blockHeight)
		}(scanner)
	}

	return candidates.WaitForCandidateResults(results, len(r.CandidateScanners))
}
