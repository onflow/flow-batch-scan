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
	"sync/atomic"
	"time"

	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog"

	"github.com/onflow/flow-batch-scan/candidates"
	"github.com/onflow/flow-batch-scan/client"
)

const DefaultIncrementalScannerBlockLag = 5

// DefaultIncrementalScannerMaxBlockGap is the maximum number of blocks that can scanned by the incremental scanner.
// If the gap is larger than this, the incremental scanner will request a full scan.
const DefaultIncrementalScannerMaxBlockGap = 100

type IncrementalScannerConfig struct {
	CandidateScanners []candidates.CandidateScanner
	// IncrementalScannerBlockLag is the number of blocks the incremental scanner lag behind the latest block from
	// GetLatestBlockHeader. This is to avoid most of the "retry for collection in finalized block" errors.
	// Another way to avoid them is to always use the same access node.
	IncrementalScannerBlockLag uint64

	// IncrementalScannerMaxBlockGap is the maximum number of blocks that can scanned by the incremental scanner.
	// If the gap is larger than this, the incremental scanner will skip ahead and request a full scan.
	IncrementalScannerMaxBlockGap uint64
}

func DefaultIncrementalScannerConfig() IncrementalScannerConfig {
	return IncrementalScannerConfig{
		CandidateScanners:             []candidates.CandidateScanner{},
		IncrementalScannerBlockLag:    DefaultIncrementalScannerBlockLag,
		IncrementalScannerMaxBlockGap: DefaultIncrementalScannerMaxBlockGap,
	}
}

type IncrementalScanner struct {
	*ComponentBase
	IncrementalScannerConfig

	client client.Client

	addressBatchChan chan<- AddressBatch
	requestFullScan  chan<- uint64

	batchSize               int
	latestBlock             uint64
	latestHandledBlock      atomic.Uint64
	pendingIncrementalScans atomic.Int32

	reporter StatusReporter
}

func NewIncrementalScanner(
	client client.Client,

	addressBatchChan chan<- AddressBatch,
	requestBatchChan chan<- uint64,

	batchSize int,
	config IncrementalScannerConfig,

	reporter StatusReporter,
	logger zerolog.Logger,

) *IncrementalScanner {
	r := &IncrementalScanner{

		client:                   client,
		addressBatchChan:         addressBatchChan,
		requestFullScan:          requestBatchChan,
		latestHandledBlock:       atomic.Uint64{},
		pendingIncrementalScans:  atomic.Int32{},
		batchSize:                batchSize,
		IncrementalScannerConfig: config,

		reporter: reporter,
	}

	r.ComponentBase = NewComponentWithStart(
		"incremental_scanner",
		r.run,
		logger,
	)
	return r
}

func (r *IncrementalScanner) run(ctx context.Context) {
	go func() {
		next := time.After(0)
		for {
			select {
			case <-ctx.Done():
				r.Finish(ctx.Err())
				return
			case <-next:
				next = time.After(2 * time.Second)
				err := r.scanNewBlocks(ctx)
				if err != nil {
					r.Finish(err)
				}
			}
		}
	}()
}

func (r *IncrementalScanner) scanNewBlocks(ctx context.Context) error {
	header, err := r.client.GetLatestBlockHeader(ctx, true)
	if err != nil {
		return err
	}
	height := header.Height - r.IncrementalScannerBlockLag

	if height <= r.latestBlock {
		return nil
	}

	r.reporter.ReportIncrementalBlockDiff(height - r.latestBlock)

	if height-r.latestBlock > r.IncrementalScannerMaxBlockGap {
		r.Logger.Info().
			Uint64("latest_block", r.latestBlock).
			Uint64("current_block", height).
			Uint64("diff", height-r.latestBlock).
			Msg("skipping blocks and requesting batch")
		r.latestBlock = height
		r.requestFullScan <- r.latestBlock
		return nil
	}

	r.Logger.Info().
		Uint64("start", r.latestBlock+1).
		Uint64("end", height).
		Uint64("diff", height-r.latestBlock).
		Msg("processing block range")
	err = r.scanBlockRange(ctx, r.latestBlock+1, height)

	r.latestBlock = height
	return err
}

// scanBlockRange scans a range of blocks for any candidates for which a script should be run.
// start and end are inclusive.
func (r *IncrementalScanner) scanBlockRange(ctx context.Context, start uint64, end uint64) error {
	candidatesResult := r.runBlockCandidateScanners(ctx, start, end)
	if candidatesResult.Err() != nil {
		return candidatesResult.Err()
	}

	if len(candidatesResult.Addresses) == 0 {
		if r.pendingIncrementalScans.Load() == 0 {
			r.latestHandledBlock.Store(end)
			r.reporter.ReportIncrementalBlockHeight(end)
		}
		return nil
	}

	addresses := make([]flow.Address, 0, len(candidatesResult.Addresses))
	for address := range candidatesResult.Addresses {
		addresses = append(addresses, address)
	}

	r.Logger.
		Info().
		Int("count", len(addresses)).
		Uint64("start", start).
		Uint64("end", end).
		Msg("Found candidates in block range.")

	wg := sync.WaitGroup{}
	r.pendingIncrementalScans.Add(1)
	for i := 0; i < len(addresses); i += r.batchSize {
		startIndex := i
		endIndex := i + r.batchSize
		if endIndex > len(addresses) {
			endIndex = len(addresses)
		}
		wg.Add(1)
		r.addressBatchChan <- NewAddressBatch(
			addresses[startIndex:endIndex],
			end,
			func() {
				wg.Done()
			},
			nil,
		)
	}

	go func() {
		wg.Wait()
		r.pendingIncrementalScans.Add(-1)
		r.latestHandledBlock.Store(end)
		r.reporter.ReportIncrementalBlockHeight(end)
	}()

	return nil
}

func (r *IncrementalScanner) runBlockCandidateScanners(ctx context.Context, start uint64, end uint64) candidates.CandidatesResult {
	results := make(chan candidates.CandidatesResult, len(r.CandidateScanners))
	defer close(results)

	for _, scanner := range r.CandidateScanners {
		go func(scanner candidates.CandidateScanner) {
			results <- scanner.Scan(ctx, r.client, candidates.BlockRange{Start: start, End: end})
		}(scanner)
	}

	return candidates.WaitForCandidateResults(results, len(r.CandidateScanners))
}

func (r *IncrementalScanner) LatestHandledBlock() uint64 {
	return r.latestHandledBlock.Load()
}
