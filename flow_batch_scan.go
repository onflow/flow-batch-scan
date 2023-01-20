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

package lib

import (
	"context"
	"errors"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog"

	"github.com/onflow/flow-batch-scan/candidates"
	"github.com/onflow/flow-batch-scan/client"
)

type BlockScanner struct {
	ctx                 context.Context
	client              client.Client
	script              []byte
	scriptResultHandler ScriptResultHandler
	candidateScanners   []candidates.CandidateScanner
	batchSize           int
	chainID             flow.ChainID
	continuousScan      bool
	reporter            StatusReporter
	logger              zerolog.Logger
}

const DefaultBatchSize = 1000

type BlockScannerOption = func(*BlockScanner)

func WithContext(ctx context.Context) BlockScannerOption {
	return func(scanner *BlockScanner) {
		scanner.ctx = ctx
	}
}

func WithScript(script []byte) BlockScannerOption {
	return func(scanner *BlockScanner) {
		scanner.script = script
	}
}

func WithScriptResultHandler(handler ScriptResultHandler) BlockScannerOption {
	return func(scanner *BlockScanner) {
		scanner.scriptResultHandler = handler
	}
}

func WithCandidateScanners(candidateScanners []candidates.CandidateScanner) BlockScannerOption {
	return func(scanner *BlockScanner) {
		scanner.candidateScanners = candidateScanners
	}
}

func WithBatchSize(batchSize int) BlockScannerOption {
	return func(scanner *BlockScanner) {
		scanner.batchSize = batchSize
	}
}

func WithChainID(chainID flow.ChainID) BlockScannerOption {
	return func(scanner *BlockScanner) {
		scanner.chainID = chainID
	}
}

func WithStatusReporter(reporter StatusReporter) BlockScannerOption {
	return func(scanner *BlockScanner) {
		scanner.reporter = reporter
	}
}

func WithLogger(logger zerolog.Logger) BlockScannerOption {
	return func(scanner *BlockScanner) {
		scanner.logger = logger
	}
}

func WithContinuousScan(continuous bool) BlockScannerOption {
	return func(scanner *BlockScanner) {
		scanner.continuousScan = continuous
	}
}

func NewBlockScanner(
	client client.Client,
	options ...BlockScannerOption,
) *BlockScanner {
	scanner := &BlockScanner{
		ctx:                 context.Background(),
		client:              client,
		script:              []byte(defaultScript),
		scriptResultHandler: NoOpScriptResultHandler{},
		candidateScanners:   []candidates.CandidateScanner{},
		batchSize:           DefaultBatchSize,
		chainID:             flow.Mainnet,
		continuousScan:      false,
		reporter:            NoOpStatusReporter{},
		logger:              zerolog.Nop(),
	}

	for _, option := range options {
		option(scanner)
	}

	return scanner
}

type ScanConcluded struct {
	LatestScannedBlockHeight uint64
	// ScanIsComplete is false if a full scan was not completed,
	// this means some accounts may have stale data, or have been missed all together.
	ScanIsComplete bool
}

func (scanner *BlockScanner) Scan() (ScanConcluded, error) {
	ctx, cancel := context.WithCancel(scanner.ctx)

	// small buffer so that the pending requests in the buffer don't encounter "state commitment not found" errors.
	scriptRequestChan := make(chan AddressBatch, 10)
	scriptResultChan := make(chan ProcessedAddressBatch, 10000)

	// this channel will be used to request a full scan
	requestBatchChan := make(chan uint64)

	var components []Component
	if c, ok := scanner.reporter.(Component); ok {
		components = append(components, c)
	}
	if c, ok := scanner.scriptResultHandler.(Component); ok {
		components = append(components, c)
	}

	incrementalScanner := NewIncrementalScanner(
		ctx,
		scanner.client,
		scriptRequestChan,
		requestBatchChan,
		0,
		scanner.batchSize,
		scanner.candidateScanners,
		scanner.reporter,
		scanner.logger,
	)
	components = append(components, incrementalScanner)

	components = append(components,
		NewScriptRunner(
			ctx,
			scanner.client,
			scanner.script,
			scriptRequestChan,
			scriptResultChan,
			scanner.logger,
		),
	)
	components = append(components,
		NewScriptResultProcessor(
			ctx,
			scriptResultChan,
			scanner.scriptResultHandler,
			scanner.logger,
		),
	)

	fullScanRunner := NewFullScanRunner(
		scanner.client,
		scriptRequestChan,
		scanner.batchSize,
		scanner.chainID,
		scanner.reporter,
		scanner.logger,
	)

	for _, component := range components {
		<-component.Started()
	}

	type fullScan struct {
		*FullScan
		cancel context.CancelFunc
	}
	continueScan := true
	var runningFullScan *fullScan
	go func() {
		for continueScan {
			switch runningFullScan {
			case nil:

				scanner.reporter.ReportIsFullScanRunning(false)
				height := <-requestBatchChan
				fullScanCtx, cancel := context.WithCancel(ctx)
				runningFullScan = &fullScan{
					FullScan: fullScanRunner.StartBatch(fullScanCtx, height),
					cancel:   cancel,
				}

			default:
				scanner.reporter.ReportIsFullScanRunning(true)
				select {
				case height := <-requestBatchChan:
					runningFullScan.cancel()

					fullScanCtx, cancel := context.WithCancel(ctx)
					runningFullScan = &fullScan{
						FullScan: fullScanRunner.StartBatch(fullScanCtx, height),
						cancel:   cancel,
					}
				case <-runningFullScan.Done():
					if runningFullScan.Err() != nil {
						// TODO: handle error
						scanner.logger.Fatal().Err(runningFullScan.Err()).Msg("Failed batch")
					}
					runningFullScan.cancel()
					runningFullScan = nil
					if !scanner.continuousScan {
						continueScan = false
					}
				}
			}
		}
		cancel()
	}()

	waitForAnyComponentToFinish(components...)
	cancel()

	merr := &multierror.Error{}
	for _, component := range components {
		<-component.Done()
		if component.Err() != nil && !errors.Is(component.Err(), context.Canceled) {
			merr = multierror.Append(merr, component.Err())
		}
	}

	return ScanConcluded{
		LatestScannedBlockHeight: incrementalScanner.LatestHandledBlock(),
		ScanIsComplete:           runningFullScan == nil,
	}, merr.ErrorOrNil()
}

func waitForAnyComponentToFinish(components ...Component) struct{} {
	doneChannels := make([]<-chan struct{}, len(components))
	for i, component := range components {
		doneChannels[i] = component.Done()
	}

	done := mergeChannels(doneChannels...)

	return <-done
}

func mergeChannels[T any](channels ...<-chan T) <-chan T {
	wg := sync.WaitGroup{}
	wg.Add(len(channels))

	out := make(chan T)

	for _, channel := range channels {
		c := channel
		go func() {
			defer wg.Done()
			for val := range c {
				out <- val
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
