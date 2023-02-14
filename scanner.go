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
	"sync"

	"github.com/hashicorp/go-multierror"

	"github.com/onflow/flow-batch-scan/client"
)

type Scanner struct {
	Config
	client client.Client
}

func NewScanner(
	client client.Client,
	config Config,
) *Scanner {
	scanner := &Scanner{
		Config: config,
		client: client,
	}

	return scanner
}

type ScanConcluded struct {
	LatestScannedBlockHeight uint64
	// ScanIsComplete is false if a full scan was not completed,
	// this means some accounts may have stale data, or have been missed all together.
	ScanIsComplete bool
}

func (scanner *Scanner) Scan(ctx context.Context) (ScanConcluded, error) {
	// small buffer so that the pending requests in the buffer don't encounter "state commitment not found" errors.
	scriptRequestChan := make(chan AddressBatch, 10)

	scriptResultChan := make(chan ProcessedAddressBatch, 10000)

	// this channel will be used to request a full scan
	requestBatchChan := make(chan uint64)

	var components []Component
	if c, ok := scanner.Reporter.(Component); ok {
		components = append(components, c)
	}
	if c, ok := scanner.ScriptResultHandler.(Component); ok {
		components = append(components, c)
	}

	incrementalScanner := NewIncrementalScanner(
		scanner.client,
		scriptRequestChan,
		requestBatchChan,
		scanner.BatchSize,
		scanner.IncrementalScannerConfig,
		scanner.Reporter,
		scanner.Logger,
	)
	components = append(components, incrementalScanner)

	components = append(components,
		NewScriptRunner(
			scanner.client,
			scriptRequestChan,
			scriptResultChan,
			scanner.ScriptRunnerConfig,
			scanner.Logger,
		),
	)
	components = append(components,
		NewScriptResultProcessor(
			scriptResultChan,
			scanner.ScriptResultHandler,
			scanner.Logger,
		),
	)

	fullScanRunner := NewFullScanRunner(
		scanner.client,
		scriptRequestChan,
		scanner.BatchSize,
		scanner.FullScanRunnerConfig,
		scanner.Reporter,
		scanner.Logger,
	)

	ctx, cancel := context.WithCancel(ctx)
	for _, component := range components {
		<-component.Start(ctx)
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

				scanner.Reporter.ReportIsFullScanRunning(false)
				height := <-requestBatchChan
				fullScanCtx, cancel := context.WithCancel(ctx)
				runningFullScan = &fullScan{
					FullScan: fullScanRunner.NewBatch(height),
					cancel:   cancel,
				}
				<-runningFullScan.Start(fullScanCtx)

			default:
				scanner.Reporter.ReportIsFullScanRunning(true)
				select {
				case height := <-requestBatchChan:
					runningFullScan.cancel()

					fullScanCtx, cancel := context.WithCancel(ctx)
					runningFullScan = &fullScan{
						FullScan: fullScanRunner.NewBatch(height),
						cancel:   cancel,
					}
					<-runningFullScan.Start(fullScanCtx)

				case <-runningFullScan.Done():
					if runningFullScan.Err() != nil {
						// TODO: handle error
						scanner.Logger.Fatal().Err(runningFullScan.Err()).Msg("Failed batch")
					}
					runningFullScan.cancel()
					runningFullScan = nil
					if !scanner.ContinuousScan {
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
