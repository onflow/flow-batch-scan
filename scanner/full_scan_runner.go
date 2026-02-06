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
	_ "embed"
	"sync"
	"sync/atomic"

	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go/module/component"
	"github.com/onflow/flow-go/module/irrecoverable"
	"github.com/rs/zerolog"

	"github.com/onflow/flow-batch-scan/client"
)

var _ component.Component = (*FullScan)(nil)

type FullScanRunnerConfig struct {
	AddressProviderConfig
	ChainID flow.ChainID
}

func DefaultFullScanRunnerConfig() FullScanRunnerConfig {
	return FullScanRunnerConfig{
		AddressProviderConfig: DefaultAddressProviderConfig(),
		ChainID:               flow.Testnet,
	}
}

type FullScanRunner struct {
	client       client.Client
	scriptRunner *ScriptRunner
	batchSize    int

	FullScanRunnerConfig

	reporter      StatusReporter
	logger        zerolog.Logger
	latestScanned *LatestIncrementallyScanned
}

func NewFullScanRunner(
	logger zerolog.Logger,
	client client.Client,
	scriptRunner *ScriptRunner,
	batchSize int,
	config FullScanRunnerConfig,
	reporter StatusReporter,
	latestScanned *LatestIncrementallyScanned,
) *FullScanRunner {
	return &FullScanRunner{
		client:               client,
		scriptRunner:         scriptRunner,
		batchSize:            batchSize,
		FullScanRunnerConfig: config,
		reporter:             reporter,
		logger:               logger.With().Str("component", "full_scan_runner").Logger(),
		latestScanned:        latestScanned,
	}
}

func (r *FullScanRunner) NewBatch(
	blockHeight uint64,
) *FullScan {
	batch := &FullScan{
		runner: r,

		initialBlockHeight: blockHeight,
		log:                r.logger.With().Uint64("initial_block_height", blockHeight).Logger(),
	}

	batch.Component = component.NewComponentManagerBuilder().
		AddWorker(batch.runWorker).
		Build()

	return batch
}

type FullScan struct {
	component.Component

	runner *FullScanRunner

	// initialBlockHeight is the height used for AddressProvider initialization.
	// The address provider needs a stable block to determine which addresses exist.
	initialBlockHeight uint64

	log zerolog.Logger
}

func (r *FullScan) runWorker(ctx irrecoverable.SignalerContext, ready component.ReadyFunc) {
	ap, err := InitAddressProvider(
		ctx,
		r.log,
		r.runner.ChainID,
		r.initialBlockHeight,
		r.runner.client,
		r.runner.AddressProviderConfig,
	)
	if err != nil {
		ctx.Throw(err)
		return
	}

	ready()

	progressChan := make(chan uint64)
	go r.reportProgress(uint64(ap.AddressesLen()), progressChan)

	batchWG := &sync.WaitGroup{}
	cancelled := atomic.Bool{}
	isBatchValid := func() bool {
		return !cancelled.Load()
	}

	addressChan := make(chan []flow.Address)

	go func() {
		ap.GenerateAddressBatches(addressChan, r.runner.batchSize)
		close(addressChan)
	}()

	for {
		select {
		case <-ctx.Done():
			cancelled.Store(true)
			batchWG.Wait()
			return
		case addresses, ok := <-addressChan:
			if !ok {
				batchWG.Wait()
				return
			}

			batchWG.Add(1)

			// Always use the latest incrementally scanned height for script execution
			scriptHeight, scanned := r.runner.latestScanned.GetIfScanned()
			if !scanned {
				// Fallback to initial height if incremental scanner hasn't processed anything
				scriptHeight = r.initialBlockHeight
			}

			r.runner.scriptRunner.Submit(NewAddressBatch(
				addresses,
				scriptHeight,
				func() {
					progressChan <- uint64(len(addresses))
					batchWG.Done()
				},
				isBatchValid,
			))
		}
	}
}

func (r *FullScan) reportProgress(total uint64, progressChan <-chan uint64) {
	current := uint64(0)
	segment := uint64(0)
	segments := uint64(10)
	for progress := range progressChan {
		current += progress

		if r.runner.reporter != nil {
			r.runner.reporter.ReportFullScanProgress(current, total)
		}

		if current > (total/segments)*(segment+1) {
			r.log.Info().
				Uint64("current", current).
				Uint64("total", total).
				Msgf("Batch progress: %d%%", (100/segments)*(segment+1))
			segment += 1
		}
	}
}
