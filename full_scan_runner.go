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
	_ "embed"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog"

	"github.com/onflow/flow-batch-scan/client"
)

const FullScanReferenceBlockSwitch = 30 * time.Second

type FullScanRunnerConfig struct {
	AddressProviderConfig

	ChainID flow.ChainID
}

func DefaultFullScanRunnerConfig() FullScanRunnerConfig {
	return FullScanRunnerConfig{
		AddressProviderConfig: DefaultAddressProviderConfig(),

		ChainID: flow.Testnet,
	}
}

type FullScanRunner struct {
	client           client.Client
	addressBatchChan chan<- AddressBatch
	batchSize        int

	FullScanRunnerConfig

	logger   zerolog.Logger
	reporter StatusReporter
}

func NewFullScanRunner(
	client client.Client,
	addressBatchChan chan<- AddressBatch,
	batchSize int,
	config FullScanRunnerConfig,
	reporter StatusReporter,
	logger zerolog.Logger,
) *FullScanRunner {
	return &FullScanRunner{
		client:               client,
		addressBatchChan:     addressBatchChan,
		batchSize:            batchSize,
		FullScanRunnerConfig: config,
		reporter:             reporter,
		logger:               logger,
	}
}

func (r *FullScanRunner) NewBatch(
	blockHeight uint64,
) *FullScan {
	batch := &FullScan{
		runner: r,

		blockHeight:              blockHeight,
		lastReferenceBlockSwitch: time.Now(),
	}

	batch.ComponentBase = NewComponentWithStart(
		fmt.Sprintf("full_scan_%d", blockHeight),
		func(ctx context.Context) { go batch.run(ctx) },
		r.logger,
	)

	return batch
}

type FullScan struct {
	*ComponentBase

	runner *FullScanRunner

	blockHeight              uint64
	lastReferenceBlockSwitch time.Time
}

var _ Component = &FullScan{}

func (r *FullScan) finish(wg *sync.WaitGroup, err error) {
	go func() {
		if wg != nil {
			// wait for all outstanding batches to finish
			wg.Wait()
		}
		r.ComponentBase.Finish(err)
	}()
}

func (r *FullScan) run(ctx context.Context) {
	ap, err := InitAddressProvider(
		ctx,
		r.runner.ChainID,
		r.blockHeight,
		r.runner.client,
		r.runner.AddressProviderConfig,
		r.Logger,
	)
	if err != nil {
		r.finish(nil, err)
		return
	}

	progressChan := make(chan uint64)
	go r.reportProgress(uint64(ap.AddressesLen()), progressChan)

	batchWG := &sync.WaitGroup{}
	cancelled := atomic.Bool{}
	isBatchValid := func() bool {
		return !cancelled.Load()
	}

	addressChan := make(chan []flow.Address)
	blockSwitchTimeChan := time.After(FullScanReferenceBlockSwitch)
	go func() {
		for {
			select {
			case <-ctx.Done():
				cancelled.Store(true)
				r.finish(batchWG, ctx.Err())
				return
			case <-blockSwitchTimeChan:
				err := r.referenceBlockSwitch(ctx)
				if err != nil {
					r.finish(batchWG, err)
					return
				}
				blockSwitchTimeChan = time.After(FullScanReferenceBlockSwitch)
			case addresses, ok := <-addressChan:
				if !ok {
					r.finish(batchWG, nil)
					return
				}

				batchWG.Add(1)
				r.runner.addressBatchChan <- NewAddressBatch(
					addresses,
					r.blockHeight,
					func() {
						progressChan <- uint64(len(addresses))
						batchWG.Done()
					},
					isBatchValid,
				)
			}
		}
	}()

	go func() {
		ap.GenerateAddressBatches(addressChan, r.runner.batchSize)
		close(addressChan)
	}()
}

func (r *FullScan) reportProgress(addresses uint64, progressChan <-chan uint64) {
	total := addresses
	current := uint64(0)
	segment := uint64(0)
	segments := uint64(10)
	for progress := range progressChan {
		current += progress

		if r.runner.reporter != nil {
			r.runner.reporter.ReportFullScanProgress(current, total)
		}

		if current > (total/segments)*(segment+1) {
			r.Logger.Info().
				Uint64("current", current).
				Uint64("total", total).
				Msgf("Batch progress: %d%%", (100/segments)*(segment+1))
			segment += 1
		}
	}
}

// referenceBlockSwitch switches the reference block height to the current block height,
// to avoid "state commitment not found" errors.
func (r *FullScan) referenceBlockSwitch(ctx context.Context) error {
	currentBlockHeader, err := r.runner.client.GetLatestBlockHeader(ctx, true)
	if err != nil {
		r.Logger.
			Error().
			Err(err).
			Msg("error getting latest block header")
		return err
	}

	r.Logger.
		Info().
		Uint64("height", currentBlockHeader.Height).
		Msg("switch to new block height")
	r.blockHeight = currentBlockHeader.Height
	return nil
}
