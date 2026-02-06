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

	"github.com/onflow/flow-go/module/component"
	"github.com/onflow/flow-go/module/irrecoverable"
	"github.com/rs/zerolog"

	"github.com/onflow/flow-batch-scan/scanner/event"
)

// FullScanCoordinator manages the lifecycle of full scans.
// It subscribes to full scan requests and coordinates starting/stopping full scans.
type FullScanCoordinator struct {
	component.Component

	runner         *FullScanRunner
	subscription   *event.Subscription[FullScanRequest]
	reporter       StatusReporter
	continuousScan bool

	onCompletedFN func()
	log           zerolog.Logger
}

func NewFullScanCoordinator(
	logger zerolog.Logger,
	runner *FullScanRunner,
	emitter event.EmitterChannel[FullScanRequest],
	reporter StatusReporter,
	continuousScan bool,
	onCompletedFN func(),
) *FullScanCoordinator {
	c := &FullScanCoordinator{
		runner:         runner,
		subscription:   emitter.Subscribe(10), // Subscribe early to avoid missing events
		reporter:       reporter,
		continuousScan: continuousScan,
		onCompletedFN:  onCompletedFN,
		log:            logger.With().Str("component", "full_scan_coordinator").Logger(),
	}

	c.Component = component.NewComponentManagerBuilder().
		AddWorker(c.coordinatorWorker).
		Build()

	return c
}

func (c *FullScanCoordinator) coordinatorWorker(ctx irrecoverable.SignalerContext, ready component.ReadyFunc) {
	defer c.subscription.Unsubscribe()

	ready()

	type runningFullScan struct {
		*FullScan
		cancel context.CancelFunc
	}

	var currentScan *runningFullScan

	for {
		if currentScan == nil {
			c.reporter.ReportIsFullScanRunning(false)
			select {
			case <-ctx.Done():
				return
			case req, ok := <-c.subscription.Channel():
				if !ok {
					return
				}
				fullScanCtx, cancel := context.WithCancel(ctx)
				signalerCtx, _ := irrecoverable.WithSignaler(fullScanCtx)

				currentScan = &runningFullScan{
					FullScan: c.runner.NewBatch(req.BlockHeight),
					cancel:   cancel,
				}
				currentScan.Start(signalerCtx)
				<-currentScan.Ready()
				c.log.Info().Uint64("block_height", req.BlockHeight).Msg("started full scan")
			}
		} else {
			c.reporter.ReportIsFullScanRunning(true)
			select {
			case <-ctx.Done():
				currentScan.cancel()
				<-currentScan.Done()
				return
			case req, ok := <-c.subscription.Channel():
				if !ok {
					currentScan.cancel()
					<-currentScan.Done()
					return
				}
				// Cancel current scan and start new one
				c.log.Info().Uint64("block_height", req.BlockHeight).Msg("cancelling current scan for new request")
				currentScan.cancel()
				<-currentScan.Done()

				fullScanCtx, cancel := context.WithCancel(ctx)
				signalerCtx, _ := irrecoverable.WithSignaler(fullScanCtx)

				currentScan = &runningFullScan{
					FullScan: c.runner.NewBatch(req.BlockHeight),
					cancel:   cancel,
				}
				currentScan.Start(signalerCtx)
				<-currentScan.Ready()
				c.log.Info().Uint64("block_height", req.BlockHeight).Msg("started full scan")

			case <-currentScan.Done():
				c.log.Info().Msg("full scan completed")
				currentScan.cancel()
				currentScan = nil

				if !c.continuousScan {
					c.onCompletedFN()
					return
				}
			}
		}
	}
}
