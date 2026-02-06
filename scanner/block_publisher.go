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
	"fmt"
	"strings"
	"time"

	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go/module/component"
	"github.com/onflow/flow-go/module/irrecoverable"
	"github.com/rs/zerolog"

	"github.com/onflow/flow-batch-scan/client"
	"github.com/onflow/flow-batch-scan/scanner/event"
)

// SealedBlock represents a sealed block with minimal information needed by the incremental scanner.
type SealedBlock struct {
	Height  uint64
	BlockID flow.Identifier
}

// BlockPublisherConfig configures the block publisher behavior.
type BlockPublisherConfig struct {
	// BlockWaitTimeout is the maximum time to wait for a block to be received.
	// If the block is not received within this time, the publisher will stop because
	// something went wrong with the network.
	BlockWaitTimeout time.Duration
}

// DefaultBlockPublisherConfig returns the default configuration for BlockPublisher.
func DefaultBlockPublisherConfig() BlockPublisherConfig {
	return BlockPublisherConfig{
		BlockWaitTimeout: 30 * time.Second,
	}
}

// BlockPublisher subscribes to sealed block digests from the Flow network and emits them
// as SealedBlock events. It handles reconnection on disconnect errors.
type BlockPublisher struct {
	component.Component
	event.EmitterChannel[SealedBlock]

	client   client.Client
	config   BlockPublisherConfig
	emitter  event.Emitter[SealedBlock]
	reporter StatusReporter
	log      zerolog.Logger
}

// NewBlockPublisher creates a new BlockPublisher that subscribes to sealed block digests
// and emits them as SealedBlock events. It will reconnect to the node if the connection is lost.
func NewBlockPublisher(
	logger zerolog.Logger,
	client client.Client,
	emitter event.Emitter[SealedBlock],
	config BlockPublisherConfig,
	reporter StatusReporter,
) *BlockPublisher {
	p := &BlockPublisher{
		EmitterChannel: emitter,
		client:         client,
		config:         config,
		emitter:        emitter,
		reporter:       reporter,
		log:            logger.With().Str("component", "block_publisher").Logger(),
	}

	p.Component = component.NewComponentManagerBuilder().
		AddWorker(p.worker).
		Build()

	return p
}

func (p *BlockPublisher) worker(ctx irrecoverable.SignalerContext, ready component.ReadyFunc) {
	var blocksChan <-chan *flow.BlockDigest
	var errChan <-chan error
	latestBlock := uint64(0)

	reconnect := func() {
		var err error
		if latestBlock > 0 {
			// Reconnect where we left off
			blocksChan, errChan, err = p.client.SubscribeBlockDigestsFromStartHeight(
				ctx,
				latestBlock+1,
				flow.BlockStatusSealed,
			)
		} else {
			// Start from the latest sealed block
			blocksChan, errChan, err = p.client.SubscribeBlockDigestsFromLatest(
				ctx,
				flow.BlockStatusSealed,
			)
		}
		if err != nil {
			ctx.Throw(fmt.Errorf("failed to subscribe to block digests: %w", err))
			return
		}
		p.log.Debug().
			Uint64("latest_block", latestBlock).
			Msg("subscribed to block digests")
	}

	reconnect()

	ready()

	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-errChan:
			if !ok {
				return
			}
			if isDisconnectError(err) {
				p.log.Debug().Err(err).Msg("server disconnected, reconnecting...")
				reconnect()
				continue
			}
			ctx.Throw(fmt.Errorf("error in block digest subscription: %w", err))
			return
		case block, ok := <-blocksChan:
			if !ok {
				return
			}
			latestBlock = block.Height
			p.reporter.ReportLatestSealedBlockHeight(block.Height)
			p.emitter.Emit(ctx, SealedBlock{
				Height:  block.Height,
				BlockID: block.BlockID,
			})
			p.log.Debug().
				Uint64("height", block.Height).
				Str("block_id", block.BlockID.String()).
				Msg("emitted sealed block")
		case <-time.After(p.config.BlockWaitTimeout):
			p.log.Error().
				Dur("timeout", p.config.BlockWaitTimeout).
				Msg("block wait timeout reached, something went wrong with the network")
			ctx.Throw(fmt.Errorf("block wait timeout reached"))
			return
		}
	}
}

// isDisconnectError checks if the error is a server disconnect that we should recover from.
func isDisconnectError(err error) bool {
	return strings.Contains(err.Error(), "stream terminated by RST_STREAM with error code: NO_ERROR")
}
