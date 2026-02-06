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
	"github.com/onflow/flow-go/module"

	"github.com/onflow/flow-batch-scan/scanner/engine"
	"github.com/onflow/flow-batch-scan/scanner/event"
)

// NewScannerEngineBuilder creates a builder for the scanner engine.
// The builder configures all components in the correct dependency order.
func NewScannerEngineBuilder(cfg *engineConfig) *engine.BuilderBase[*engineConfig] {
	builder := engine.NewBuilderBase[*engineConfig](cfg.Config.Logger)

	// Startup: create event emitters and shared objects
	var fullScanChannel event.Emitter[FullScanRequest]
	var sealedBlockChannel event.Emitter[SealedBlock]
	builder.StartupFunc(func(cfg *engineConfig) error {
		fullScanChannel = event.NewEmitter[FullScanRequest]()
		sealedBlockChannel = event.NewEmitter[SealedBlock]()
		cfg.LatestScanned = NewLatestIncrementallyScanned(
			cfg.Config.Logger,
			cfg.Config.Reporter,
		)
		return nil
	}).ShutdownFunc(
		func() error {
			fullScanChannel.Close()
			sealedBlockChannel.Close()
			return nil
		},
	)

	// Component: resultProcessor
	var resultProcessor *ScriptResultProcessor
	builder.Component("result_processor", func(cfg *engineConfig) (module.ReadyDoneAware, error) {
		resultProcessor = NewScriptResultProcessor(
			cfg.Config.Logger,
			cfg.Config.ScriptResultHandler,
		)
		return resultProcessor, nil
	})

	// Component: ScriptRunner (depends on ResultProcessor)
	var scriptRunner *ScriptRunner
	builder.Component("script_runner", func(cfg *engineConfig) (module.ReadyDoneAware, error) {
		scriptRunner = NewScriptRunner(
			cfg.Config.Logger,
			cfg.Client,
			resultProcessor,
			cfg.Config.ScriptRunnerConfig,
		)
		return scriptRunner, nil
	})

	// Component: BlockPublisher (emits sealed blocks)
	var blockPublisher *BlockPublisher
	builder.Component("block_publisher", func(cfg *engineConfig) (module.ReadyDoneAware, error) {
		blockPublisher = NewBlockPublisher(
			cfg.Config.Logger,
			cfg.Client,
			sealedBlockChannel,
			cfg.Config.BlockPublisherConfig,
			cfg.Config.Reporter,
		)
		return blockPublisher, nil
	})

	// Component: IncrementalScanner (depends on ScriptRunner, BlockPublisher)
	builder.Component("incremental_scanner", func(cfg *engineConfig) (module.ReadyDoneAware, error) {
		incrementalScanner := NewIncrementalScanner(
			cfg.Config.Logger,
			cfg.Client,
			scriptRunner,
			fullScanChannel,
			sealedBlockChannel,
			cfg.Config.BatchSize,
			cfg.Config.IncrementalScannerConfig,
			cfg.Config.Reporter,
			cfg.LatestScanned,
		)
		return incrementalScanner, nil
	})

	// Component: Coordinator (depends on ScriptRunner, Emitter)
	// Also creates FullScanRunner as it's just a factory, not a component
	builder.Component("coordinator", func(cfg *engineConfig) (module.ReadyDoneAware, error) {
		fullScanRunner := NewFullScanRunner(
			cfg.Config.Logger,
			cfg.Client,
			scriptRunner,
			cfg.Config.BatchSize,
			cfg.Config.FullScanRunnerConfig,
			cfg.Config.Reporter,
			cfg.LatestScanned,
		)
		coordinator := NewFullScanCoordinator(
			cfg.Config.Logger,
			fullScanRunner,
			fullScanChannel,
			cfg.Config.Reporter,
			cfg.Config.ContinuousScan,
			cfg.OnCompletedFN,
		)
		return coordinator, nil
	})

	return builder
}
