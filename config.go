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
	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog"

	"github.com/onflow/flow-batch-scan/candidates"
)

const DefaultBatchSize = 1000

type Config struct {
	ScriptRunnerConfig
	FullScanRunnerConfig
	IncrementalScannerConfig

	ScriptResultHandler ScriptResultHandler
	Reporter            StatusReporter

	ContinuousScan bool
	BatchSize      int

	Logger zerolog.Logger
}

func DefaultConfig() Config {
	return Config{
		ScriptRunnerConfig:       DefaultScriptRunnerConfig(),
		FullScanRunnerConfig:     DefaultFullScanRunnerConfig(),
		IncrementalScannerConfig: DefaultIncrementalScannerConfig(),
		ScriptResultHandler:      NoOpScriptResultHandler{},
		Reporter:                 NoOpStatusReporter{},
		ContinuousScan:           false,
		BatchSize:                DefaultBatchSize,
		Logger:                   zerolog.Nop(),
	}
}

func (c Config) WithLogger(
	value zerolog.Logger,
) Config {
	c.Logger = value
	return c
}

func (c Config) WithBatchSize(
	value int,
) Config {
	c.BatchSize = value
	return c
}

func (c Config) WithContinuousScan(
	value bool,
) Config {
	c.ContinuousScan = value
	return c
}

func (c Config) WithCandidateScanners(
	value []candidates.CandidateScanner,
) Config {
	c.CandidateScanners = value
	return c
}

func (c Config) WithStatusReporter(
	value StatusReporter,
) Config {
	c.Reporter = value
	return c
}

func (c Config) WithScriptResultHandler(
	value ScriptResultHandler,
) Config {
	c.ScriptResultHandler = value
	return c
}

func (c Config) WithChainID(
	value flow.ChainID,
) Config {
	c.ChainID = value
	return c
}

func (c Config) WithExcludeAddress(
	value func(id flow.ChainID, address flow.Address) bool,
) Config {
	c.ExcludeAddress = value
	return c
}

func (c Config) WithScript(
	value []byte,
) Config {
	c.Script = value
	return c
}

func (c Config) WithMaxConcurrentScripts(
	value int,
) Config {
	c.MaxConcurrentScripts = value
	return c
}

func (c Config) WithHandleScriptError(
	value func(AddressBatch, error) ScriptErrorAction,
) Config {
	c.HandleScriptError = value
	return c
}
