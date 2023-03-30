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

package main

import (
	"context"
	_ "embed"
	"github.com/onflow/cadence"
	"github.com/onflow/flow-batch-scan"
	"github.com/onflow/flow-batch-scan/candidates"
	"github.com/onflow/flow-batch-scan/client"
	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
)

//go:embed get_contract_deployed.cdc
var Script string

// This is a very similar example to the contract_names example. Please see that one first.
func main() {
	log.Logger = log.
		Output(zerolog.ConsoleWriter{Out: os.Stderr}).
		Level(zerolog.InfoLevel)

	flowClient, err := client.NewClient(
		"access.testnet.nodes.onflow.org:9000",
		client.WithLog(log.Logger),
	)
	if err != nil {
		log.Fatal().Err(err).Msg("could not create client")
	}
	defer func() {
		err := flowClient.Close()
		if err != nil {
			log.Error().Err(err).Msg("failed to close client")
		}
	}()

	candidateScanners := []candidates.CandidateScanner{
		candidates.NewAuthorizerCandidatesScanner(log.Logger),
		candidates.NewEventCandidatesScanner(
			"flow.AccountContractUpdated",
			func(event cadence.Event) (flow.Address, error) {
				return flow.BytesToAddress(event.Fields[0].(cadence.Address).Bytes()), nil
			},
			log.Logger,
		),
	}

	// Create a reporter.
	// This will expose metrics on port :2112 /metrics
	// The metrics will be the custom ones `monitor_contract_deployments_contracts_deployed`
	// and some default ones from the `flow-batch-scan` package. See `scan.NewStatusReporter`
	reporter := NewReporter(log.Logger)

	// NewScriptResultHandler is different from the previous example
	scriptResultHandler := NewScriptResultHandler(reporter, log.Logger)

	batchSize := 5000

	config := scanner.DefaultConfig().
		WithScript([]byte(Script)).
		WithCandidateScanners(candidateScanners).
		WithScriptResultHandler(scriptResultHandler).
		WithBatchSize(batchSize).
		WithChainID(flow.Testnet).
		WithLogger(log.Logger).
		// This is new.
		WithStatusReporter(reporter).
		// This example uses a continuous scan, which means that it will keep scanning the chain for changes.
		WithContinuousScan(true)

	scan := scanner.NewScanner(
		flowClient,
		config,
	)

	// Start the scanner. We don't need to wait for it to finish, because it will keep running.
	// It is important to note, the while a full-scan is running (either because the scanner was just started,
	// or because the incremental scan was lagging behind too much), the results of the scanner are not complete/accurate,
	// there could be accounts that have never been scanned yet, or have changed since the last scan.
	_, err = scan.Scan(context.Background())
	if err != nil {
		log.Fatal().Err(err).Msg("scanner failed")
	}
}
