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

package main

import (
	"context"
	_ "embed"
	"net/http"
	"os"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/onflow/flow-batch-scan/candidates"
	"github.com/onflow/flow-batch-scan/client"
	scanner "github.com/onflow/flow-batch-scan/scanner"
)

//go:embed get_contract_deployed.cdc
var Script string

// This example demonstrates a continuous scan that monitors for contract deployments.
// It shows how to set up custom metrics with a user-managed HTTP server.
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
				return flow.BytesToAddress(event.FieldsMappedByName()["address"].(cadence.Address).Bytes()), nil
			},
			log.Logger,
		),
	}

	// Create a reporter that combines library metrics with custom app metrics.
	// The reporter uses a custom Prometheus registry for metric isolation.
	reporter := NewReporter(log.Logger)

	// Start the metrics HTTP server.
	// The library no longer starts its own server - you have full control over
	// the port, path, TLS, authentication, etc.
	http.Handle("/metrics", reporter.Handler())
	go func() {
		log.Info().Msg("Starting metrics server on :2112")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Fatal().Err(err).Msg("metrics server failed")
		}
	}()

	// NewScriptResultHandler is different from the previous example
	scriptResultHandler := NewScriptResultHandler(log.Logger, reporter)

	batchSize := 5000

	config := scanner.DefaultConfig().
		WithScript([]byte(Script)).
		WithCandidateScanners(candidateScanners).
		WithScriptResultHandler(scriptResultHandler).
		WithBatchSize(batchSize).
		WithChainID(flow.Testnet).
		WithLogger(log.Logger).
		WithStatusReporter(reporter).
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
