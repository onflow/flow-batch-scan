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
	"github.com/onflow/flow-batch-scan"
	"github.com/onflow/flow-batch-scan/client"
	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
)

//go:embed load_script.cdc
var Script string

const ReportingNamespace = "script_load"

// This example just sends scripts to the network and exposes grpc metrics.
// Scripts are sent to the network at a rate of `scriptExecutionRateLimit` per second.
// It does not do anything with the results.
// The cadence script is arbitrary but must take a single argument of type [Address].
func main() {
	nodeURL := "access.testnet.nodes.onflow.org:9000"
	scriptExecutionRateLimit := 2
	// number of addresses to send as the script params in each script
	// if script is timing out this should be reduced
	batchSize := 1000

	log.Logger = log.
		Output(zerolog.ConsoleWriter{Out: os.Stderr}).
		Level(zerolog.InfoLevel)

	flowClient, err := client.NewClient(
		nodeURL,
		client.WithLog(log.Logger),
		func(config *client.Config) {
			config.SpecificRateLimits["/flow.access.AccessAPI/ExecuteScriptAtBlockHeight"] =
				scriptExecutionRateLimit
			config.MetricsNamespace = ReportingNamespace
			config.WithMetrics = true
		},
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

	// Create a reporter.
	// This will expose metrics on port :2112 /metrics
	// with the namespace ReportingNamespace
	reporter := scanner.NewStatusReporter(ReportingNamespace, log.Logger)

	// The script result handler does nothing in this example.
	scriptResultHandler := NewScriptResultHandler(log.Logger)

	config := scanner.DefaultConfig().
		WithScript([]byte(Script)).
		WithScriptResultHandler(scriptResultHandler).
		WithBatchSize(batchSize).
		WithChainID(flow.Testnet).
		WithLogger(log.Logger).
		WithStatusReporter(reporter).
		// since this example is just used to send a lot of scripts to the network,
		// we want to restart the scan from the beginning after it is done.
		WithContinuousScan(false)

	scan := scanner.NewScanner(
		flowClient,
		config,
	)

	for true {
		_, err = scan.Scan(context.Background())
		log.Logger.Info().Msg("restarting scan")
	}
}
