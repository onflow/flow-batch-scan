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
	fbs "github.com/onflow/flow-batch-scan"
	"github.com/onflow/flow-batch-scan/candidates"
	"github.com/onflow/flow-batch-scan/client"
	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
)

//go:embed get_contract_names.cdc
var Script string

func main() {
	log.Logger = log.
		Output(zerolog.ConsoleWriter{Out: os.Stderr}).
		Level(zerolog.InfoLevel)

	flowClient, err := client.NewClient("access.mainnet.nodes.onflow.org:9000", log.Logger)
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

	scriptResultHandler := NewScriptResultHandler(log.Logger)

	script := []byte(Script)
	batchSize := 5000 // simple scripts can have a bigger batch size

	scanner := fbs.NewBlockScanner(
		flowClient,
		fbs.WithContext(context.Background()),
		fbs.WithScript(script),
		fbs.WithCandidateScanners(candidateScanners),
		fbs.WithScriptResultHandler(scriptResultHandler),
		fbs.WithBatchSize(batchSize),
		fbs.WithChainID(flow.Mainnet),
		fbs.WithLogger(log.Logger),
	)

	result, err := scanner.Scan()
	if err != nil {
		log.Fatal().Err(err).Msg("scanner failed")
	}
	log.Info().
		Uint64("scan_complete_at_block", result.LatestScannedBlockHeight).
		Bool("result_accurate", result.ScanIsAccurate).
		Msg("scanner finished")
}
