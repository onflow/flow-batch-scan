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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"

	scan "github.com/onflow/flow-batch-scan"
)

type Reporter struct {
	scan.StatusReporter
}

func NewReporter(
	logger zerolog.Logger,
) *Reporter {
	return &Reporter{
		// this also has the status reporter which already reports some status metrics
		StatusReporter: scan.NewStatusReporter(
			context.Background(),
			"monitor_contract_deployments",
			logger),
	}
}

var (
	contractsDeployed = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "monitor_contract_deployments_contracts_deployed",
		Help: "The number of deployed contracts.",
	})
)

func (r *Reporter) ReportContractsDeployed(accounts int64) {
	contractsDeployed.Set(float64(accounts))
}
