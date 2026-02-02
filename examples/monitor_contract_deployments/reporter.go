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
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"

	scan "github.com/onflow/flow-batch-scan/scanner"
)

// Reporter combines the scanner's status reporter with custom application metrics.
// It uses a custom Prometheus registry to isolate metrics.
type Reporter struct {
	scan.StatusReporter
	registry *prometheus.Registry

	contractsDeployed prometheus.Gauge
}

// NewReporter creates a new reporter that combines library metrics with custom app metrics.
func NewReporter(logger zerolog.Logger) *Reporter {
	// Create a custom registry for metric isolation
	reg := prometheus.NewRegistry()

	// Create the scanner's status reporter with our custom registry
	// This ensures all scanner metrics are registered to our isolated registry
	statusReporter := scan.NewStatusReporter(
		logger,
		"monitor_contract_deployments",
		scan.WithRegistry(reg),
	)

	// Define custom application metrics
	contractsDeployed := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "monitor_contract_deployments_contracts_deployed",
		Help: "The number of deployed contracts.",
	})
	reg.MustRegister(contractsDeployed)

	return &Reporter{
		registry:          reg,
		StatusReporter:    statusReporter,
		contractsDeployed: contractsDeployed,
	}
}

// Handler returns an HTTP handler that exposes all metrics.
// Mount this at your preferred metrics endpoint (e.g., /metrics).
func (r *Reporter) Handler() http.Handler {
	return promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{})
}

// ReportContractsDeployed reports the number of deployed contracts.
func (r *Reporter) ReportContractsDeployed(accounts int64) {
	r.contractsDeployed.Set(float64(accounts))
}
