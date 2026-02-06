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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

type StatusReporter interface {
	ReportIncrementalBlockHeight(height uint64)
	ReportLatestSealedBlockHeight(height uint64)
	ReportIsFullScanRunning(running bool)
	ReportFullScanProgress(current uint64, total uint64)
}

type DefaultStatusReporter struct {
	latestReportedBlockHeight       uint64
	latestReportedSealedBlockHeight uint64
	namespace                       string
	log                             zerolog.Logger

	incBlockHeight          prometheus.Counter
	latestSealedBlockHeight prometheus.Counter
	fullScanRunning         prometheus.Gauge
	fullScanProgress        prometheus.Gauge
}

type StatusReporterOption = func(*DefaultStatusReporter, prometheus.Registerer) prometheus.Registerer

// WithRegistry configures the reporter to register metrics with the provided registry.
// If not specified, uses prometheus.DefaultRegisterer.
func WithRegistry(registry prometheus.Registerer) StatusReporterOption {
	return func(r *DefaultStatusReporter, _ prometheus.Registerer) prometheus.Registerer {
		return registry
	}
}

// NewStatusReporter creates a new status reporter that reports scanner metrics to Prometheus.
// The namespace is used to prefix all metrics.
//
// The reporter will report:
//   - the incremental block height (the block height last handled by the incremental scanner)
//   - the latest sealed block height (from block publisher subscription)
//   - if a full scan is currently running (if it is any data the scanner is tracking is inaccurate)
//   - if a full scan is currently running, the progress of the full scan (from 0 to 1)
//
// By default, metrics are registered with prometheus.DefaultRegisterer. Use WithRegistry
// to use a custom registry.
func NewStatusReporter(
	logger zerolog.Logger,
	namespace string,
	options ...StatusReporterOption,
) *DefaultStatusReporter {
	r := &DefaultStatusReporter{
		namespace: namespace,
		log:       logger.With().Str("component", "reporter").Logger(),
	}

	// Start with default registry, options may override
	registry := prometheus.DefaultRegisterer
	for _, option := range options {
		registry = option(r, registry)
	}

	r.initMetrics(registry)

	return r
}

func (r *DefaultStatusReporter) initMetrics(registry prometheus.Registerer) {
	r.incBlockHeight = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: r.namespace,
		Name:      "inc_block_height",
		Help: "The block height last handled by the incremental scanner. " +
			"If no batch scanner is running at this moment, all other results are considered accurate at this block height.",
	})
	r.latestSealedBlockHeight = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: r.namespace,
		Name:      "latest_sealed_block_height",
		Help:      "The latest sealed block height received from the network subscription.",
	})
	r.fullScanRunning = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: r.namespace,
		Name:      "full_scan_running",
		Help:      "If a full scan is currently running. If it is, other statistics may not be accurate.",
	})
	r.fullScanProgress = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: r.namespace,
		Name:      "full_scan_progress",
		Help:      "If a full scan is currently running, this is the progress of the full scan.",
	})

	// Register metrics with the provided registry
	registry.MustRegister(r.incBlockHeight, r.latestSealedBlockHeight, r.fullScanRunning, r.fullScanProgress)
}

func (r *DefaultStatusReporter) ReportIncrementalBlockHeight(height uint64) {
	if r.latestReportedBlockHeight >= height {
		return
	}
	diff := height - r.latestReportedBlockHeight
	r.latestReportedBlockHeight = height
	r.incBlockHeight.Add(float64(diff))
}

func (r *DefaultStatusReporter) ReportLatestSealedBlockHeight(height uint64) {
	if r.latestReportedSealedBlockHeight >= height {
		return
	}
	diff := height - r.latestReportedSealedBlockHeight
	r.latestReportedSealedBlockHeight = height
	r.latestSealedBlockHeight.Add(float64(diff))
}

func (r *DefaultStatusReporter) ReportIsFullScanRunning(running bool) {
	if running {
		r.fullScanRunning.Set(1)
	} else {
		r.fullScanRunning.Set(0)
	}
}

func (r *DefaultStatusReporter) ReportFullScanProgress(current uint64, total uint64) {
	progress := float64(current) / float64(total)
	r.fullScanProgress.Set(progress)
}

var _ StatusReporter = (*DefaultStatusReporter)(nil)
