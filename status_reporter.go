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

package scanner

import (
	"context"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

const DefaultStatusReporterPort = 2112

type StatusReporter interface {
	ReportIncrementalBlockDiff(diff uint64)
	ReportIncrementalBlockHeight(height uint64)
	ReportIsFullScanRunning(running bool)
	ReportFullScanProgress(current uint64, total uint64)
}

type DefaultStatusReporter struct {
	*ComponentBase

	latestReportedBlockHeight uint64
	port                      int
	shouldStartServer         bool

	incBlockDiff     prometheus.Gauge
	incBlockHeight   prometheus.Counter
	fullScanRunning  prometheus.Gauge
	fullScanProgress prometheus.Gauge

	namespace string
}

type StatusReporterOption = func(*DefaultStatusReporter)

func WithStatusReporterPort(port int) StatusReporterOption {
	return func(r *DefaultStatusReporter) {
		r.port = port
	}
}

func WithStartServer(shouldStartServer bool) StatusReporterOption {
	return func(r *DefaultStatusReporter) {
		r.shouldStartServer = shouldStartServer
	}
}

// NewStatusReporter creates a new status reporter that reports the status of the indexer to prometheus.
// It will start a http server on the given port that exposes the metrics
// (unless this is disabled for the case where you would want to serve metrics yourself).
// the namespace is used to namespace all metrics.
// The status reporter will report:
// - the incremental block diff (the difference between the last block height handled by the incremental scanner and the current block height)
// - the incremental block height (the block height last handled by the incremental scanner)
// - if a full scan is currently running (if it is any data the scanner is tracking is inaccurate)
// - if a full scan is currently running, the progress of the full scan (from 0 to 1)
func NewStatusReporter(
	namespace string,
	logger zerolog.Logger,
	options ...StatusReporterOption,
) *DefaultStatusReporter {
	r := &DefaultStatusReporter{
		port:              DefaultStatusReporterPort,
		shouldStartServer: true,
		namespace:         namespace,
	}
	r.ComponentBase = NewComponentWithStart("reporter", r.start, logger)

	for _, option := range options {
		option(r)
	}

	return r
}

func (r *DefaultStatusReporter) start(ctx context.Context) {
	r.initMetrics(r.namespace)
	if !r.shouldStartServer {
		r.startServerless(ctx)
		return
	}
	r.startWithServer(ctx)
}

func (r *DefaultStatusReporter) startWithServer(ctx context.Context) {
	r.Logger.Info().
		Int("port", r.port).
		Msg("serving /metrics")

	http.Handle("/metrics", promhttp.Handler())
	server := &http.Server{Addr: fmt.Sprintf(":%d", r.port)}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			r.Logger.Error().
				Err(err).
				Msg("server error")
		}
	}()

	<-ctx.Done()

	err := server.Close()
	if err != nil {
		r.Logger.Warn().
			Err(err).
			Msg("error while closing server")
	}

	r.Finish(ctx.Err())
}

// startServerless just wait for the context to be done, so it properly closes the component.
func (r *DefaultStatusReporter) startServerless(ctx context.Context) {
	<-ctx.Done()
	r.Finish(ctx.Err())
}

func (r *DefaultStatusReporter) initMetrics(namespace string) {
	r.incBlockDiff = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "inc_block_diff",
		Help: "The block difference between last incremental check." +
			"If this is to high the incremental scanner will skip to the latest block" +
			" and a full scan will be StartupDone to catch up.",
	})
	r.incBlockHeight = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "inc_block_height",
		Help: "The block height last handled by the incremental scanner. " +
			"If no batch scanner is running at this moment, all other results are considered accurate at this block height.",
	})
	r.fullScanRunning = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "full_scan_running",
		Help:      "If a full scan is currently running. If It is other statistics may not be accurate.",
	})
	r.fullScanProgress = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "full_scan_progress",
		Help:      "If a full scan is currently running, this is the progress of the full scan.",
	})
}

func (r *DefaultStatusReporter) ReportIncrementalBlockDiff(diff uint64) {
	r.incBlockDiff.Set(float64(diff))
}

func (r *DefaultStatusReporter) ReportIncrementalBlockHeight(height uint64) {
	if r.latestReportedBlockHeight >= height {
		return
	}
	diff := height - r.latestReportedBlockHeight
	r.latestReportedBlockHeight = height
	r.incBlockHeight.Add(float64(diff))
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
