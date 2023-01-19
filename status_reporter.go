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

package lib

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

type StatusReporter interface {
	ReportIncrementalBlockDiff(diff uint64)
	ReportIncrementalBlockHeight(height uint64)
	ReportIsFullScanRunning(running bool)
	ReportFullScanProgress(current uint64, total uint64)
}

type statusReporter struct {
	*ComponentBase

	latestReportedBlockHeight uint64

	incBlockDiff     prometheus.Gauge
	incBlockHeight   prometheus.Counter
	fullScanRunning  prometheus.Gauge
	fullScanProgress prometheus.Gauge
}

func NewStatusReporter(
	ctx context.Context,
	prefix string,
	logger zerolog.Logger,
) StatusReporter {
	r := &statusReporter{
		ComponentBase: NewComponent("reporter", logger),
	}
	r.initMetrics(prefix)
	go r.start(ctx)
	r.StartupDone()
	return r
}
func (r *statusReporter) start(_ context.Context) {
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(":2112", nil)
	if err != nil {
		r.Logger.Err(err).Msg("failed to start reporter")
	}
	r.Finish(err)
}

func (r *statusReporter) initMetrics(prefix string) {
	r.incBlockDiff = promauto.NewGauge(prometheus.GaugeOpts{
		Name: prefix + "_inc_block_diff",
		Help: "The block difference between last incremental check." +
			"If this is to high the incremental scanner will skip to the latest block" +
			" and a full scan will be StartupDone to catch up.",
	})
	r.incBlockHeight = promauto.NewCounter(prometheus.CounterOpts{
		Name: prefix + "_inc_block_height",
		Help: "The block height last handled by the incremental scanner. " +
			"If no batch scanner is running at this moment, all other results are considered accurate at this block height.",
	})
	r.fullScanRunning = promauto.NewGauge(prometheus.GaugeOpts{
		Name: prefix + "_full_scan_running",
		Help: "If a full scan is currently running. If It is other statistics may not be accurate.",
	})
	r.fullScanProgress = promauto.NewGauge(prometheus.GaugeOpts{
		Name: prefix + "_full_scan_progress",
		Help: "If a full scan is currently running, this is the progress of the full scan.",
	})
}

func (r *statusReporter) ReportIncrementalBlockDiff(diff uint64) {
	r.incBlockDiff.Set(float64(diff))
}

func (r *statusReporter) ReportIncrementalBlockHeight(height uint64) {
	if r.latestReportedBlockHeight >= height {
		return
	}
	diff := height - r.latestReportedBlockHeight
	r.latestReportedBlockHeight = height
	r.incBlockHeight.Add(float64(diff))
}

func (r *statusReporter) ReportIsFullScanRunning(running bool) {
	if running {
		r.fullScanRunning.Set(1)
	} else {
		r.fullScanRunning.Set(0)
	}
}

func (r *statusReporter) ReportFullScanProgress(current uint64, total uint64) {
	progress := float64(current) / float64(total)
	r.fullScanProgress.Set(progress)
}
