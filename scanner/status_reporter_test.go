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

package scanner_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	scan "github.com/onflow/flow-batch-scan/scanner"
)

func TestStatusReporterWithDefaultRegistry(t *testing.T) {
	reporter := scan.NewStatusReporter(zerolog.Nop(), "test")
	require.NotNil(t, reporter)

	// Test that metrics are registered to default registry
	// by checking we can gather them
	gatherer := prometheus.DefaultGatherer
	metrics, err := gatherer.Gather()
	require.NoError(t, err)

	foundBlockHeight := false
	for _, mf := range metrics {
		if mf.GetName() == "test_inc_block_height" {
			foundBlockHeight = true
			break
		}
	}
	require.True(t, foundBlockHeight, "test_inc_block_height metric should be registered")
}

func TestStatusReporterWithCustomRegistry(t *testing.T) {
	// Create a custom registry
	registry := prometheus.NewRegistry()

	// Create reporter with custom registry
	reporter := scan.NewStatusReporter(
		zerolog.Nop(),
		"custom_test",
		scan.WithRegistry(registry),
	)
	require.NotNil(t, reporter)

	// Test that metrics are registered to the custom registry
	metrics, err := registry.Gather()
	require.NoError(t, err)

	foundBlockHeight := false
	foundLatestSealedBlockHeight := false
	foundFullScanRunning := false
	foundFullScanProgress := false

	for _, mf := range metrics {
		switch mf.GetName() {
		case "custom_test_inc_block_height":
			foundBlockHeight = true
		case "custom_test_latest_sealed_block_height":
			foundLatestSealedBlockHeight = true
		case "custom_test_full_scan_running":
			foundFullScanRunning = true
		case "custom_test_full_scan_progress":
			foundFullScanProgress = true
		}
	}

	require.True(t, foundBlockHeight, "custom_test_inc_block_height metric should be registered")
	require.True(t, foundLatestSealedBlockHeight, "custom_test_latest_sealed_block_height metric should be registered")
	require.True(t, foundFullScanRunning, "custom_test_full_scan_running metric should be registered")
	require.True(t, foundFullScanProgress, "custom_test_full_scan_progress metric should be registered")
}
