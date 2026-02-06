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
	"context"
	"errors"
	"fmt"

	"github.com/onflow/flow-batch-scan/client"
	"github.com/onflow/flow-batch-scan/scanner/engine"
)

// ErrScanCompleted is returned when the scan completed successfully
// (i.e., not in continuous mode and full scan finished).
var ErrScanCompleted = errors.New("scan completed successfully")

// Scanner orchestrates blockchain scanning using an engine-based component system.
type Scanner struct {
	engine *engine.Engine[*engineConfig]

	// onCompletedFN is a way for the scanner to stop itself when it's done' if it
	// is not in continuous mode.
	onCompletedFN func()
}

// NewScanner creates a new scanner with all components wired together.
func NewScanner(
	client client.Client,
	config Config,
) *Scanner {
	// Create scanner first so the closure can reference it
	s := &Scanner{
		onCompletedFN: func() {},
	}

	// Create engine config - closure captures s, so s.onCompletedFN can be updated later
	engineConfig := &engineConfig{
		Client: client,
		Config: config,
		OnCompletedFN: func() {
			s.onCompletedFN()
		},
	}

	// Build engine using the builder
	builder := NewScannerEngineBuilder(engineConfig)
	eng, err := builder.Build(engineConfig)
	if err != nil {
		// This is a panic rather than an error,
		// because the engine should always be built successfully.
		// There are no components returning errors on registration.
		panic(fmt.Errorf("failed to build scanner engine: %w", err))
	}

	s.engine = eng
	return s
}

// ScanConcluded contains the results of a completed scan.
type ScanConcluded struct {
	LatestScannedBlockHeight uint64
	// ScanIsComplete is false if a full scan was not completed,
	// this means some accounts may have stale data, or have been missed all together.
	ScanIsComplete bool
}

// Scan runs the scanner until completion or context cancellation.
func (s *Scanner) Scan(ctx context.Context) (ScanConcluded, error) {
	ctx, cancel := context.WithCancelCause(ctx)

	s.onCompletedFN = func() {
		cancel(ErrScanCompleted)
	}

	// Run the engine
	err := s.engine.Run(ctx)

	// Check the cause of the context cancellation to determine if scan completed successfully
	cause := context.Cause(ctx)
	scanCompleted := errors.Is(cause, ErrScanCompleted)

	// Populate result from engine config
	latestHeight, _ := s.engine.Config.LatestScanned.GetIfScanned()
	result := ScanConcluded{
		LatestScannedBlockHeight: latestHeight,
		ScanIsComplete:           scanCompleted,
	}

	// Don't return context.Canceled or ErrScanCompleted as errors (normal shutdown)
	if errors.Is(err, context.Canceled) || scanCompleted {
		return result, nil
	}

	return result, err
}
