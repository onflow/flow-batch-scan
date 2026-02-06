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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
	flowgrpc "github.com/onflow/flow-go-sdk/access/grpc"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/onflow/flow-batch-scan/client"
)

func TestScanner_ExitsWhenContinuousScanFalse(t *testing.T) {
	const testTimeout = 2 * time.Second

	logger := zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.DebugLevel)
	mockClient := newMockClient()

	config := DefaultConfig().
		WithContinuousScan(false).
		WithBatchSize(10).
		WithChainID(flow.Testnet).
		WithLogger(logger).
		WithScript([]byte(`access(all) fun main(addresses: [Address]): [AnyStruct] { return [] }`))

	scanner := NewScanner(mockClient, config)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	done := make(chan struct{})
	var scanErr error
	var result ScanConcluded

	go func() {
		result, scanErr = scanner.Scan(ctx)
		close(done)
	}()

	// Wait a moment for scanner to start and subscribe to blocks
	time.Sleep(100 * time.Millisecond)

	// Emit some blocks to trigger the full scan completion
	// The full scan needs blocks to be emitted to trigger incremental processing
	go func() {
		for i := uint64(0); i < 10; i++ {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			default:
				mockClient.emitBlock(1000 + i)
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()

	select {
	case <-done:
		// Scanner exited as expected
		require.NoError(t, scanErr, "scan should complete without error")

		// Verify result fields are populated correctly
		// Note: LatestScannedBlockHeight may be 0 if incremental scanner didn't process any blocks
		// (e.g., when it skips ahead due to large block gap)
		require.True(t, result.ScanIsComplete, "scan should be marked as complete")
	case <-ctx.Done():
		t.Fatal("scanner did not exit within timeout - ContinuousScan=false should cause exit after full scan")
	}

	// Verify that scripts were called (proving the scan actually ran)
	require.Greater(t, mockClient.scriptCallCount.Load(), int32(0), "script should have been called at least once")
}

func TestScanner_ContinuesWhenContinuousScanTrue(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.DebugLevel)
	mockClient := newMockClient()

	config := DefaultConfig().
		WithContinuousScan(true).
		WithBatchSize(10).
		WithChainID(flow.Testnet).
		WithLogger(logger).
		WithScript([]byte(`access(all) fun main(addresses: [Address]): [AnyStruct] { return [] }`))

	scanner := NewScanner(mockClient, config)

	// Run scan with a short timeout - it should NOT exit on its own
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		_, _ = scanner.Scan(ctx)
		close(done)
	}()

	// Wait a moment for scanner to start and subscribe to blocks
	time.Sleep(100 * time.Millisecond)

	// Emit blocks periodically to keep the incremental scanner running
	go func() {
		for i := uint64(0); ; i++ {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			default:
				mockClient.emitBlock(1000 + i)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// Wait for the scan to complete its first full scan
	// (4 address provider scripts + 1 batch script = 5 total)
	require.Eventually(t, func() bool {
		return mockClient.scriptCallCount.Load() >= 5
	}, 2*time.Second, 50*time.Millisecond, "full scan should have executed scripts")

	// Scanner should still be running (not exited) after full scan completes
	select {
	case <-done:
		require.NotNil(t, ctx.Err(), "scanner exited prematurely - ContinuousScan=true should keep running")
	default:
		// Scanner is still running
	}

	// Clean up
	cancel()
	<-done
}

// mockClient implements client.Client for testing
type mockClient struct {
	blockHeight        atomic.Uint64
	scriptCallCount    atomic.Int32
	subscribers        []chan *flow.BlockDigest
	subscriberErrChans []chan error
	mu                 sync.Mutex
}

var _ client.Client = (*mockClient)(nil)

func newMockClient() *mockClient {
	m := &mockClient{}
	m.blockHeight.Store(1000)
	return m
}

// emitBlock simulates receiving a new block - used by tests
func (m *mockClient) emitBlock(height uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ch := range m.subscribers {
		select {
		case ch <- &flow.BlockDigest{Height: height, BlockID: flow.HexToID("0x01")}:
		default:
		}
	}
}

func (m *mockClient) GetLatestBlockHeader(ctx context.Context, isSealed bool) (*flow.BlockHeader, error) {
	return &flow.BlockHeader{
		Height: m.blockHeight.Load(),
	}, nil
}

func (m *mockClient) ExecuteScriptAtBlockHeight(ctx context.Context, height uint64, script []byte, arguments []cadence.Value) (cadence.Value, error) {
	m.scriptCallCount.Add(1)

	// For address provider: return storage usage for addresses 1-5, error for 6+
	// This simulates a chain with only 5 accounts
	if len(arguments) == 1 {
		// Check if this is the address provider script (has single Address argument)
		if _, ok := arguments[0].(cadence.Address); ok {
			// Get the address index from the generator pattern
			// For simplicity, we'll allow first 5 addresses
			addr := arguments[0].(cadence.Address)
			gen := flow.NewAddressGenerator(flow.Testnet)

			// Check if address is in first 5
			for i := uint(1); i <= 5; i++ {
				gen.SetIndex(i)
				if gen.Address() == flow.Address(addr) {
					return cadence.NewUInt64(100), nil
				}
			}
			// Address doesn't exist
			return nil, &scriptError{msg: "get storage used failed"}
		}
	}

	// For batch scripts, return empty array (no results to process)
	return cadence.NewArray([]cadence.Value{}), nil
}

func (m *mockClient) GetBlockByHeight(ctx context.Context, height uint64) (*flow.Block, error) {
	return &flow.Block{
		BlockHeader: flow.BlockHeader{Height: height},
	}, nil
}

func (m *mockClient) GetTransaction(ctx context.Context, txID flow.Identifier) (*flow.Transaction, error) {
	return &flow.Transaction{}, nil
}

func (m *mockClient) GetEventsForHeightRange(ctx context.Context, query flowgrpc.EventRangeQuery) ([]flow.BlockEvents, error) {
	return nil, nil
}

func (m *mockClient) GetCollection(ctx context.Context, colID flow.Identifier) (*flow.Collection, error) {
	return &flow.Collection{}, nil
}

func (m *mockClient) SubscribeBlockDigestsFromLatest(
	ctx context.Context,
	blockStatus flow.BlockStatus,
) (<-chan *flow.BlockDigest, <-chan error, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	blocksChan := make(chan *flow.BlockDigest, 100)
	errChan := make(chan error, 1)
	m.subscribers = append(m.subscribers, blocksChan)
	m.subscriberErrChans = append(m.subscriberErrChans, errChan)

	return blocksChan, errChan, nil
}

func (m *mockClient) SubscribeBlockDigestsFromStartHeight(
	ctx context.Context,
	startHeight uint64,
	blockStatus flow.BlockStatus,
) (<-chan *flow.BlockDigest, <-chan error, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	blocksChan := make(chan *flow.BlockDigest, 100)
	errChan := make(chan error, 1)
	m.subscribers = append(m.subscribers, blocksChan)
	m.subscriberErrChans = append(m.subscriberErrChans, errChan)

	return blocksChan, errChan, nil
}

// scriptError implements error for simulating script failures
type scriptError struct {
	msg string
}

func (e *scriptError) Error() string {
	return e.msg
}
