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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
	flowgrpc "github.com/onflow/flow-go-sdk/access/grpc"
	"github.com/onflow/flow-go/module/irrecoverable"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// fakeClient implements client.Client for script runner tests.
type fakeClient struct {
	mu sync.Mutex

	// execute is called by ExecuteScriptAtBlockHeight; heights records every call.
	execute func(height uint64) (cadence.Value, error)
	heights []uint64
}

func (c *fakeClient) ExecuteScriptAtBlockHeight(
	_ context.Context,
	height uint64,
	_ []byte,
	_ []cadence.Value,
) (cadence.Value, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.heights = append(c.heights, height)
	return c.execute(height)
}

func (c *fakeClient) GetLatestBlockHeader(context.Context, bool) (*flow.BlockHeader, error) {
	return nil, errors.New("not implemented")
}
func (c *fakeClient) GetBlockByHeight(context.Context, uint64) (*flow.Block, error) {
	return nil, errors.New("not implemented")
}
func (c *fakeClient) GetTransaction(context.Context, flow.Identifier) (*flow.Transaction, error) {
	return nil, errors.New("not implemented")
}
func (c *fakeClient) GetEventsForHeightRange(context.Context, flowgrpc.EventRangeQuery) ([]flow.BlockEvents, error) {
	return nil, errors.New("not implemented")
}
func (c *fakeClient) GetCollection(context.Context, flow.Identifier) (*flow.Collection, error) {
	return nil, errors.New("not implemented")
}
func (c *fakeClient) SubscribeBlockDigestsFromLatest(context.Context, flow.BlockStatus) (<-chan *flow.BlockDigest, <-chan error, error) {
	return nil, nil, errors.New("not implemented")
}
func (c *fakeClient) SubscribeBlockDigestsFromStartHeight(context.Context, uint64, flow.BlockStatus) (<-chan *flow.BlockDigest, <-chan error, error) {
	return nil, nil, errors.New("not implemented")
}

type recordingResultHandler struct {
	mu      sync.Mutex
	heights []uint64
}

func (h *recordingResultHandler) Handle(batch ProcessedAddressBatch) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.heights = append(h.heights, batch.BlockHeight)
	return nil
}

// TestScriptRunner_RetriesAtLatestHeightOnPruned verifies that a batch failing
// with "execution state is pruned" is retried unsplit once the latest scanned
// height advances to a new block, and that done-tracking fires exactly once.
func TestScriptRunner_RetriesAtLatestHeightOnPruned(t *testing.T) {
	// stub the poll interval to keep the test fast
	origPoll := newScannedHeightPollInterval
	newScannedHeightPollInterval = time.Millisecond
	defer func() { newScannedHeightPollInterval = origPoll }()

	prunedErr := errors.New(`rpc error: code = InvalidArgument desc = failed to create storage snapshot: state not found in ledger: execution state is pruned`)

	const originalHeight uint64 = 100

	// the latest scanned height advances by one block on every pruned failure,
	// so each retry waits for and then runs at a new block
	var latest atomic.Uint64
	latest.Store(200)

	failuresLeft := 2
	client := &fakeClient{
		execute: func(height uint64) (cadence.Value, error) {
			if failuresLeft > 0 {
				failuresLeft--
				latest.Add(1)
				return nil, prunedErr
			}
			return cadence.NewArray(nil), nil
		},
	}

	resultHandler := &recordingResultHandler{}
	resultProcessor := NewScriptResultProcessor(zerolog.Nop(), resultHandler)

	runner := NewScriptRunner(
		zerolog.Nop(),
		client,
		resultProcessor,
		ScriptRunnerConfig{
			Script:               []byte("access(all) fun main(addresses: [Address]): [Int] { return [] }"),
			MaxConcurrentScripts: 2,
			HandleScriptError:    DefaultHandleScriptError,
			LatestScannedHeight:  func() (uint64, bool) { return latest.Load(), true },
		},
	)
	ctx, cancel := context.WithCancel(context.Background())
	signalerCtx := irrecoverable.NewMockSignalerContext(t, ctx)
	resultProcessor.Start(signalerCtx)
	runner.Start(signalerCtx)
	defer func() {
		cancel()
		<-resultProcessor.Done()
		<-runner.Done()
	}()

	<-resultProcessor.Ready()
	<-runner.Ready()

	done := make(chan struct{})
	batch := NewAddressBatch(
		[]flow.Address{flow.HexToAddress("0x01"), flow.HexToAddress("0x02")},
		originalHeight,
		func() { close(done) },
		nil,
	)
	runner.Submit(batch)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("batch was not processed in time")
	}

	client.mu.Lock()
	heights := append([]uint64(nil), client.heights...)
	client.mu.Unlock()

	// first attempt at the original height, each retry at the newly scanned
	// height, and never split (every call has the full address set)
	require.Len(t, heights, 3)
	assert.Equal(t, originalHeight, heights[0])
	assert.Equal(t, uint64(201), heights[1])
	assert.Equal(t, uint64(202), heights[2])

	resultHandler.mu.Lock()
	defer resultHandler.mu.Unlock()
	require.Len(t, resultHandler.heights, 1)
	assert.Equal(t, uint64(202), resultHandler.heights[0])
}

// TestScriptRunner_RetryUsesLatestHeight verifies that any resubmission (not
// just ScriptErrorActionRetryAtLatestHeight) moves the batch to the latest
// scanned height when it is higher than the batch's height.
func TestScriptRunner_RetryUsesLatestHeight(t *testing.T) {
	const originalHeight uint64 = 100
	const latestHeight uint64 = 200

	failuresLeft := 1
	client := &fakeClient{
		execute: func(height uint64) (cadence.Value, error) {
			if failuresLeft > 0 {
				failuresLeft--
				return nil, errors.New("boom")
			}
			return cadence.NewArray(nil), nil
		},
	}

	resultHandler := &recordingResultHandler{}
	resultProcessor := NewScriptResultProcessor(zerolog.Nop(), resultHandler)

	runner := NewScriptRunner(
		zerolog.Nop(),
		client,
		resultProcessor,
		ScriptRunnerConfig{
			Script:               []byte("access(all) fun main(addresses: [Address]): [Int] { return [] }"),
			MaxConcurrentScripts: 2,
			HandleScriptError: func(AddressBatch, error) ScriptErrorAction {
				return ScriptErrorActionRetry{}
			},
			LatestScannedHeight: func() (uint64, bool) { return latestHeight, true },
		},
	)
	ctx, cancel := context.WithCancel(context.Background())
	signalerCtx := irrecoverable.NewMockSignalerContext(t, ctx)
	resultProcessor.Start(signalerCtx)
	runner.Start(signalerCtx)
	defer func() {
		cancel()
		<-resultProcessor.Done()
		<-runner.Done()
	}()

	<-resultProcessor.Ready()
	<-runner.Ready()

	done := make(chan struct{})
	batch := NewAddressBatch(
		[]flow.Address{flow.HexToAddress("0x01")},
		originalHeight,
		func() { close(done) },
		nil,
	)
	runner.Submit(batch)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("batch was not processed in time")
	}

	client.mu.Lock()
	heights := append([]uint64(nil), client.heights...)
	client.mu.Unlock()

	require.Len(t, heights, 2)
	assert.Equal(t, originalHeight, heights[0])
	assert.Equal(t, latestHeight, heights[1])
}

// TestDefaultHandleScriptError_Pruned verifies the default handler maps the
// pruned-state error to ScriptErrorActionRetryAtLatestHeight.
func TestDefaultHandleScriptError_Pruned(t *testing.T) {
	err := errors.New("rpc error: code = InvalidArgument desc = failed to create storage snapshot: state not found in ledger for commit abc (block def): execution state is pruned")
	action := DefaultHandleScriptError(AddressBatch{}, err)
	assert.IsType(t, ScriptErrorActionRetryAtLatestHeight{}, action)
}

// TestAddressBatch_WithBlockHeightSharesDoneTracking verifies that a
// height-adjusted copy still completes the original batch's done-tracking.
func TestAddressBatch_WithBlockHeightSharesDoneTracking(t *testing.T) {
	done := make(chan struct{})
	var once sync.Once
	original := NewAddressBatch(
		[]flow.Address{flow.HexToAddress("0x01")},
		100,
		func() { once.Do(func() { close(done) }) },
		nil,
	)

	moved := original.WithBlockHeight(200)
	assert.Equal(t, uint64(200), moved.BlockHeight)
	assert.Equal(t, uint64(100), original.BlockHeight)

	moved.DoneHandling()
	moved.DoneHandling()
	original.DoneHandling()

	select {
	case <-done:
	default:
		t.Fatal("DoneHandling on the copy did not complete the original batch")
	}
}

// TestScriptRunner_PrunedFatalWhenGapTooSmall verifies that a pruned-state
// error for a batch whose height is within PrunedStateFatalHeightGap of the
// latest scanned height is fatal (ctx.Throw) instead of being rescheduled:
// state that close to the tip should never be pruned, so retrying cannot help.
func TestScriptRunner_PrunedFatalWhenGapTooSmall(t *testing.T) {
	prunedErr := errors.New(`rpc error: code = InvalidArgument desc = failed to create storage snapshot: state not found in ledger: execution state is pruned`)

	const batchHeight uint64 = 100
	const latestScanned uint64 = 105 // gap of 5 < threshold of 10

	client := &fakeClient{
		execute: func(uint64) (cadence.Value, error) {
			return nil, prunedErr
		},
	}

	resultProcessor := NewScriptResultProcessor(zerolog.Nop(), &recordingResultHandler{})

	runner := NewScriptRunner(
		zerolog.Nop(),
		client,
		resultProcessor,
		ScriptRunnerConfig{
			Script:                    []byte("access(all) fun main(addresses: [Address]): [Int] { return [] }"),
			MaxConcurrentScripts:      2,
			HandleScriptError:         DefaultHandleScriptError,
			LatestScannedHeight:       func() (uint64, bool) { return latestScanned, true },
			PrunedStateFatalHeightGap: 10,
		},
	)
	ctx, cancel := context.WithCancel(context.Background())
	signalerCtx := irrecoverable.NewMockSignalerContext(t, ctx)
	thrown := make(chan error, 1)
	signalerCtx.On("Throw", mock.Anything).
		Once().
		Run(func(args mock.Arguments) { thrown <- args.Error(0) }).
		Return()
	resultProcessor.Start(signalerCtx)
	runner.Start(signalerCtx)
	defer func() {
		cancel()
		<-resultProcessor.Done()
		<-runner.Done()
	}()

	<-resultProcessor.Ready()
	<-runner.Ready()

	runner.Submit(NewAddressBatch(
		[]flow.Address{flow.HexToAddress("0x01")},
		batchHeight,
		func() {},
		nil,
	))

	select {
	case err := <-thrown:
		assert.ErrorIs(t, err, prunedErr)
		assert.Contains(t, err.Error(), "100")
		assert.Contains(t, err.Error(), "105")
	case <-time.After(10 * time.Second):
		t.Fatal("expected fatal Throw for unservable near-tip batch")
	}

	// the script ran once and was never rescheduled
	client.mu.Lock()
	defer client.mu.Unlock()
	assert.Equal(t, []uint64{batchHeight}, client.heights)
}

// TestScriptRunner_PrunedFatalWhenNothingScanned verifies that a pruned-state
// error is fatal when no height has been scanned yet: there is no fresh block
// to wait for, so rescheduling cannot help.
func TestScriptRunner_PrunedFatalWhenNothingScanned(t *testing.T) {
	prunedErr := errors.New(`rpc error: code = InvalidArgument desc = failed to create storage snapshot: state not found in ledger: execution state is pruned`)

	const batchHeight uint64 = 100

	client := &fakeClient{
		execute: func(uint64) (cadence.Value, error) {
			return nil, prunedErr
		},
	}

	resultProcessor := NewScriptResultProcessor(zerolog.Nop(), &recordingResultHandler{})

	runner := NewScriptRunner(
		zerolog.Nop(),
		client,
		resultProcessor,
		ScriptRunnerConfig{
			Script:                    []byte("access(all) fun main(addresses: [Address]): [Int] { return [] }"),
			MaxConcurrentScripts:      2,
			HandleScriptError:         DefaultHandleScriptError,
			LatestScannedHeight:       func() (uint64, bool) { return 0, false },
			PrunedStateFatalHeightGap: 10,
		},
	)
	ctx, cancel := context.WithCancel(context.Background())
	signalerCtx := irrecoverable.NewMockSignalerContext(t, ctx)
	thrown := make(chan error, 1)
	signalerCtx.On("Throw", mock.Anything).
		Once().
		Run(func(args mock.Arguments) { thrown <- args.Error(0) }).
		Return()
	resultProcessor.Start(signalerCtx)
	runner.Start(signalerCtx)
	defer func() {
		cancel()
		<-resultProcessor.Done()
		<-runner.Done()
	}()

	<-resultProcessor.Ready()
	<-runner.Ready()

	runner.Submit(NewAddressBatch(
		[]flow.Address{flow.HexToAddress("0x01")},
		batchHeight,
		func() {},
		nil,
	))

	select {
	case err := <-thrown:
		assert.ErrorIs(t, err, prunedErr)
	case <-time.After(10 * time.Second):
		t.Fatal("expected fatal Throw when no height has been scanned yet")
	}
}

// TestScriptRunner_PrunedRetriedWhenGapEqualsThreshold verifies the boundary:
// a gap exactly equal to PrunedStateFatalHeightGap is not fatal, and the batch
// is rescheduled at the latest scanned height as usual.
func TestScriptRunner_PrunedRetriedWhenGapEqualsThreshold(t *testing.T) {
	// stub the poll interval to keep the test fast
	origPoll := newScannedHeightPollInterval
	newScannedHeightPollInterval = time.Millisecond
	defer func() { newScannedHeightPollInterval = origPoll }()

	prunedErr := errors.New(`rpc error: code = InvalidArgument desc = failed to create storage snapshot: state not found in ledger: execution state is pruned`)

	const batchHeight uint64 = 100
	const latestScanned uint64 = 110 // gap of 10 == threshold of 10

	failuresLeft := 1
	client := &fakeClient{
		execute: func(uint64) (cadence.Value, error) {
			if failuresLeft > 0 {
				failuresLeft--
				return nil, prunedErr
			}
			return cadence.NewArray(nil), nil
		},
	}

	resultHandler := &recordingResultHandler{}
	resultProcessor := NewScriptResultProcessor(zerolog.Nop(), resultHandler)

	runner := NewScriptRunner(
		zerolog.Nop(),
		client,
		resultProcessor,
		ScriptRunnerConfig{
			Script:                    []byte("access(all) fun main(addresses: [Address]): [Int] { return [] }"),
			MaxConcurrentScripts:      2,
			HandleScriptError:         DefaultHandleScriptError,
			LatestScannedHeight:       func() (uint64, bool) { return latestScanned, true },
			PrunedStateFatalHeightGap: 10,
		},
	)
	ctx, cancel := context.WithCancel(context.Background())
	// no Throw expectation: any Throw call fails the test
	signalerCtx := irrecoverable.NewMockSignalerContext(t, ctx)
	resultProcessor.Start(signalerCtx)
	runner.Start(signalerCtx)
	defer func() {
		cancel()
		<-resultProcessor.Done()
		<-runner.Done()
	}()

	<-resultProcessor.Ready()
	<-runner.Ready()

	done := make(chan struct{})
	runner.Submit(NewAddressBatch(
		[]flow.Address{flow.HexToAddress("0x01")},
		batchHeight,
		func() { close(done) },
		nil,
	))

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("batch was not processed in time")
	}

	client.mu.Lock()
	heights := append([]uint64(nil), client.heights...)
	client.mu.Unlock()

	require.Len(t, heights, 2)
	assert.Equal(t, batchHeight, heights[0])
	assert.Equal(t, latestScanned, heights[1])
}
