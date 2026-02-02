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

package event

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmitter_SubscribeAndEmit(t *testing.T) {
	emitter := NewEmitter[int]()
	defer emitter.Close()

	sub := emitter.Subscribe(10)
	ch := sub.Channel()

	ctx := context.Background()
	emitter.Emit(ctx, 42)

	select {
	case val := <-ch:
		assert.Equal(t, 42, val)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for event")
	}
}

func TestEmitter_MultipleSubscribers(t *testing.T) {
	emitter := NewEmitter[string]()
	defer emitter.Close()

	sub1 := emitter.Subscribe(10)
	sub2 := emitter.Subscribe(10)
	sub3 := emitter.Subscribe(10)

	ctx := context.Background()
	emitter.Emit(ctx, "hello")

	for i, ch := range []<-chan string{sub1.Channel(), sub2.Channel(), sub3.Channel()} {
		select {
		case val := <-ch:
			assert.Equal(t, "hello", val, "subscriber %d", i)
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("timeout waiting for event on subscriber %d", i)
		}
	}
}

func TestEmitter_Unsubscribe(t *testing.T) {
	emitter := NewEmitter[int]()
	defer emitter.Close()

	sub1 := emitter.Subscribe(10)
	sub2 := emitter.Subscribe(10)

	// Unsubscribe sub1
	sub1.Unsubscribe()

	ctx := context.Background()
	emitter.Emit(ctx, 42)

	// sub1's channel should be closed
	_, ok := <-sub1.Channel()
	assert.False(t, ok, "sub1 channel should be closed")

	// sub2 should receive the event
	select {
	case val := <-sub2.Channel():
		assert.Equal(t, 42, val)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for event on sub2")
	}
}

func TestEmitter_Close(t *testing.T) {
	emitter := NewEmitter[int]()

	sub1 := emitter.Subscribe(10)
	sub2 := emitter.Subscribe(10)

	emitter.Close()

	// Both channels should be closed
	_, ok1 := <-sub1.Channel()
	_, ok2 := <-sub2.Channel()
	assert.False(t, ok1, "sub1 channel should be closed")
	assert.False(t, ok2, "sub2 channel should be closed")

	// New subscriptions after close should return closed channel
	sub3 := emitter.Subscribe(10)
	_, ok3 := <-sub3.Channel()
	assert.False(t, ok3, "sub3 channel should be closed immediately")
}

func TestEmitter_EmitAfterClose(t *testing.T) {
	emitter := NewEmitter[int]()
	sub := emitter.Subscribe(10)
	emitter.Close()

	// Should not panic
	ctx := context.Background()
	emitter.Emit(ctx, 42)

	// Channel should be closed, not receive the event
	_, ok := <-sub.Channel()
	assert.False(t, ok)
}

func TestEmitter_EmitWithCancelledContext(t *testing.T) {
	emitter := NewEmitter[int]()
	defer emitter.Close()

	// Subscribe with zero buffer to force blocking
	sub := emitter.Subscribe(0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Emit should return without blocking due to cancelled context
	done := make(chan struct{})
	go func() {
		emitter.Emit(ctx, 42)
		close(done)
	}()

	select {
	case <-done:
		// Good, emit returned
	case <-time.After(100 * time.Millisecond):
		t.Fatal("emit should have returned due to cancelled context")
	}

	// Channel should be empty
	select {
	case <-sub.Channel():
		t.Fatal("should not have received event")
	default:
		// Good
	}
}

func TestEmitter_ConcurrentAccess(t *testing.T) {
	emitter := NewEmitter[int]()
	defer emitter.Close()

	const numGoroutines = 10
	const numEvents = 100

	var wg sync.WaitGroup
	ctx := context.Background()

	// Start subscribers
	subs := make([]*Subscription[int], numGoroutines)
	for i := range numGoroutines {
		subs[i] = emitter.Subscribe(numEvents * numGoroutines)
	}

	// Start emitters
	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range numEvents {
				emitter.Emit(ctx, id*numEvents+j)
			}
		}(i)
	}

	wg.Wait()

	// Each subscriber should have received all events
	for i, sub := range subs {
		count := 0
		timeout := time.After(time.Second)
	drain:
		for {
			select {
			case _, ok := <-sub.Channel():
				if !ok {
					break drain
				}
				count++
				if count == numGoroutines*numEvents {
					break drain
				}
			case <-timeout:
				break drain
			}
		}
		require.Equal(t, numGoroutines*numEvents, count, "subscriber %d received wrong number of events", i)
	}
}

func TestEmitter_UnsubscribeTwice(t *testing.T) {
	emitter := NewEmitter[int]()
	defer emitter.Close()

	sub := emitter.Subscribe(10)
	sub.Unsubscribe()
	// Should not panic
	sub.Unsubscribe()
}

func TestEmitter_CloseTwice(t *testing.T) {
	emitter := NewEmitter[int]()
	emitter.Close()
	// Should not panic
	emitter.Close()
}
