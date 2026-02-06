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
)

// Subscription represents an active subscription to an emitter.
type Subscription[Event any] struct {
	ch      chan Event
	emitter *emitter[Event]
}

// Channel returns the receive-only channel for this subscription.
func (s *Subscription[Event]) Channel() <-chan Event {
	return s.ch
}

// Unsubscribe removes this subscription and closes the channel.
func (s *Subscription[Event]) Unsubscribe() {
	s.emitter.unsubscribe(s.ch)
}

// EmitterChannel provides read-only subscription capability.
type EmitterChannel[Event any] interface {
	// Subscribe returns a subscription that receives emitted events.
	// The bufferSize determines how many events can be buffered before blocking.
	Subscribe(bufferSize uint) *Subscription[Event]
}

// Emitter extends EmitterChannel with the ability to emit events.
type Emitter[Event any] interface {
	EmitterChannel[Event]
	// Emit sends an event to all subscribers.
	// Blocks if any subscriber's buffer is full, unless ctx is cancelled.
	Emit(ctx context.Context, event Event)
	// Close closes all subscriber channels and prevents new subscriptions.
	Close()
}

type emitter[Event any] struct {
	mu          sync.RWMutex
	subscribers map[chan Event]struct{}
	closed      bool
}

// NewEmitter creates a new event emitter.
func NewEmitter[Event any]() Emitter[Event] {
	return &emitter[Event]{
		subscribers: make(map[chan Event]struct{}),
	}
}

func (e *emitter[Event]) Subscribe(bufferSize uint) *Subscription[Event] {
	ch := make(chan Event, bufferSize)
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.closed {
		e.subscribers[ch] = struct{}{}
	} else {
		close(ch)
	}
	return &Subscription[Event]{
		ch:      ch,
		emitter: e,
	}
}

func (e *emitter[Event]) unsubscribe(ch chan Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.subscribers[ch]; exists {
		delete(e.subscribers, ch)
		close(ch)
	}
}

func (e *emitter[Event]) Emit(ctx context.Context, event Event) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return
	}
	for ch := range e.subscribers {
		select {
		case ch <- event:
		case <-ctx.Done():
			return
		}
	}
}

func (e *emitter[Event]) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	e.closed = true
	for ch := range e.subscribers {
		close(ch)
	}
	// Clear the map
	e.subscribers = make(map[chan Event]struct{})
}
