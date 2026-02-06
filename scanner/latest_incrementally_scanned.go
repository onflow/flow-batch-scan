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
	"container/heap"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
)

// LatestIncrementallyScanned holds the latest block height that has been
// successfully processed by the incremental scanner.
// This is used by the full scanner to determine safe reference block heights.
//
// It maintains a min-heap of pending heights to handle out-of-order updates.
// Only consecutive heights are applied to maintain a monotonically increasing
// sequence without gaps.
//
// When a full scan is requested (due to large block gaps), ResetScanned() should
// be called to create a clean discontinuity and purge stale pending heights.
type LatestIncrementallyScanned struct {
	height   atomic.Uint64
	mu       sync.Mutex
	reporter StatusReporter
	log      zerolog.Logger
	// pending is a min-heap of heights waiting to be applied
	pending *uint64Heap
}

// NewLatestIncrementallyScanned creates a new instance with height 0.
func NewLatestIncrementallyScanned(
	logger zerolog.Logger,
	reporter StatusReporter,
) *LatestIncrementallyScanned {
	h := &uint64Heap{}
	heap.Init(h)
	return &LatestIncrementallyScanned{
		pending:  h,
		reporter: reporter,
		log:      logger.With().Str("component", "latest_scanned").Logger(),
	}
}

// GetIfScanned returns the latest incrementally scanned block height and a bool
// indicating whether any height has been scanned yet.
// Returns (0, false) if nothing has been scanned yet.
func (l *LatestIncrementallyScanned) GetIfScanned() (uint64, bool) {
	height := l.height.Load()
	return height, height > 0
}

// SetScanned updates the latest incrementally scanned block height.
// Heights can arrive out of order. The stored height is only updated when
// a consecutive height is received. Non-consecutive heights are queued
// and applied later when the gap is filled.
//
// Special case: if no height has been set yet (height is 0), the first
// height received becomes the starting point.
//
// Heights less than or equal to the current height are ignored.
func (l *LatestIncrementallyScanned) SetScanned(height uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	current := l.height.Load()

	// Ignore heights that are at or below current
	if height <= current {
		return
	}

	if current == 0 {
		l.height.Store(height)
		l.processPending()
		l.reporter.ReportIncrementalBlockHeight(height)
		return
	}

	// If this is the next consecutive height, apply it and process pending
	if height == current+1 {
		l.height.Store(height)
		l.processPending()
		l.reporter.ReportIncrementalBlockHeight(height)
		return
	}

	// Otherwise, queue it for later
	heap.Push(l.pending, height)
}

// ResetScanned resets the tracked height to the given value and purges
// the pending queue of any heights at or below the new height.
// This is called when a full scan is requested, creating a discontinuity
// in the incremental scanning process.
func (l *LatestIncrementallyScanned) ResetScanned(height uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	current := l.height.Load()

	// Only reset if the new height is greater than current
	if height <= current {
		return
	}

	// Purge pending heap of heights at or below the new height.
	// Since it's a min-heap, we only need to pop from the front while
	// the smallest element is <= height. The heap property guarantees
	// that once we find an element > height, all remaining are also > height.
	for l.pending.Len() > 0 && (*l.pending)[0] <= height {
		heap.Pop(l.pending)
	}

	// Set the new height and report it
	l.height.Store(height)
	l.reporter.ReportIncrementalBlockHeight(height)

	l.log.Debug().
		Uint64("old_height", current).
		Uint64("new_height", height).
		Msg("reset scanned height due to full scan request")
}

// processPending processes the heap to apply any now-consecutive heights.
// Must be called with lock held.
func (l *LatestIncrementallyScanned) processPending() {
	for l.pending.Len() > 0 {
		next := (*l.pending)[0]
		if next != l.height.Load()+1 {
			break
		}
		heap.Pop(l.pending)
		l.height.Store(next)
		l.reporter.ReportIncrementalBlockHeight(next)
	}
}

// uint64Heap implements heap.Interface for a min-heap of uint64 values.
type uint64Heap []uint64

func (h uint64Heap) Len() int           { return len(h) }
func (h uint64Heap) Less(i, j int) bool { return h[i] < h[j] }
func (h uint64Heap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *uint64Heap) Push(x interface{}) {
	*h = append(*h, x.(uint64))
}

func (h *uint64Heap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
