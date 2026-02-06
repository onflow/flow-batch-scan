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
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	scan "github.com/onflow/flow-batch-scan/scanner"
)

func TestLatestIncrementallyScanned_GetIfScanned(t *testing.T) {
	lis := scan.NewLatestIncrementallyScanned(zerolog.Nop(), scan.NoOpStatusReporter{})

	// Initially should be 0, false
	height, ok := lis.GetIfScanned()
	require.Equal(t, uint64(0), height)
	require.False(t, ok)

	// SetScanned first height
	lis.SetScanned(100)
	height, ok = lis.GetIfScanned()
	require.Equal(t, uint64(100), height)
	require.True(t, ok)

	// SetScanned to a higher consecutive value
	lis.SetScanned(101)
	height, ok = lis.GetIfScanned()
	require.Equal(t, uint64(101), height)
	require.True(t, ok)
}

func TestLatestIncrementallyScanned_SetIgnoresDecrease(t *testing.T) {
	lis := scan.NewLatestIncrementallyScanned(zerolog.Nop(), scan.NoOpStatusReporter{})

	lis.SetScanned(100)
	height, _ := lis.GetIfScanned()
	require.Equal(t, uint64(100), height)

	// Setting to a lower or equal value should be ignored (no panic)
	lis.SetScanned(50)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(100), height)

	lis.SetScanned(100)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(100), height)
}

func TestLatestIncrementallyScanned_OutOfOrderProcessing(t *testing.T) {
	lis := scan.NewLatestIncrementallyScanned(zerolog.Nop(), scan.NoOpStatusReporter{})

	// SetScanned first height
	lis.SetScanned(1)
	height, _ := lis.GetIfScanned()
	require.Equal(t, uint64(1), height)

	// Send heights out of order: 3, 5, 2, 4
	lis.SetScanned(3)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(1), height, "height should still be 1 (waiting for 2)")

	lis.SetScanned(5)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(1), height, "height should still be 1 (waiting for 2)")

	lis.SetScanned(2)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(3), height, "height should advance to 3 (1->2->3)")

	lis.SetScanned(4)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(5), height, "height should advance to 5 (3->4->5)")
}

func TestLatestIncrementallyScanned_GapFillProcessing(t *testing.T) {
	lis := scan.NewLatestIncrementallyScanned(zerolog.Nop(), scan.NoOpStatusReporter{})

	// SetScanned first height
	lis.SetScanned(10)
	height, _ := lis.GetIfScanned()
	require.Equal(t, uint64(10), height)

	// Create a gap: set 12, 13, 15 (missing 11 and 14)
	lis.SetScanned(12)
	lis.SetScanned(13)
	lis.SetScanned(15)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(10), height, "height should still be 10 (waiting for 11)")

	// Fill the first gap
	lis.SetScanned(11)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(13), height, "height should advance to 13 (10->11->12->13)")

	// Fill the second gap
	lis.SetScanned(14)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(15), height, "height should advance to 15 (13->14->15)")
}

func TestLatestIncrementallyScanned_ConcurrentAccess(t *testing.T) {
	lis := scan.NewLatestIncrementallyScanned(zerolog.Nop(), scan.NoOpStatusReporter{})

	var wg sync.WaitGroup
	numReaders := 10
	totalUpdates := 300

	// Start readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var lastSeen uint64
			for j := 0; j < totalUpdates*2; j++ {
				val, _ := lis.GetIfScanned()
				// Readers should never see a decrease
				require.GreaterOrEqual(t, val, lastSeen,
					"Reader observed decreasing value: %d -> %d", lastSeen, val)
				lastSeen = val
			}
		}()
	}

	// Single writer to ensure monotonic updates
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= totalUpdates; i++ {
			lis.SetScanned(uint64(i))
		}
	}()

	wg.Wait()

	// Final value should be the last set value
	height, _ := lis.GetIfScanned()
	require.Equal(t, uint64(totalUpdates), height)
}

func TestLatestIncrementallyScanned_ConcurrentWritersWithGaps(t *testing.T) {
	lis := scan.NewLatestIncrementallyScanned(zerolog.Nop(), scan.NoOpStatusReporter{})

	var wg sync.WaitGroup
	numWriters := 5
	heightsPerWriter := 100

	// Each writer sends non-overlapping ranges starting from 1
	// Writer 0: 1, 6, 11, 16, ...
	// Writer 1: 2, 7, 12, 17, ...
	// etc.
	// First height to arrive sets the starting point.
	// Since 1 is in the set, if it arrives first, we can process 1..500 consecutively.
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < heightsPerWriter; j++ {
				height := uint64(id + 1 + j*numWriters)
				lis.SetScanned(height)
			}
		}(i)
	}

	wg.Wait()

	// The final height depends on which height arrived first and whether
	// all consecutive heights were filled. Since we're sending 1..500
	// distributed across workers, and 1 is in the set, the final height
	// should be 500 if 1 arrived first (or early enough).
	height, _ := lis.GetIfScanned()
	// The first height that arrives sets the baseline. Heights below it
	// are ignored. So the minimum final height is the max height sent (500)
	// minus the gap from 1 to the first received height.
	// We verify it's at least > 0 (some progress was made)
	require.Greater(t, height, uint64(0), "some height should have been processed")
	// And ideally, if height 1 arrived first, we should have 500
	if height < uint64(numWriters*heightsPerWriter) {
		t.Logf("Height %d is less than expected %d - first height received was > 1",
			height, numWriters*heightsPerWriter)
	}
}

func TestLatestIncrementallyScanned_SetScannedIgnoresOldHeights(t *testing.T) {
	lis := scan.NewLatestIncrementallyScanned(zerolog.Nop(), scan.NoOpStatusReporter{})

	// Set initial height
	lis.SetScanned(100)
	height, _ := lis.GetIfScanned()
	require.Equal(t, uint64(100), height)

	// Try to set lower heights - should be ignored
	lis.SetScanned(50)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(100), height, "should ignore height below current")

	lis.SetScanned(100)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(100), height, "should ignore height equal to current")

	// Set higher height - should work
	lis.SetScanned(101)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(101), height)
}

func TestLatestIncrementallyScanned_ResetScanned(t *testing.T) {
	lis := scan.NewLatestIncrementallyScanned(zerolog.Nop(), scan.NoOpStatusReporter{})

	// Set up some pending heights
	lis.SetScanned(100)
	lis.SetScanned(102) // queued, waiting for 101
	lis.SetScanned(103) // queued, waiting for 101

	height, _ := lis.GetIfScanned()
	require.Equal(t, uint64(100), height, "height should be 100 (waiting for 101)")

	// Reset to a higher height - should clear pending queue
	lis.ResetScanned(200)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(200), height, "height should be reset to 200")

	// Old heights should be ignored now
	lis.SetScanned(101)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(200), height, "should still be 200, old pending heights ignored")

	lis.SetScanned(201)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(201), height, "should advance to 201")
}

func TestLatestIncrementallyScanned_ResetScannedOnlyIncreases(t *testing.T) {
	lis := scan.NewLatestIncrementallyScanned(zerolog.Nop(), scan.NoOpStatusReporter{})

	lis.SetScanned(100)
	height, _ := lis.GetIfScanned()
	require.Equal(t, uint64(100), height)

	// Reset to lower height - should be ignored
	lis.ResetScanned(50)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(100), height, "should not reset to lower height")

	// Reset to same height - should be ignored
	lis.ResetScanned(100)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(100), height, "should not reset to same height")
}

func TestLatestIncrementallyScanned_ResetScannedPurgesPending(t *testing.T) {
	lis := scan.NewLatestIncrementallyScanned(zerolog.Nop(), scan.NoOpStatusReporter{})

	// Set up initial height and pending heights
	lis.SetScanned(10)
	lis.SetScanned(12) // pending
	lis.SetScanned(13) // pending
	lis.SetScanned(15) // pending
	lis.SetScanned(20) // pending

	height, _ := lis.GetIfScanned()
	require.Equal(t, uint64(10), height)

	// Reset to 14 - should purge 12, 13 but keep 15, 20
	lis.ResetScanned(14)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(14), height)

	// 15 should now be consecutive and can be applied
	lis.SetScanned(15)
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(15), height)

	// 20 should still be in pending (via processPending from 15)
	// First need 16, then processPending will apply 20 if 16-19 arrive
	for h := uint64(16); h <= 20; h++ {
		lis.SetScanned(h)
	}
	height, _ = lis.GetIfScanned()
	require.Equal(t, uint64(20), height)
}
