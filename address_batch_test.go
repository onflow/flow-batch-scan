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
	"testing"
	"time"

	"github.com/onflow/flow-go-sdk"
	"github.com/stretchr/testify/require"
)

func TestAddressBatch_DoneHandling(t *testing.T) {
	t.Run("doneHandling can be nil", func(t *testing.T) {
		var doneHandling func()

		b := NewAddressBatch(
			nil,
			0,
			doneHandling,
			nil,
		)
		require.NotPanics(t, b.DoneHandling)
	})
	t.Run("doneHandling is called once", func(t *testing.T) {
		calls := 0
		doneHandling := func() {
			calls++
		}

		b := NewAddressBatch(
			nil,
			0,
			doneHandling,
			nil,
		)
		b.DoneHandling()
		b.DoneHandling()
		require.Equal(t, 1, calls)
	})
}

func TestAddressBatch_ExcludeAddress(t *testing.T) {
	a1, a2, a3 :=
		flow.HexToAddress("0x1"),
		flow.HexToAddress("0x2"),
		flow.HexToAddress("0x3")

	t.Run("ExcludeAddress does nothing if address is not present", func(t *testing.T) {
		b := NewAddressBatch(
			[]flow.Address{a1, a2},
			0,
			nil,
			nil,
		)
		b.ExcludeAddress(a3)
		require.Len(t, b.Addresses, 2)
	})

	t.Run("ExcludeAddress excludes the address from the list", func(t *testing.T) {
		b := NewAddressBatch(
			[]flow.Address{a1, a2},
			0,
			nil,
			nil,
		)
		b.ExcludeAddress(a1)
		require.Len(t, b.Addresses, 1)
	})
}

func TestAddressBatch_IsValid(t *testing.T) {
	t.Run("if isValid is nil, batch is always valid", func(t *testing.T) {
		var isValid func() bool

		b := NewAddressBatch(
			nil,
			0,
			nil,
			isValid,
		)
		require.NotPanics(t, b.DoneHandling)
	})
	t.Run("isValid can be called multiple times", func(t *testing.T) {
		calls := 0
		isValid := func() bool {
			calls++
			return true
		}

		b := NewAddressBatch(
			nil,
			0,
			nil,
			isValid,
		)
		b.IsValid()
		b.IsValid()
		require.Equal(t, 2, calls)
	})

	t.Run("isValid false calls is done", func(t *testing.T) {
		doneCalls := 0
		isDone := func() {
			doneCalls++
		}
		validCalls := 0
		isValid := func() bool {
			validCalls++
			return false
		}

		b := NewAddressBatch(
			nil,
			0,
			isDone,
			isValid,
		)
		b.IsValid()
		require.Equal(t, 1, validCalls)
		require.Equal(t, 1, doneCalls)
	})
}

func TestAddressBatch_Split(t *testing.T) {
	a1, a2, a3 :=
		flow.HexToAddress("0x1"),
		flow.HexToAddress("0x2"),
		flow.HexToAddress("0x3")

	doneCalls := 0
	isDone := func() {
		doneCalls++
	}
	validCalls := 0
	isValid := func() bool {
		validCalls++
		return true
	}

	b := NewAddressBatch(
		[]flow.Address{a1, a2, a3},
		10,
		isDone,
		isValid,
	)
	b1, b2 := b.Split()

	require.Equal(t, b.BlockHeight, b1.BlockHeight)
	require.Equal(t, b.BlockHeight, b2.BlockHeight)

	require.Equal(t, b.Addresses[:1], b1.Addresses)
	require.Equal(t, b.Addresses[1:], b2.Addresses)

	require.True(t, b.IsValid())
	require.True(t, b1.IsValid())
	require.True(t, b2.IsValid())

	require.Equal(t, 3, validCalls)

	b1.DoneHandling()
	require.Equal(t, 0, doneCalls)

	b2.DoneHandling()
	<-time.After(1 * time.Millisecond) // wait for doneHandling to be called
	require.Equal(t, 1, doneCalls)
}
