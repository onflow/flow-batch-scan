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
	"sync"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
)

// AddressBatch is a batch of addresses that will be the input to the script being run byt the script runner
// at the given block height.
type AddressBatch struct {
	Addresses    []flow.Address
	BlockHeight  uint64
	doneHandling func()
	isCancelled  func() bool

	doneOnce *sync.Once
}

// ProcessedAddressBatch contains the result of running the script on the given batch of addresses.
type ProcessedAddressBatch struct {
	AddressBatch
	Result cadence.Value
}

func NewAddressBatch(
	addresses []flow.Address,
	blockHeight uint64,
	doneHandling func(),
	isCancelled func() bool,
) AddressBatch {
	return AddressBatch{
		Addresses:    addresses,
		BlockHeight:  blockHeight,
		doneHandling: doneHandling,
		isCancelled:  isCancelled,

		doneOnce: &sync.Once{},
	}
}

// IsValid if the batch is cancelled, it should not be processed.
func (b *AddressBatch) IsValid() bool {
	if b.isCancelled != nil && b.isCancelled() {
		b.DoneHandling()
		return false
	}
	return true
}

// DoneHandling should be called when the batch has been processed.
func (b *AddressBatch) DoneHandling() {
	b.doneOnce.Do(func() {
		if b.doneHandling != nil {
			b.doneHandling()
		}
	})
}

func (b *AddressBatch) ExcludeAddress(address flow.Address) {
	for i, a := range b.Addresses {
		if a == address {
			b.Addresses = append(b.Addresses[:i], b.Addresses[i+1:]...)
			return
		}
	}
}
