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
	"strings"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog"

	"github.com/onflow/flow-batch-scan/client"
)

type AddressProviderConfig struct {
	ExcludeAddress func(id flow.ChainID, address flow.Address) bool
}

// These Addresses are known to be broken on Mainnet
var brokenAddresses = map[flow.ChainID]map[flow.Address]struct{}{
	flow.Mainnet: {
		flow.HexToAddress("bf48a20670f179b8"): {},
		flow.HexToAddress("5eba0297874a2bfd"): {},
		flow.HexToAddress("474ec037bcd8accf"): {},
		flow.HexToAddress("b0e80595d267f4eb"): {},
	},
}

func DefaultExcludeAddress(id flow.ChainID, address flow.Address) bool {
	_, ok := brokenAddresses[id][address]
	return ok
}

func DefaultAddressProviderConfig() AddressProviderConfig {
	return AddressProviderConfig{
		ExcludeAddress: DefaultExcludeAddress,
	}
}

// AddressProvider Is used to get all the addresses that exists at a certain referenceBlockId
// this relies on the fact that a certain `endOfAccountsError` will be returned by the
// `accountStorageUsageScript` if the address doesn't exist yet
type AddressProvider struct {
	log              zerolog.Logger
	lastAddress      flow.Address
	generator        *flow.AddressGenerator
	lastAddressIndex uint
	blockHeight      uint64
	currentIndex     uint
	chainID          flow.ChainID
	config           AddressProviderConfig
}

const endOfAccountsError = "get storage used failed"

const accountStorageUsageScript = `
	access(all) fun main(Address: Address): UInt64 {
	  return getAccount(Address).storage.used
	}
`

// InitAddressProvider uses bisection to get the last existing address.
func InitAddressProvider(
	ctx context.Context,
	chain flow.ChainID,
	blockHeight uint64,
	client client.Client,
	config AddressProviderConfig,
	log zerolog.Logger,
) (*AddressProvider, error) {
	ap := &AddressProvider{
		log:          log.With().Str("component", "address_provider").Logger(),
		generator:    flow.NewAddressGenerator(chain),
		blockHeight:  blockHeight,
		currentIndex: 1,
		chainID:      chain,
		config:       config,
	}

	searchStep := 0
	addressExistsAtIndex := func(index uint) (bool, error) {
		searchStep += 1
		address := ap.indexToAddress(index)

		log.Debug().Msgf("testing account %d = %s", index, address)

		// This script will fail with endOfAccountsError
		// if the account (address at given index) doesn't exist yet
		_, err := client.ExecuteScriptAtBlockHeight(
			ctx,
			blockHeight,
			[]byte(accountStorageUsageScript),
			[]cadence.Value{cadence.NewAddress(address)},
		)
		if err == nil {
			return true, nil
		}
		if strings.Contains(err.Error(), endOfAccountsError) {
			return false, nil
		}
		return false, err
	}

	// We assume address #2 exists
	lastAddressIndex, err := ap.getLastAddress(1, 2, true, addressExistsAtIndex)
	if err != nil {
		return nil, err
	}

	log.Info().
		Str("lastAddress", ap.indexToAddress(lastAddressIndex).Hex()).
		Uint("numAccounts", lastAddressIndex).
		Int("stepsNeeded", searchStep).
		Msg("Found last address")

	ap.lastAddress = ap.indexToAddress(lastAddressIndex)
	ap.lastAddressIndex = lastAddressIndex
	return ap, nil
}

// getLastAddress is a recursive function that finds the last address. Will use max 2 * log2(number_of_addresses) steps
// If the last address is at index 7 the algorithm goes like this:
// (x,y) <- lower and upper index
// 0. start at (1,2); address exists at 2
// 1. (2,4): address exists at 4
// 2. (4,8): address doesnt exist at 8
// 3. (4,8): check address (8 - 4) / 2 = 6  address exists so next pair is (6,8)
// 4. (6,8): check address 7 address exists so next pair is (7,8)
// 5. (7,8): check address (8 - 7) / 2 = 7 ... ok already checked so this is the last existing address
func (p *AddressProvider) getLastAddress(
	lowerIndex uint,
	upperIndex uint,
	upperExists bool,
	addressExistsAtIndex func(uint) (bool, error),
) (uint, error) {
	// Does the address exist at upper bound?
	if upperExists {
		// double the upper bound, the current upper bound is now the lower upper bound
		newUpperIndex := upperIndex * 2
		newUpperExists, err := addressExistsAtIndex(newUpperIndex)
		if err != nil {
			return 0, err
		}
		return p.getLastAddress(upperIndex, newUpperIndex, newUpperExists, addressExistsAtIndex)
	}

	midIndex := (upperIndex-lowerIndex)/2 + lowerIndex
	if midIndex == lowerIndex {
		// we found the last address
		return midIndex, nil
	}

	// Check if the address exists in the middle of the interval.
	// If yes, then take the (mid, upper) as the next pair,
	// else take (lower, mid) as the next pair
	midIndexExists, err := addressExistsAtIndex(midIndex)
	if err != nil {
		return 0, err
	}
	if midIndexExists {
		return p.getLastAddress(midIndex, upperIndex, upperExists, addressExistsAtIndex)
	} else {
		return p.getLastAddress(lowerIndex, midIndex, midIndexExists, addressExistsAtIndex)
	}
}

func (p *AddressProvider) indexToAddress(index uint) flow.Address {
	p.generator.SetIndex(index)
	return p.generator.Address()
}

func (p *AddressProvider) GetNextAddress() (address flow.Address, isOutOfBounds bool) {
	address = p.indexToAddress(p.currentIndex)

	if p.currentIndex > p.lastAddressIndex {
		isOutOfBounds = true
	}
	p.currentIndex += 1

	return
}

func (p *AddressProvider) AddressesLen() uint {
	return p.lastAddressIndex - uint(len(brokenAddresses[p.chainID]))
}

func (p *AddressProvider) GenerateAddressBatches(addressChan chan<- []flow.Address, batchSize int) {
	var done bool
	for !done {
		addresses := make([]flow.Address, 0)

		for i := 0; i < batchSize; i++ {
			addr, oob := p.GetNextAddress()
			if oob {
				// Out of bounds, there are no more addresses
				done = true
				break
			}

			// Skip address if known broken
			if p.config.ExcludeAddress(p.chainID, addr) {
				i--
				continue
			}
			addresses = append(addresses, addr)
		}

		if len(addresses) > 0 {
			addressChan <- addresses
		}
	}
}

func (p *AddressProvider) LastAddress() flow.Address {
	return p.lastAddress
}
