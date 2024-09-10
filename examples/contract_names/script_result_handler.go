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

package main

import (
	"github.com/onflow/cadence"
	fbs "github.com/onflow/flow-batch-scan"
	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog"
	"sync"
)

type Record struct {
	Contracts   []string
	BlockHeight uint64
}

type scriptResultHandler struct {
	mu *sync.RWMutex

	deployedContracts map[flow.Address]Record
	logger            zerolog.Logger
}

// NewScriptResultHandler is a simple result handler that prints the results to the log.
func NewScriptResultHandler(
	logger zerolog.Logger,
) fbs.ScriptResultHandler {
	h := &scriptResultHandler{
		mu:                &sync.RWMutex{},
		deployedContracts: map[flow.Address]Record{},

		logger: logger,
	}
	return h
}

func (r *scriptResultHandler) Handle(batch fbs.ProcessedAddressBatch) error {
	// parse the script result
	addressContracts := Parse(batch.Result)

	// the script returns only addresses that have contracts.
	// the fbs.ProcessedAddressBatch contains all addresses that were processed.
	// so we can get a list of addresses that don't have contracts.
	addressesWithNoContracts := make(map[flow.Address]struct{}, len(batch.Addresses))
	for _, address := range batch.Addresses {
		if _, ok := addressContracts[address]; ok {
			continue
		}
		addressesWithNoContracts[address] = struct{}{}
	}

	// this will be called for each address that has contracts.
	// so we need to put a lock on it (or a sync.Map, or use channels, ...)
	r.mu.Lock()
	defer r.mu.Unlock()

	// for each address that has contracts
	for address, contractNames := range addressContracts {

		// If the record we have saved is from a newer block, don't update it.
		// This happens if the account is scanned by the incremental scan, before it is scanned by the full scan.
		if record, ok := r.deployedContracts[address]; ok && record.BlockHeight >= batch.BlockHeight {
			continue
		}

		r.deployedContracts[address] = Record{
			Contracts:   contractNames,
			BlockHeight: batch.BlockHeight,
		}

		// Some output to the log, so we can see the results.
		r.logger.Info().
			Str("address", address.String()).
			Strs("contract_names", contractNames).
			Msg("Address contracts")
	}

	// for each address that doesn't have contracts
	for address := range addressesWithNoContracts {
		// If the record we have saved is from a newer block, don't update it.
		if record, ok := r.deployedContracts[address]; ok && record.BlockHeight >= batch.BlockHeight {
			continue
		}
		delete(r.deployedContracts, address)
	}

	return nil
}

// Parse parses the script result which is the AccountInfo in get_contract_deployed.cdc struct in this case.
func Parse(values cadence.Value) map[flow.Address][]string {
	result := make(map[flow.Address][]string)
	for _, value := range values.(cadence.Array).Values {
		s := value.(cadence.Struct)
		address := flow.BytesToAddress(s.Fields[0].(cadence.Address).Bytes())
		var contractNames []string
		for _, name := range s.Fields[1].(cadence.Array).Values {
			contractNames = append(contractNames, name.(cadence.String).ToGoValue().(string))
		}
		result[address] = contractNames
	}
	return result
}
