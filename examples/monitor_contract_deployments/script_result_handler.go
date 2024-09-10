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
	Contracts   int64
	BlockHeight uint64
}

type scriptResultHandler struct {
	mu *sync.RWMutex

	deployedContracts map[flow.Address]Record
	reporter          *Reporter

	totalDeployedContractCount int64

	logger zerolog.Logger
}

func NewScriptResultHandler(reporter *Reporter, logger zerolog.Logger) fbs.ScriptResultHandler {
	h := &scriptResultHandler{
		mu:                &sync.RWMutex{},
		deployedContracts: map[flow.Address]Record{},

		reporter: reporter,
		logger:   logger,
	}
	return h
}

func (r *scriptResultHandler) Handle(batch fbs.ProcessedAddressBatch) error {
	addressContracts := Parse(batch.Result)

	addressesWithNoContracts := make(map[flow.Address]struct{}, len(batch.Addresses))
	for _, address := range batch.Addresses {
		if _, ok := addressContracts[address]; ok {
			continue
		}
		addressesWithNoContracts[address] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	changeInContractCount := int64(0)

	for address, contracts := range addressContracts {
		// If the record we have saved is from a newer block, don't update it.
		// This happens if the account is scanned by the incremental scan, before it is scanned by the full scan.
		if record, ok := r.deployedContracts[address]; ok {
			if record.BlockHeight >= batch.BlockHeight {
				continue
			}
			changeInContractCount -= record.Contracts
		}
		changeInContractCount += contracts

		r.deployedContracts[address] = Record{
			Contracts:   contracts,
			BlockHeight: batch.BlockHeight,
		}
	}

	// for each address that doesn't have contracts
	for address := range addressesWithNoContracts {
		// If the record we have saved is from a newer block, don't update it.
		if record, ok := r.deployedContracts[address]; ok {
			if record.BlockHeight >= batch.BlockHeight {
				continue
			}
			changeInContractCount -= record.Contracts
		}
		delete(r.deployedContracts, address)
	}
	r.totalDeployedContractCount += changeInContractCount
	r.reporter.ReportContractsDeployed(r.totalDeployedContractCount)
	r.logger.Info().
		Int64("contracts_deployed_change", changeInContractCount).
		Int64("total_contracts_deployed", r.totalDeployedContractCount).
		Msg("Batch Handled")

	return nil
}

// Parse parses the script result which is the AccountInfo in get_contract_deployed.cdc struct in this case.
func Parse(values cadence.Value) map[flow.Address]int64 {
	result := make(map[flow.Address]int64)
	for _, value := range values.(cadence.Array).Values {
		s := value.(cadence.Struct)
		address := flow.BytesToAddress(s.Fields[0].(cadence.Address).Bytes())
		value := s.Fields[1].(cadence.Int).Int()
		result[address] = int64(value)
	}
	return result
}
