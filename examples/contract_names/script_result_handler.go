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

	duplicatedContracts map[flow.Address]Record
	logger              zerolog.Logger
}

func NewScriptResultHandler(
	logger zerolog.Logger,
) fbs.ScriptResultHandler {
	h := &scriptResultHandler{
		mu:                  &sync.RWMutex{},
		duplicatedContracts: map[flow.Address]Record{},

		logger: logger,
	}
	return h
}

func (r *scriptResultHandler) Handle(out fbs.ProcessedAddressBatch) error {
	addressContracts := Parse(out.Result)
	okAddresses := make(map[flow.Address]struct{}, len(out.Addresses))
	for _, address := range out.Addresses {
		if _, ok := addressContracts[address]; ok {
			continue
		}
		okAddresses[address] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for address, contractNames := range addressContracts {
		if record, ok := r.duplicatedContracts[address]; ok && record.BlockHeight >= out.BlockHeight {
			continue
		}
		r.duplicatedContracts[address] = Record{
			Contracts:   contractNames,
			BlockHeight: out.BlockHeight,
		}
		r.logger.Info().
			Str("address", address.String()).
			Strs("contract_names", contractNames).
			Msg("Address contracts")
	}

	for address, _ := range okAddresses {
		if record, ok := r.duplicatedContracts[address]; ok && record.BlockHeight >= out.BlockHeight {
			continue
		}
		delete(r.duplicatedContracts, address)
	}

	return nil
}

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
