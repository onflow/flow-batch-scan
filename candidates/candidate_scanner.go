package candidates

import (
	"context"

	"github.com/hashicorp/go-multierror"

	"github.com/onflow/flow-batch-scan/utils"

	"github.com/onflow/flow-go-sdk"

	"github.com/onflow/flow-batch-scan/client"
)

type BlockRange struct {
	Start uint64
	End   uint64 // inclusive
}

type CandidatesResult struct {
	Addresses map[flow.Address]struct{}
	err       error
}

func NewCandidatesResult(addresses map[flow.Address]struct{}) CandidatesResult {
	return CandidatesResult{
		Addresses: addresses,
	}
}

func NewCandidatesResultError(err error) CandidatesResult {
	return CandidatesResult{
		err: err,
	}
}

func (r *CandidatesResult) MergeWith(r2 CandidatesResult) {
	utils.MergeInto(r.Addresses, r2.Addresses)
	r.err = multierror.Append(r.err, r2.err)
}

func (r *CandidatesResult) Err() error {
	if merr, ok := r.err.(*multierror.Error); ok {
		return merr.ErrorOrNil()
	}
	return r.err
}

type CandidateScanner interface {
	Scan(ctx context.Context, client client.Client, blocks BlockRange) CandidatesResult
}

func WaitForCandidateResults(
	candidatesChan <-chan CandidatesResult,
	expectedResults int,
) CandidatesResult {
	results := 0
	result := CandidatesResult{}
	if expectedResults == 0 {
		return result
	}
	for candidates := range candidatesChan {
		result.MergeWith(candidates)
		results++
		if results == expectedResults {
			break
		}
	}
	return result
}
