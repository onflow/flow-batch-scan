package candidates

import (
	"context"
	"strings"
	"time"

	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog"

	"github.com/onflow/flow-batch-scan/client"
)

type AuthorizerCandidatesScanner struct {
	logger zerolog.Logger
}

func NewAuthorizerCandidatesScanner(logger zerolog.Logger) AuthorizerCandidatesScanner {
	return AuthorizerCandidatesScanner{
		logger: logger.With().Str("component", "authorizer_candidates_scanner").Logger(),
	}
}

var _ CandidateScanner = AuthorizerCandidatesScanner{}

func (s AuthorizerCandidatesScanner) Scan(
	ctx context.Context,
	client client.Client,
	blocks BlockRange,
) CandidatesResult {
	candidatesChan := make(chan CandidatesResult, blocks.End-blocks.Start+1)
	defer close(candidatesChan)

	blockHeight := blocks.Start
	for blockHeight <= blocks.End {
		go func(blockHeight uint64) {
			candidatesChan <- s.scanBlock(ctx, client, blockHeight)
		}(blockHeight)
		blockHeight++
	}

	candidates := WaitForCandidateResults(candidatesChan, int(blocks.End-blocks.Start+1))

	if candidates.Err() != nil {
		return candidates
	}

	s.logger.
		Debug().
		Int("count", len(candidates.Addresses)).
		Uint64("start", blocks.Start).
		Uint64("end", blocks.End).
		Msg("Found authorizer candidates")

	return candidates
}

func (s AuthorizerCandidatesScanner) scanBlock(
	ctx context.Context,
	client client.Client,
	blockHeight uint64,
) CandidatesResult {
	s.logger.
		Debug().
		Uint64("block_height", blockHeight).
		Msg("getting authorizers for block")

	block, err := client.GetBlockByHeight(ctx, blockHeight)
	if err != nil {
		s.logger.Error().
			Err(err).
			Uint64("block_height", blockHeight).
			Msg("Could not get block by height.")
		return NewCandidatesResultError(err)
	}

	candidatesChan := make(chan CandidatesResult, len(block.CollectionGuarantees))
	defer close(candidatesChan)

	for _, guarantee := range block.CollectionGuarantees {
		go func(guarantee *flow.CollectionGuarantee) {
			candidatesChan <- s.scanCollection(ctx, client, guarantee.CollectionID)
		}(guarantee)
	}

	return WaitForCandidateResults(candidatesChan, len(block.CollectionGuarantees))
}

func (s AuthorizerCandidatesScanner) scanCollection(
	ctx context.Context,
	client client.Client,
	collectionID flow.Identifier,
) CandidatesResult {
	coll, err := s.GetCollection(client, ctx, collectionID)
	if err != nil {
		if err != context.Canceled {
			s.logger.Error().
				Err(err).
				Str("collection_id", collectionID.Hex()).
				Msg("could not get collection")
		}

		return NewCandidatesResultError(err)
	}

	candidatesChan := make(chan CandidatesResult, len(coll.TransactionIDs))
	defer close(candidatesChan)

	for _, transactionID := range coll.TransactionIDs {
		go func(transactionID flow.Identifier) {
			candidatesChan <- s.scanTransaction(ctx, client, transactionID)
		}(transactionID)
	}

	return WaitForCandidateResults(candidatesChan, len(coll.TransactionIDs))
}

func (s AuthorizerCandidatesScanner) scanTransaction(
	ctx context.Context,
	client client.Client,
	TransactionId flow.Identifier,
) CandidatesResult {
	tx, err := client.GetTransaction(ctx, TransactionId)
	if err != nil {
		s.logger.Error().Err(err).Msg("could not get transaction")
		return NewCandidatesResultError(err)
	}
	addresses := make(map[flow.Address]struct{}, len(tx.Authorizers))
	for _, authorizer := range tx.Authorizers {
		addresses[authorizer] = struct{}{}
	}
	return NewCandidatesResult(addresses)
}

func (s AuthorizerCandidatesScanner) GetCollection(
	client client.Client,
	ctx context.Context,
	id flow.Identifier,
) (coll *flow.Collection, err error) {
	for {
		coll, err = client.GetCollection(ctx, id)
		if err != nil {
			// TODO: move this into global retry policy
			if strings.Contains(err.Error(), "retry for collection in finalized block") {
				s.logger.Debug().Err(err).Msg("retrying collection")
				<-time.After(500 * time.Millisecond) // they won't be immediately available
				continue
			}
		}
		return
	}
}
