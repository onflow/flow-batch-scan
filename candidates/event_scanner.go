package candidates

import (
	"context"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
	flowgrpc "github.com/onflow/flow-go-sdk/access/grpc"
	"github.com/rs/zerolog"

	"github.com/onflow/flow-batch-scan/client"
)

type EventCandidatesScanner struct {
	eventType                 string
	candidateAddressFromEvent func(event cadence.Event) (flow.Address, error)

	logger zerolog.Logger
}

func NewEventCandidatesScanner(
	eventType string,
	candidateAddressFromEvent func(event cadence.Event) (flow.Address, error),
	logger zerolog.Logger,
) *EventCandidatesScanner {
	return &EventCandidatesScanner{
		eventType:                 eventType,
		candidateAddressFromEvent: candidateAddressFromEvent,

		logger: logger.With().Str("component", "authorizer_candidates_scanner").Logger(),
	}
}

var _ CandidateScanner = (*EventCandidatesScanner)(nil)

func (s *EventCandidatesScanner) Scan(
	ctx context.Context,
	client client.Client,
	blocks BlockRange,
) CandidatesResult {
	l := s.logger.With().
		Uint64("start", blocks.Start).
		Uint64("end", blocks.End).
		Logger()

	blockEvents, err := client.GetEventsForHeightRange(ctx, flowgrpc.EventRangeQuery{
		Type:        s.eventType,
		StartHeight: blocks.Start,
		EndHeight:   blocks.End,
	})
	if err != nil {
		l.Error().
			Err(err).
			Str("event_type", s.eventType).
			Msg("could not get events")
		return NewCandidatesResultError(err)
	}
	addresses := make(map[flow.Address]struct{})
	for _, events := range blockEvents {
		for _, event := range events.Events {
			address, err := s.candidateAddressFromEvent(event.Value)
			if err != nil {
				l.Error().
					Err(err).
					Uint64("block_height", events.Height).
					Str("event_type", s.eventType).
					Str("event", event.String()).
					Msg("could not get candidate address from event")
				return NewCandidatesResultError(err)
			}
			addresses[address] = struct{}{}
		}
	}
	l.Debug().
		Int("count", len(addresses)).
		Str("event_type", s.eventType).
		Msg("Found event candidates")

	return NewCandidatesResult(addresses)
}
