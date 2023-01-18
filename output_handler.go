package lib

import (
	"context"

	"github.com/rs/zerolog"
)

type ScriptResultProcessor struct {
	*ComponentBase

	scriptResultsChan <-chan ProcessedAddressBatch

	handler ScriptResultHandler
}

var _ Component = (*ScriptResultProcessor)(nil)

func NewScriptResultProcessor(
	ctx context.Context,
	outChan <-chan ProcessedAddressBatch,
	handler ScriptResultHandler,
	logger zerolog.Logger,
) *ScriptResultProcessor {
	r := &ScriptResultProcessor{
		ComponentBase: NewComponent("script_result_processor", logger),

		scriptResultsChan: outChan,

		handler: handler,
	}

	go r.start(ctx)
	r.StartupDone()
	return r
}

func (r *ScriptResultProcessor) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			r.Finish(ctx.Err())
			return
		case result, ok := <-r.scriptResultsChan:
			if !ok {
				r.Finish(nil)
				return
			}
			if !result.IsValid() {
				continue
			}
			go func(result ProcessedAddressBatch) {
				err := r.handler.Handle(result)
				result.DoneHandling()
				if err != nil {
					r.Finish(err)
				}
			}(result)
		}
	}
}

type ScriptResultHandler interface {
	Handle(batch ProcessedAddressBatch) error
}
