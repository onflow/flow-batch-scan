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
	outChan <-chan ProcessedAddressBatch,
	handler ScriptResultHandler,
	logger zerolog.Logger,
) *ScriptResultProcessor {
	r := &ScriptResultProcessor{

		scriptResultsChan: outChan,

		handler: handler,
	}
	r.ComponentBase = NewComponentWithStart("script_result_processor", r.start, logger)

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
	// Handle will be called concurrently for each ProcessedAddressBatch.
	Handle(batch ProcessedAddressBatch) error
}
