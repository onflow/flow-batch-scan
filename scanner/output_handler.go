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
	"github.com/onflow/flow-go/module/component"
	"github.com/onflow/flow-go/module/irrecoverable"
	"github.com/rs/zerolog"
)

type ScriptResultProcessor struct {
	component.Component

	resultsChan chan ProcessedAddressBatch
	handler     ScriptResultHandler
	log         zerolog.Logger
}

func NewScriptResultProcessor(
	logger zerolog.Logger,
	handler ScriptResultHandler,
) *ScriptResultProcessor {
	r := &ScriptResultProcessor{
		resultsChan: make(chan ProcessedAddressBatch, 10000),
		handler:     handler,
		log:         logger.With().Str("component", "script_result_processor").Logger(),
	}

	r.Component = component.NewComponentManagerBuilder().
		AddWorker(r.processWorker).
		Build()

	return r
}

// Submit queues a processed batch for handling.
func (r *ScriptResultProcessor) Submit(result ProcessedAddressBatch) {
	r.resultsChan <- result
}

func (r *ScriptResultProcessor) processWorker(ctx irrecoverable.SignalerContext, ready component.ReadyFunc) {
	ready()

	for {
		select {
		case <-ctx.Done():
			return
		case result, ok := <-r.resultsChan:
			if !ok {
				return
			}
			if !result.IsValid() {
				continue
			}
			go func(result ProcessedAddressBatch) {
				err := r.handler.Handle(result)
				result.DoneHandling()
				if err != nil {
					ctx.Throw(err)
				}
			}(result)
		}
	}
}

type ScriptResultHandler interface {
	// Handle will be called concurrently for each ProcessedAddressBatch.
	Handle(batch ProcessedAddressBatch) error
}
