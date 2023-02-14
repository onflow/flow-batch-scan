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
	"sync"

	"github.com/rs/zerolog"
)

type Component interface {
	Err() error
	Done() <-chan struct{}
	Start(ctx context.Context) <-chan struct{}
}

var _ Component = (*ComponentBase)(nil)

type ComponentBase struct {
	start       func(ctx context.Context)
	startedChan chan struct{}
	startOnce   sync.Once

	doneChan chan struct{}
	doneOnce sync.Once
	doneErr  error

	Logger zerolog.Logger
}

func NewComponentWithStart(
	name string,
	start func(ctx context.Context),
	logger zerolog.Logger,
) *ComponentBase {
	return &ComponentBase{
		doneChan:    make(chan struct{}, 1),
		startedChan: make(chan struct{}, 1),

		start: start,

		Logger: logger.With().Str("component", name).Logger(),
	}
}

func (c *ComponentBase) Start(ctx context.Context) <-chan struct{} {
	c.startOnce.Do(func() {
		go func() {
			defer close(c.startedChan)
			go c.start(ctx)
			c.start = nil
			c.Logger.Info().Msg("Started")
		}()
	})

	return c.startedChan
}

func (c *ComponentBase) Done() <-chan struct{} {
	return c.doneChan
}

func (c *ComponentBase) Err() error {
	return c.doneErr
}

func (c *ComponentBase) Finish(err error) {
	c.doneOnce.Do(func() {
		c.doneErr = err
		c.doneChan <- struct{}{}
		close(c.doneChan)
		c.Logger.Info().Msg("Stopped")
	})
}
