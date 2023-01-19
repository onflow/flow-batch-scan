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

package lib

import (
	"sync"

	"github.com/rs/zerolog"
)

type Component interface {
	Err() error
	Done() <-chan struct{}
	Started() <-chan struct{}
}

type ComponentBase struct {
	doneChan    chan struct{}
	startedChan chan struct{}

	startedOnce sync.Once
	doneOnce    sync.Once
	doneErr     error

	Logger zerolog.Logger
}

func NewComponent(name string, logger zerolog.Logger) *ComponentBase {
	return &ComponentBase{
		doneChan:    make(chan struct{}, 1),
		startedChan: make(chan struct{}, 1),

		Logger: logger.With().Str("component", name).Logger(),
	}
}

func (c *ComponentBase) Done() <-chan struct{} {
	return c.doneChan
}

func (c *ComponentBase) Err() error {
	return c.doneErr
}

func (c *ComponentBase) Started() <-chan struct{} {
	return c.startedChan
}

func (c *ComponentBase) StartupDone() {
	c.startedOnce.Do(func() {
		c.startedChan <- struct{}{}
		close(c.startedChan)
		c.Logger.Info().Msg("Started")
	})
}

func (c *ComponentBase) Finish(err error) {
	c.doneOnce.Do(func() {
		c.doneErr = err
		c.doneChan <- struct{}{}
		close(c.doneChan)
		c.Logger.Info().Msg("Stopped")
	})
}
