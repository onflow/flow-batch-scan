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
