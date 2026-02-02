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

package engine

import (
	"context"
	"fmt"

	"github.com/onflow/flow-go/module"
	"github.com/onflow/flow-go/module/component"
	"github.com/onflow/flow-go/module/irrecoverable"
	"github.com/rs/zerolog"
)

// BuilderFunc is a startup/initialization function that runs before components are created.
type BuilderFunc[Config any] func(config Config) error

// ComponentFactory creates a component from config.
// The returned component must implement module.ReadyDoneAware.
type ComponentFactory[Config any] func(config Config) (module.ReadyDoneAware, error)

// NamedComponentFactory pairs a factory with its name for logging.
type NamedComponentFactory[Config any] struct {
	Name    string
	Factory ComponentFactory[Config]
}

// BuilderBase provides generic application composition using the builder pattern.
// It manages component lifecycle, startup functions, and shutdown functions.
type BuilderBase[Config any] struct {
	Log zerolog.Logger

	components  []NamedComponentFactory[Config]
	startupFns  []BuilderFunc[Config]
	shutdownFns []func() error
}

// NewBuilderBase creates a new builder base with the given logger.
func NewBuilderBase[Config any](logger zerolog.Logger) *BuilderBase[Config] {
	return &BuilderBase[Config]{
		Log: logger,
	}
}

// Component registers a component factory to be created during Build.
// Components are created in the order they are registered.
func (b *BuilderBase[Config]) Component(name string, factory ComponentFactory[Config]) *BuilderBase[Config] {
	b.components = append(b.components, NamedComponentFactory[Config]{
		Name:    name,
		Factory: factory,
	})
	return b
}

// StartupFunc registers an initialization function that runs before components are created.
// Startup functions run in the order they are registered.
func (b *BuilderBase[Config]) StartupFunc(fn BuilderFunc[Config]) *BuilderBase[Config] {
	b.startupFns = append(b.startupFns, fn)
	return b
}

// ShutdownFunc registers a cleanup function that runs after all components stop.
// Shutdown functions run in the order they are registered.
func (b *BuilderBase[Config]) ShutdownFunc(fn func() error) *BuilderBase[Config] {
	b.shutdownFns = append(b.shutdownFns, fn)
	return b
}

// Build creates the runnable engine by:
// 1. Running all startup functions
// 2. Creating all components via their factories
// 3. Wrapping components in a ComponentManager
func (b *BuilderBase[Config]) Build(config Config) (*Engine[Config], error) {
	// Run startup functions
	for _, fn := range b.startupFns {
		if err := fn(config); err != nil {
			return nil, fmt.Errorf("startup function failed: %w", err)
		}
	}

	// Create component manager builder
	cmb := component.NewComponentManagerBuilder()

	// Create components and add as workers
	for _, nc := range b.components {
		comp, err := nc.Factory(config)
		if err != nil {
			return nil, fmt.Errorf("creating component %s: %w", nc.Name, err)
		}

		// Skip nil components (allows conditional component registration)
		if comp == nil {
			continue
		}

		name := nc.Name
		log := b.Log.With().Str("component", name).Logger()

		// Wrap each component as a worker that:
		// 1. Starts the component
		// 2. Waits for it to be ready, then signals ready
		// 3. Waits for it to be done
		cmb.AddWorker(func(ctx irrecoverable.SignalerContext, ready component.ReadyFunc) {
			log.Debug().Msg("Starting component")

			// Start the component if it implements Startable
			if startable, ok := comp.(module.Startable); ok {
				startable.Start(ctx)
			}

			// Wait for component to be ready
			<-comp.Ready()
			log.Debug().Msg("Component ready")
			ready()

			// Wait for component to be done
			<-comp.Done()
			log.Debug().Msg("Component done")
		})
	}

	return &Engine[Config]{
		Config:      config,
		component:   cmb.Build(),
		shutdownFns: b.shutdownFns,
		log:         b.Log,
	}, nil
}

// Engine wraps the component manager with shutdown handling.
type Engine[Config any] struct {
	Config      Config
	component   *component.ComponentManager
	shutdownFns []func() error
	log         zerolog.Logger
}

// Run starts the engine and blocks until completion or error.
// It handles graceful shutdown when the context is cancelled.
func (e *Engine[Config]) Run(ctx context.Context) error {
	// Create signaler context for error propagation
	signalerCtx, errChan := irrecoverable.WithSignaler(ctx)

	// Start the component manager
	e.component.Start(signalerCtx)

	// Wait for ready
	<-e.component.Ready()
	e.log.Info().Msg("All components ready")

	// Wait for shutdown or error
	var runErr error
	select {
	case <-ctx.Done():
		runErr = ctx.Err()
		e.log.Info().Msg("Context cancelled, shutting down")
	case err := <-errChan:
		runErr = err
		e.log.Err(err).Msg("Irrecoverable error, shutting down")
	case <-e.component.Done():
		e.log.Info().Msg("All components completed")
	}

	// Wait for all components to finish
	<-e.component.Done()

	// Run shutdown functions
	for _, fn := range e.shutdownFns {
		if err := fn(); err != nil {
			e.log.Err(err).Msg("Shutdown function error")
		}
	}

	e.log.Info().Msg("Engine stopped")
	return runErr
}

// Ready returns a channel that closes when all components are ready.
func (e *Engine[Config]) Ready() <-chan struct{} {
	return e.component.Ready()
}

// Done returns a channel that closes when all components have stopped.
func (e *Engine[Config]) Done() <-chan struct{} {
	return e.component.Done()
}
