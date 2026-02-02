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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onflow/flow-go/module"
	"github.com/onflow/flow-go/module/component"
	"github.com/onflow/flow-go/module/irrecoverable"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testConfig struct {
	Value string
}

// mockComponent is a simple component for testing
type mockComponent struct {
	component.Component
	started atomic.Bool
	name    string
}

func newMockComponent(name string) *mockComponent {
	m := &mockComponent{name: name}
	m.Component = component.NewComponentManagerBuilder().
		AddWorker(func(ctx irrecoverable.SignalerContext, ready component.ReadyFunc) {
			m.started.Store(true)
			ready()
			<-ctx.Done()
		}).
		Build()
	return m
}

func (m *mockComponent) Started() bool {
	return m.started.Load()
}

func TestBuilderBase_Build(t *testing.T) {
	logger := zerolog.Nop()
	builder := NewBuilderBase[testConfig](logger)

	comp1 := newMockComponent("comp1")
	comp2 := newMockComponent("comp2")

	builder.
		Component("Component1", func(config testConfig) (module.ReadyDoneAware, error) {
			return comp1, nil
		}).
		Component("Component2", func(config testConfig) (module.ReadyDoneAware, error) {
			return comp2, nil
		})

	engine, err := builder.Build(testConfig{Value: "test"})
	require.NoError(t, err)
	require.NotNil(t, engine)
	assert.Equal(t, "test", engine.Config.Value)
}

func TestBuilderBase_StartupFunctions(t *testing.T) {
	logger := zerolog.Nop()
	builder := NewBuilderBase[testConfig](logger)

	var order []string

	builder.
		StartupFunc(func(config testConfig) error {
			order = append(order, "startup1")
			return nil
		}).
		StartupFunc(func(config testConfig) error {
			order = append(order, "startup2")
			return nil
		}).
		Component("Comp", func(config testConfig) (module.ReadyDoneAware, error) {
			order = append(order, "component")
			return newMockComponent("comp"), nil
		})

	_, err := builder.Build(testConfig{})
	require.NoError(t, err)

	assert.Equal(t, []string{"startup1", "startup2", "component"}, order)
}

func TestBuilderBase_StartupError(t *testing.T) {
	logger := zerolog.Nop()
	builder := NewBuilderBase[testConfig](logger)

	expectedErr := errors.New("startup failed")

	builder.
		StartupFunc(func(config testConfig) error {
			return expectedErr
		}).
		Component("Comp", func(config testConfig) (module.ReadyDoneAware, error) {
			t.Fatal("component should not be created")
			return nil, nil
		})

	_, err := builder.Build(testConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "startup failed")
}

func TestBuilderBase_ComponentError(t *testing.T) {
	logger := zerolog.Nop()
	builder := NewBuilderBase[testConfig](logger)

	expectedErr := errors.New("component creation failed")

	builder.Component("FailingComp", func(config testConfig) (module.ReadyDoneAware, error) {
		return nil, expectedErr
	})

	_, err := builder.Build(testConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "FailingComp")
	assert.Contains(t, err.Error(), "component creation failed")
}

func TestEngine_Run(t *testing.T) {
	logger := zerolog.Nop()
	builder := NewBuilderBase[testConfig](logger)

	comp := newMockComponent("comp")

	builder.Component("Comp", func(config testConfig) (module.ReadyDoneAware, error) {
		return comp, nil
	})

	engine, err := builder.Build(testConfig{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	// Run engine in goroutine
	done := make(chan error)
	go func() {
		done <- engine.Run(ctx)
	}()

	// Wait for ready
	select {
	case <-engine.Ready():
		// Good
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for engine ready")
	}

	assert.True(t, comp.Started())

	// Cancel context to stop
	cancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for engine to stop")
	}
}

func TestEngine_ShutdownFunctions(t *testing.T) {
	logger := zerolog.Nop()
	builder := NewBuilderBase[testConfig](logger)

	var shutdownOrder []string

	builder.
		Component("Comp", func(config testConfig) (module.ReadyDoneAware, error) {
			return newMockComponent("comp"), nil
		}).
		ShutdownFunc(func() error {
			shutdownOrder = append(shutdownOrder, "shutdown1")
			return nil
		}).
		ShutdownFunc(func() error {
			shutdownOrder = append(shutdownOrder, "shutdown2")
			return nil
		})

	engine, err := builder.Build(testConfig{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error)
	go func() {
		done <- engine.Run(ctx)
	}()

	<-engine.Ready()
	cancel()
	<-done

	assert.Equal(t, []string{"shutdown1", "shutdown2"}, shutdownOrder)
}

func TestEngine_ShutdownFunctionError(t *testing.T) {
	logger := zerolog.Nop()
	builder := NewBuilderBase[testConfig](logger)

	builder.
		Component("Comp", func(config testConfig) (module.ReadyDoneAware, error) {
			return newMockComponent("comp"), nil
		}).
		ShutdownFunc(func() error {
			return errors.New("shutdown error")
		})

	engine, err := builder.Build(testConfig{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error)
	go func() {
		done <- engine.Run(ctx)
	}()

	<-engine.Ready()
	cancel()

	// Should still complete despite shutdown error
	select {
	case <-done:
		// Good
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for engine to stop")
	}
}

func TestEngine_ComponentOrdering(t *testing.T) {
	logger := zerolog.Nop()
	builder := NewBuilderBase[testConfig](logger)

	var readyOrder []string
	var mu = make(chan struct{}, 1)
	mu <- struct{}{}

	addToOrder := func(s string) {
		<-mu
		readyOrder = append(readyOrder, s)
		mu <- struct{}{}
	}

	// Create components that signal when ready
	for _, name := range []string{"A", "B", "C"} {
		n := name
		builder.Component(n, func(config testConfig) (module.ReadyDoneAware, error) {
			comp := component.NewComponentManagerBuilder().
				AddWorker(func(ctx irrecoverable.SignalerContext, ready component.ReadyFunc) {
					addToOrder(n)
					ready()
					<-ctx.Done()
				}).
				Build()
			return comp, nil
		})
	}

	engine, err := builder.Build(testConfig{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error)
	go func() {
		done <- engine.Run(ctx)
	}()

	<-engine.Ready()
	cancel()
	<-done

	// All components should have started
	<-mu
	assert.Len(t, readyOrder, 3)
	assert.Contains(t, readyOrder, "A")
	assert.Contains(t, readyOrder, "B")
	assert.Contains(t, readyOrder, "C")
}
