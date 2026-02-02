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

package client

import (
	"context"
	"io"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/onflow/flow-batch-scan/client/interceptors"

	"github.com/onflow/cadence/encoding/json"
	protoAccess "github.com/onflow/flow/protobuf/go/flow/access"
	"google.golang.org/grpc"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
	flowgrpc "github.com/onflow/flow-go-sdk/access/grpc"
)

type Client interface {
	GetLatestBlockHeader(ctx context.Context, isSealed bool) (*flow.BlockHeader, error)
	ExecuteScriptAtBlockHeight(ctx context.Context, height uint64, script []byte, arguments []cadence.Value) (cadence.Value, error)
	GetBlockByHeight(ctx context.Context, height uint64) (*flow.Block, error)
	GetTransaction(ctx context.Context, txID flow.Identifier) (*flow.Transaction, error)
	GetEventsForHeightRange(ctx context.Context, query flowgrpc.EventRangeQuery) ([]flow.BlockEvents, error)
	GetCollection(ctx context.Context, colID flow.Identifier) (*flow.Collection, error)
	SubscribeBlockDigestsFromLatest(ctx context.Context, blockStatus flow.BlockStatus) (<-chan *flow.BlockDigest, <-chan error, error)
	SubscribeBlockDigestsFromStartHeight(ctx context.Context, startHeight uint64, blockStatus flow.BlockStatus) (<-chan *flow.BlockDigest, <-chan error, error)
}

type ClosableClient interface {
	Client
	io.Closer
}

type closableClient struct {
	Client
	*grpc.ClientConn
}

func NewClientFromConnection(
	conn grpc.ClientConnInterface,
) Client {
	grpcClient := protoAccess.NewAccessAPIClient(conn)
	flowClient := flowgrpc.NewFromRPCClient(grpcClient)
	// TODO: open PR in flow-go-sdk to include this line inside NewFromRPCClient
	flowClient.SetJSONOptions([]json.Option{json.WithAllowUnstructuredStaticTypes(true)})

	return &client{
		flowClient,
	}
}

type Config struct {
	Log zerolog.Logger

	DefaultCallOptions []grpc.CallOption

	DefaultRateLimit   int
	SpecificRateLimits map[string]int

	Timeout time.Duration

	Retries int

	ClientMetrics *grpc_prometheus.ClientMetrics
	Registry      prometheus.Registerer
}

func DefaultConfig() Config {
	return Config{
		Log: zerolog.Nop(),
		DefaultCallOptions: []grpc.CallOption{
			grpc.MaxCallRecvMsgSize(1024 * 1024 * 1024),
		},
		DefaultRateLimit: 10,
		SpecificRateLimits: map[string]int{
			"/flow.access.AccessAPI/GetLatestBlockHeader":       50,
			"/flow.access.AccessAPI/GetEventsForHeightRange":    50,
			"/flow.access.AccessAPI/GetBlockByHeight":           50,
			"/flow.access.AccessAPI/GetCollectionByID":          50,
			"/flow.access.AccessAPI/GetTransaction":             50,
			"/flow.access.AccessAPI/ExecuteScriptAtBlockHeight": 5,
		},
		Timeout:       60 * time.Second,
		Retries:       5,
		ClientMetrics: DefaultClientMetrics(""),
		// Use a new registry by default to avoid duplicate registration errors
		// when multiple clients are created. Applications can use WithRegistry
		// to register metrics with the default registry if needed.
		Registry: prometheus.NewRegistry(),
	}
}

func DefaultClientMetrics(namespace string) *grpc_prometheus.ClientMetrics {
	metrics := grpc_prometheus.NewClientMetrics(
		func(opts *prometheus.CounterOpts) {
			opts.Namespace = namespace
		},
	)
	metrics.EnableClientHandlingTimeHistogram(
		func(opts *prometheus.HistogramOpts) {
			opts.Namespace = namespace
		})
	return metrics
}

func (c Config) Interceptors() []grpc.UnaryClientInterceptor {
	inter := []grpc.UnaryClientInterceptor{
		interceptors.UnpackCancelledUnaryClientInterceptor(),
		interceptors.LogUnaryClientInterceptor(c.Log),
		interceptors.RetryUnaryClientInterceptor(c.Retries),
		interceptors.RateLimitUnaryClientInterceptor(
			c.DefaultRateLimit,
			c.SpecificRateLimits,
			c.Log,
		),
		// timeout per retried request, not per call
		// timout is after waiting for rate limit
		interceptors.TimeoutUnaryClientInterceptor(c.Timeout),
	}

	if c.ClientMetrics != nil {
		// register to the configured registry
		err := c.Registry.Register(c.ClientMetrics)
		if err != nil {
			c.Log.Warn().Err(err).Msg("prometheus registration error")
		}

		inter = append(inter, c.ClientMetrics.UnaryClientInterceptor())
	}

	return inter
}

type Option func(*Config)

func WithLog(log zerolog.Logger) Option {
	return func(c *Config) {
		c.Log = log
	}
}

// WithRegistry configures the client to register gRPC metrics with the provided registry.
// If not specified, uses prometheus.DefaultRegisterer.
func WithRegistry(registry prometheus.Registerer) Option {
	return func(c *Config) {
		c.Registry = registry
	}
}

func NewClient(
	target string,
	opts ...Option,
) (ClosableClient, error) {
	conn, err := NewConnection(target, opts...)
	if err != nil {
		return nil, err
	}
	client := NewClientFromConnection(conn)
	return &closableClient{
		Client:     client,
		ClientConn: conn,
	}, nil
}

func NewConnection(
	target string,
	opts ...Option,
) (*grpc.ClientConn, error) {
	conf := DefaultConfig()
	for _, opt := range opts {
		opt(&conf)
	}

	return grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			conf.DefaultCallOptions...,
		),
		grpc.WithChainUnaryInterceptor(
			conf.Interceptors()...,
		),
	)
}

var _ Client = (*client)(nil)

type client struct {
	*flowgrpc.BaseClient
}

func (c *client) GetLatestBlockHeader(ctx context.Context, isSealed bool) (*flow.BlockHeader, error) {
	return c.BaseClient.GetLatestBlockHeader(ctx, isSealed)
}

func (c *client) ExecuteScriptAtBlockHeight(
	ctx context.Context,
	height uint64,
	script []byte,
	arguments []cadence.Value,
) (cadence.Value, error) {
	return c.BaseClient.ExecuteScriptAtBlockHeight(ctx, height, script, arguments)
}

func (c *client) GetBlockByHeight(
	ctx context.Context,
	height uint64,
) (*flow.Block, error) {
	return c.BaseClient.GetBlockByHeight(ctx, height)
}

func (c *client) GetTransaction(
	ctx context.Context,
	txID flow.Identifier,
) (*flow.Transaction, error) {
	return c.BaseClient.GetTransaction(ctx, txID)
}

func (c *client) GetEventsForHeightRange(
	ctx context.Context,
	query flowgrpc.EventRangeQuery,
) ([]flow.BlockEvents, error) {
	return c.BaseClient.GetEventsForHeightRange(ctx, query)
}

func (c *client) GetCollection(
	ctx context.Context,
	colID flow.Identifier,
) (*flow.Collection, error) {
	return c.BaseClient.GetCollection(ctx, colID)
}

func (c *client) SubscribeBlockDigestsFromLatest(
	ctx context.Context,
	blockStatus flow.BlockStatus,
) (<-chan *flow.BlockDigest, <-chan error, error) {
	return c.BaseClient.SubscribeBlockDigestsFromLatest(ctx, blockStatus)
}

func (c *client) SubscribeBlockDigestsFromStartHeight(
	ctx context.Context,
	startHeight uint64,
	blockStatus flow.BlockStatus,
) (<-chan *flow.BlockDigest, <-chan error, error) {
	return c.BaseClient.SubscribeBlockDigestsFromStartHeight(ctx, startHeight, blockStatus)
}
