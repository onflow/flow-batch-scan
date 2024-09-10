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

package interceptors

import (
	"context"

	"github.com/rs/zerolog"
	"go.uber.org/ratelimit"
	"google.golang.org/grpc"
)

func RateLimitUnaryClientInterceptor(
	defaultRateLimit int,
	methodRateLimits map[string]int,
	logger zerolog.Logger,
) grpc.UnaryClientInterceptor {
	limiter := newLimiter(
		defaultRateLimit,
		methodRateLimits,
		logger)

	return func(
		ctx context.Context,
		method string, req,
		reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		limiter.Limit(method)

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

type limiter struct {
	ratelimit.Limiter
	methodLimiters map[string]ratelimit.Limiter

	logger zerolog.Logger
}

func (l *limiter) Limit(method string) {
	if limiter, ok := l.methodLimiters[method]; ok {
		limiter.Take()
		return
	}
	l.logger.Debug().Str("method", method).Msg("Using default rate limit for method.")
	l.Take()
}

func newLimiter(
	defaultRate int,
	methodLimiters map[string]int,
	logger zerolog.Logger,
) *limiter {
	l := &limiter{
		Limiter: ratelimit.New(defaultRate, ratelimit.WithoutSlack),
		logger:  logger.With().Str("component", "rate_limiter").Logger(),
	}
	l.methodLimiters = make(map[string]ratelimit.Limiter)
	for method, rate := range methodLimiters {
		l.methodLimiters[method] = ratelimit.New(rate, ratelimit.WithoutSlack)
	}
	return l
}
