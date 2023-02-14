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

package interceptors

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func UnpackCancelledUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string, req,
		reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		err := invoker(ctx, method, req, reply, cc, opts...)

		if err != nil &&
			status.Code(err) == codes.Canceled {
			// if the context was cancelled, we don't need the inner RPC error
			// as it is likely to be a result of the context being cancelled
			// just return the context error
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}

		return err
	}
}
