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
	"fmt"

	"github.com/hashicorp/go-multierror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func RetryUnaryClientInterceptor(
	retries int,
) grpc.UnaryClientInterceptor {
	if retries <= 0 {
		retries = -1
	}

	canRetry := func(ctx context.Context, err error) bool {
		if ctx.Err() != nil {
			return false
		}

		code := status.Code(err)

		if code == codes.ResourceExhausted {
			return true
		}

		if code == codes.DeadlineExceeded {
			return true
		}

		if code == codes.Internal {
			return true
		}

		return false
	}

	return func(
		ctx context.Context,
		method string, req,
		reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		var err *multierror.Error

		for r := 0; r != retries; r++ {
			ierr := invoker(ctx, method, req, reply, cc, opts...)
			if ierr != nil && canRetry(ctx, ierr) {
				err = multierror.Append(err, ierr)
				continue
			}
			return ierr
		}
		return fmt.Errorf("reached maximum number of retries (%d): %w", retries, err)
	}
}
