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

package scanner_test

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	scan "github.com/onflow/flow-batch-scan"
)

func TestServerlessReporterShutsDownProperly(t *testing.T) {
	// this test should not take long
	timeout := time.After(3 * time.Second)
	go func() {
		<-timeout
		require.Fail(t, "test timed out")
	}()

	ctx, cancel := context.WithCancel(context.Background())

	reporter := scan.NewStatusReporter(
		"test",
		zerolog.Nop(),
		scan.WithStartServer(false),
	)
	<-reporter.Start(ctx)

	err := reporter.Err()
	require.NoError(t, err)

	select {
	case <-reporter.Done():
		require.Fail(t, "reporter should not be done yet")
	case <-time.After(100 * time.Millisecond):
		// wait a bit to make sure reporter is not done
	}

	cancel()

	<-reporter.Done()

	err = reporter.Err()
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}
