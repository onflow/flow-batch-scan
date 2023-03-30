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

package main

import (
	fbs "github.com/onflow/flow-batch-scan"
	"github.com/rs/zerolog"
)

type scriptResultHandler struct {
	logger zerolog.Logger
}

func NewScriptResultHandler(logger zerolog.Logger) fbs.ScriptResultHandler {
	h := &scriptResultHandler{
		logger: logger,
	}
	return h
}

func (r *scriptResultHandler) Handle(_ fbs.ProcessedAddressBatch) error {
	// Does not need to do anything
	return nil
}
