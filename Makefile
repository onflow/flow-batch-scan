# Copyright 2023 Dapper Labs, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: fix
fix: tidy fix-lint fix-licence

.PHONY: fix-lint
fix-lint:
	golangci-lint run --fix

.PHONY: fix-licence
fix-licence:
	license-eye -c .github/licenserc.yml header fix

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: install-tools
install-tools:
	go install github.com/apache/skywalking-eyes/cmd/license-eye@latest
