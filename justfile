# Flow Batch Scan
#
# Copyright Flow Foundation
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

# List available recipes
default:
    @just --list

# Run all fixes (tidy, lint, licence)
fix: tidy fix-lint fix-licence

# Run all checks (tidy, lint, licence)
# If it fails, CI will fail
check: check-tidy check-lint check-licence

# Run tests
# If it fails, CI will fail
test +FLAGS="":
    go test {{FLAGS}} ./...

# Fix linting issues
fix-lint:
    golangci-lint run --fix

# Fix licence headers
fix-licence:
    license-eye -c .github/licenserc.yml header fix

# Tidy go modules
tidy:
    go mod tidy

# Check linting issues without fixing
check-lint:
    golangci-lint run

# Check licence headers without fixing
check-licence:
    license-eye -c .github/licenserc.yml header check

# Check go modules are tidy
check-tidy:
    go mod tidy
    @git diff --exit-code go.mod go.sum
