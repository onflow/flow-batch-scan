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

package scanner

const defaultScript = `
pub struct AccountInfo {
	pub(set) var address: Address
	init(_ address: Address) {
		self.address = address
	}
}

pub fun main(addresses: [Address]) [AccountInfo]{ return [] }
`

type NoOpScriptResultHandler struct{}

func (d NoOpScriptResultHandler) Handle(_ ProcessedAddressBatch) error {
	return nil
}

var _ ScriptResultHandler = NoOpScriptResultHandler{}

type NoOpStatusReporter struct{}

func (n NoOpStatusReporter) ReportIncrementalBlockDiff(uint64) {}

func (n NoOpStatusReporter) ReportIncrementalBlockHeight(uint64) {}

func (n NoOpStatusReporter) ReportIsFullScanRunning(bool) {}

func (n NoOpStatusReporter) ReportFullScanProgress(uint64, uint64) {}

var _ StatusReporter = NoOpStatusReporter{}
