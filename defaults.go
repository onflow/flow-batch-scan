package lib

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
