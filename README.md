# Flow Batch Scan

A Go library for scanning all accounts on the Flow blockchain.

## What It Does

Scans the entire Flow blockchain using a Cadence script, running two concurrent processes:

- **Full Scan**: Iterates through all addresses to build a complete dataset
- **Incremental Scan**: Monitors new blocks for changed accounts to keep data fresh

When a full scan completes, you have accurate data up to a specific block height. In continuous mode, the incremental scan keeps running to track ongoing changes.

## Features

- **Batch script execution** - Execute Cadence scripts across many addresses efficiently
- **Automatic change detection** - Detects which accounts changed using event scanning or other candidate scanners
- **Resumable scans** - Handles interruptions and resumes from where it left off
- **Incremental updates** - Keeps data fresh by monitoring new blocks
- **Prometheus metrics** - Built-in observability for scan progress and performance
- **Rate limiting & retries** - Configurable client-side rate limiting with automatic retries

## Installation

```bash
go get github.com/onflow/flow-batch-scan
```

## Quick Start

```go
config := scanner.DefaultConfig().
    WithScript(myScript).
    WithCandidateScanners(myScanners).
    WithScriptResultHandler(myHandler).
    WithChainID(flow.Mainnet)

scan := scanner.NewScanner(flowClient, config)
result, err := scan.Scan(ctx)
```

## Examples

See the `examples/` directory for complete working implementations:

- **`contract_names/`** - One-time scan of all contract names on the chain. Demonstrates basic setup with a custom result handler.

- **`monitor_contract_deployments/`** - Continuous scan monitoring contract deployments. Shows how to combine the library's Prometheus metrics with custom application metrics and run your own metrics HTTP server.

## Observability

The library exposes Prometheus metrics. You control how they're served:

> **Note**: While a full scan is running, the collected data is incomplete. The `full_scan_running` metric indicates when a scan is in progress.

```go
// Create reporter (optionally with custom registry)
reporter := scanner.NewStatusReporter("myapp", logger)

// You run your own server
http.Handle("/metrics", promhttp.Handler())
go http.ListenAndServe(":2112", nil)
```

Available metrics:

| Metric | Description |
|--------|-------------|
| `inc_block_height` | Latest block height successfully processed by the incremental scanner |
| `latest_sealed_block_height` | Latest sealed block height received from the network subscription |
| `full_scan_running` | Whether a full scan is currently running (1) or not (0) |
| `full_scan_progress` | Progress of the current full scan (0.0 to 1.0) |

> **Tip**: The difference between `latest_sealed_block_height` and `inc_block_height` shows how far behind the incremental scanner is from the chain's latest block.
