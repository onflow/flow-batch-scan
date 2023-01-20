# Flow Batch Scan

A library to make it easy to scan the entire flow chain.

## How does it work

The `BlockScanner.Scan` method will perform a **full scan** of all the addresses on chain starting at the latest available block using the provided cadence `script`.
This will take some time, during which the **full scan** has to switch newer reference blocks, because the old ones are no longer available.
To ensure that the final data is accurate when the scan ends, an **incremental scan** also runs besides the **full scan**.
The incremental scan looks at new blocks for any accounts could have had their data changed during that block (the data that the `script` is looking for).
If there are candidates to be scanned, the incremental scanner will scan them with the same `script` and update the results of the `full scan`.

In continuous mode the incremental scan will keep running and will scan any candidates that might have changed.

The library expects 3 components:

- a cadence `script` that has to accept an address array as input `addresses: [Address]` and returns any cadence value as the `result`.
- an array of candidate scanners which scan a block range looking for accounts that could have had changed, so that the `script` would now return a different result.
- a `result handler` that will be called with the results of the `script` for each address array.

## Use case

Any quantity can be scanned for if:
- it can be observed by a cadence script
- the change of the quantity can be observed by looking at transaction results of a block (e.g: events)

Example:
1. Scanning for contracts deployed on accounts. (see `examples/contracts`)
2. Scanning for accounts FT or NFT balance.
3. Scanning for public keys added to accounts.

## Examples

See the `examples` folder, there are a lot of comments in there.
The `contract_names` example is a one time scan example.
the `monitoer_contract_deployments` example is a continuous scan example and builds on the `contract_names` example.