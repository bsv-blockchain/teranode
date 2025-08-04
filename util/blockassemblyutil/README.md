# Block Assembly Utilities

This package provides utility functions for coordinating with the block assembly service.

## WaitForBlockAssemblyReady

The `WaitForBlockAssemblyReady` function ensures that the block assembly service has processed all necessary data (such as coinbase transactions) before allowing block validation to proceed.

### Usage

```go
err := blockassemblyutil.WaitForBlockAssemblyReady(
    ctx,
    logger,
    blockAssemblyClient,
    blockHeight,
)
```

### Parameters

- `ctx`: Context for cancellation
- `logger`: Logger for recording operations  
- `blockAssemblyClient`: Client interface to the block assembly service
- `blockHeight`: The height of the block to be processed

### Behavior

The function implements a retry mechanism with aggressive exponential backoff:
- Retry count: 100
- Initial backoff: 1ms
- Backoff multiplier: 10 (1ms, 10ms, 100ms, ...)
- Max blocks behind: 1 (constant defined in the package)

This aggressive configuration is intentional to support fast catchup when processing thousands of blocks.

The function returns immediately (no error) if the block assembly client is nil, which supports testing scenarios.

### Integration Points

This utility is used in:
- Legacy service: `services/legacy/netsync/handle_block.go`
- Block validation service: `services/blockvalidation/Server.go`

Both services wait for block assembly to be ready before processing new blocks to ensure coinbase transactions have been properly handled.