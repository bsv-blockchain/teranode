# UTracer Migration Guide

This guide explains how to use the new standardized `UTracer` for consistent tracing throughout the Teranode application.

## Overview

`UTracer` provides a unified interface that combines:
- OpenTelemetry spans for distributed tracing
- gocore.Stat for performance metrics
- Structured logging with timing information
- Prometheus metrics integration

## Basic Usage

### Creating a Tracer

```go
// Create a tracer for your service or component
tracer := tracing.NewUTracer("service-name")
```

### Starting a Span

```go
// Start a new span
ctx, span := tracer.Start(ctx, "OperationName")
defer span.End()

// With error handling
ctx, span := tracer.Start(ctx, "OperationName")
defer func() {
    if err != nil {
        span.End(err) // Automatically marks span as error
    } else {
        span.End()
    }
}()
```

### Using Options

```go
ctx, span := tracer.Start(ctx, "OperationName",
    // Link to parent statistics
    tracing.WithParentStat(parentStat),
    
    // Add tags for searchability
    tracing.WithTag("user.id", userID),
    tracing.WithTag("tx.id", txID),
    
    // Add metrics
    tracing.WithHistogram(prometheusHistogram),
    tracing.WithCounter(prometheusCounter),
    
    // Add logging
    tracing.WithLogMessage(logger, "Processing transaction %s", txID),
)
defer span.End()
```

### Adding Attributes During Execution

```go
// Add attributes to provide more context
span.SetAttribute("result.count", count)
span.SetAttribute("cache.hit", true)
span.SetAttribute("processing.time_ms", processingTime)
```

### Recording Events

```go
// Record significant events within the span
span.AddEvent("validation_started")
span.AddEvent("cache_miss", attribute.String("cache.key", key))
span.AddEvent("validation_completed")
```

### Creating Child Spans

```go
// Method 1: Using StartChild
ctx, childSpan := parentSpan.StartChild("ChildOperation",
    tracing.WithTag("step", "validation"),
)
defer childSpan.End()

// Method 2: Using tracer with parent context
childCtx, childSpan := tracer.Start(parentSpan.Context(), "ChildOperation")
defer childSpan.End()
```

## Migration from StartTracing

The existing `StartTracing` function remains for backward compatibility:

```go
// Old way (still works)
ctx, stat, deferFn := tracing.StartTracing(ctx, "Operation",
    tracing.WithLogMessage(logger, "Processing"),
)
defer deferFn()

// New way (recommended)
tracer := tracing.NewUTracer("my-service")
ctx, span := tracer.Start(ctx, "Operation",
    tracing.WithLogMessage(logger, "Processing"),
)
defer span.End()

// Access the stat if needed
stat := span.Stat()
```

## Best Practices

### 1. Service-Level Tracers

Create one tracer per service/component and reuse it:

```go
// In your service struct
type MyService struct {
    tracer *tracing.UTracer
    logger ulogger.Logger
    // ... other fields
}

func NewMyService() *MyService {
    return &MyService{
        tracer: tracing.NewUTracer("my-service"),
        logger: ulogger.New("my-service"),
    }
}
```

### 2. Consistent Naming

Use hierarchical names for operations:

```go
// Good
span := tracer.Start(ctx, "Validator.ValidateTransaction")
span := tracer.Start(ctx, "BlockAssembly.ProcessSubtree")
span := tracer.Start(ctx, "UTXO.Store.Get")

// Less descriptive
span := tracer.Start(ctx, "validate")
span := tracer.Start(ctx, "process")
```

### 3. Error Handling Pattern

Always pass errors to End() for proper tracking:

```go
func (s *Service) ProcessTransaction(ctx context.Context, tx *Transaction) (err error) {
    ctx, span := s.tracer.Start(ctx, "ProcessTransaction",
        tracing.WithTag("tx.id", tx.ID),
        tracing.WithLogMessage(s.logger, "Processing transaction %s", tx.ID),
    )
    defer func() {
        span.End(err) // Captures the named return error
    }()
    
    // Your processing logic
    if err := s.validate(tx); err != nil {
        return err // span.End(err) will mark this as failed
    }
    
    return nil
}
```

### 4. Important Attributes

Always include relevant identifiers:

```go
// For transactions
span.SetAttribute("tx.id", txID)
span.SetAttribute("tx.size", txSize)
span.SetAttribute("tx.fee", fee)

// For blocks
span.SetAttribute("block.height", height)
span.SetAttribute("block.hash", hash)
span.SetAttribute("block.tx_count", txCount)

// For operations
span.SetAttribute("cache.hit", cacheHit)
span.SetAttribute("retry.count", retryCount)
span.SetAttribute("queue.size", queueSize)
```

### 5. Log Levels

Use appropriate log levels:

```go
// Info - Normal operations
tracing.WithLogMessage(logger, "Processing block %d", height)

// Debug - Detailed information
tracing.WithDebugLogMessage(logger, "Cache lookup for key %s", key)

// Warn - Degraded operations
tracing.WithWarnLogMessage(logger, "Retry attempt %d for tx %s", retry, txID)
```

## Example: Complete Service Method

```go
func (v *Validator) ValidateTransactionBatch(
    ctx context.Context, 
    txs []*Transaction,
) (results []*ValidationResult, err error) {
    // Start tracing
    ctx, span := v.tracer.Start(ctx, "ValidateTransactionBatch",
        tracing.WithParentStat(v.stats),
        tracing.WithHistogram(v.metrics.batchValidationDuration),
        tracing.WithCounter(v.metrics.batchValidationCount),
        tracing.WithLogMessage(v.logger, "Validating batch of %d transactions", len(txs)),
    )
    defer func() {
        span.End(err)
    }()
    
    // Set attributes
    span.SetAttribute("batch.size", len(txs))
    span.SetAttribute("validator.type", v.scriptEngine)
    
    // Record start event
    span.AddEvent("validation_started")
    
    // Process transactions
    results = make([]*ValidationResult, len(txs))
    for i, tx := range txs {
        // Create child span for each transaction
        _, txSpan := span.StartChild("ValidateTransaction",
            tracing.WithTag("tx.id", tx.ID),
            tracing.WithTag("tx.index", fmt.Sprintf("%d", i)),
        )
        
        result, txErr := v.validateSingleTransaction(txSpan.Context(), tx)
        results[i] = result
        
        if txErr != nil {
            txSpan.SetAttribute("validation.error", txErr.Error())
            txSpan.End(txErr)
        } else {
            txSpan.SetAttribute("validation.success", true)
            txSpan.End()
        }
    }
    
    // Record completion
    span.AddEvent("validation_completed")
    
    // Set final attributes
    validCount := countValidTransactions(results)
    span.SetAttribute("batch.valid_count", validCount)
    span.SetAttribute("batch.invalid_count", len(txs)-validCount)
    
    return results, nil
}
```

## Testing with UTracer

For unit tests, you can verify tracing behavior:

```go
func TestMyOperation(t *testing.T) {
    // Initialize test tracer
    tracer := tracing.NewUTracer("test-service")
    
    // Create mock logger to verify logging
    logger := newMockLogger()
    
    // Execute operation
    ctx := context.Background()
    ctx, span := tracer.Start(ctx, "TestOperation",
        tracing.WithLogMessage(logger, "Test message"),
    )
    
    // Your test logic here
    
    span.End()
    
    // Verify logging occurred
    assert.Contains(t, logger.LastMessage(), "Test message DONE in")
}
```

## Summary

The `UTracer` provides a consistent way to:
1. Create OpenTelemetry spans for distributed tracing
2. Automatically track performance with gocore.Stat
3. Log operation start/completion with timing
4. Integrate with Prometheus metrics
5. Handle errors consistently

By using `UTracer` throughout the application, we ensure consistent observability and make it easier to debug and monitor the system.