# Tracing Module

This module provides distributed tracing capabilities for Teranode using OpenTelemetry. It offers a unified interface that combines tracing spans, performance metrics, structured logging, and Prometheus metrics integration.

## Adding Tracing to Functions

### Basic Usage

To add tracing to a function, follow this pattern:

```go
import "github.com/bitcoin-sv/teranode/util/tracing"

func MyFunction(ctx context.Context) error {
    // Create a tracer for your component
    tracer := tracing.Tracer("my-component")
    
    // Start a span
    ctx, _, endSpan := tracer.Start(ctx, "MyFunction")
    defer endSpan() // IMPORTANT: Always defer endSpan()
    
    // Your function logic here
    return nil
}
```

or, as a one-liner:

```go
func MyFunction(ctx context.Context) error {
    // Create a tracer for your component and replace the passed in ctx with the new ctx which includes the span
    ctx, _, endSpan := tracing.Tracer("my-component").Start(ctx, "MyFunction")
    defer endSpan() // IMPORTANT: Always defer endSpan()
    
    // Your function logic here
    return nil
}
``` 

### With Error Handling

When your function returns an error, pass it to the endSpan function:

```go
func ProcessTransaction(ctx context.Context, txID string) (err error) {
    tracer := tracing.Tracer("transaction-processor")
    
    ctx, span, endSpan := tracer.Start(ctx, "ProcessTransaction",
        tracing.WithTag("txid", txID),
    )
    defer endSpan(err) // Pass the named return error
    
    // Your processing logic
    if err := validate(txID); err != nil {
        return err // The error will be recorded in the span
    }
    
    return nil
}
```

### With Additional Options

You can enhance your spans with various options:

```go
ctx, span, endSpan := tracer.Start(ctx, "OperationName",
    // Add searchable tags
    tracing.WithTag("user.id", userID),
    tracing.WithTag("request.type", "batch"),
    
    // Add logging
    tracing.WithLogMessage(logger, "Processing batch for user %s", userID),
    
    // Add metrics
    tracing.WithHistogram(myHistogram),
    tracing.WithCounter(myCounter),
    
    // Link to parent statistics
    tracing.WithParentStat(parentStat),
)
defer endSpan()
```

## Recording Errors in Spans

### Method 1: Pass Error to endSpan

The recommended approach is to pass errors to the endSpan function:

```go
func MyOperation(ctx context.Context) (err error) {
    ctx, span, endSpan := tracer.Start(ctx, "MyOperation")
    defer endSpan(err) // Automatically records the error
    
    if err := doSomething(); err != nil {
        return fmt.Errorf("failed to do something: %w", err)
    }
    
    return nil
}
```

### Method 2: Direct Error Recording

For cases where you want to record an error but continue processing:

```go
ctx, span, endSpan := tracer.Start(ctx, "BatchOperation")
defer endSpan()

for _, item := range items {
    if err := processItem(item); err != nil {
        // Record error but continue
        span.RecordError(err)
        span.AddEvent("item_failed", 
            attribute.String("item.id", item.ID),
            attribute.String("error", err.Error()),
        )
        continue
    }
}
```

### Method 3: Multiple Errors

When handling multiple potential errors:

```go
func ComplexOperation(ctx context.Context) error {
    ctx, span, endSpan := tracer.Start(ctx, "ComplexOperation")
    
    var finalErr error
    defer func() {
        endSpan(finalErr) // Pass the final error state
    }()
    
    if err := step1(); err != nil {
        finalErr = fmt.Errorf("step1 failed: %w", err)
        return finalErr
    }
    
    if err := step2(); err != nil {
        finalErr = fmt.Errorf("step2 failed: %w", err)
        return finalErr
    }
    
    return nil
}
```

## The Importance of Ending Spans

### Why You Must Call endSpan

The `endSpan` function performs several critical operations:

1. **Span Completion**: Marks the span as complete in the distributed trace
2. **Duration Recording**: Calculates and records the operation duration
3. **Error Status**: Sets the span status to error if an error is passed
4. **Metrics Recording**: Updates any configured Prometheus metrics
5. **Performance Stats**: Updates gocore.Stat performance statistics
6. **Logging**: Logs completion with timing information

### What Happens If You Don't End a Span

- **Memory Leak**: Spans remain in memory and are never sent to the tracing backend
- **Incomplete Traces**: The distributed trace will show incomplete operations
- **Missing Metrics**: Prometheus metrics won't be updated
- **No Timing Data**: Operation duration won't be recorded
- **Lost Error Information**: Errors won't be associated with the trace

### Best Practices for Ending Spans

1. **Always Use defer**: Place `defer endSpan()` immediately after starting a span
   ```go
   ctx, span, endSpan := tracer.Start(ctx, "Operation")
   defer endSpan() // Do this immediately
   ```

2. **Handle Panics**: The defer ensures endSpan is called even if the function panics
   ```go
   defer func() {
       if r := recover(); r != nil {
           err := fmt.Errorf("panic: %v", r)
           endSpan(err)
           panic(r) // Re-panic after recording
       }
   }()
   ```

3. **Named Returns for Errors**: Use named returns to automatically capture errors
   ```go
   func MyFunc() (result string, err error) {
       ctx, span, endSpan := tracer.Start(ctx, "MyFunc")
       defer endSpan(err) // Will capture the final error value
       
       // Function logic
       return result, err
   }
   ```

## Complete Example

```go
package myservice

import (
    "context"
    "fmt"
    
    "github.com/bitcoin-sv/teranode/util/tracing"
    "go.opentelemetry.io/otel/attribute"
)

type Service struct {
    tracer *tracing.UTracer
}

func NewService() *Service {
    return &Service{
        tracer: tracing.Tracer("myservice"),
    }
}

func (s *Service) ProcessBatch(ctx context.Context, items []Item) (err error) {
    // Start tracing with comprehensive options
    ctx, span, endSpan := s.tracer.Start(ctx, "ProcessBatch",
        tracing.WithTag("batch.size", fmt.Sprintf("%d", len(items))),
        tracing.WithLogMessage(logger, "Processing batch of %d items", len(items)),
    )
    defer endSpan(err) // Ensures span is ended with final error state
    
    // Add runtime attributes
    span.SetAttribute("service.version", "1.0.0")
    span.AddEvent("processing_started")
    
    // Process items
    var processed, failed int
    for i, item := range items {
        // Create child span for each item
        _, itemSpan, endItemSpan := s.tracer.Start(ctx, "ProcessItem",
            tracing.WithTag("item.id", item.ID),
            tracing.WithTag("item.index", fmt.Sprintf("%d", i)),
        )
        
        if err := s.processItem(item); err != nil {
            // Record item error
            itemSpan.RecordError(err)
            endItemSpan(err)
            failed++
            continue
        }
        
        endItemSpan() // Success - no error
        processed++
    }
    
    // Record final metrics
    span.SetAttribute("batch.processed", processed)
    span.SetAttribute("batch.failed", failed)
    span.AddEvent("processing_completed")
    
    if failed > 0 {
        err = fmt.Errorf("failed to process %d out of %d items", failed, len(items))
        return err
    }
    
    return nil
}
```

## Summary

- Always create spans for significant operations
- Always defer the endSpan function immediately after starting a span
- Pass errors to endSpan to automatically record them
- Use meaningful span names and attributes for better observability
- Create child spans for sub-operations to maintain trace hierarchy