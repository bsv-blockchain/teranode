# Trace Context Propagation Guide

This document explains how to ensure trace context is properly propagated across all communication mechanisms in Teranode for distributed tracing to work correctly.

## Why Propagation Matters

Teranode uses `ParentBased` sampling, which means:
- **Root spans** (no parent): Sampled at the configured rate (e.g., 10%)
- **Child spans** (have a parent): Inherit the parent's sampling decision

For this to work, trace context must be passed between services. Without propagation, each service makes independent sampling decisions, resulting in fragmented traces.

## Propagation Status by Communication Type

| Communication | Status | Notes |
|---------------|--------|-------|
| gRPC | ✅ Implemented | Automatic via otelgrpc interceptors |
| gRPC Batch | ✅ Implemented | Explicit context in protobuf messages |
| HTTP/RPC | ✅ Implemented | Extracted from HTTP headers |
| Kafka | ❌ **Not Implemented** | Requires manual header handling - see [Known Gaps](#known-gaps-kafka-propagation) |
| P2P | ❌ **Not Implemented** | Depends on Kafka propagation |
| UDP Multicast | ⚠️ **Not Planned** | No context in wire protocol - by design |

## Known Gaps: Kafka Propagation

**Status: NOT IMPLEMENTED**

Kafka-based communication currently does NOT propagate trace context. This affects:

1. **Transaction validation via Kafka** (`services/propagation/Server.go` → `services/validator/Server.go`)
   - When `validateTransactionViaKafka()` publishes to Kafka, no trace context is included
   - When the validator consumes from Kafka, it cannot link back to the original trace

2. **Block events flowing through Kafka topics**
   - Block announcements and processing events lose trace linkage

### Impact

When transactions or blocks are processed via Kafka:
- A **new root span** is created instead of a child span
- The trace appears disconnected in Jaeger/tracing UI
- Cannot trace the full journey of a transaction from RPC → Propagation → Kafka → Validator

### Affected Code Paths

| Producer | Consumer | Message Type |
|----------|----------|--------------|
| `services/propagation/Server.go:validateTransactionViaKafka()` | `services/validator/Server.go:kafkaMessageHandler` | `KafkaTxValidationTopicMessage` |

### Required Changes to Fix

1. **Update protobuf** (`util/kafka/kafka_message/kafka_messages.proto`):
   ```protobuf
   message KafkaTxValidationTopicMessage {
       bytes tx = 1;
       int64 height = 2;
       KafkaTxValidationOptions options = 3;
       map<string, string> trace_context = 4;  // ADD THIS
   }
   ```

2. **Inject on publish** (propagation server):
   ```go
   traceContext := make(map[string]string)
   otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(traceContext))
   msg.TraceContext = traceContext
   ```

3. **Extract on consume** (validator server):
   ```go
   if len(msg.TraceContext) > 0 {
       ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(msg.TraceContext))
   }
   ```

---

## gRPC Propagation (Automatic)

gRPC trace context propagation is handled automatically by OpenTelemetry interceptors configured in `util/grpc_helper.go`.

### How It Works

**Client-side** (outgoing requests):
```go
// Configured in grpc_helper.go:146
stats.NewClientHandler(stats.WithClientHandler(otelgrpc.NewClientHandler()))
```

**Server-side** (incoming requests):
```go
// Configured in grpc_helper.go:234
stats.NewServerHandler(stats.WithServerHandler(otelgrpc.NewServerHandler()))
```

### Developer Action Required
**None** - trace context is automatically injected into gRPC metadata on outgoing calls and extracted on incoming calls.

---

## gRPC Batch Propagation (Manual)

For batch operations where multiple items are sent in a single gRPC call, each item needs its own trace context.

### Current Implementation

**Producer** (`services/propagation/Client.go`):
```go
import "go.opentelemetry.io/otel"

// Inject trace context into each batch item
traceContext := make(map[string]string)
prop := otel.GetTextMapPropagator()
prop.Inject(item.ctx, propagation.MapCarrier(traceContext))

items[i] = &propagation_api.BatchTransactionItem{
    Tx:           item.tx.SerializeBytes(),
    TraceContext: traceContext,  // Carry trace context per-item
}
```

**Consumer** (`services/propagation/Server.go`):
```go
// Extract trace context from each batch item
if len(item.TraceContext) > 0 {
    prop := otel.GetTextMapPropagator()
    txCtx = prop.Extract(parentCtx, propagation.MapCarrier(item.TraceContext))
} else {
    txCtx = parentCtx
}
```

### Developer Action Required
When creating new batch APIs:
1. Add `map<string, string> trace_context` field to protobuf message
2. Inject context on producer side using `otel.GetTextMapPropagator().Inject()`
3. Extract context on consumer side using `otel.GetTextMapPropagator().Extract()`

---

## HTTP Propagation (Implemented)

HTTP trace context is extracted from standard W3C trace headers.

### Current Implementation

**Server** (`services/rpc/Server.go`):
```go
import (
    "go.opentelemetry.io/otel"
    otelPropagation "go.opentelemetry.io/otel/propagation"
)

// In HTTP handler middleware
ctx := r.Context()
ctx = otel.GetTextMapPropagator().Extract(ctx, otelPropagation.HeaderCarrier(r.Header))
r = r.WithContext(ctx)
```

### Developer Action Required
When making outgoing HTTP requests:
```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/propagation"
)

req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
```

---

## Kafka Propagation (NOT IMPLEMENTED)

**This is a gap that needs to be addressed for complete distributed tracing.**

Kafka messages currently do not carry trace context, meaning traces are broken when:
- Transactions are validated via Kafka
- Block events flow through Kafka topics
- Any async processing uses Kafka

### Required Implementation

#### 1. Update Protobuf Messages

Add trace context field to Kafka message types in `util/kafka/kafka_message/kafka_messages.proto`:

```protobuf
message KafkaTxValidationTopicMessage {
    bytes tx = 1;
    int64 height = 2;
    KafkaTxValidationOptions options = 3;
    map<string, string> trace_context = 4;  // ADD THIS
}

message KafkaBlockTopicMessage {
    bytes block_hash = 1;
    int64 block_height = 2;
    // ... other fields
    map<string, string> trace_context = 10;  // ADD THIS
}
```

#### 2. Inject Context When Publishing

In producer code (e.g., `services/propagation/Server.go`):

```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/propagation"
)

func (ps *PropagationServer) validateTransactionViaKafka(ctx context.Context, btTx *bt.Tx) error {
    // Inject trace context
    traceContext := make(map[string]string)
    otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(traceContext))

    msg := &kafkamessage.KafkaTxValidationTopicMessage{
        Tx:           btTx.SerializeBytes(),
        Height:       0,
        Options:      validationOptions,
        TraceContext: traceContext,  // Include trace context
    }

    // ... publish message
}
```

#### 3. Extract Context When Consuming

In consumer code:

```go
func (v *Validator) processKafkaMessage(msg *kafkamessage.KafkaTxValidationTopicMessage) {
    ctx := context.Background()

    // Extract trace context if present
    if len(msg.TraceContext) > 0 {
        ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(msg.TraceContext))
    }

    // Use ctx for all tracing operations
    ctx, span, endFn := tracer.Start(ctx, "ValidateTransaction")
    defer endFn()

    // ... process message
}
```

---

## P2P Propagation (NOT IMPLEMENTED)

P2P message propagation depends on the underlying transport:
- Messages routed via Kafka inherit the Kafka propagation gap
- Direct libp2p messages would need custom header handling

### Required Implementation

Once Kafka propagation is implemented, P2P messages that flow through Kafka will automatically benefit.

For direct P2P messages, add trace context to message metadata or protobuf definitions.

---

## UDP Multicast Propagation (NOT IMPLEMENTED)

UDP multicast transactions (`services/propagation/`) currently have no trace context in the wire protocol.

### Considerations

- Adding trace context to UDP packets increases message size
- May not be practical for high-throughput transaction propagation
- Consider tracing only at service boundaries (when transactions enter/exit via gRPC/HTTP)

---

## Testing Propagation

### Verify gRPC Propagation
```go
// Parent service
ctx, span, end := tracer.Start(ctx, "ParentOperation")
defer end()
response, err := grpcClient.SomeMethod(ctx, request)  // Context carries trace
```

### Verify Trace Continuity
In Jaeger UI:
1. Find a sampled trace from the entry service
2. Verify spans from all downstream services appear in the same trace
3. Check that the trace ID is consistent across all spans

---

## Quick Reference: Inject/Extract Pattern

```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/propagation"
)

// INJECT: When sending a message (producer side)
traceContext := make(map[string]string)
otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(traceContext))
// Add traceContext to your message

// EXTRACT: When receiving a message (consumer side)
ctx := context.Background()
if len(message.TraceContext) > 0 {
    ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(message.TraceContext))
}
// Use ctx for tracing
```

---

## W3C Trace Context Headers

The propagator uses W3C standard headers:
- `traceparent`: Contains trace ID, span ID, and sampling flag
- `tracestate`: Optional vendor-specific trace data

When using `map[string]string` carriers, these are the keys that will be set/read.
