# subtreeprocessor: guard newSubtreeChan sends with context to avoid processor deadlocks

## Summary

Several `SubtreeProcessor` paths sent to `stp.newSubtreeChan` with a bare,
blocking send — no select on context cancellation — even though the rest of the
processor already uses context-aware sends. If the block-assembly listener has
exited during shutdown, or is backpressured, those sends can block the **single**
subtree-processor goroutine indefinitely. Because that goroutine is the main
serialized processing loop, one blocked announcement stalls transaction
processing, state transitions, and reorg completion.

This PR makes the remaining unguarded sends honour the processor's lifecycle
context, so cancellation/backpressure unblocks the goroutine instead of
deadlocking it.

## What was actually unguarded

I audited every `newSubtreeChan` send before changing anything:

| Path | Status before this PR |
|------|------------------------|
| `Start()` incomplete / periodic / on-demand announcements | ✅ Already context-aware |
| `reorgBlocks` batch announcement loop | ✅ Already context-aware (ctx send + ctx receive + buffered `errCh`) |
| `processCompleteSubtree` (complete-subtree rotation) | ❌ **Bare blocking send** |
| `parallelBuildRemainderSubtrees` (reorg remainder announcement) | ❌ **Bare blocking send** |

Only the last two were genuinely unguarded. `reorgBlocks` was already fixed in a
prior change, so it is intentionally left untouched here.

## Changes

### `services/blockassembly/subtreeprocessor/SubtreeProcessor.go`

- **Publish the processor lifecycle context.** New field
  `processorCtx atomic.Pointer[context.Context]`, stored in `Start()` alongside
  the existing `cancelPtr`. This lets sends that happen *outside* the `Start()`
  select loop observe shutdown.
- **`processorContext()` helper** — returns the stored context, falling back to
  `context.Background()` when `Start()` has not run yet (e.g. `AddDirectly` used
  to seed transactions pre-start). The fallback preserves the historical
  blocking-send behaviour in that case, so there is no behavioural change before
  the processor is started.
- **`sendNewSubtree(ctx, req)` helper** — performs the send inside a
  `select { case chan <- req; case <-ctx.Done() }`, mirroring the pattern already
  used by the `Start()` loop and `reorgBlocks`.
- **`processCompleteSubtree`** now sends via `sendNewSubtree(processorContext(), …)`
  and returns a `ProcessingError` on cancellation instead of blocking.
- **`parallelBuildRemainderSubtrees`** now sends via `sendNewSubtree(ctx, …)`
  using its already-in-scope context.

### Drain-goroutine leak (defence in depth)

`sendCallerErr` (in `Server.go`) already abandons the worker's `ErrChan` send on
context cancellation, so the storage worker itself never blocks. But the
processor-side **drain goroutine** (`<-errCh`) could leak during shutdown if the
worker returns before processing the request. For both fixed paths:

- the `errCh` is now **buffered (size 1)**, so the worker can always report its
  result without blocking, and
- the drain goroutine now selects on `ctx.Done()`, so it cannot leak.

## Why not thread `ctx` through everything (as originally suggested)

`processCompleteSubtree` has no `ctx` parameter and is reached through the
**public `AddDirectly` / `AddNodesDirectly` interface methods** (which have mocks
and tests, some of which call them *before* `Start()` runs). Threading `ctx`
through the call chain would change the public `Interface`, its mocks, and
existing callers/tests — a large, rippling diff.

Storing the lifecycle context on the struct (consistent with the existing
`cancelPtr atomic.Pointer` pattern) achieves the same correctness with a minimal,
localized change and no public-API churn.

## Behavioural notes

- On cancellation, the affected paths now return a `ProcessingError` rather than
  blocking. In the `Start()` loop this is logged; during shutdown that is the
  intended outcome.
- Pre-`Start()` behaviour is unchanged (Background fallback → same blocking send
  as before).

## Tests

Added `TestNewSubtreeChanContextCancellation` covering an **unconsumed**
`newSubtreeChan` with an already-cancelled processor context:

- `sendNewSubtree` returns instead of blocking.
- `processCompleteSubtree` (complete-subtree rotation) returns instead of blocking.
- `parallelBuildRemainderSubtrees` (reorg announcement) returns instead of blocking.

Each subtest fails (via timeout) if the call blocks, proving the deadlock is gone.

```
go build ./services/blockassembly/...                                  # pass
go test -race -tags testtxmetacache \
  -run TestNewSubtreeChanContextCancellation \
  ./services/blockassembly/subtreeprocessor/                           # pass (3/3 subtests)
```

## Risk

Low. The change is localized to two send sites plus two small helpers and one
struct field; it adds cancellation awareness without altering the success path,
and preserves pre-`Start()` semantics.
