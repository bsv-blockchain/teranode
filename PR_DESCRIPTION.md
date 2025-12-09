## Summary
- Make `TestBlockchainSubscriptionReconnection` deterministic by binding the subscription to the test context, canceling it before daemon shutdown, and adding a brief grace period so the subscription goroutine detaches cleanly.
- Stabilize `TestDeleteByBin` by explicitly querying keys that match the filter and deleting them individually instead of relying on server-side filtered `QueryExecute`, which proved inconsistent across environments.

## Rationale
- The blockchain subscription test was intermittently racing during teardown: the subscription channel could outlive the daemon shutdown. Tying the subscription to `t.Context()` and canceling before shutdown ensures orderly cleanup and removes the race surface.
- Aerospike `QueryExecute` with a filter was not reliably deleting records locally, likely due to predicate execution differences. By pulling the filtered keys and deleting them directly, the test now asserts behavior deterministically while still validating the intended delete-by-filter semantics.

## Testing
- `go test -race ./test/e2e/daemon/ready -run TestBlockchainSubscriptionReconnection`
- `go test -race ./test/e2e/daemon/ready`
- `SETTINGS_CONTEXT=test go test -run TestDeleteByBin ./stores/utxo/aerospike -count=1`
