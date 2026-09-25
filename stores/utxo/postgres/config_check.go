package postgres

import (
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
)

// numConnectionBatchers is the count of pgxpool-backed batchers in the postgres UTXO
// store that each independently dispatch up to BatcherMaxConcurrent in-flight batch
// callbacks: create, spend, get, unlock. Update this if a batcher is added or removed.
const numConnectionBatchers = 4

// checkBatcherPoolConfig validates that the connection pool can serve the configured
// batcher concurrency. It is pure (no DB, no logger) so it can be unit-tested directly.
//
//   - maxConcurrent <= 0: the batcher is uncapped; pgxpool.Acquire provides natural
//     back-pressure, so there is nothing to validate.
//   - maxConcurrent > maxConns: a single batcher can check out every connection in the
//     pool, so its in-flight dispatches deadlock on pgxpool.Acquire (the betfair-pc
//     mainnet outage: maxConcurrent=512 vs pool=100). Returned as an error.
//   - numBatchers*maxConcurrent > maxConns (but maxConcurrent <= maxConns): all batchers
//     saturating at once can starve the pool. Returned as a warning string.
//   - otherwise: empty warning, nil error.
func checkBatcherPoolConfig(maxConns int32, maxConcurrent, numBatchers int) (string, error) {
	if maxConcurrent <= 0 {
		return "", nil
	}

	poolMax := int(maxConns)

	if maxConcurrent > poolMax {
		return "", errors.NewConfigurationError(
			"unsafe postgres UTXO store config: utxostore_batcherMaxConcurrent=%d exceeds pool_max_conns=%d "+
				"- a single batcher can exhaust the pool and deadlock (all dispatches block on pgxpool.Acquire). "+
				"Raise pool_max_conns to >= %d or lower utxostore_batcherMaxConcurrent to <= %d",
			maxConcurrent, poolMax, maxConcurrent, poolMax)
	}

	if numBatchers*maxConcurrent > poolMax {
		return fmt.Sprintf(
			"postgres UTXO store: %d batchers x utxostore_batcherMaxConcurrent=%d (=%d) exceeds pool_max_conns=%d. "+
				"Fine for normal load (batchers rarely all saturate); for high throughput raise postgres "+
				"max_connections and pool_max_conns together",
			numBatchers, maxConcurrent, numBatchers*maxConcurrent, poolMax), nil
	}

	return "", nil
}
