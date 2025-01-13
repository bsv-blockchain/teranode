// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
// It offers high performance, distributed storage capabilities with support for large-scale
// UTXO sets and complex operations like freezing, reassignment, and batch processing.
//
// # Architecture
//
// The implementation uses a combination of Aerospike Key-Value store and Lua scripts
// for atomic operations. Transactions are stored with the following structure:
//   - Main Record: Contains transaction metadata and up to 20,000 UTXOs
//   - Pagination Records: Additional records for transactions with >20,000 outputs
//   - External Storage: Optional blob storage for large transactions
//
// # Features
//
//   - Efficient UTXO lifecycle management (create, spend, unspend)
//   - Support for batched operations with LUA scripting
//   - Automatic cleanup of spent UTXOs through TTL
//   - Alert system integration for freezing/unfreezing UTXOs
//   - Metrics tracking via Prometheus
//   - Support for large transactions through external blob storage
//
// # Usage
//
//	store, err := aerospike.New(ctx, logger, settings, &url.URL{
//	    Scheme: "aerospike",
//	    Host:   "localhost:3000",
//	    Path:   "/test/utxos",
//	    RawQuery: "expiration=3600&set=txmeta",
//	})
//
// # Database Structure
//
// Normal Transaction:
//   - inputs: Transaction input data
//   - outputs: Transaction output data
//   - utxos: List of UTXO hashes
//   - nrUtxos: Total number of UTXOs
//   - spentUtxos: Number of spent UTXOs
//   - blockIDs: Block references
//   - isCoinbase: Coinbase flag
//   - spendingHeight: Coinbase maturity height
//   - frozen: Frozen status
//
// Large Transaction with External Storage:
//   - Same as normal but with external=true
//   - Transaction data stored in blob storage
//   - Multiple records for >20k outputs
//
// # Thread Safety
//
// The implementation is fully thread-safe and supports concurrent access through:
//   - Atomic operations via Lua scripts
//   - Batched operations for better performance
//   - Lock-free reads with optimistic concurrency
package aerospike

import (
	"encoding/binary"
	"testing"

	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
)

func TestCalculateKeySource(t *testing.T) {
	hash := chainhash.HashH([]byte("test"))

	h := uaerospike.CalculateKeySource(&hash, 0)
	assert.Equal(t, hash[:], h)

	h = uaerospike.CalculateKeySource(&hash, 1)
	extra := make([]byte, 4)
	binary.LittleEndian.PutUint32(extra, uint32(1))
	assert.Equal(t, append(hash[:], extra...), h)

	h = uaerospike.CalculateKeySource(&hash, 2)
	extra = make([]byte, 4)
	binary.LittleEndian.PutUint32(extra, uint32(2))
	assert.Equal(t, append(hash[:], extra...), h)
}

func TestCalculateOffsetOutput(t *testing.T) {
	db := &Store{utxoBatchSize: 20_000}

	offset := db.calculateOffsetForOutput(0)
	assert.Equal(t, uint32(0), offset)

	offset = db.calculateOffsetForOutput(30_000)
	assert.Equal(t, uint32(10_000), offset)

	offset = db.calculateOffsetForOutput(40_000)
	assert.Equal(t, uint32(0), offset)
}
