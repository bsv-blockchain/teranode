// //go:build aerospike

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
//   - totalUtxos: Total number of UTXOs in the transaction
//   - recordUtxos: Total number of UTXO in this record
//   - spentUtxos: Number of spent UTXOs in this record
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
	"context"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
)

// Unspend operations handle reverting spent UTXOs back to an unspent state.
// This is primarily used during blockchain reorganizations to handle
// transaction rollbacks.
//
// # Operation Flow
//
//	Validation → Lua Script → Update Records → Handle External Storage
//
// The operation:
//   1. Verifies UTXO exists
//   2. Clears spending transaction ID
//   3. Updates record counts for pagination
//   4. Manages external storage TTL
//   5. Updates metrics

// Unspend reverts spent UTXOs to unspent state.
// Parameters:
//   - ctx: Context for cancellation
//   - spends: Array of UTXOs to unspend
//
// Returns error if:
//   - Context is cancelled
//   - Timeout occurs
//   - UTXO doesn't exist
//   - Operation fails
//
// Thread Safety:
//   - Uses Lua scripts for atomic operations
//   - Handles concurrent unspend operations
//   - Coordinates with external storage
func (s *Store) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsUnspendable ...bool) (err error) {
	return s.unspend(ctx, spends, flagAsUnspendable...)
}

// unspend implements the core unspend logic.
// For each UTXO:
//  1. Checks context cancellation
//  2. Logs operation details
//  3. Executes Lua script
//  4. Handles response
func (s *Store) unspend(ctx context.Context, spends []*utxo.Spend, flagAsUnspendable ...bool) (err error) {
	for i, spend := range spends {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return errors.NewStorageError("timeout un-spending %d of %d utxos", i, len(spends))
			}

			return errors.NewStorageError("context cancelled un-spending %d of %d utxos", i, len(spends))
		default:
			if spend != nil {
				var txID string
				if spend.SpendingTxID != nil {
					txID = spend.SpendingTxID.String()
				}

				s.logger.Warnf("un-spending utxo %s of tx %s:%d, spending tx: %s", spend.UTXOHash.String(), spend.TxID.String(), spend.Vout, txID)

				if err = s.unspendLua(spend); err != nil {
					// just return the raw error, should already be wrapped
					return err
				}
			}
		}
	}

	return nil
}

// unspendLua executes the Lua script for a single UTXO unspend.
// The operation:
//  1. Calculates key and offset
//  2. Executes Lua script
//  3. Processes response
//  4. Updates record counts
//  5. Manages external storage
//
// Lua Return Values:
//   - OK:NOTALLSPENT - Success, some UTXOs still unspent
//   - ERROR:* - Various error conditions
//
// Metrics:
//   - prometheusUtxoMapReset: Successful unspends
//   - prometheusUtxoMapErrors: Failed operations
func (s *Store) unspendLua(spend *utxo.Spend) error {
	policy := util.GetAerospikeWritePolicy(s.settings, 0, aerospike.TTLDontExpire)

	sUtxoBatchSizeUint32, err := util.SafeIntToUint32(s.utxoBatchSize)
	if err != nil {
		s.logger.Errorf("Could not convert utxoBatchSize (%d) to uint32", s.utxoBatchSize)
	}

	keySource := uaerospike.CalculateKeySource(spend.TxID, spend.Vout/sUtxoBatchSizeUint32)

	key, aErr := aerospike.NewKey(s.namespace, s.setName, keySource)
	if aErr != nil {
		prometheusUtxoMapErrors.WithLabelValues("Reset", aErr.Error()).Inc()
		return errors.NewProcessingError("error in aerospike NewKey", aErr)
	}

	offset := s.calculateOffsetForOutput(spend.Vout)

	ret, aErr := s.client.Execute(policy, key, LuaPackage, "unspend",
		aerospike.NewIntegerValue(int(offset)), // vout adjusted for utxoBatchSize
		aerospike.NewValue(spend.UTXOHash[:]),  // utxo hash
	)
	if aErr != nil {
		prometheusUtxoMapErrors.WithLabelValues("Reset", aErr.Error()).Inc()
		return errors.NewStorageError("error in aerospike unspend record", aErr)
	}

	responseMsg, ok := ret.(string)
	if !ok {
		prometheusUtxoMapErrors.WithLabelValues("Reset", "response not string").Inc()
		return errors.NewStorageError("error in aerospike unspend record", aErr)
	}

	res, err := s.ParseLuaReturnValue(responseMsg)
	if err != nil {
		prometheusUtxoMapErrors.WithLabelValues("Reset", "error parsing response").Inc()
		return errors.NewProcessingError("error parsing response %s", responseMsg, err)
	}

	switch res.ReturnValue {
	case LuaOk:
		if res.Signal == LuaNotAllSpent {
			if err := s.SetTTLForChildRecords(spend.TxID, res.ChildCount, aerospike.TTLDontExpire); err != nil {
				return err
			}

			if err := s.setTTLExternalTransaction(s.ctx, spend.TxID, 0); err != nil {
				return err
			}
		}

	case LuaError:
		prometheusUtxoMapErrors.WithLabelValues("Reset", "error response").Inc()
		return errors.NewStorageError("error in aerospike unspend record: %s", responseMsg)

	default:
		prometheusUtxoMapErrors.WithLabelValues("Reset", "default response").Inc()
		return errors.NewStorageError("error in aerospike unspend record: %s", responseMsg)
	}

	prometheusUtxoMapReset.Inc()

	return nil
}

// txa & txb both spending txp

// processConflicting(txb)
//    mark txa conflicting
//    change spend of txp
//    mark txb not conflicting
