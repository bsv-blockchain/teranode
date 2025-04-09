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

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

// SetMinedMulti updates the block references for multiple transactions in batch.
// This operation marks transactions as mined in a specific block using Lua scripts
// for atomic updates.
//
// # Operation Details
//
// For each transaction:
//  1. Creates a batch UDF operation to update the block reference
//  2. Executes all updates in a single batch operation
//  3. Handles TTL settings for record expiration
//  4. Tracks metrics for successful and failed updates
//
// The operation is idempotent and handles cases where:
//   - Transaction doesn't exist (silently continues)
//   - Transaction is already mined in the block
//   - Partial batch failures occur
//
// # Error Handling
//
// The function aggregates errors across the batch and:
//   - Continues on KEY_NOT_FOUND errors (transaction deleted)
//   - Counts successful and failed updates
//   - Provides detailed error context per transaction
//   - Updates error metrics for monitoring
//
// # Performance
//
// Uses batch operations to optimize performance:
//   - Single network round trip for multiple updates
//   - Concurrent processing via Lua scripts
//   - No read-modify-write cycle required
//   - Efficient error handling without transaction rollback
//
// Parameters:
//   - ctx: Context for tracing and cancellation
//   - hashes: Array of transaction hashes to update
//   - blockID: Block height where transactions were mined
//
// Returns:
//   - error: Aggregated errors from batch operation or nil if successful
//
// Example:
//
//	hashes := []*chainhash.Hash{tx1Hash, tx2Hash, tx3Hash}
//	err := store.SetMinedMulti(ctx, hashes, blockHeight)
//	if err != nil {
//	    // Handle errors, some updates may have succeeded
//	}
//
// Metrics:
//   - prometheusTxMetaAerospikeMapSetMinedBatch: Batch operation count
//   - prometheusTxMetaAerospikeMapSetMinedBatchN: Successful updates
//   - prometheusTxMetaAerospikeMapSetMinedBatchErrN: Failed updates
func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) error {
	_, _, deferFn := tracing.StartTracing(ctx, "aerospike:SetMinedMulti2")
	defer deferFn()

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	// math.MaxUint32 - 1 does not update expiration of the record
	policy := util.GetAerospikeBatchWritePolicy(s.settings, 0, aerospike.TTLDontUpdate)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, len(hashes))

	for idx, hash := range hashes {
		key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
		if err != nil {
			return errors.NewProcessingError("aerospike NewKey error", err)
		}

		batchUDFPolicy := aerospike.NewBatchUDFPolicy()

		batchRecords[idx] = aerospike.NewBatchUDF(
			batchUDFPolicy,
			key,
			LuaPackage,
			"setMined",
			aerospike.NewValue(minedBlockInfo.BlockID),
			aerospike.NewValue(minedBlockInfo.BlockHeight),
			aerospike.NewValue(minedBlockInfo.SubtreeIdx),
			aerospike.NewValue(uint32(s.expiration.Seconds())), // ttl
		)
	}

	err := s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		return errors.NewStorageError("aerospike BatchOperate error", err)
	}

	prometheusTxMetaAerospikeMapSetMinedBatch.Inc()

	var errs error

	okUpdates := 0
	nrErrors := 0

	for idx, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err
		if err != nil {
			var aErr *aerospike.AerospikeError
			if errors.As(err, &aErr) && aErr != nil && aErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
				// the tx Meta does not exist anymore, so we do not have to set the mined status
				continue
			}

			errs = errors.NewStorageError("aerospike batchRecord error: %s", hashes[idx].String(), errors.Join(errs, err))

			nrErrors++
		} else {
			response := batchRecord.BatchRec().Record
			if response != nil && response.Bins != nil && response.Bins["SUCCESS"] != nil {
				responseMsg, ok := response.Bins["SUCCESS"].(string)
				if ok {
					res, err := s.ParseLuaReturnValue(responseMsg)
					if err != nil {
						nrErrors++
						errs = errors.NewError("aerospike batchRecord %s ParseLuaReturnValue error: %s", hashes[idx].String(), errors.Join(errs, err))
					} else {
						switch res.ReturnValue {
						case LuaOk:
							switch res.Signal {
							case LuaAllSpent:
								if err := s.handleExtraRecords(ctx, hashes[idx], 1); err != nil {
									errs = errors.Join(errs, err)
								}

							case LuaTTLSet:
								if err := s.SetTTLForChildRecords(hashes[idx], res.ChildCount, uint32(s.expiration.Seconds())); err != nil {
									errs = errors.Join(errs, err)
								}

								if err := s.setTTLExternalTransaction(ctx, hashes[idx], s.expiration); err != nil {
									errs = errors.Join(errs, err)
								}

							case LuaTTLUnset:
								if err := s.SetTTLForChildRecords(hashes[idx], res.ChildCount, aerospike.TTLDontExpire); err != nil {
									errs = errors.Join(errs, err)
								}

								if err := s.setTTLExternalTransaction(ctx, hashes[idx], 0); err != nil {
									errs = errors.Join(errs, err)
								}
							}

							okUpdates++
						default:
							nrErrors++
							errs = errors.NewError("aerospike batchRecord %s bins[SUCCESS] msg: %s", hashes[idx].String(), responseMsg, errors.Join(errs, batchRecord.BatchRec().Err))
						}
					}
				}
			} else {
				nrErrors++
				errs = errors.NewError("aerospike batchRecord %s !bins[SUCCESS] err: %s", hashes[idx].String(), errors.Join(errs, batchRecord.BatchRec().Err))
			}
		}
	}

	prometheusTxMetaAerospikeMapSetMinedBatchN.Add(float64(okUpdates))

	if errs != nil || nrErrors > 0 {
		prometheusTxMetaAerospikeMapSetMinedBatchErrN.Add(float64(nrErrors))
		return errors.NewError("aerospike batchRecord errors", errs)
	}

	return nil
}
