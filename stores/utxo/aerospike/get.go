// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
// It offers high performance, distributed storage capabilities with support for large-scale
// UTXO sets and complex operations like freezing, reassignment, and batch processing.
//
// # Architecture
//
// The implementation uses a combination of Aerospike Key-Value store and Lua scripts
// for atomic operations. Transactions are stored with the following structure:
//   - Main Record: Contains transaction metadata and up to utxostore_utxoBatchSize UTXOs (default 128)
//   - Pagination Records: Additional records for transactions with more outputs than utxostore_utxoBatchSize (default 128)
//   - External Storage: Optional blob storage for large transactions
//
// # Features
//
//   - Efficient UTXO lifecycle management (create, spend, unspend)
//   - Support for batched operations with LUA scripting
//   - Automatic cleanup of spent UTXOs through DAH
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
//   - totalUtxos: Total number of UTXOs
//   - spentUtxos: Number of spent UTXOs
//   - blockIDs: Block references
//   - isCoinbase: Coinbase flag
//   - spendingHeight: Coinbase maturity height
//   - frozen: Frozen status
//
// Large Transaction with External Storage:
//   - Same as normal but with external=true
//   - Transaction data stored in blob storage
//   - Multiple records when outputs exceed utxostore_utxoBatchSize
//
// # Thread Safety
//
// The implementation is fully thread-safe and supports concurrent access through:
//   - Atomic operations via Lua scripts
//   - Batched operations for better performance
//   - Lock-free reads with optimistic concurrency
package aerospike

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"slices"
	"sync/atomic"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/stores/utxo/txparse"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

var (
	gocoreStat                  = gocore.NewStat("Aerospike")
	previousOutputsDecorateStat = gocoreStat.NewStat("PreviousOutputsDecorate").AddRanges(0, 1, 100, 1_000, 10_000, 100_000)
)

const errCouldNotReadInput = "could not read input"

// classifyRecordError picks the error class for a failure to assemble a
// transaction's metadata out of its Aerospike bins.
//
// The bins were written from our own validated bytes, so failing to read them
// back means this node's stored copy is wrong. That is a storage fault, not
// evidence that the transaction is consensus-invalid — the same reasoning that
// applies to the external blob in getExternalTransaction (issue 1439).
//
// Every processor reached through this helper already codes its store-attributable
// failures — a missing or wrong-typed bin, an input that will not parse back — as
// either a StorageError or a ProcessingError. (fields.Utxos, whose paginated
// extra-record read is a live Aerospike client.Get and so fails on any transient
// store problem, has its own classifier: classifyUTXOReadError.) The callers used
// to re-wrap all of them in TxInvalid regardless. Because errors.Is walks the whole chain and
// every downstream switch tests ErrTxInvalid first, that wrap presented a
// transient store read as a proven consensus violation: the block persisted
// invalid, its descendants poisoned through the parent-invalid cascade, and the
// serving peer flagged malicious. Preserving the inner class keeps TxInvalid for
// the case it is meant for — bins that were read successfully but do not make a
// valid transaction.
//
// Context errors and local service faults are treated the same way for the same
// reason: neither is evidence about the block. ErrProcessing is included because
// every producer of it on these paths is unambiguously ours rather than the
// block's: processInputsToTxInpoints failing to read back an input it wrote, and
// getAllExtraUTXOs failing to build an Aerospike key.
func classifyRecordError(message string, err error) error {
	if errors.IsTransientLocalError(err) || errors.IsContextError(err) || errors.Is(err, errors.ErrProcessing) {
		return errors.NewStorageError(message, err)
	}

	return errors.NewTxInvalidError(message, err)
}

const (
	// minSerializedOutputSize is the smallest an output can be on the wire: eight
	// bytes of satoshis plus a one-byte varint length for an empty locking script.
	minSerializedOutputSize = 9

	// defaultExcessiveBlockSize mirrors the documented default of the
	// excessiveblocksize policy setting (4 GB). Used only when the setting is
	// unset or explicitly 0, which for block acceptance means "no limit" but here
	// would mean "no bound on an allocation sized from local storage".
	// Typed uint64 rather than left untyped, so it cannot silently overflow the
	// int it would otherwise be inferred as on a 32-bit build.
	defaultExcessiveBlockSize uint64 = 4 * 1024 * 1024 * 1024

	// maxExternalOutputSlots is the absolute ceiling on the reconstructed output
	// slice: ~16.7M entries, 128 MB of pointers.
	//
	// The policy-derived count below is a correctness ceiling — it never rejects
	// a parent the node could have accepted — but it is not a resource ceiling.
	// At the shipped excessiveblocksize of 10 GB it permits ~1.19e9 slots, 8.9 GB
	// of pointers, so a single flipped bit still OOMs a 32 GB node instead of
	// producing the storage fault this function exists to produce. Bounding the
	// allocation is worth the theoretical rejection: a parent above this needs
	// >144 MB of standard serialization just for its outputs.
	maxExternalOutputSlots uint64 = 1 << 24
)

// maxExternalOutputCount bounds the number of outputs this node will reconstruct
// from an externally stored outputs-only blob.
//
// Two ceilings apply and the lower wins.
//
// The derived one: a transaction cannot be larger than the largest block this
// node would accept, and each of its outputs costs at least
// minSerializedOutputSize bytes, so no acceptable transaction can carry an
// output index at or above excessiveBlockSize/minSerializedOutputSize. That is a
// correctness ceiling — it never rejects a parent that could legitimately exist.
//
// The absolute one, maxExternalOutputSlots: a resource ceiling, because the
// derived value is not one. It is what actually bounds the allocation.
//
// The derived value is floored at defaultExcessiveBlockSize so the bound is
// monotonic in the setting. Without that floor, lowering excessiveblocksize
// would retroactively reject an outputs blob written under a higher value, and
// since both writers swallow ErrBlobAlreadyExists nothing would ever rewrite it:
// a config edit would become a permanent read failure presenting as a rotted
// blob. Historic BSV values such as 128 MB are low enough to reach that band.
// With the floor in place the absolute ceiling binds for every documented policy
// value, and the derivation only matters if that ceiling is ever raised.
func maxExternalOutputCount(tSettings *settings.Settings) uint64 {
	excessiveBlockSize := defaultExcessiveBlockSize

	if tSettings != nil && tSettings.Policy != nil && tSettings.Policy.ExcessiveBlockSize > 0 {
		excessiveBlockSize = uint64(tSettings.Policy.ExcessiveBlockSize) //nolint:gosec // guarded > 0 on the line above
	}

	if excessiveBlockSize < defaultExcessiveBlockSize {
		excessiveBlockSize = defaultExcessiveBlockSize
	}

	count := excessiveBlockSize / minSerializedOutputSize

	if count > maxExternalOutputSlots {
		count = maxExternalOutputSlots
	}

	return count
}

// batchGetItemData holds the result of a batch get operation
type batchGetItemData struct {
	Data *meta.Data // Retrieved data
	Err  error      // Any error encountered
}

// batchGetItem represents a single item in a batch get operation
type batchGetItem struct {
	hash      chainhash.Hash     // Transaction hash
	fields    []fields.FieldName // Fields to retrieve
	group     *completion.Group  // Shared completion group for the submitting get() call
	completed atomic.Bool        // guards exactly-once completion
	result    *batchGetItemData  // caller-allocated result slot; written by the CAS winner, after the CAS and before group.Done() (see complete)
}

// complete writes data into the item's caller-allocated result slot and marks
// the shared group's completion counter. Idempotent: only the first call has
// any effect, so a panic-recovery sweep over an already-completed item never
// double-signals or races a second write into the slot.
//
// The slot write happens inside the CAS-winner branch, after the CAS succeeds
// and before group.Done(); group.Done()'s close(done) synchronizes-with a nil
// group.Wait(), making the slot safe to read only after group.Wait returns
// nil. completed is the exactly-once guard (CAS), not a publication flag by
// itself.
func (it *batchGetItem) complete(data batchGetItemData) {
	if it.completed.CompareAndSwap(false, true) {
		if it.result != nil {
			*it.result = data
		}
		if it.group != nil {
			it.group.Done()
		}
	}
}

// batchOutpoint represents a single outpoint in a batch previous output operation.
// It is used to efficiently retrieve previous output data for transaction inputs
// by batching multiple requests together to optimize database access.
type batchOutpoint struct {
	outpoint  *bt.Input         // The previous output to retrieve data for
	group     *completion.Group // Shared completion group for the submitting decorate call
	completed atomic.Bool       // guards exactly-once completion
	result    error             // written by the CAS winner, after the CAS and before group.Done(); see complete
}

// complete writes err into the item's result slot and marks the shared group's
// completion counter. Idempotent: only the first call has any effect, so a
// panic-recovery sweep over an already-completed item never double-signals or
// races a second write into result.
func (it *batchOutpoint) complete(err error) {
	if it.completed.CompareAndSwap(false, true) {
		it.result = err
		if it.group != nil {
			it.group.Done()
		}
	}
}

// GetSpend checks if a UTXO has been spent and returns its current status.
// This method performs efficient UTXO status verification by querying the Aerospike
// database for the specified transaction output.
//
// Parameters:
//   - ctx: Context for cancellation (currently unused but kept for interface consistency)
//   - spend: The UTXO spend information containing transaction ID and output index
//
// Returns:
//   - *utxo.SpendResponse: Response containing UTXO status and spending details, or nil if error
//   - error: Any error encountered during the operation
//
// The response includes:
//   - Current UTXO status (OK, SPENT, FROZEN, etc)
//   - Spending transaction ID if spent
//   - Spending transaction Vin (input index) if spent
//   - Lock time if applicable
//
// This operation verifies:
//   - UTXO exists in the database
//   - UTXO hash matches the expected value
//   - Frozen status for compliance operations
//   - Current spend state and spending transaction details
func (s *Store) GetSpend(_ context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	prometheusUtxoMapGet.Inc()

	keySource := uaerospike.CalculateKeySource(spend.TxID, spend.Vout, s.utxoBatchSize)

	key, aErr := aerospike.NewKey(s.namespace, s.setName, keySource)
	if aErr != nil {
		if e, ok := aErr.(*aerospike.AerospikeError); ok {
			prometheusUtxoMapErrors.WithLabelValues("GetSpend", e.ResultCode.String()).Inc()
		} else {
			prometheusUtxoMapErrors.WithLabelValues("GetSpend", "unknown").Inc()
		}
		s.logger.Errorf("Failed to init new aerospike key: %v\n", aErr)

		return nil, aErr
	}

	policy := util.GetAerospikeReadPolicy(s.settings)
	// we only want to read from the master for tx metadata, due to blockIDs being updated
	// however we still want to read from the replica for the utxos in case of aerospike failures
	policy.ReplicaPolicy = aerospike.SEQUENCE

	value, aErr := s.client.Get(policy, key, fields.FieldNamesToStrings(binNames)...)
	if aErr != nil {
		if e, ok := aErr.(*aerospike.AerospikeError); ok {
			prometheusUtxoMapErrors.WithLabelValues("GetSpend", e.ResultCode.String()).Inc()
		} else {
			prometheusUtxoMapErrors.WithLabelValues("GetSpend", "unknown").Inc()
		}

		if errors.Is(aErr, aerospike.ErrKeyNotFound) {
			return &utxo.SpendResponse{
				Status: int(utxo.Status_NOT_FOUND),
			}, nil
		}

		s.logger.Errorf("Failed to get aerospike key: %v\n", aErr)

		return nil, aErr
	}

	var (
		spendingData *spendpkg.SpendingData
		spendableIn  int
		conflicting  bool
		locked       bool
	)

	if value != nil {
		utxos, ok := value.Bins[fields.Utxos.String()].([]interface{})
		if ok {
			// The Utxos bin holds only the actual outputs for this batch, not a
			// fixed-size slot table padded to utxoBatchSize. A caller passing a
			// vout greater than the tx's output count (possible via the HTTP
			// /api/v1/utxo and /api/v1/utxos endpoints, which no longer
			// pre-validate vout against the output count) would index past the
			// slice and panic with index-out-of-range — crashing the asset
			// process when the panic surfaces in an errgroup goroutine outside
			// echo's recover middleware. Treat it as NOT_FOUND, mirroring
			// ErrKeyNotFound above.
			idx := spend.Vout % uint32(s.utxoBatchSize)
			if int(idx) >= len(utxos) {
				return &utxo.SpendResponse{
					Status: int(utxo.Status_NOT_FOUND),
				}, nil
			}

			b, ok := utxos[idx].([]byte)
			if ok {
				if len(b) < 32 {
					return nil, errors.NewProcessingError("invalid utxo hash length", nil)
				}

				// Verify the caller-supplied hash matches the stored one. When the
				// caller passes nil (e.g. the bulk /api/v1/utxos endpoint, which
				// intentionally avoids fetching the full transaction to recompute
				// it) we trust the stored hash — the record was located by primary
				// key (txid, vout) and the stored hash is canonical.
				if spend.UTXOHash != nil {
					utxoHash := chainhash.Hash(b[:32])
					if !utxoHash.IsEqual(spend.UTXOHash) {
						return nil, errors.NewProcessingError("utxo hash mismatch", nil)
					}
				}

				if len(b) == 68 {
					txID, err := chainhash.NewHash(b[32:64])
					if err != nil {
						return nil, errors.NewProcessingError("chain hash error", err)
					}

					vin := binary.LittleEndian.Uint32(b[64:])

					spendingData = spendpkg.NewSpendingData(txID, int(vin))
				}
			}
		}

		utxoSpendableInBin, found := value.Bins[fields.UtxoSpendableIn.String()]
		if found {
			utxoSpendableIn, ok := utxoSpendableInBin.(map[interface{}]interface{})
			if !ok {
				return nil, errors.NewProcessingError("invalid utxoSpendableIn", nil)
			}

			spendableInIfc := utxoSpendableIn[int(spend.Vout)]
			if spendableInIfc != nil {
				spendableIn, ok = spendableInIfc.(int)
				if !ok {
					return nil, errors.NewProcessingError("invalid utxoSpendableIn", nil)
				}
			}
		}

		conflictingBin, found := value.Bins[fields.Conflicting.String()]
		if found {
			conflicting, ok = conflictingBin.(bool)
			if !ok {
				return nil, errors.NewProcessingError("invalid conflicting", nil)
			}
		}

		lockedBin, found := value.Bins[fields.Locked.String()]
		if found {
			locked, ok = lockedBin.(bool)
			if !ok {
				return nil, errors.NewProcessingError("invalid locked", nil)
			}
		}
	}

	utxoStatus := utxo.CalculateUtxoStatus2(spendingData)

	// check utxo is spendable
	if spendableIn != 0 && spendableIn > int(s.GetBlockHeight()) {
		utxoStatus = utxo.Status_IMMATURE
	}

	// check if frozen
	if spendingData != nil && bytes.Equal(spendingData.Bytes(), frozenUTXOBytes) {
		utxoStatus = utxo.Status_FROZEN
		// this is needed in for instance conflict resolution where we check the spending data
		spendingData = spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, int(spend.Vout))
	}

	if conflicting {
		utxoStatus = utxo.Status_CONFLICTING
	}

	if locked {
		utxoStatus = utxo.Status_LOCKED
	}

	return &utxo.SpendResponse{
		Status:       int(utxoStatus),
		SpendingData: spendingData,
	}, nil
}

// Get retrieves transaction data with optional field selection.
// This method provides flexible access to transaction data stored in Aerospike,
// allowing callers to specify which fields to retrieve for optimal performance.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - hash: Transaction hash to retrieve data for
//   - fields: Optional variadic list of specific fields to retrieve. If empty, defaults to all metadata fields with transaction data
//
// Returns:
//   - *meta.Data: Complete transaction metadata including specified fields, or nil if transaction not found
//   - error: Any error encountered during retrieval operation
//
// Field Selection:
// When no fields are specified, retrieves all metadata fields plus transaction data.
// When specific fields are provided, only those fields are retrieved from the database,
// which can significantly improve performance for large transactions.
func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error) {
	bins := utxo.MetaFieldsWithTx
	if len(fields) > 0 {
		bins = fields
	}

	return s.get(ctx, hash, bins)
}

// GetMeta retrieves only transaction metadata without the full transaction data.
// This is an optimized version of Get that excludes transaction body data to improve
// performance when only metadata fields are needed.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - hash: Transaction hash to retrieve metadata for
//   - data: Pre-allocated meta.Data struct to populate with the retrieved metadata
//
// Returns:
//   - error: Any error encountered during retrieval
//
// This method is more efficient than Get() when you only need metadata fields such as:
//   - UTXO information and spend status
//   - Block height and block ID references
//   - Transaction flags (coinbase, frozen status)
//   - Subtree indices and validation data
func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	result, err := s.get(ctx, hash, utxo.MetaFields)
	if err != nil {
		return err
	}

	if result != nil {
		*data = *result
	}

	return nil
}

// get is an internal method that retrieves transaction data using batch processing.
// It queues the request for batch processing to optimize database access by reducing
// the number of individual database queries and improving overall throughput.
//
// This method uses a channel-based batching system where multiple get requests are
// collected and processed together in a single database operation.
//
// Parameters:
//   - ctx: Context for cancellation (currently unused but kept for interface consistency)
//   - hash: Transaction hash to retrieve data for
//   - bins: Field names to retrieve from the database (specific Aerospike bins)
//
// Returns:
//   - *meta.Data: Transaction metadata containing the requested fields, or nil if transaction not found
//   - error: Any error encountered during retrieval, including database connection errors or data parsing failures
//
// Implementation Details:
// The method creates a batchGetItem with the request parameters and sends it to the
// getBatcher for processing. It then waits on a shared completion.Group and reads
// the result from the item's result slot once the wait returns.
func (s *Store) get(ctx context.Context, hash *chainhash.Hash, bins []fields.FieldName) (*meta.Data, error) {
	// Abort early on a cancelled context (e.g. graceful shutdown) before touching
	// the batcher. Store.Close may have already closed the get batcher's input
	// channel, and enqueuing into it then panics "send on closed channel". The
	// putGetBatch guard below converts that panic into a returned error for the
	// residual race where the store closes while this caller's context is live.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var res batchGetItemData

	group := completion.NewGroup(1)
	item := &batchGetItem{hash: *hash, fields: bins, group: group, result: &res}

	if s.getBatcher != nil {
		if err := s.putGetBatch(ctx, item); err != nil {
			return nil, err
		}
	} else {
		// if the batcher is disabled, we still want to process the request in a go routine
		go func() {
			s.sendGetBatch([]*batchGetItem{item})
		}()
	}

	// One shared wait for the item, bounded so a wedged batcher (e.g. a stuck v8
	// batch op, or a dispatch fn that failed to signal) cannot pin this goroutine
	// for the life of the process. The legacy/validation callers thread a
	// deadline-less context down here, so ctx.Done() alone is not enough. A
	// non-positive batcherWait (e.g. a Store built without New) disables the
	// timeout arm — Group.Wait then waits on ctx/completion only — preserving the
	// original behaviour.
	if waitErr := group.Wait(ctx, s.batcherWait); waitErr != nil {
		// Do not read res on the error path: the dispatcher may still be writing
		// to the slot after we have given up waiting on it.
		if ctxErr := ctx.Err(); ctxErr != nil {
			prometheusTxMetaAerospikeMapErrors.WithLabelValues("Get", "ContextCanceled").Inc()
			return nil, ctxErr
		}

		prometheusTxMetaAerospikeMapErrors.WithLabelValues("Get", "BatchTimeout").Inc()
		return nil, errors.NewServiceUnavailableError("aerospike get batch did not complete within %s", s.batcherWait)
	}

	// group.Wait returned nil: the item has completed, so the slot is safe to read.
	if res.Err != nil {
		if e, ok := res.Err.(*errors.Error); ok {
			prometheusTxMetaAerospikeMapErrors.WithLabelValues("Get", e.Code().Enum().String()).Inc()
		} else {
			prometheusTxMetaAerospikeMapErrors.WithLabelValues("Get", "unknown").Inc()
		}
	} else {
		prometheusTxMetaAerospikeMapGet.Inc()
	}

	return res.Data, res.Err
}

// putGetBatch enqueues item into the get batcher, converting the
// "send on closed channel" panic — which go-batcher v2.0.6 raises when Put is
// called after Close — into a returned error. Store.Close closes the get batcher
// during shutdown while external callers (e.g. an in-flight block-validation
// goroutine in checkParentsExistOnChain) may still be calling Get. That race
// must abort the read, not crash the process.
func (s *Store) putGetBatch(ctx context.Context, item *batchGetItem) error {
	return safeBatcherPutCtx(s.getBatcher, ctx, item, "get")
}

// getTxFromBins reconstructs a Bitcoin transaction from Aerospike bin data.
// This internal method parses the binary transaction data stored in Aerospike
// bins and converts it back into a structured Bitcoin transaction object.
//
// The function handles:
//   - Version and locktime field extraction and validation
//   - Transaction data deserialization from binary format
//   - Error handling for malformed or missing data
//
// Parameters:
//   - bins: Aerospike BinMap containing transaction data fields
//
// Returns:
//   - *bt.Tx: Reconstructed Bitcoin transaction, or nil if error
//   - error: Any error encountered during reconstruction, including:
//   - Type conversion errors for version/locktime fields
//   - Missing required transaction data
//   - Malformed binary transaction data
func (s *Store) getTxFromBins(bins aerospike.BinMap) (tx *bt.Tx, err error) {
	versionUint32, err := safeconversion.IntToUint32(bins[fields.Version.String()].(int))
	if err != nil {
		return nil, errors.NewStorageError("invalid version", err)
	}

	locktimeUint32, err := safeconversion.IntToUint32(bins[fields.LockTime.String()].(int))
	if err != nil {
		return nil, errors.NewStorageError("invalid locktime", err)
	}

	tx = &bt.Tx{
		Version:  versionUint32,
		LockTime: locktimeUint32,
	}

	inputInterfaces, ok := bins[fields.Inputs.String()].([]interface{})
	if ok {
		tx.Inputs = make([]*bt.Input, len(inputInterfaces))

		for i, inputInterface := range inputInterfaces {
			input := inputInterface.([]byte)
			tx.Inputs[i] = &bt.Input{}

			_, err = tx.Inputs[i].ReadFromExtended(bytes.NewReader(input))
			if err != nil {
				return nil, errors.NewStorageError(errCouldNotReadInput, err)
			}
		}
	}

	outputInterfaces, ok := bins[fields.Outputs.String()].([]interface{})
	if ok {
		tx.Outputs = make([]*bt.Output, len(outputInterfaces))

		for i, outputInterface := range outputInterfaces {
			if outputInterface == nil {
				continue
			}

			tx.Outputs[i] = &bt.Output{}

			_, err = tx.Outputs[i].ReadFrom(bytes.NewReader(outputInterface.([]byte)))
			if err != nil {
				return nil, errors.NewStorageError("could not read output", err)
			}
		}
	}

	return tx, nil
}

// addAbstractedBins expands the list of field names to include dependent fields.
// This internal method ensures that when certain abstracted fields are requested,
// all necessary underlying fields are also retrieved from the database.
//
// The function handles field dependencies such as:
//   - TxInpoints requires Inputs and External fields
//   - BlockIDs requires BlockHeights field
//   - Other abstracted field mappings
//
// This abstraction layer allows callers to request high-level fields without
// needing to know the underlying storage implementation details.
//
// Parameters:
//   - bins: Original list of field names to retrieve
//
// Returns:
//   - []fields.FieldName: Expanded list including all dependent fields
func (s *Store) addAbstractedBins(bins []fields.FieldName) []fields.FieldName {
	// copy the bins slice to avoid modifying the original
	newBins := append([]fields.FieldName{}, bins...)

	// add missing bins
	if slices.Contains(newBins, fields.TxInpoints) {
		if !slices.Contains(newBins, fields.Inputs) {
			newBins = append(newBins, fields.Inputs)
			newBins = append(newBins, fields.External)
		}
	}

	if slices.Contains(newBins, fields.Tx) {
		if !slices.Contains(newBins, fields.Inputs) {
			newBins = append(newBins, fields.Inputs)
		}

		if !slices.Contains(newBins, fields.Outputs) {
			newBins = append(newBins, fields.Outputs)
		}

		if !slices.Contains(newBins, fields.Version) {
			newBins = append(newBins, fields.Version)
		}

		if !slices.Contains(newBins, fields.LockTime) {
			newBins = append(newBins, fields.LockTime)
		}

		if !slices.Contains(newBins, fields.External) {
			newBins = append(newBins, fields.External)
		}
	}

	if slices.Contains(newBins, fields.BlockIDs) {
		if !slices.Contains(newBins, fields.BlockHeights) {
			newBins = append(newBins, fields.BlockHeights)
		}

		if !slices.Contains(newBins, fields.SubtreeIdxs) {
			newBins = append(newBins, fields.SubtreeIdxs)
		}
	}

	if slices.Contains(newBins, fields.Utxos) {
		if !slices.Contains(newBins, fields.TotalExtraRecs) {
			newBins = append(newBins, fields.TotalExtraRecs)
		}

		if !slices.Contains(newBins, fields.TotalUtxos) {
			newBins = append(newBins, fields.TotalUtxos)
		}
	}

	return newBins
}

// buildBatchRecords constructs the per-item aerospike BatchRead records for a
// BatchDecorate call: one aerospike.Key per item (digest of the tx hash) plus
// the expanded field-name set to read. It also records the expanded field set
// back onto each item (item.Fields) so the result-parsing pass knows which bins
// were requested. This is pure construction — no network I/O — which makes it
// the unit the allocation benchmark targets.
func (s *Store) buildBatchRecords(items []*utxo.UnresolvedMetaData, policy *aerospike.BatchReadPolicy, optionalFields []fields.FieldName) ([]aerospike.BatchRecordIfc, error) {
	batchRecords := make([]aerospike.BatchRecordIfc, len(items))

	// Most BatchDecorate calls request a uniform field set for every item:
	// callers either pass optionalFields or rely on the default and leave
	// item.Fields nil. Expand that shared set and convert it to wire names ONCE
	// and reuse the (read-only) results across every such item, instead of
	// re-expanding and re-allocating per item. Items that carry their own
	// item.Fields (the rare per-item path) are expanded individually below.
	//
	// The shared slices are only read downstream: the result-parsing pass ranges
	// over item.Fields, and the aerospike client only reads BatchRead BinNames
	// (it never sorts or mutates them in place), so sharing one backing array
	// across records is safe.
	sharedBins := optionalFields
	if len(sharedBins) == 0 {
		sharedBins = defaultDecorateBins
	}

	sharedExpanded := s.addAbstractedBins(sharedBins)
	sharedNames := fields.FieldNamesToStrings(sharedExpanded)

	for idx, item := range items {
		key, err := aerospike.NewKey(s.namespace, s.setName, item.Hash[:])
		if err != nil {
			return nil, errors.NewProcessingError("failed to init new aerospike key for txMeta", err)
		}

		expanded := sharedExpanded
		names := sharedNames

		if len(item.Fields) > 0 {
			// Item-specific field set: expand and convert it on its own.
			expanded = s.addAbstractedBins(item.Fields)
			names = fields.FieldNamesToStrings(expanded)
		}

		item.Fields = expanded

		// Add to batch
		batchRecords[idx] = aerospike.NewBatchRead(policy, key, names)
	}

	return batchRecords, nil
}

// defaultDecorateBins is the field set BatchDecorate reads when the caller
// requests none — optimized for common use cases without scripts. Services that
// need the full transaction (persister, API) explicitly request fields.Tx. Kept
// package-level so the common path does not re-allocate it per item.
var defaultDecorateBins = []fields.FieldName{fields.Fee, fields.SizeInBytes, fields.TxInpoints, fields.BlockIDs, fields.IsCoinbase}

// BatchDecorate efficiently fetches metadata for multiple transactions.
// It optimizes database access by:
//   - Batching multiple queries
//   - Deduplicating requests
//   - Managing external storage access
//   - Handling partial responses
//
// Parameters:
//   - ctx: Context for cancellation
//   - items: Transactions to fetch
//   - fields: Optional fields to retrieve
func (s *Store) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, optionalFields ...fields.FieldName) error {
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	// we only want to read from the master for tx metadata, due to blockIDs being updated
	// however we still want to read from the replica for the utxos in case of aerospike failures
	batchPolicy.ReplicaPolicy = aerospike.SEQUENCE

	policy := util.GetAerospikeBatchReadPolicy(s.settings)

	batchRecords, err := s.buildBatchRecords(items, policy, optionalFields)
	if err != nil {
		return err
	}

	if len(batchRecords) == 0 {
		return nil
	}

	err = s.batchOperate(batchPolicy, batchRecords)
	if err != nil {
		return errors.NewStorageError("error in aerospike map store batch records", err)
	}

	// Track external transaction fetches for debugging memory usage
	externalTxFetched := 0
	externalTxSkipped := 0

NEXT_BATCH_RECORD:
	for idx, batchRecord := range batchRecords {
		if err := batchRecord.BatchRec().Err; err != nil {
			items[idx].Data = nil

			if !subtree.CoinbasePlaceholderHashValue.Equal(items[idx].Hash) {
				if errors.Is(err, aerospike.ErrKeyNotFound) {
					items[idx].Err = errors.NewTxNotFoundError("%v not found", items[idx].Hash)
				} else {
					items[idx].Err = err
				}
			}

			continue // because there was an error for this batch item.
		}

		bins := batchRecord.BatchRec().Record.Bins

		items[idx].Data = &meta.Data{}

		// If the tx is external, we need to fetch it from the external store...
		var externalTx *bt.Tx

		// Check if we need the full external transaction (with all scripts)
		// TxInpoints can be computed without scripts using optimized parser
		needsFullExternalTx := false
		for _, field := range items[idx].Fields {
			if field == fields.Tx || field == fields.Inputs {
				needsFullExternalTx = true
				break
			}
		}

		external, ok := bins[fields.External.String()].(bool)
		if ok && external {
			if needsFullExternalTx {
				externalTxFetched++
				if externalTx, err = s.GetTxFromExternalStore(ctx, items[idx].Hash); err != nil {
					items[idx].Err = err

					continue // because there was an error reading the transaction from the external store.
				}
			} else {
				externalTxSkipped++
			}
		}

		for _, key := range items[idx].Fields {
			switch key {
			case fields.Tx:
				// If the tx is external, we already have it, otherwise we need to build it from the bins.
				if external {
					items[idx].Data.Tx = externalTx
				} else {
					tx, txErr := s.getTxFromBins(bins)
					if txErr != nil {
						items[idx].Err = classifyRecordError("invalid tx", txErr)

						continue NEXT_BATCH_RECORD // because there was an error building the transaction from the store.
					}

					items[idx].Data.Tx = tx
				}

			case fields.Inputs:
				// check that we are not also getting the tx, as this will be handled above
				if slices.Contains(items[idx].Fields, fields.Tx) {
					continue
				}

				// If the tx is external, we already have it, otherwise we need to build it from the bins.
				if external {
					items[idx].Data.Tx = externalTx
				} else {
					tx := &bt.Tx{}

					if inputInterfaces, ok := bins[fields.Inputs.String()].([]interface{}); ok {
						tx.Inputs = make([]*bt.Input, len(inputInterfaces))

						for i, inputInterface := range inputInterfaces {
							input := inputInterface.([]byte)
							tx.Inputs[i] = &bt.Input{}

							_, err = tx.Inputs[i].ReadFromExtended(bytes.NewReader(input))
							if err != nil {
								return errors.NewStorageError(errCouldNotReadInput, err)
							}
						}
					}

					items[idx].Data.Tx = tx
				}

			case fields.Fee:
				fee, ok := bins[key.String()].(int)
				if !ok {
					items[idx].Err = errors.NewStorageError("missing fee")

					continue NEXT_BATCH_RECORD // because there was an error reading the fee from the store.
				}

				items[idx].Data.Fee = uint64(fee) // nolint: gosec

			case fields.SizeInBytes:
				sizeInBytes, ok := bins[key.String()].(int)
				if !ok {
					items[idx].Err = errors.NewStorageError("missing size in bytes")

					continue NEXT_BATCH_RECORD // because there was an error reading the size in bytes from the store.
				}

				items[idx].Data.SizeInBytes = uint64(sizeInBytes) // nolint:gosec

			case fields.TxInpoints:
				if external {
					// If we already fetched the full external tx (for fields.Tx or fields.Inputs), reuse it
					// Otherwise use optimized function that skips scripts (90%+ memory savings)
					if externalTx != nil {
						items[idx].Data.TxInpoints, err = subtree.NewTxInpointsFromTx(externalTx)
					} else {
						items[idx].Data.TxInpoints, err = s.GetTxInpointsFromExternalStore(ctx, items[idx].Hash)
					}
					if err != nil {
						// Preserve a storage fault instead of laundering it into a
						// consensus violation. errors.Is walks the whole wrap chain and
						// every downstream switch tests ErrTxInvalid first, so wrapping a
						// local blob-store failure in TxInvalid presents this node's own
						// bad disk as a proven invalid transaction — the misclassification
						// getExternalTransaction was fixed for. Only a genuine failure to
						// build inpoints from well-formed data is a transaction fault.
						items[idx].Err = classifyRecordError("could not process tx inpoints", err)

						continue NEXT_BATCH_RECORD // because there was an error processing the tx inpoints.
					}
				} else {
					txInpoints, err := processInputsToTxInpoints(bins)
					if err != nil {
						items[idx].Err = classifyRecordError("could not process input interfaces", err)

						continue NEXT_BATCH_RECORD // because there was an error reading the tx inpoints from the store.
					}

					items[idx].Data.TxInpoints = txInpoints
				}

			case fields.BlockIDs:
				res, err := processBlockIDs(bins)
				if err != nil {
					items[idx].Err = classifyRecordError("could not process block IDs", err)

					continue NEXT_BATCH_RECORD // because there was an error processing the block IDs.
				}

				items[idx].Data.BlockIDs = res

			case fields.BlockHeights:
				res, err := processBlockHeights(bins)
				if err != nil {
					items[idx].Err = classifyRecordError("could not process block heights", err)

					continue NEXT_BATCH_RECORD // because there was an error processing the block heights.
				}

				items[idx].Data.BlockHeights = res

			case fields.SubtreeIdxs:
				res, err := processSubtreeIdxs(bins)
				if err != nil {
					items[idx].Err = classifyRecordError("could not process subtree idxs", err)

					continue NEXT_BATCH_RECORD // because there was an error processing the subtree idxs.
				}

				items[idx].Data.SubtreeIdxs = res

			case fields.IsCoinbase:
				coinbaseBool, ok := bins[key.String()].(bool)
				if !ok {
					items[idx].Err = errors.NewStorageError("missing is coinbase")

					continue NEXT_BATCH_RECORD // because there was an error reading the is coinbase from the store.
				}

				items[idx].Data.IsCoinbase = coinbaseBool

			case fields.Utxos:
				res, err := s.processUTXOs(ctx, &items[idx].Hash, bins)
				if err != nil {
					items[idx].Err = classifyUTXOReadError(err)

					continue NEXT_BATCH_RECORD // because there was an error processing the utxos.
				}

				items[idx].Data.SpendingDatas = res

			case fields.Locked:
				// NOTE: not all records will have the locked field, so we need to check if it exists
				// for instance seeded nodes will not have the locked field for all records.
				lockedBool, ok := bins[key.String()].(bool)
				if ok {
					items[idx].Data.Locked = lockedBool
				} else {
					// if the locked field is not present, we assume it is not locked
					items[idx].Data.Locked = false
				}

			case fields.Conflicting:
				conflictingBool, ok := bins[key.String()].(bool)
				if ok {
					items[idx].Data.Conflicting = conflictingBool
				}

			case fields.Creating:
				creatingBool, ok := bins[key.String()].(bool)
				if ok {
					items[idx].Data.Creating = creatingBool
				}

			case fields.ConflictingChildren:
				res, err := processConflictingChildren(bins)
				if err != nil {
					items[idx].Err = classifyRecordError("could not process conflicting children", err)

					continue NEXT_BATCH_RECORD // because there was an error processing the conflicting children.
				}

				items[idx].Data.ConflictingChildren = res

			case fields.UnminedSince:
				unminedSince, ok := bins[key.String()].(int)
				if ok {
					unminedSinceUint32, err := safeconversion.IntToUint32(unminedSince)
					if err != nil {
						items[idx].Err = errors.NewStorageError("invalid unmined since", err)

						continue NEXT_BATCH_RECORD // because there was an error processing the unmined since.
					}

					items[idx].Data.UnminedSince = unminedSinceUint32
				}

			case fields.CreatedAt:
				if v := bins[key.String()]; v != nil {
					switch t := v.(type) {
					case int:
						items[idx].Data.CreatedAt = int64(t)
					case int64:
						items[idx].Data.CreatedAt = t
					}
				}
			}
		}
	}

	prometheusTxMetaAerospikeMapGetMulti.Inc()
	prometheusTxMetaAerospikeMapGetMultiN.Add(float64(len(batchRecords)))

	// Log external transaction fetch statistics for memory usage debugging
	if externalTxFetched > 0 || externalTxSkipped > 0 {
		s.logger.Infof("[BatchDecorate] Processed %d items - External txs: %d fetched, %d skipped (fields didn't require fetch)", len(items), externalTxFetched, externalTxSkipped)
	}

	return nil
}

// processInputsToTxInpoints converts stored input data into transaction input points.
// This function processes the raw input data from Aerospike bins and reconstructs
// the transaction input references (previous transaction outputs being spent).
//
// The function:
//   - Extracts input data from the bins
//   - Reconstructs transaction inputs with previous output references
//   - Converts to TxInpoints format for metadata processing
//   - Handles malformed or missing input data
//
// Parameters:
//   - bins: Aerospike BinMap containing transaction input data
//
// Returns:
//   - meta.TxInpoints: Processed transaction input points
//   - error: Any error encountered during processing, including:
//   - Missing or malformed input data
//   - Invalid transaction input structure
//   - Data conversion errors
func processInputsToTxInpoints(bins aerospike.BinMap) (txInpoints subtree.TxInpoints, err error) {
	inputInterfaces, ok := bins[fields.Inputs.String()].([]interface{})
	if !ok {
		return txInpoints, errors.NewProcessingError("failed to get inputs")
	}

	tx := &bt.Tx{}
	tx.Inputs = make([]*bt.Input, len(inputInterfaces))

	for i, inputInterface := range inputInterfaces {
		input := inputInterface.([]byte)
		tx.Inputs[i] = &bt.Input{}

		_, err = tx.Inputs[i].ReadFromExtended(bytes.NewReader(input))
		if err != nil {
			return txInpoints, errors.NewProcessingError(errCouldNotReadInput, err)
		}
	}

	if txInpoints, err = subtree.NewTxInpointsFromInputs(tx.Inputs); err != nil {
		return txInpoints, errors.NewProcessingError("could not create tx inpoints from tx", err)
	}

	return txInpoints, nil
}

// processBlockIDs extracts and validates block ID data from Aerospike bins.
// This function processes the stored block ID information and converts it
// from the raw interface{} format to a properly typed uint32 slice.
//
// Block IDs represent the blocks that contain this transaction, supporting
// scenarios where a transaction may appear in multiple blocks during
// blockchain reorganizations.
//
// Parameters:
//   - bins: Aerospike BinMap containing block ID data
//
// Returns:
//   - []uint32: Array of block IDs containing this transaction
//   - error: Any error encountered during processing, including:
//   - Missing block ID data
//   - Invalid data format or type conversion errors
//   - Empty block ID arrays (when not expected)
func processBlockIDs(bins aerospike.BinMap) ([]uint32, error) {
	blockIDs, ok := bins[fields.BlockIDs.String()].([]interface{})
	if !ok {
		return []uint32{}, nil
	}

	if len(blockIDs) == 0 {
		return []uint32{}, nil
	}

	res := make([]uint32, len(blockIDs))

	for i, blockID := range blockIDs {
		blockIDInt, ok := blockID.(int)
		if !ok {
			return nil, errors.NewStorageError("failed to get block ID")
		}

		blockIDUint32, err := safeconversion.IntToUint32(blockIDInt)
		if err != nil {
			return nil, errors.NewStorageError("invalid block ID")
		}

		res[i] = blockIDUint32
	}

	return res, nil
}

// processBlockHeights extracts and validates block height data from Aerospike bins.
// This function processes the stored block height information and converts it
// from the raw interface{} format to a properly typed uint32 slice.
//
// Block heights represent the heights of the blocks that contain this transaction,
// supporting scenarios where a transaction may appear in multiple blocks during
// blockchain reorganizations.
//
// Parameters:
//   - bins: Aerospike BinMap containing block height data
//
// Returns:
//   - []uint32: Array of block heights containing this transaction
//   - error: Any error encountered during processing, including:
//   - Missing block height data
//   - Invalid data format or type conversion errors
//   - Empty block height arrays (when not expected)
func processBlockHeights(bins aerospike.BinMap) ([]uint32, error) {
	blockHeights, ok := bins[fields.BlockHeights.String()].([]interface{})
	if !ok {
		return nil, errors.NewStorageError("missing block heights")
	}

	if len(blockHeights) == 0 {
		return nil, nil
	}

	res := make([]uint32, len(blockHeights))

	for i, blockHeight := range blockHeights {
		blockHeightInt, ok := blockHeight.(int)
		if !ok {
			return nil, errors.NewStorageError("failed to get block height")
		}

		blockHeightUint32, err := safeconversion.IntToUint32(blockHeightInt)
		if err != nil {
			return nil, errors.NewStorageError("invalid block height")
		}

		res[i] = blockHeightUint32
	}

	return res, nil
}

// processSubtreeIdxs extracts and validates subtree index data from Aerospike bins.
// This function processes the stored subtree index information and converts it
// from the raw interface{} format to a properly typed int slice.
//
// Subtree indices represent the indices of the subtrees that contain this transaction,
// supporting scenarios where a transaction may appear in multiple subtrees during
// blockchain reorganizations.
//
// Parameters:
//   - bins: Aerospike BinMap containing subtree index data
//
// Returns:
//   - []int: Array of subtree indices containing this transaction
//   - error: Any error encountered during processing, including:
//   - Missing subtree index data
//   - Invalid data format or type conversion errors
//   - Empty subtree index arrays (when not expected)
func processSubtreeIdxs(bins aerospike.BinMap) ([]int, error) {
	subtreeIdxs, ok := bins[fields.SubtreeIdxs.String()].([]interface{})
	if !ok {
		return nil, errors.NewStorageError("missing subtree idxs")
	}

	if len(subtreeIdxs) == 0 {
		return nil, nil
	}

	res := make([]int, len(subtreeIdxs))

	for i, subtreeIdx := range subtreeIdxs {
		subtreeIdxInt, ok := subtreeIdx.(int)
		if !ok {
			return nil, errors.NewStorageError("failed to get subtree idx")
		}

		res[i] = subtreeIdxInt
	}

	return res, nil
}

// classifyUTXOReadError decides what class of error a failed UTXO read carries.
//
// ErrTxInvalid is a consensus verdict: isUnvalidatablePeerError treats it as a
// genuine consensus failure, so ValidateBlock persists the block as permanently
// invalid and flags the peer that served it. Nothing processUTXOs can discover
// justifies that. It only decodes a record this node wrote itself, so every way
// it can fail — a torn record, a failed fetch, a cancelled context, a key that
// would not build — means our own read went wrong, never that the transaction
// or the peer is at fault.
//
// So the rule is to preserve whatever class the error already carries, and hand
// out a consensus verdict only for an error that already is one. That keeps
// routine runtime events, above all context cancellation at shutdown, from
// permanently condemning a valid block.
func classifyUTXOReadError(err error) error {
	if errors.Is(err, errors.ErrTxInvalid) {
		return errors.NewTxInvalidError("could not process utxos", err)
	}

	return err
}

// processUTXOs extracts and processes UTXO data from Aerospike bins.
// This function handles the reconstruction of UTXO spending data from stored
// binary format, including handling of paginated records for large transactions.
//
// The function:
//   - Extracts total UTXO count and UTXO array data
//   - Converts binary UTXO data to SpendingData structures
//   - Handles extra UTXOs from child records (pagination)
//   - Manages nil entries for spent or invalid UTXOs
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - txid: Transaction ID for retrieving additional records
//   - bins: Aerospike BinMap containing UTXO data
//
// Returns:
//   - []*spendpkg.SpendingData: Array of UTXO spending data (may contain nil entries)
//   - error: Any error encountered during processing
func (s *Store) processUTXOs(ctx context.Context, txid *chainhash.Hash, bins aerospike.BinMap) ([]*spendpkg.SpendingData, error) {
	totalUtxos, ok := bins[fields.TotalUtxos.String()].(int)
	if !ok {
		return nil, errors.NewStorageError("failed to get totalUtxos")
	}

	utxos, ok := bins[fields.Utxos.String()].([]interface{})
	if !ok {
		// Every record this store writes carries a utxos bin, so a missing or
		// wrong-typed one is our own record being malformed — storage damage, not
		// evidence about the transaction. The extra-record reader classifies the
		// identical condition the same way.
		return nil, errors.NewStorageError("[processUTXOs] missing or malformed utxos bin (%T) on %s — torn or partially-applied record", bins[fields.Utxos.String()], txid.String())
	}

	spendingDatas := make([]*spendpkg.SpendingData, totalUtxos)

	for i, ui := range utxos {
		if i >= len(spendingDatas) {
			// The totalUtxos bin disagrees with the stored list, so this record is
			// torn or mis-keyed. Error rather than index past the end and panic.
			return nil, errors.NewStorageError("[processUTXOs][%s] record holds more outputs than totalUtxos says (%d of %d) — torn record", txid.String(), i, len(spendingDatas))
		}

		u, ok := ui.([]uint8)
		if ok && len(u) == 68 {
			spendingData, err := spendpkg.NewSpendingDataFromBytes(u[32:])
			if err != nil {
				return nil, errors.NewStorageError("failed to get spending data", err)
			}

			spendingDatas[i] = spendingData
		} else {
			spendingDatas[i] = nil
		}
	}

	// Add any extra UTXOs from child records...
	totalExtraRecs, ok := bins[fields.TotalExtraRecs.String()].(int)
	if ok {
		if err := s.getAllExtraUTXOs(ctx, txid, totalExtraRecs, spendingDatas); err != nil {
			return nil, err
		}
	}

	return spendingDatas, nil
}

// processConflictingChildren extracts and processes conflicting children data from Aerospike bins.
// This function handles the reconstruction of conflicting transaction child references
// from the stored binary format, supporting double-spend detection and conflict resolution.
//
// Conflicting children represent transactions that attempt to spend the same UTXOs,
// which is essential for managing blockchain reorganizations and double-spend scenarios.
//
// Parameters:
//   - bins: Aerospike BinMap containing conflicting children data
//
// Returns:
//   - []chainhash.Hash: Array of conflicting child transaction hashes
//   - error: Any error encountered during processing, including:
//   - Invalid hash format or length
//   - Data conversion errors
//   - Malformed conflicting children data
func processConflictingChildren(bins aerospike.BinMap) (conflictingChildren []chainhash.Hash, err error) {
	conflictingChildrenIfc, ok := bins[fields.ConflictingChildren.String()].([]interface{})
	if ok {
		conflictingChildren = make([]chainhash.Hash, len(conflictingChildrenIfc))

		for i, child := range conflictingChildrenIfc {
			childHash, ok := child.([]uint8)
			if !ok {
				return nil, errors.NewStorageError("failed to get conflicting child")
			}

			conflictingChildren[i] = chainhash.Hash(childHash)
		}
	}

	return conflictingChildren, nil
}

// getAllExtraUTXOs retrieves all UTXOs from the transaction's extra (paginated) child records
func (s *Store) getAllExtraUTXOs(ctx context.Context, txID *chainhash.Hash, totalExtraRecs int, spendingDatas []*spendpkg.SpendingData) error {
	if totalExtraRecs <= 0 {
		return nil
	}

	// Fetch each extra record
	for recordNum := 1; recordNum <= totalExtraRecs; recordNum++ {
		// Check context before each iteration
		select {
		case <-ctx.Done():
			return ctx.Err()
		default: // Empty default to prevent blocking
		}

		keySource := uaerospike.CalculateKeySourceInternal(txID, uint32(recordNum)) // nolint: gosec

		extraKey, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			return errors.NewProcessingError("failed to create key for extra record", err)
		}

		policy := util.GetAerospikeReadPolicy(s.settings)

		extraRecord, err := s.client.Get(policy, extraKey, fields.Utxos.String())
		if err != nil {
			return errors.NewStorageError("failed to get extra record", err)
		}

		// Calculate the base offset for this pagination record
		baseOffset := recordNum * s.utxoBatchSize

		// How many outputs this record must hold. splitIntoBatches gives every
		// record a full batch except the last, which takes the remainder, so
		// the transaction's output count fixes the expected length exactly.
		remaining := len(spendingDatas) - baseOffset
		if remaining <= 0 {
			return errors.NewStorageError("[getAllExtraUTXOs][%s] extra record %d starts at offset %d, past the transaction's %d outputs — torn or mis-keyed record", txID.String(), recordNum, baseOffset, len(spendingDatas))
		}

		expected := min(remaining, s.utxoBatchSize)

		if applyErr := applyExtraRecordBins(txID, recordNum, extraRecord, baseOffset, spendingDatas, expected); applyErr != nil {
			return applyErr
		}
	}

	return nil
}

// applyExtraRecordBins interprets one fetched extra (paginated) record and
// writes the spend state it holds into the caller's slice.
//
// It is split from the fetch so the interpretation can be tested without an
// Aerospike container.
//
// The nil-record guard is defence in depth rather than the primary handling of
// a missing extra record. This client version reports an absent key as
// ErrKeyNotFound, which getAllExtraUTXOs already turns into a storage error
// before calling here, and it never hands back a nil BinMap (a record whose
// utxos bin is absent arrives with an empty map, and falls through to the
// malformed-bin branch below). The guard exists so that a client or wrapper
// that ever did return a nil record with no error would error here rather than
// panic on the deref.
func applyExtraRecordBins(txID *chainhash.Hash, recordNum int, extraRecord *aerospike.Record, baseOffset int, spendingDatas []*spendpkg.SpendingData, expectedCount int) error {
	if extraRecord == nil || extraRecord.Bins == nil {
		return errors.NewStorageError("[getAllExtraUTXOs][%s] extra record %d is missing — torn or partially-applied write", txID.String(), recordNum)
	}

	extraUtxos, ok := extraRecord.Bins[fields.Utxos.String()].([]interface{})
	if !ok {
		return errors.NewStorageError("[getAllExtraUTXOs][%s] extra record has a missing or malformed utxos bin (%T) at record %d — torn or partially-applied record", txID.String(), extraRecord.Bins[fields.Utxos.String()], recordNum)
	}

	return applyExtraRecordUTXOs(txID, recordNum, extraUtxos, baseOffset, spendingDatas, expectedCount)
}

// applyExtraRecordUTXOs writes the spend state held in one extra (paginated)
// record into the caller's slice.
//
// Whether an output is spent is consensus state, and here "no data" reads as
// UNSPENT. So anything unexpected in the record fails the read rather than
// being skipped: silently leaving a slot nil turns a spent output back into a
// spendable one, and a double-spend of it is then accepted with no error
// anywhere (issue 1440).
//
// Three element shapes are legitimate and must all keep working:
//
//   - nil — the output was never entered into the UTXO set because it is
//     provably unspendable (see utxo.ShouldStoreOutputAsUTXO: OP_FALSE
//     OP_RETURN in every era, plus bare OP_RETURN and oversized locking
//     scripts pre-Genesis). GetBinsToStore leaves those slots nil and the
//     Aerospike client round-trips them as a nil list element, so a large
//     transaction carrying a data output has nils in its extra records.
//   - 32 bytes — an unspent output: the utxo hash alone, no spending data.
//   - 68 bytes — a spent (or frozen) output: 32-byte utxo hash + 36-byte
//     spending data.
//
// All three correctly leave, or set, the caller's slot. Any other length, or a
// non-nil non-byte element, is a torn or partially-applied record.
//
// The element count is checked first, because a short list is the same hazard
// arriving from the other direction: the loop would simply never visit the
// missing offsets, leaving those slots nil, and nil reads as UNSPENT. Trailing
// nils survive the round trip (pinned by TestTrailingNilSlotsSurviveRoundTrip),
// so a healthy record ending in unspendable outputs still comes back at its
// full length and exact equality cannot misfire on it.
func applyExtraRecordUTXOs(txID *chainhash.Hash, recordNum int, extraUtxos []interface{}, baseOffset int, spendingDatas []*spendpkg.SpendingData, expectedCount int) error {
	if len(extraUtxos) != expectedCount {
		return errors.NewStorageError("[getAllExtraUTXOs][%s] extra record %d holds %d outputs, expected %d — truncated or over-long record", txID.String(), recordNum, len(extraUtxos), expectedCount)
	}

	for i, ui := range extraUtxos {
		offset := baseOffset + i
		if offset >= len(spendingDatas) {
			return errors.NewStorageError("[getAllExtraUTXOs][%s] extra record %d holds more outputs than the transaction has (offset %d of %d) — torn or mis-keyed record", txID.String(), recordNum, offset, len(spendingDatas))
		}

		if ui == nil {
			// Provably unspendable output, never stored as a UTXO. Slot stays nil.
			continue
		}

		u, ok := ui.([]uint8)
		if !ok {
			return errors.NewStorageError("[getAllExtraUTXOs][%s] extra record %d output is %T, not bytes, at offset %d — torn or partially-applied record", txID.String(), recordNum, ui, offset)
		}

		switch len(u) {
		case 32:
			// Unspent: hash only, no spending data. Slot stays nil.
		case 68:
			spendingData, err := spendpkg.NewSpendingDataFromBytes(u[32:])
			if err != nil {
				return errors.NewStorageError("failed to parse spending data from extra record", err)
			}

			spendingDatas[offset] = spendingData
		default:
			return errors.NewStorageError("[getAllExtraUTXOs][%s] extra record %d output %d is %d bytes, expected 32 (unspent) or 68 (spent) — torn or short record", txID.String(), recordNum, i, len(u))
		}
	}

	return nil
}

// PreviousOutputsDecorate fetches output data for transaction inputs.
// Uses batching to optimize retrieval of previous output data:
//   - Deduplicates requests for the same transaction
//   - Handles both internal and external storage
//   - Returns locking scripts and amounts
func (s *Store) PreviousOutputsDecorate(_ context.Context, tx *bt.Tx) error {
	items := make([]*batchOutpoint, 0, len(tx.Inputs))

	for _, input := range tx.Inputs {
		if input.PreviousTxScript != nil {
			// skip if the input already has a previous output script
			continue
		}

		items = append(items, &batchOutpoint{outpoint: input})
	}

	group := completion.NewGroup(int32(len(items)))

	for _, item := range items {
		item.group = group
		// Guard the enqueue: Store.Close may have closed the outpoint batcher while
		// this caller is still decorating during shutdown. safeBatcherPut converts a
		// send-on-closed-channel panic into an error instead of crashing the process;
		// complete the item so the shared group wait below does not hang on it.
		if err := safeBatcherPut(s.outpointBatcher, item, "outpoint"); err != nil {
			item.complete(err)
		}
	}

	// One shared wait for the whole group, bounded so a wedged outpoint batcher
	// cannot pin this goroutine for the life of the process. This site never had
	// a ctx.Done() arm (the ctx param is unused), so wait on completion/timeout
	// only. A non-positive batcherWait disables the timeout arm, mirroring the
	// previous nil-timeoutCh (unbounded) behaviour.
	if waitErr := group.Wait(context.Background(), s.batcherWait); waitErr != nil {
		// Do not read item slots on the error path: the dispatcher may still be
		// writing to them after we have given up waiting.
		return errors.NewServiceUnavailableError("aerospike outpoint batch did not complete within %s", s.batcherWait)
	}

	// group.Wait returned nil: every item has completed, so every slot is safe to
	// read. Report the first failure found, matching the previous first-error
	// return behaviour.
	for _, item := range items {
		if item.result != nil {
			return item.result
		}
	}

	return nil
}

// BatchPreviousOutputsDecorate fetches previous output information for inputs across
// multiple transactions, fanning the per-tx decorations out across goroutines so the
// shared outpoint batcher fills by size from concurrent pushes instead of idling at
// its per-tx duration timer.
//
// A serial per-tx loop was correct but pathologically slow during legacy sync: a
// typical tx contributes ~2 inputs - far below OutpointBatcherSize - so each call
// waited the full OutpointBatcherDurationMillis before the batch fired, making wall
// time scale as O(N_tx * duration) - e.g. 2856 tx x 5 ms ~= 14 s observed in
// production.
//
// Fan-out is bounded by UtxoStore.OutpointBatcherSize to keep memory predictable
// on large blocks and to mirror the Phase 1 errgroup bound in the legacy caller
// (services/legacy/netsync/handle_block.go). Throughput is not affected by the
// bound: the actual ceiling is BatcherMaxConcurrent aerospike batches in flight,
// not the goroutine count, since producers are mostly parked on the shared completion group's Wait.
func (s *Store) BatchPreviousOutputsDecorate(ctx context.Context, txs []*bt.Tx) error {
	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(s.logger, g, s.settings.UtxoStore.OutpointBatcherSize)

	for _, tx := range txs {
		tx := tx
		g.Go(func() error {
			return s.PreviousOutputsDecorate(gCtx, tx)
		})
	}

	return g.Wait()
}

func (s *Store) sendOutpointBatch(batch []*batchOutpoint) {
	// go-batcher recovers panics in this fn; complete every item on panic so a
	// crash mid-decoration cannot orphan the waiting submitters.
	defer func() {
		signalBatchPanic(recover(), batch, "sendOutpointBatch", s.logger, func(it *batchOutpoint, err error) {
			it.complete(err)
		})
	}()

	start := gocore.CurrentTime()
	defer func() {
		previousOutputsDecorateStat.AddTimeForRange(start, len(batch))
	}()

	var err error

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	// we only want to read from the master for tx metadata, due to blockIDs being updated
	// however we still want to read from the replica for the utxos in case of aerospike failures
	batchPolicy.ReplicaPolicy = aerospike.SEQUENCE

	policy := util.GetAerospikeBatchReadPolicy(s.settings)

	// Create a batch of records to read, with a max size of the batch
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(batch))
	batchRecordHashes := make([]chainhash.Hash, 0, len(batch))

	// we de-dupe the txs we need to lookup, since we may have multiple outpoints for the same tx
	// this is done by using a map of txHashes
	uniqueTxHashes := make(map[chainhash.Hash]struct{})
	for _, item := range batch {
		uniqueTxHashes[*item.outpoint.PreviousTxIDChainHash()] = struct{}{}
	}

	// Create a batch of records to read from the txHashes
	for txHash := range uniqueTxHashes {
		key, err := aerospike.NewKey(s.namespace, s.setName, txHash[:])
		if err != nil {
			for _, item := range batch {
				item.complete(errors.NewProcessingError("failed to init new aerospike key for txMeta", err))
			}

			return
		}

		// BlockHeights is read so external parents can be reconstructed with the
		// era-aware unspendable rule keyed to their creation height.
		bins := []fields.FieldName{fields.Version, fields.LockTime, fields.Inputs, fields.Outputs, fields.External, fields.BlockHeights}
		record := aerospike.NewBatchRead(policy, key, fields.FieldNamesToStrings(bins))

		// Add to batch records
		batchRecords = append(batchRecords, record)
		batchRecordHashes = append(batchRecordHashes, txHash)
	}

	// send the batch to aerospike
	err = s.batchOperate(batchPolicy, batchRecords)
	if err != nil {
		for _, item := range batch {
			item.complete(errors.NewStorageError("error in aerospike send outpoint batch records", err))
		}

		return
	}

	txs := make(map[chainhash.Hash]*bt.Tx, len(batchRecords))
	txErrors := make(map[chainhash.Hash]error)

	// Process the batch records
	for idx, batchRecordIfc := range batchRecords {
		previousTxHash := batchRecordHashes[idx]

		batchRecord := batchRecordIfc.BatchRec()
		if batchRecord.Err != nil {
			if errors.Is(batchRecord.Err, aerospike.ErrKeyNotFound) {
				txErrors[previousTxHash] = errors.NewTxNotFoundError("could not find transaction %s in aerospike", previousTxHash.String(), batchRecord.Err)
			} else {
				txErrors[previousTxHash] = errors.NewProcessingError("error in aerospike get outpoint batch record", batchRecord.Err)
			}

			continue
		}

		bins := batchRecord.Record.Bins

		var previousTx *bt.Tx

		external, ok := bins[fields.External.String()].(bool)
		if ok && external {
			// Resolve the parent's creation-height era so the reconstruction
			// applies the same unspendable rule create used. A missing/empty
			// BlockHeights (unmined parent) yields the Genesis activation height
			// (post-Genesis), which only ever over-retains — never over-excludes.
			blockHeights, _ := processBlockHeights(bins)
			creationHeight := creationHeightFromBlockHeights(blockHeights, s.settings.ChainCfgParams.GenesisActivationHeight)

			if previousTx, err = s.GetOutpointsFromExternalStore(s.ctx, previousTxHash, creationHeight); err != nil {
				txErrors[previousTxHash] = err

				continue
			}
		} else {
			previousTx, err = s.getTxFromBins(bins)
			if err != nil {
				txErrors[previousTxHash] = classifyRecordError("invalid tx", err)

				continue
			}
		}

		txs[previousTxHash] = previousTx
	}

	// Now we have all the txs, we can decorate the outpoints
	for _, batchItem := range batch {
		previousTx := txs[*batchItem.outpoint.PreviousTxIDChainHash()]
		if previousTx == nil {
			if err, ok := txErrors[*batchItem.outpoint.PreviousTxIDChainHash()]; ok {
				batchItem.complete(err)
			} else {
				batchItem.complete(errors.NewTxNotFoundError("previous tx not found: %v", batchItem.outpoint.PreviousTxID))
			}

			continue
		}

		// Guard the output index: a corrupt/short Outputs slice (or a nil-padded
		// entry from OP_RETURN removal) would otherwise panic here and, because
		// go-batcher recovers the panic, orphan every remaining item in the batch.
		//
		// This is deliberately the one TxInvalid left on this read path, and the
		// only one that can be. Every other failure above is this node failing to
		// read back its own stored bytes, which says nothing about the block; this
		// one compares the child's claim against the parent and so can genuinely
		// establish that the child is invalid. Reclassifying it would leave the
		// store unable to report a real consensus violation at all, and a
		// genuinely invalid spend would stall catchup indefinitely instead of
		// being rejected.
		//
		// The caveat is that it is only as trustworthy as the parent read beneath
		// it. Reached via the fully re-hashed FileTypeTx path it is sound. Reached
		// via the outputs-only blob (self-declared txid only) or the inline bins
		// (not re-hashed at all), corruption that yields a short output list still
		// arrives here and is reported as a proven violation. That is the residual
		// hazard the scope note in getExternalTransaction describes, and the
		// strongest argument for extending the re-hash inline.
		outIdx := batchItem.outpoint.PreviousTxOutIndex
		if int(outIdx) >= len(previousTx.Outputs) || previousTx.Outputs[outIdx] == nil {
			batchItem.complete(errors.NewTxInvalidError("previous tx %s has no output at index %d", batchItem.outpoint.PreviousTxID, outIdx))
			continue
		}

		batchItem.outpoint.PreviousTxSatoshis = previousTx.Outputs[outIdx].Satoshis
		batchItem.outpoint.PreviousTxScript = previousTx.Outputs[outIdx].LockingScript
		batchItem.complete(nil)
	}

	prometheusTxMetaAerospikeMapGetMulti.Inc()
	prometheusTxMetaAerospikeMapGetMultiN.Add(float64(len(batchRecords)))
}

// GetOutpointsFromExternalStore reconstructs an externally-stored parent tx's
// spendable outputs for outpoint resolution. creationHeight is the parent's
// mining height, used to apply the era-aware unspendable rule (see
// getExternalOutpoints).
//
// The reconstruction is cached by txid (externalOutpointsCache — never the full
// transaction's cache; see the field comments on Store). That is correct
// because the era-filtered output set is fixed per tx once the parent is mined,
// and an unmined parent is necessarily post-Genesis on production networks
// (mainnet/testnet/teratestnet activation heights sit far below any live tip),
// so the unmined fallback and a real mining at that height resolve to the same
// era.
//
// Known, accepted seam on low-activation networks only (regtest=10000, stn=100):
// there a node can be at a genuinely pre-Genesis height with an unmined external
// parent. The fallback then classifies it post-Genesis and caches that set; if
// the parent later mines at a pre-Genesis height, nothing re-derives the era or
// evicts the entry, so the stale post-Genesis (over-retaining) reconstruction
// persists. This only ever over-retains a provably-unspendable output, never
// over-excludes a spendable one, so it cannot orphan a valid spend; it is
// unreachable on production networks.
func (s *Store) GetOutpointsFromExternalStore(ctx context.Context, previousTxHash chainhash.Hash, creationHeight uint32) (*bt.Tx, error) {
	ctx, _, _ = tracing.Tracer("aerospike").Start(ctx, "GetOutpointsFromExternalStore",
		tracing.WithHistogram(prometheusTxMetaAerospikeMapGetExternal),
	)

	// Deliberately a different cache from GetTxFromExternalStore's: the value here
	// has its inputs stripped and its era-unspendable outputs nil'd, so sharing a
	// cache keyed on the txid would let either reader receive the other's shape.
	if s.externalOutpointsCache != nil {
		return s.externalOutpointsCache.GetOrSet(previousTxHash, func() (*bt.Tx, bool, error) {
			tx, numberOfActiveOutputs, err := s.getExternalOutpoints(ctx, previousTxHash, creationHeight)
			if err != nil {
				return nil, false, err
			}

			// determine whether to cache the tx or just return it once
			allowCaching := true

			if numberOfActiveOutputs < 2 {
				// do not cache 1 output transactions, they are not going to be requested again
				allowCaching = false
			}

			return tx, allowCaching, nil
		})
	}

	tx, _, err := s.getExternalOutpoints(ctx, previousTxHash, creationHeight)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

// creationHeightFromBlockHeights returns the era height used to reconstruct an
// externally-stored parent's spendable outputs: the earliest block the tx was
// mined in. An unmined parent (no recorded block heights) falls back to the
// Genesis activation height (post-Genesis). On production networks this is exact
// — a live unmined parent is always post-Genesis. On low-activation networks
// (regtest and stn both activate at 100) the fallback can misclassify a pre-Genesis
// unmined parent as post-Genesis; this only over-retains a provably-unspendable
// output (never over-excludes a spendable one), so it is safe and accepted. See
// GetOutpointsFromExternalStore for the matching cache-coherence note.
func creationHeightFromBlockHeights(blockHeights []uint32, genesisActivationHeight uint32) uint32 {
	minHeight := uint32(0)
	found := false

	for _, h := range blockHeights {
		if !found || h < minHeight {
			minHeight, found = h, true
		}
	}

	if !found {
		return genesisActivationHeight
	}

	return minHeight
}

func (s *Store) getExternalOutpoints(ctx context.Context, previousTxHash chainhash.Hash, creationHeight uint32) (*bt.Tx, int, error) {
	// get the full transaction from the external store
	tx, err := s.getExternalTransaction(ctx, previousTxHash)
	if err != nil {
		return nil, 0, err
	}

	// remove inputs, don't need them for outpoints
	tx.Inputs = nil

	numberOfActiveOutputs := 0

	// Drop the provably-unspendable outputs that were never stored as UTXOs,
	// using the same era-aware, value-agnostic rule as create
	// (ShouldStoreOutputAsUTXO) keyed to the parent's creation height. This
	// keeps the reconstructed outpoint set identical to what create stored —
	// in particular it retains post-Genesis bare OP_RETURN outputs, which are
	// spendable and must resolve when later spent.
	for i, output := range tx.Outputs {
		if output != nil && output.LockingScript != nil {
			if utxo.ShouldStoreOutputAsUTXO(output, creationHeight, s.settings.ChainCfgParams.GenesisActivationHeight) {
				numberOfActiveOutputs++
			} else {
				tx.Outputs[i] = nil
			}
		}
	}

	return tx, numberOfActiveOutputs, nil
}

func (s *Store) GetTxFromExternalStore(ctx context.Context, previousTxHash chainhash.Hash) (*bt.Tx, error) {
	ctx, _, _ = tracing.Tracer("aerospike").Start(ctx, "GetTxFromExternalStore",
		tracing.WithHistogram(prometheusTxMetaAerospikeMapGetExternal),
	)

	if s.externalTxCache != nil {
		return s.externalTxCache.GetOrSet(previousTxHash, func() (*bt.Tx, bool, error) {
			tx, err := s.getExternalTransaction(ctx, previousTxHash)
			if err != nil {
				return nil, false, err
			}

			return tx, true, nil
		})
	}

	return s.getExternalTransaction(ctx, previousTxHash)
}

func (s *Store) getExternalTransaction(ctx context.Context, previousTxHash chainhash.Hash) (*bt.Tx, error) {
	fileType := fileformat.FileTypeTx

	// Stream parse from external store rather than reading the whole blob into a
	// byte slice first, so the raw bytes are not held alongside the parsed tx for
	// the duration of the parse. Note that the FileTypeTx branch below transiently
	// re-creates a buffer the size of the standard serialization when it re-hashes
	// the parsed tx (TxIDChainHash goes via tx.Bytes(), which is smaller than the
	// extended form the blob is stored in), so the peak-memory saving does not
	// hold for that branch.
	reader, err := s.externalStore.GetIoReader(ctx, previousTxHash[:], fileType)
	if err != nil {
		// Fall back to the outputs-only blob only when the full transaction is
		// genuinely absent. Falling back on ANY error sent real read failures — a
		// permission or I/O error, a context deadline, an S3 throttle, and the
		// pruner's delete-then-write window that motivates the re-hash below — down
		// the weaker path, where the only key check is a self-declared txid rather
		// than a re-hash. It did so silently: a present-but-unreadable tx blob
		// alongside a valid outputs blob returned a zero-input transaction whose
		// txid is not the key, with no error at all, which BatchDecorate then
		// handed to fields.Tx and fields.Inputs consumers. Absence is the only
		// condition the outputs blob legitimately covers. Every blob backend
		// distinguishes the two: memory, s3 and file all return ErrNotFound for a
		// missing blob and a StorageError for a failed read.
		if !errors.Is(err, errors.ErrNotFound) {
			return nil, errors.NewStorageError("[GetTxFromExternalStore][%s] could not read tx from external store", previousTxHash.String(), err)
		}

		fileType = fileformat.FileTypeOutputs

		reader, err = s.externalStore.GetIoReader(ctx, previousTxHash[:], fileType)
		if err != nil {
			return nil, errors.NewStorageError("[GetTxFromExternalStore][%s] could not get tx from external store", previousTxHash.String(), err)
		}
	}
	defer reader.Close()

	tx := &bt.Tx{}

	// Use buffered reader for all file types to reduce syscalls
	bufferedReader := bufio.NewReader(reader)

	if fileType == fileformat.FileTypeTx {
		// Stream parse directly from buffered reader. A parse failure here is a
		// storage fault, not a transaction fault: the blob was written from our
		// own bytes, so bytes that no longer parse mean this node's stored copy is
		// truncated or torn — never evidence about the block or the peer that
		// served it. See the classification note on the re-hash below; a truncated
		// blob is the more likely corruption mode of the two.
		if _, err = tx.ReadFrom(bufferedReader); err != nil {
			return nil, errors.NewStorageError("[GetTxFromExternalStore][%s] could not read tx from stream", previousTxHash.String(), err)
		}

		// Re-hash the reconstructed parent against the key it was fetched under
		// (issue 1439). The parent's outputs are consensus inputs to spend
		// validation: bytes that are the wrong bytes but still parse as a valid
		// transaction — a rotted or torn blob, a stale file from the pruner's
		// delete-then-write race, a mis-keyed return — would have the child's
		// signature checked against the wrong locking script and its value against
		// the wrong satoshis, silently. A body checksum cannot catch a
		// stale-but-intact file; only re-hashing against the requested outpoint
		// can. It must fail here rather than be cached: the caller memoizes by
		// txid, so one bad read would be re-served to every later spend of this
		// parent, and again after a restart because the source is on disk.
		//
		// What the check does NOT cover, stated plainly because the txid is a
		// narrower guarantee than "the blob is verified": the blob is written as
		// tx.ExtendedBytes(), but TxIDChainHash() hashes tx.Bytes(), the standard
		// serialization. Every input's PreviousTxSatoshis and PreviousTxScript
		// therefore sits outside the hash preimage, and no stored digest covers
		// those bytes, so corruption confined to them passes this check. What spend
		// validation reads here — this parent's own outputs — is covered. The
		// uncovered fields are consumed only by legacy netsync's
		// extendPerTxFallback, which reads a transaction back by its own txid and
		// copies them onto the in-block transaction; that transaction is then
		// IsExtended(), so the validator's extend := !tx.IsExtended() skips
		// re-fetching the real parent output and both the script and fee checks run
		// against them. Closing that needs a digest over the stored form, which is a
		// storage-format change rather than a check, so it is out of scope here.
		//
		// A storage fault, never a transaction fault. The blob is written from our
		// own bytes under our own computed hash, so a mismatch can only mean this
		// node's stored bytes are wrong. TxInvalid would be read downstream as a
		// proven consensus violation — the block persisted invalid, its
		// descendants poisoned via the parent-invalid cascade, and the serving
		// peer flagged malicious — so a single flipped bit on local disk would
		// fork this node off the honest chain until someone manually revalidated.
		// The sibling failures in this function are classified the same way, for
		// the same reason.
		//
		// Deliberately no fallback to FileTypeOutputs on mismatch, even where that
		// blob exists and carries the same satoshis and locking script: it cannot
		// be re-hashed either, so trusting it here would reintroduce the
		// unverified-parent-outputs hazard in precisely the case where we have
		// positive evidence this node's stored bytes are wrong.
		//
		// Scope, and why each other read path is left alone. None of them can take
		// this check as it stands:
		//
		//   - FileTypeOutputs (below) reconstructs outputs only, with no inputs,
		//     version or locktime, so there is no body to hash. It gets the weaker
		//     self-declared-txid check documented there.
		//   - GetTxInpointsFromExternalStore reads the same FileTypeTx blob, so
		//     unlike the outputs file it does have a full body to hash. It is
		//     excluded on cost, not impossibility: the function exists to stream
		//     past every script and output and keep only the input references
		//     (a ~99% memory saving on a blob that is external precisely because
		//     it is large), and re-hashing means parsing the whole transaction,
		//     which is the thing it is built to avoid. The residual exposure is
		//     real and named here rather than implied: TxMetaFieldsForDecorate
		//     routes through this path, so a stale or mis-keyed blob yields the
		//     wrong parent set for that transaction. What that set drives is
		//     dependency bookkeeping — subtree parent-presence and ordering
		//     checks, and block assembly's ordering (Validator.go's
		//     sendToBlockAssembler) — never a script or value check, so it is a
		//     lower priority than the outputs rather than unreachable.
		//   - getTxFromBins (the inline path, for transactions small enough to live
		//     inside the Aerospike record) re-hashes correctly only for a record
		//     created from a complete transaction. For those the bins do round-trip
		//     faithfully: inputs and outputs are written once at create and never
		//     mutated afterwards, because spending touches the utxos/spentUtxos
		//     bins, not these. But a record seeded from a UTXO-set snapshot
		//     (cmd/seeder, cmd/seedimport) keeps no inputs and a nil hole at every
		//     output index already spent at snapshot time, so it cannot hash back to
		//     its key — and TxIDChainHash() panics on the nil output rather than
		//     returning a mismatch. (cmd/seeder's restoreCoinbaseInput is the one
		//     exception: it keeps a rebuilt coinbase input only when the result does
		//     hash to the key, and only when no output is missing.) See
		//     meta.Data.TxIsSerializable, which documents
		//     that shape and which the existing txid comparisons (e.g.
		//     services/asset/repository) gate on. So extending the re-hash inline is
		//     not a straight cost/benefit trade: it needs that predicate in front of
		//     it, and a benchmark, since inline parents are the common case and
		//     would take a double-SHA256 plus a full re-serialization on essentially
		//     every spend validation, whereas external parents are a size-gated
		//     minority sitting behind a ten-second cache.
		//
		// (The pruner reads FileTypeTx too, in pruner/pruner_service.go, but only to
		// collect input references for deletion bookkeeping — it never feeds
		// consensus. A stale or mis-keyed blob there would mis-target that
		// bookkeeping, which is a separate problem this check does not cover.)
		if actual := tx.TxIDChainHash(); !actual.Equal(previousTxHash) {
			return nil, errors.NewStorageError("[GetTxFromExternalStore][%s] external tx does not hash to the key it was stored under (got %s) — stale, rotted or mis-keyed blob", previousTxHash.String(), actual.String())
		}
	} else {
		// As with the FileTypeTx branch, a parse failure is a storage fault: these
		// bytes were written from our own outputs, so bytes that no longer parse
		// mean this node's stored copy is wrong, not that the block or peer is.
		uw, err := utxopersister.NewUTXOWrapperFromReader(ctx, bufferedReader)
		if err != nil {
			return nil, errors.NewStorageError("[GetTxFromExternalStore][%s] could not read outputs from reader", previousTxHash.String(), err)
		}

		// This blob cannot be re-hashed — it holds outputs only, so there is no
		// body to hash back to a txid — but it does carry a self-declared txid,
		// written from the same hash it is keyed under (see
		// StorePartialTransactionExternally). Comparing it is not cryptographic
		// and a rotted blob could carry a matching txid with corrupt outputs, so
		// this is strictly weaker than the FileTypeTx re-hash above. It does
		// however catch the stale-file and mis-keyed cases — the pruner's
		// delete-then-write race being the motivating one — which would otherwise
		// surface further downstream as "previous tx has no output at index N",
		// i.e. a consensus violation manufactured from a local fault.
		if !uw.TxID.Equal(previousTxHash) {
			return nil, errors.NewStorageError("[GetTxFromExternalStore][%s] external outputs blob declares a different txid (got %s) — stale, rotted or mis-keyed blob", previousTxHash.String(), uw.TxID.String())
		}

		// Size the output slice from the highest index in the blob, bounded.
		//
		// u.Index is a raw uint32 read straight out of local storage with no
		// bound of its own (utxopersister.UTXOWrapper.NewUTXOFromReader), and the
		// slice below is sized to maxIndex+1. A single flipped bit in that field
		// therefore allocates up to 2^32 pointers, ~34 GB, and OOMs the node
		// instead of producing the storage fault this whole function exists to
		// produce. The neighbouring untrusted fields in this format are already
		// capped this way — the UTXO count and each script length both refuse to
		// pre-size from the claim — so the index was the gap.
		//
		// maxExternalOutputCount is the lower of a policy-derived correctness
		// ceiling and a fixed resource ceiling; see its doc comment. The resource
		// ceiling is the one that binds in practice, so unlike the derivation
		// alone this CAN reject a parent the node would otherwise accept: one
		// carrying an output index at or above ~16.7M. Such a parent needs
		// >144 MB of outputs on the wire and none is known to exist, but the
		// rejection is permanent if one ever does, because neither writer rewrites
		// an existing blob. That is the deliberate trade: a diagnosable stall on a
		// parent nobody has seen, against an OOM kill on a single flipped bit,
		// which is not diagnosable because the node is gone before it can report.
		//
		// This replaces a utxopersister.PadUTXOsWithNil call whose padded slice was
		// discarded after its length was read, so it also drops one full-size
		// throwaway allocation per external outputs read.
		var maxIndex uint32
		for _, u := range uw.UTXOs {
			if u.Index > maxIndex {
				maxIndex = u.Index
			}
		}

		if bound := maxExternalOutputCount(s.settings); uint64(maxIndex) >= bound {
			return nil, errors.NewStorageError("[GetTxFromExternalStore][%s] external outputs blob declares output index %d, at or beyond the %d-slot reconstruction bound — rotted blob, or a parent larger than this node will rebuild", previousTxHash.String(), maxIndex, bound)
		}

		// maxIndex+1 unconditionally, including for an empty UTXO list: that is
		// what PadUTXOsWithNil returned for the same input (it starts maxIndex at
		// zero too), so an outputs blob with every output already pruned still
		// yields one nil slot rather than an empty slice. Downstream both read the
		// same — the outpoint decorator bounds-checks and nil-checks before use —
		// but keeping it identical means the bound is the only behaviour this
		// change introduces.
		tx.Outputs = make([]*bt.Output, int(maxIndex)+1)

		for _, u := range uw.UTXOs {
			lockingScript := bscript.NewFromBytes(u.Script)

			tx.Outputs[u.Index] = &bt.Output{
				Satoshis:      u.Value,
				LockingScript: lockingScript,
			}
		}
	}

	// Log transaction size for memory usage debugging
	if tx != nil && s.logger != nil {
		inputCount := len(tx.Inputs)
		outputCount := len(tx.Outputs)
		s.logger.Debugf("[GetTxFromExternalStore] Stream-parsed external tx %s: %d inputs, %d outputs", previousTxHash.String(), inputCount, outputCount)
	}

	return tx, nil
}

// GetTxInpointsFromExternalStore efficiently extracts TxInpoints from an external transaction
// by streaming and parsing only the input references (prevTxID + index), skipping all scripts and outputs.
// This provides massive memory savings (99%+) by avoiding loading potentially large output scripts into memory.
func (s *Store) GetTxInpointsFromExternalStore(ctx context.Context, txHash chainhash.Hash) (subtree.TxInpoints, error) {
	ctx, _, _ = tracing.Tracer("aerospike").Start(ctx, "GetTxInpointsFromExternalStore",
		tracing.WithHistogram(prometheusTxMetaAerospikeMapGetExternal),
	)

	// Stream from external store - don't load entire file into memory
	reader, err := s.externalStore.GetIoReader(ctx, txHash[:], fileformat.FileTypeTx)
	if err != nil {
		return subtree.TxInpoints{}, errors.NewStorageError("[GetTxInpointsFromExternalStore][%s] could not get tx from external store", txHash.String(), err)
	}
	defer reader.Close()

	// Parse only input references from stream, skipping all scripts and outputs
	// A storage fault, not a transaction fault, for the same reason as the parse
	// failures in getExternalTransaction above: this blob was written from our own
	// bytes, so bytes that no longer parse mean this node's stored copy is torn or
	// truncated — never evidence about the block or the peer that served it.
	inputs, err := txparse.ParseInputReferencesFromExtendedTx(reader)
	if err != nil {
		return subtree.TxInpoints{}, errors.NewStorageError("[GetTxInpointsFromExternalStore][%s] could not parse input references", txHash.String(), err)
	}

	s.logger.Debugf("[GetTxInpointsFromExternalStore] Streamed and parsed %d input references from external tx %s, skipped all scripts", len(inputs), txHash.String())

	return subtree.NewTxInpointsFromInputs(inputs)
}

// sendGetBatch processes a batch of get requests efficiently
func (s *Store) sendGetBatch(batch []*batchGetItem) {
	// go-batcher recovers panics raised in this fn, so without completing every
	// item (via batchGetItem.complete(), which writes its result slot and marks
	// the shared completion.Group) a panic (e.g. a malformed bin tripping an
	// unchecked type assertion in getTxFromBins) would orphan every waiter in
	// this batch and leak their goroutines permanently.
	defer func() {
		signalBatchPanic(recover(), batch, "sendGetBatch", s.logger, func(it *batchGetItem, err error) {
			it.complete(batchGetItemData{Err: err})
		})
	}()

	items := make([]*utxo.UnresolvedMetaData, 0, len(batch))

	for idx, item := range batch {
		items = append(items, &utxo.UnresolvedMetaData{
			Hash:   item.hash,
			Idx:    idx,
			Fields: item.fields,
		})
	}

	// BatchDecorate already retries internally up to the batch policy MaxRetries
	// within its TotalTimeout. The previous extra 3x retry-with-sleep loop here
	// stacked on top of that (worst case ~3 x TotalTimeout, observed ~15m),
	// turning a transient stall into a multi-minute pin of every waiting
	// submitter. Make a single attempt and surface the error.
	if err := s.BatchDecorate(s.ctx, items); err != nil {
		s.logger.Errorf("failed to get batch of txmeta: %v", err)

		for _, bItem := range batch {
			bItem.complete(batchGetItemData{Err: err})
		}

		return
	}

	for _, item := range items {
		// send the data back to the original caller
		batch[item.Idx].complete(batchGetItemData{
			Data: item.Data,
			Err:  item.Err,
		})
	}
}
