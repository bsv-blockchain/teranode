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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/utxopersister"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	spendpkg "github.com/bitcoin-sv/teranode/stores/utxo/spend"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/exp/slices"
)

var (
	gocoreStat                  = gocore.NewStat("Aerospike")
	previousOutputsDecorateStat = gocoreStat.NewStat("PreviousOutputsDecorate").AddRanges(0, 1, 100, 1_000, 10_000, 100_000)
)

// batchGetItemData holds the result of a batch get operation
type batchGetItemData struct {
	Data *meta.Data // Retrieved data
	Err  error      // Any error encountered
}

// batchGetItem represents a single item in a batch get operation
type batchGetItem struct {
	hash   chainhash.Hash        // Transaction hash
	fields []fields.FieldName    // Fields to retrieve
	done   chan batchGetItemData // Channel for result
}

type batchOutpoint struct {
	outpoint *meta.PreviousOutput
	errCh    chan error
}

// GetSpend checks if a UTXO has been spent and returns its current status.
// The response includes:
//   - Current UTXO status (OK, SPENT, FROZEN, etc)
//   - Spending transaction ID if spent
//   - Spending transaction Vin (input index) if spent
//   - Lock time if applicable
//
// This operation verifies:
//   - UTXO exists
//   - UTXO hash matches
//   - Frozen status
//   - Current spend state
func (s *Store) GetSpend(_ context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	prometheusUtxoMapGet.Inc()

	sUtxoBatchSizeUint32, err := util.SafeIntToUint32(s.utxoBatchSize)
	if err != nil {
		return nil, err
	}

	keySource := uaerospike.CalculateKeySource(spend.TxID, spend.Vout/sUtxoBatchSizeUint32)

	key, aErr := aerospike.NewKey(s.namespace, s.setName, keySource)
	if aErr != nil {
		prometheusUtxoMapErrors.WithLabelValues("Get", aErr.Error()).Inc()
		s.logger.Errorf("Failed to init new aerospike key: %v\n", aErr)

		return nil, aErr
	}

	policy := util.GetAerospikeReadPolicy(s.settings)
	policy.ReplicaPolicy = aerospike.MASTER // we only want to read from the master for tx metadata, due to blockIDs being updated

	value, aErr := s.client.Get(policy, key, fields.FieldNamesToStrings(binNames)...)
	if aErr != nil {
		prometheusUtxoMapErrors.WithLabelValues("Get", aErr.Error()).Inc()

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
		frozen       bool
		conflicting  bool
	)

	if value != nil {
		utxos, ok := value.Bins[fields.Utxos.String()].([]interface{})
		if ok {
			b, ok := utxos[spend.Vout%sUtxoBatchSizeUint32].([]byte)
			if ok {
				if len(b) < 32 {
					return nil, errors.NewProcessingError("invalid utxo hash length", nil)
				}

				// check utxoHash is the same as the one we expect
				utxoHash := chainhash.Hash(b[:32])
				if !utxoHash.IsEqual(spend.UTXOHash) {
					return nil, errors.NewProcessingError("utxo hash mismatch", nil)
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
	}

	utxoStatus := utxo.CalculateUtxoStatus2(spendingData)

	// check utxo is spendable
	if spendableIn != 0 && spendableIn > int(s.blockHeight.Load()) {
		utxoStatus = utxo.Status_UNSPENDABLE
	}

	// check if frozen
	if frozen || (spendingData != nil && bytes.Equal(spendingData.Bytes(), frozenUTXOBytes)) {
		utxoStatus = utxo.Status_FROZEN
		spendingData = nil
	}

	if conflicting {
		utxoStatus = utxo.Status_CONFLICTING
	}

	return &utxo.SpendResponse{
		Status:       int(utxoStatus),
		SpendingData: spendingData,
	}, nil
}

// GetMeta retrieves only transaction metadata without the full transaction data.
// This is an optimized version of Get that excludes transaction body.
func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	return s.get(ctx, hash, utxo.MetaFields)
}

// Get operations in the Aerospike UTXO store support efficient retrieval
// of transaction data with configurable field selection and batching.
// The store provides several interfaces for data retrieval:
//   - Get: Retrieves full transaction data
//   - GetMeta: Retrieves only metadata
//   - GetSpend: Checks UTXO spend status
//   - BatchDecorate: Efficiently fetches data for multiple transactions
//   - PreviousOutputsDecorate: Retrieves previous output data

// Get retrieves transaction data with optional field selection.
// Parameters:
//   - ctx: Context for cancellation
//   - hash: Transaction hash
//   - fields: Optional list of fields to retrieve, defaults to all fields
//
// Returns:
//   - Transaction metadata
//   - Any error encountered
func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error) {
	bins := utxo.MetaFieldsWithTx
	if len(fields) > 0 {
		bins = fields
	}

	return s.get(ctx, hash, bins)
}

func (s *Store) get(_ context.Context, hash *chainhash.Hash, bins []fields.FieldName) (*meta.Data, error) {
	done := make(chan batchGetItemData)
	item := &batchGetItem{hash: *hash, fields: bins, done: done}

	if s.getBatcher != nil {
		s.getBatcher.Put(item)
	} else {
		// if the batcher is disabled, we still want to process the request in a go routine
		go func() {
			s.sendGetBatch([]*batchGetItem{item})
		}()
	}

	data := <-done
	if data.Err != nil {
		prometheusTxMetaAerospikeMapErrors.WithLabelValues("Get", data.Err.Error()).Inc()
	} else {
		prometheusTxMetaAerospikeMapGet.Inc()
	}

	return data.Data, data.Err
}

func (s *Store) getTxFromBins(bins aerospike.BinMap) (tx *bt.Tx, err error) {
	versionUint32, err := util.SafeIntToUint32(bins[fields.Version.String()].(int))
	if err != nil {
		return nil, err
	}

	locktimeUint32, err := util.SafeIntToUint32(bins[fields.LockTime.String()].(int))
	if err != nil {
		return nil, err
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
				return nil, errors.NewTxInvalidError("could not read input", err)
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
				return nil, errors.NewTxInvalidError("could not read output", err)
			}
		}
	}

	return tx, nil
}

func (s *Store) addAbstractedBins(bins []fields.FieldName) []fields.FieldName {
	// copy the bins slice to avoid modifying the original
	newBins := append([]fields.FieldName{}, bins...)

	// add missing bins
	if slices.Contains(newBins, fields.ParentTxHashes) {
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
	var err error

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	batchPolicy.ReplicaPolicy = aerospike.MASTER // we only want to read from the master for tx metadata, due to blockIDs being updated

	policy := util.GetAerospikeBatchReadPolicy(s.settings)

	batchRecords := make([]aerospike.BatchRecordIfc, len(items))

	for idx, item := range items {
		key, err := aerospike.NewKey(s.namespace, s.setName, item.Hash[:])
		if err != nil {
			return errors.NewProcessingError("failed to init new aerospike key for txMeta", err)
		}

		bins := []fields.FieldName{fields.Tx, fields.Fee, fields.SizeInBytes, fields.ParentTxHashes, fields.BlockIDs, fields.IsCoinbase}
		if len(item.Fields) > 0 {
			bins = item.Fields
		} else if len(optionalFields) > 0 {
			bins = optionalFields
		}

		item.Fields = s.addAbstractedBins(bins)

		record := aerospike.NewBatchRead(policy, key, fields.FieldNamesToStrings(item.Fields))
		// Add to batch
		batchRecords[idx] = record
	}

	if len(batchRecords) == 0 {
		return nil
	}

	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		s.logger.Errorf("error in aerospike map store batch records:\n%#v\n%v", batchRecords, err)
		return errors.NewStorageError("error in aerospike map store batch records", err)
	}

NEXT_BATCH_RECORD:
	for idx, batchRecord := range batchRecords {
		if err := batchRecord.BatchRec().Err; err != nil {
			items[idx].Data = nil

			if !util.CoinbasePlaceholderHash.Equal(items[idx].Hash) {
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

		external, ok := bins[fields.External.String()].(bool)
		if ok && external {
			if externalTx, err = s.GetTxFromExternalStore(ctx, items[idx].Hash); err != nil {
				items[idx].Err = err

				continue // because there was an error reading the transaction from the external store.
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
						items[idx].Err = errors.NewTxInvalidError("invalid tx", txErr)

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
								return errors.NewTxInvalidError("could not read input", err)
							}
						}
					}

					items[idx].Data.Tx = tx
				}

			case fields.Fee:
				fee, ok := bins[key.String()].(int)
				if !ok {
					items[idx].Err = errors.NewTxInvalidError("missing fee")

					continue NEXT_BATCH_RECORD // because there was an error reading the fee from the store.
				}

				items[idx].Data.Fee = uint64(fee) // nolint: gosec

			case fields.SizeInBytes:
				sizeInBytes, ok := bins[key.String()].(int)
				if !ok {
					items[idx].Err = errors.NewTxInvalidError("missing size in bytes")

					continue NEXT_BATCH_RECORD // because there was an error reading the size in bytes from the store.
				}

				items[idx].Data.SizeInBytes = uint64(sizeInBytes) // nolint:gosec

			case fields.ParentTxHashes:
				if external {
					items[idx].Data.ParentTxHashes = make([]chainhash.Hash, len(externalTx.Inputs))

					for i, input := range externalTx.Inputs {
						items[idx].Data.ParentTxHashes[i] = *input.PreviousTxIDChainHash()
					}
				} else {
					res, err := processInputs(bins)
					if err != nil {
						items[idx].Err = errors.NewTxInvalidError("could not process input interfaces", err)

						continue NEXT_BATCH_RECORD // because there was an error processing the input interfaces.
					}

					items[idx].Data.ParentTxHashes = res
				}

			case fields.BlockIDs:
				res, err := processBlockIDs(bins)
				if err != nil {
					items[idx].Err = errors.NewTxInvalidError("could not process block IDs", err)

					continue NEXT_BATCH_RECORD // because there was an error processing the block IDs.
				}

				items[idx].Data.BlockIDs = res

			case fields.BlockHeights:
				res, err := processBlockHeights(bins)
				if err != nil {
					items[idx].Err = errors.NewTxInvalidError("could not process block heights", err)

					continue NEXT_BATCH_RECORD // because there was an error processing the block heights.
				}

				items[idx].Data.BlockHeights = res

			case fields.SubtreeIdxs:
				res, err := processSubtreeIdxs(bins)
				if err != nil {
					items[idx].Err = errors.NewTxInvalidError("could not process subtree idxs", err)

					continue NEXT_BATCH_RECORD // because there was an error processing the subtree idxs.
				}

				items[idx].Data.SubtreeIdxs = res

			case fields.IsCoinbase:
				coinbaseBool, ok := bins[key.String()].(bool)
				if !ok {
					items[idx].Err = errors.NewTxInvalidError("missing is coinbase")

					continue NEXT_BATCH_RECORD // because there was an error reading the is coinbase from the store.
				}

				items[idx].Data.IsCoinbase = coinbaseBool

			case fields.Utxos:
				res, err := s.processUTXOs(ctx, &items[idx].Hash, bins)
				if err != nil {
					items[idx].Err = errors.NewTxInvalidError("could not process utxos", err)

					continue NEXT_BATCH_RECORD // because there was an error processing the utxos.
				}

				items[idx].Data.SpendingDatas = res

			case fields.Unspendable:
				unspendableBool, ok := bins[key.String()].(bool)
				if !ok {
					items[idx].Err = errors.NewTxInvalidError("missing unspendable")

					continue NEXT_BATCH_RECORD // because there was an error reading the unspendable from the store.
				}

				items[idx].Data.Unspendable = unspendableBool

			case fields.Conflicting:
				conflictingBool, ok := bins[key.String()].(bool)
				if ok {
					items[idx].Data.Conflicting = conflictingBool
				}

			case fields.ConflictingChildren:
				res, err := processConflictingChildren(bins)
				if err != nil {
					items[idx].Err = errors.NewTxInvalidError("could not process conflicting children", err)

					continue NEXT_BATCH_RECORD // because there was an error processing the conflicting children.
				}

				items[idx].Data.ConflictingChildren = res
			}
		}
	}

	prometheusTxMetaAerospikeMapGetMulti.Inc()
	prometheusTxMetaAerospikeMapGetMultiN.Add(float64(len(batchRecords)))

	return nil
}

func processInputs(bins aerospike.BinMap) ([]chainhash.Hash, error) {
	inputInterfaces, ok := bins[fields.Inputs.String()].([]interface{})
	if !ok {
		return nil, errors.NewStorageError("failed to get inputs")
	}

	res := make([]chainhash.Hash, len(inputInterfaces))

	for i, inputInterface := range inputInterfaces {
		input, ok := inputInterface.([]byte)
		if !ok {
			return nil, errors.NewStorageError("failed to get input")
		}

		res[i] = chainhash.Hash(input[:32])
	}

	return res, nil
}

func processBlockIDs(bins aerospike.BinMap) ([]uint32, error) {
	blockIDs, ok := bins[fields.BlockIDs.String()].([]interface{})
	if !ok {
		return nil, errors.NewTxInvalidError("missing block IDs")
	}

	if len(blockIDs) == 0 {
		return nil, nil
	}

	res := make([]uint32, len(blockIDs))

	for i, blockID := range blockIDs {
		blockIDInt, ok := blockID.(int)
		if !ok {
			return nil, errors.NewStorageError("failed to get block ID")
		}

		blockIDUint32, err := util.SafeIntToUint32(blockIDInt)
		if err != nil {
			return nil, errors.NewTxInvalidError("invalid block ID")
		}

		res[i] = blockIDUint32
	}

	return res, nil
}

func processBlockHeights(bins aerospike.BinMap) ([]uint32, error) {
	blockHeights, ok := bins[fields.BlockHeights.String()].([]interface{})
	if !ok {
		return nil, errors.NewTxInvalidError("missing block heights")
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

		blockHeightUint32, err := util.SafeIntToUint32(blockHeightInt)
		if err != nil {
			return nil, errors.NewTxInvalidError("invalid block height")
		}

		res[i] = blockHeightUint32
	}

	return res, nil
}

func processSubtreeIdxs(bins aerospike.BinMap) ([]int, error) {
	subtreeIdxs, ok := bins[fields.SubtreeIdxs.String()].([]interface{})
	if !ok {
		return nil, errors.NewTxInvalidError("missing subtree idxs")
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

func (s *Store) processUTXOs(ctx context.Context, txid *chainhash.Hash, bins aerospike.BinMap) ([]*spendpkg.SpendingData, error) {
	totalUtxos, ok := bins[fields.TotalUtxos.String()].(int)
	if !ok {
		return nil, errors.NewStorageError("failed to get totalUtxos")
	}

	utxos, ok := bins[fields.Utxos.String()].([]interface{})
	if !ok {
		return nil, errors.NewTxInvalidError("missing utxos")
	}

	spendingDatas := make([]*spendpkg.SpendingData, totalUtxos)

	for i, ui := range utxos {
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

// getAllExtraUTXOs retrieves all UTXOs from child records recursively
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

		keySource := uaerospike.CalculateKeySource(txID, uint32(recordNum)) // nolint: gosec

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

		// Extract UTXOs from the extra record
		if extraUtxos, ok := extraRecord.Bins[fields.Utxos.String()].([]interface{}); ok {
			for i, ui := range extraUtxos {
				if u, ok := ui.([]uint8); ok && len(u) == 68 {
					spendingData, err := spendpkg.NewSpendingDataFromBytes(u[32:])
					if err != nil {
						return errors.NewStorageError("failed to parse spending data from extra record", err)
					}

					spendingDatas[baseOffset+i] = spendingData
				}
			}
		}
	}

	return nil
}

// PreviousOutputsDecorate fetches output data for transaction inputs.
// Uses batching to optimize retrieval of previous output data:
//   - Deduplicates requests for the same transaction
//   - Handles both internal and external storage
//   - Returns locking scripts and amounts
func (s *Store) PreviousOutputsDecorate(_ context.Context, outpoints []*meta.PreviousOutput) error {
	errChans := make([]chan error, len(outpoints))

	for i, outpoint := range outpoints {
		errChan := make(chan error, 1)
		errChans[i] = errChan

		// Wrap the outpoint in OutpointRequest and put it in the batcher
		s.outpointBatcher.Put(&batchOutpoint{
			outpoint: outpoint,
			errCh:    errChan,
		})
	}

	// Wait for all error channels to receive a result
	for _, errChan := range errChans {
		if err := <-errChan; err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) sendOutpointBatch(batch []*batchOutpoint) {
	start := gocore.CurrentTime()
	defer func() {
		previousOutputsDecorateStat.AddTimeForRange(start, len(batch))
	}()

	var err error

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	batchPolicy.ReplicaPolicy = aerospike.MASTER // we only want to read from the master for tx metadata, due to blockIDs being updated

	policy := util.GetAerospikeBatchReadPolicy(s.settings)

	// Create a batch of records to read, with a max size of the batch
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(batch))
	batchRecordHashes := make([]chainhash.Hash, 0, len(batch))

	// we de-dupe the txs we need to lookup, since we may have multiple outpoints for the same tx
	// this is done by using a map of txHashes
	uniqueTxHashes := make(map[chainhash.Hash]struct{})
	for _, item := range batch {
		uniqueTxHashes[item.outpoint.PreviousTxID] = struct{}{}
	}

	// Create a batch of records to read from the txHashes
	for txHash := range uniqueTxHashes {
		key, err := aerospike.NewKey(s.namespace, s.setName, txHash[:])
		if err != nil {
			for _, item := range batch {
				sendErrorAndClose(item.errCh, errors.NewProcessingError("failed to init new aerospike key for txMeta", err))
			}

			return
		}

		bins := []fields.FieldName{fields.Version, fields.LockTime, fields.Inputs, fields.Outputs, fields.External}
		record := aerospike.NewBatchRead(policy, key, fields.FieldNamesToStrings(bins))

		// Add to batch records
		batchRecords = append(batchRecords, record)
		batchRecordHashes = append(batchRecordHashes, txHash)
	}

	// send the batch to aerospike
	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		for _, item := range batch {
			sendErrorAndClose(item.errCh, errors.NewStorageError("error in aerospike send outpoint batch records", err))
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
			if previousTx, err = s.GetOutpointsFromExternalStore(s.ctx, previousTxHash); err != nil {
				txErrors[previousTxHash] = err

				continue
			}
		} else {
			previousTx, err = s.getTxFromBins(bins)
			if err != nil {
				txErrors[previousTxHash] = errors.NewTxInvalidError("invalid tx", err)

				continue
			}
		}

		txs[previousTxHash] = previousTx
	}

	// Now we have all the txs, we can decorate the outpoints
	for _, batchItem := range batch {
		previousTx := txs[batchItem.outpoint.PreviousTxID]
		if previousTx == nil {
			if err, ok := txErrors[batchItem.outpoint.PreviousTxID]; ok {
				sendErrorAndClose(batchItem.errCh, err)
			} else {
				sendErrorAndClose(batchItem.errCh, errors.NewTxNotFoundError("previous tx not found: %v", batchItem.outpoint.PreviousTxID))
			}

			continue
		}

		batchItem.outpoint.Satoshis = previousTx.Outputs[batchItem.outpoint.Vout].Satoshis
		batchItem.outpoint.LockingScript = *previousTx.Outputs[batchItem.outpoint.Vout].LockingScript
		batchItem.errCh <- nil
		close(batchItem.errCh)
	}

	prometheusTxMetaAerospikeMapGetMulti.Inc()
	prometheusTxMetaAerospikeMapGetMultiN.Add(float64(len(batchRecords)))
}

func sendErrorAndClose(errCh chan error, err error) {
	select {
	case errCh <- err:
	default:
	}
	close(errCh)
}

func (s *Store) GetOutpointsFromExternalStore(ctx context.Context, previousTxHash chainhash.Hash) (*bt.Tx, error) {
	ctx, _, _ = tracing.StartTracing(ctx, "GetOutpointsFromExternalStore",
		tracing.WithHistogram(prometheusTxMetaAerospikeMapGetExternal),
	)

	if s.externalTxCache != nil {
		return s.externalTxCache.GetOrSet(previousTxHash, func() (*bt.Tx, bool, error) {
			tx, numberOfActiveOutputs, err := s.getExternalOutpoints(ctx, previousTxHash)
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

	tx, _, err := s.getExternalOutpoints(ctx, previousTxHash)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func (s *Store) getExternalOutpoints(ctx context.Context, previousTxHash chainhash.Hash) (*bt.Tx, int, error) {
	// get the full transaction from the external store
	tx, err := s.getExternalTransaction(ctx, previousTxHash)
	if err != nil {
		return nil, 0, err
	}

	// remove inputs, don't need them for outpoints
	tx.Inputs = nil

	numberOfActiveOutputs := 0

	// remove all non-spendable (OP_RETURN) outputs
	for i, output := range tx.Outputs {
		if output != nil && output.LockingScript != nil {
			script := *output.LockingScript

			// check whether this output is an OP_RETURN with 0 sat value
			if len(script) > 0 && (script[0] == 0x00 || script[0] == 0x6a) && output.Satoshis == 0 {
				tx.Outputs[i] = nil
			} else {
				numberOfActiveOutputs++
			}
		}
	}

	return tx, numberOfActiveOutputs, nil
}

func (s *Store) GetTxFromExternalStore(ctx context.Context, previousTxHash chainhash.Hash) (*bt.Tx, error) {
	ctx, _, _ = tracing.StartTracing(ctx, "GetTxFromExternalStore",
		tracing.WithHistogram(prometheusTxMetaAerospikeMapGetExternal),
	)

	return s.getExternalTransaction(ctx, previousTxHash)
}

func (s *Store) getExternalTransaction(ctx context.Context, previousTxHash chainhash.Hash) (*bt.Tx, error) {
	ext := "tx"

	// Get the raw transaction from the externalStore...
	txBytes, err := s.externalStore.Get(
		ctx,
		previousTxHash[:],
		options.WithFileExtension(ext),
	)
	if err != nil {
		// Try to get the data from an output file instead
		ext = "outputs"

		txBytes, err = s.externalStore.Get(
			ctx,
			previousTxHash[:],
			options.WithFileExtension(ext),
		)
		if err != nil {
			return nil, errors.NewStorageError("[GetTxFromExternalStore][%s] could not get tx from external store", previousTxHash.String(), err)
		}
	}

	tx := &bt.Tx{}

	if ext == "tx" {
		tx, err = bt.NewTxFromBytes(txBytes)
		if err != nil {
			return nil, errors.NewTxInvalidError("[GetTxFromExternalStore][%s] could not read tx from bytes", previousTxHash.String(), err)
		}
	} else {
		bufferedReader := bufio.NewReader(bytes.NewReader(txBytes))

		uw, err := utxopersister.NewUTXOWrapperFromReader(ctx, bufferedReader)
		if err != nil {
			return nil, errors.NewTxInvalidError("[GetTxFromExternalStore][%s] could not read outputs from reader", previousTxHash.String(), err)
		}

		utxos := utxopersister.PadUTXOsWithNil(uw.UTXOs)

		tx.Outputs = make([]*bt.Output, len(utxos))

		for _, u := range uw.UTXOs {
			lockingScript := bscript.NewFromBytes(u.Script)

			tx.Outputs[u.Index] = &bt.Output{
				Satoshis:      u.Value,
				LockingScript: lockingScript,
			}
		}
	}

	return tx, nil
}

// sendGetBatch processes a batch of get requests efficiently
func (s *Store) sendGetBatch(batch []*batchGetItem) {
	items := make([]*utxo.UnresolvedMetaData, 0, len(batch))

	for idx, item := range batch {
		items = append(items, &utxo.UnresolvedMetaData{
			Hash:   item.hash,
			Idx:    idx,
			Fields: item.fields,
		})
	}

	retries := 0

	for {
		if err := s.BatchDecorate(s.ctx, items); err != nil {
			if retries < 3 {
				retries++

				s.logger.Errorf("failed to get batch of txmeta", err)
				time.Sleep(time.Duration(retries) * time.Second)

				continue
			}

			// mark all items as errored
			for _, bItem := range batch {
				bItem.done <- batchGetItemData{
					Err: err,
				}
			}

			return
		}

		break
	}

	for _, item := range items {
		// send the data back to the original caller
		batch[item.Idx].done <- batchGetItemData{
			Data: item.Data,
			Err:  item.Err,
		}
	}
}
