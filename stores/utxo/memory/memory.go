// Package memory provides an in-memory implementation of the UTXO store interface
// primarily used for testing and development purposes.
//
// The implementation is thread-safe and provides full UTXO lifecycle management
// including spending, freezing, and reassignment operations. All data is stored
// in memory and will be lost when the process exits.
//
// # Memory Usage
//
// The memory store keeps all transactions and UTXOs in memory, which means:
//   - Memory usage grows linearly with the number of transactions
//   - No data persists between process restarts
//   - Not suitable for production use with large UTXO sets
//
// # Concurrency
//
// The implementation is thread-safe through:
//   - A mutex protecting the transaction map
//   - Atomic operations for block height and median time
//   - Copy-on-write for internal data structures
//
// # Usage
//
// The memory store is primarily intended for:
//   - Testing
//   - Development
//   - Small-scale simulations
//   - Scenarios where persistence is not required
package memory

import (
	"context"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// memoryData holds transaction data and UTXO state in memory.
type memoryData struct {
	tx                  *bt.Tx                             // The full transaction
	blockHeight         uint32                             // Block height where tx appears
	lockTime            uint32                             // Transaction lock time
	blockIDs            []uint32                           // Block heights where tx appears
	blockHeights        []uint32                           // Block heights where tx appears
	subtreeIdxs         []int                              // Subtree indexes where tx appears
	utxoMap             map[chainhash.Hash]*chainhash.Hash // Maps UTXO hash to spending tx hash
	utxoSpendableIn     map[uint32]uint32                  // Maps output index to spendable height
	frozenMap           map[chainhash.Hash]bool            // Tracks frozen UTXOs
	frozen              bool                               // Tracks whether the transaction is frozen
	conflicting         bool                               // Tracks whether the transaction is conflicting
	conflictingChildren []chainhash.Hash                   // Tracks conflicting children
	unspendable         bool                               // Tracks whether the transaction is unspendable
}

// Memory implements the UTXO store interface using in-memory data structures.
// It is thread-safe for concurrent access.
type Memory struct {
	logger          ulogger.Logger
	txs             map[chainhash.Hash]*memoryData // Main transaction storage
	txsMu           sync.Mutex                     // Protects txs map
	blockHeight     atomic.Uint32                  // Current block height
	medianBlockTime atomic.Uint32                  // Current median block time
}

// New creates a new in-memory UTXO store.
func New(logger ulogger.Logger) *Memory {
	return &Memory{
		logger:          logger,
		txs:             make(map[chainhash.Hash]*memoryData),
		txsMu:           sync.Mutex{},
		blockHeight:     atomic.Uint32{},
		medianBlockTime: atomic.Uint32{},
	}
}

// Health checks the store's health status.
// The memory store is always considered healthy if it exists.
func (m *Memory) Health(_ context.Context, _ bool) (int, string, error) {
	return http.StatusOK, "Memory Store available", nil
}

// Create stores a new transaction and its UTXOs in memory.
// Returns an error if the transaction already exists.
func (m *Memory) Create(_ context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var txHash *chainhash.Hash

	if options.TxID != nil {
		txHash = options.TxID
	} else {
		txHash = tx.TxIDChainHash()
	}

	if _, ok := m.txs[*txHash]; ok {
		return nil, errors.NewTxExistsError("%v already exists", txHash)
	}

	m.txs[*txHash] = &memoryData{
		tx:              tx,
		blockHeight:     blockHeight,
		lockTime:        tx.LockTime,
		blockIDs:        make([]uint32, 0),
		blockHeights:    make([]uint32, 0),
		subtreeIdxs:     make([]int, 0),
		utxoMap:         make(map[chainhash.Hash]*chainhash.Hash),
		utxoSpendableIn: map[uint32]uint32{},
		frozenMap:       make(map[chainhash.Hash]bool),
		frozen:          false,
		conflicting:     options.Conflicting,
		unspendable:     options.Unspendable,
	}

	if len(options.MinedBlockInfos) > 0 {
		for _, blockMeta := range options.MinedBlockInfos {
			m.txs[*txHash].blockIDs = append(m.txs[*txHash].blockIDs, blockMeta.BlockID)
			m.txs[*txHash].blockHeights = append(m.txs[*txHash].blockHeights, blockMeta.BlockHeight)
			m.txs[*txHash].subtreeIdxs = append(m.txs[*txHash].subtreeIdxs, blockMeta.SubtreeIdx)
		}
	}

	txMetaData, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, err
	}

	if options.Conflicting {
		txMetaData.Conflicting = true

		// set this transaction as a conflicting child in all its parents
		for idx, input := range tx.Inputs {
			parentTxHash := input.PreviousTxIDChainHash()

			parentTx, ok := m.txs[*parentTxHash]
			if !ok {
				return nil, errors.NewTxNotFoundError("parent tx %s of %s:%d not found", parentTxHash, txHash, idx)
			}

			if parentTx.conflictingChildren == nil {
				parentTx.conflictingChildren = make([]chainhash.Hash, 0)
			}

			if !slices.Contains(parentTx.conflictingChildren, *txHash) {
				parentTx.conflictingChildren = append(parentTx.conflictingChildren, *txHash)
			}
		}
	}

	if options.Unspendable {
		txMetaData.Unspendable = true
	}

	utxoHashes, err := utxo.GetUtxoHashes(tx)
	if err != nil {
		return nil, errors.NewProcessingError("failed to get utxo hashes", err)
	}

	for _, utxoHash := range utxoHashes {
		m.txs[*txHash].utxoMap[*utxoHash] = nil
	}

	return txMetaData, nil
}

func (m *Memory) Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error) {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	if data, ok := m.txs[*hash]; ok {
		txMeta, err := util.TxMetaDataFromTx(data.tx)
		if err != nil {
			return nil, err
		}

		utxoHashes, err := utxo.GetUtxoHashes(data.tx)
		if err != nil {
			return nil, errors.NewProcessingError("failed to get utxo hashes", err)
		}

		txMeta.BlockIDs = data.blockIDs
		txMeta.BlockHeights = data.blockHeights
		txMeta.SubtreeIdxs = data.subtreeIdxs
		txMeta.Conflicting = data.conflicting
		txMeta.ConflictingChildren = data.conflictingChildren
		txMeta.Frozen = data.frozen
		txMeta.Unspendable = data.unspendable
		txMeta.SpendingTxIDs = make([]*chainhash.Hash, len(utxoHashes))

		for idx, utxoHash := range utxoHashes {
			txMeta.SpendingTxIDs[idx] = data.utxoMap[*utxoHash]
		}

		return txMeta, nil
	}

	return nil, errors.NewTxNotFoundError("%v not found", hash)
}

// GetSpend checks the spend status of a UTXO.
// It verifies:
//   - Transaction exists
//   - UTXO exists
//   - UTXO frozen status
//   - Spending state
func (m *Memory) GetSpend(_ context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	metaData, err := m.Get(context.Background(), spend.TxID)
	if err != nil {
		return nil, err
	}

	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	txMemoryData, ok := m.txs[*spend.TxID]
	if !ok {
		return nil, errors.NewTxNotFoundError("%v not found", spend.TxID)
	}

	txSpend, ok := txMemoryData.utxoMap[*spend.UTXOHash]
	if !ok {
		return nil, errors.NewTxNotFoundError("%v not found", spend.TxID)
	}

	utxoStatus := utxo.CalculateUtxoStatus(txSpend, metaData.LockTime, m.blockHeight.Load())

	// check utxo is spendable
	if txMemoryData.utxoSpendableIn[spend.Vout] != 0 && txMemoryData.utxoSpendableIn[spend.Vout] > m.blockHeight.Load() {
		utxoStatus = utxo.Status_UNSPENDABLE
	}

	// check if frozen
	if txMemoryData.frozenMap[*spend.UTXOHash] {
		utxoStatus = utxo.Status_FROZEN
	}

	return &utxo.SpendResponse{
		Status:       int(utxoStatus),
		SpendingTxID: txSpend,
		LockTime:     metaData.LockTime,
	}, nil
}

func (m *Memory) GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	return m.Get(ctx, hash)
}

func (m *Memory) Delete(ctx context.Context, hash *chainhash.Hash) error {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	if _, ok := m.txs[*hash]; !ok {
		return errors.NewTxNotFoundError("%v not found", hash)
	}

	delete(m.txs, *hash)

	return nil
}

// Spend marks UTXOs as spent by updating their spending transaction ID.
// It verifies:
//   - Transaction exists
//   - UTXO exists and matches hash
//   - UTXO is not frozen
//   - UTXO is spendable (maturity/timelock)
//   - UTXO is not already spent by different tx
func (m *Memory) Spend(ctx context.Context, tx *bt.Tx, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	spends, err := utxo.GetSpends(tx)
	if err != nil {
		return nil, err
	}

	var (
		errorFound bool
		spendsDone = make([]*utxo.Spend, 0, len(spends))
	)

	for _, spend := range spends {
		tx, ok := m.txs[*spend.TxID]
		if !ok {
			errorFound = true
			spend.Err = errors.NewTxNotFoundError("%v not found", spend.TxID)

			continue
		}

		if tx.frozen {
			errorFound = true
			spend.Err = errors.NewUtxoFrozenError("%v is frozen", spend.TxID)

			continue
		}

		if tx.conflicting {
			errorFound = true
			spend.Err = errors.NewTxConflictingError("%v is conflicting", spend.TxID)

			continue
		}

		spendTxID, ok := tx.utxoMap[*spend.UTXOHash]
		if !ok {
			errorFound = true
			spend.Err = errors.NewTxNotFoundError("%v not found", spend.TxID)

			continue
		}

		if spendTxID != nil {
			if *spendTxID != *spend.SpendingTxID {
				errorFound = true
				spend.Err = errors.NewUtxoSpentError(*spendTxID, spend.Vout, *spend.UTXOHash, *spend.SpendingTxID)

				spend.ConflictingTxID = spendTxID
			}

			// same spend tx ID, just ignore and continue
			continue
		}

		// check utxo is frozen
		if tx.frozenMap[*spend.UTXOHash] {
			errorFound = true
			spend.Err = errors.NewUtxoFrozenError("%v is frozen", spend.TxID)

			break
		}

		// check utxo is spendable
		if tx.utxoSpendableIn[spend.Vout] != 0 && m.txs[*spend.TxID].utxoSpendableIn[spend.Vout] > m.blockHeight.Load() {
			errorFound = true
			spend.Err = errors.NewTxLockTimeError("%v is not spendable until %d", spend.TxID, m.txs[*spend.TxID].utxoSpendableIn[spend.Vout])

			break
		}

		tx.utxoMap[*spend.UTXOHash] = spend.SpendingTxID

		spendsDone = append(spendsDone, spend)
	}

	if errorFound {
		unspendErr := m.unspendUnlocked(spendsDone)
		return spends, errors.NewTxInvalidError("spend failed: %v", unspendErr)
	}

	return spends, nil
}

// Unspend marks UTXOs as unspent by clearing their spending transaction ID.
func (m *Memory) Unspend(_ context.Context, spends []*utxo.Spend, flagAsUnspendable ...bool) error {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	return m.unspendUnlocked(spends)
}

func (m *Memory) unspendUnlocked(spends []*utxo.Spend) error {
	for _, spend := range spends {
		if _, ok := m.txs[*spend.TxID]; !ok {
			return errors.NewTxNotFoundError("%v not found", spend.TxID)
		}

		_, ok := m.txs[*spend.TxID].utxoMap[*spend.UTXOHash]
		if !ok {
			return errors.NewTxNotFoundError("%v not found", spend.TxID)
		}

		m.txs[*spend.TxID].utxoMap[*spend.UTXOHash] = nil
	}

	return nil
}

// SetMinedMulti records which blocks a transaction appears in.
func (m *Memory) SetMinedMulti(_ context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) error {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	for _, hash := range hashes {
		if _, ok := m.txs[*hash]; !ok {
			return errors.NewTxNotFoundError("%v not found", hash)
		}

		m.txs[*hash].blockIDs = append(m.txs[*hash].blockIDs, minedBlockInfo.BlockID)
		m.txs[*hash].blockHeights = append(m.txs[*hash].blockHeights, minedBlockInfo.BlockHeight)
		m.txs[*hash].subtreeIdxs = append(m.txs[*hash].subtreeIdxs, minedBlockInfo.SubtreeIdx)
	}

	return nil
}

// BatchDecorate efficiently fetches metadata for multiple transactions.
// Unlike other implementations, this always fetches full transaction data.
func (m *Memory) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxo.UnresolvedMetaData, fields ...fields.FieldName) error {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	for _, unresolvedMetaData := range unresolvedMetaDataSlice {
		if _, ok := m.txs[unresolvedMetaData.Hash]; !ok {
			// do not throw error, MetaBatchDecorate should not fail if a tx is not found
			// just add the error to the item itself
			unresolvedMetaData.Err = errors.NewTxNotFoundError("%v not found", unresolvedMetaData.Hash)
			continue
		}

		txMeta, err := util.TxMetaDataFromTx(m.txs[unresolvedMetaData.Hash].tx)
		if err != nil {
			return err
		}

		txMeta.BlockIDs = m.txs[unresolvedMetaData.Hash].blockIDs
		txMeta.BlockHeights = m.txs[unresolvedMetaData.Hash].blockHeights
		txMeta.SubtreeIdxs = m.txs[unresolvedMetaData.Hash].subtreeIdxs
		txMeta.LockTime = m.txs[unresolvedMetaData.Hash].lockTime

		unresolvedMetaData.Data = txMeta
	}

	return nil
}

// PreviousOutputsDecorate fetches previous output data for transaction inputs.
func (m *Memory) PreviousOutputsDecorate(ctx context.Context, outpoints []*meta.PreviousOutput) error {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	for _, outpoint := range outpoints {
		data, ok := m.txs[outpoint.PreviousTxID]
		if !ok {
			return errors.NewTxNotFoundError("previous Tx %v not found", outpoint.PreviousTxID)
		}

		if len(data.tx.Outputs) <= int(outpoint.Vout) {
			return errors.NewTxNotFoundError("previous Tx %v not found", outpoint.PreviousTxID)
		}

		input := data.tx.Inputs[outpoint.Vout]
		if input == nil {
			return errors.NewTxNotFoundError("previous Tx %v not found", outpoint.PreviousTxID)
		}

		outpoint.LockingScript = *input.PreviousTxScript
		outpoint.Satoshis = input.PreviousTxSatoshis
	}

	return nil
}

// FreezeUTXOs marks UTXOs as frozen, preventing them from being spent.
// Returns an error if any UTXO:
//   - Doesn't exist
//   - Is already frozen
//   - Is already spent
func (m *Memory) FreezeUTXOs(_ context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	for _, spend := range spends {
		if _, ok := m.txs[*spend.TxID]; !ok {
			return errors.NewTxNotFoundError("%v not found", spend.TxID)
		}

		if _, ok := m.txs[*spend.TxID].utxoMap[*spend.UTXOHash]; !ok {
			return errors.NewTxNotFoundError("%v not found", spend.TxID)
		}

		// check that it is not already frozen, or spent
		if m.txs[*spend.TxID].frozenMap[*spend.UTXOHash] {
			return errors.NewUtxoFrozenError("%v is already frozen", spend.TxID)
		}

		if m.txs[*spend.TxID].utxoMap[*spend.UTXOHash] != nil {
			return errors.NewUtxoFrozenError("%v is already spent", spend.TxID)
		}

		m.txs[*spend.TxID].frozenMap[*spend.UTXOHash] = true
	}

	return nil
}

// UnFreezeUTXOs removes the frozen status from UTXOs.
// Returns an error if any UTXO:
//   - Doesn't exist
//   - Is not frozen
func (m *Memory) UnFreezeUTXOs(_ context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	for _, spend := range spends {
		if _, ok := m.txs[*spend.TxID]; !ok {
			return errors.NewTxNotFoundError("%v not found", spend.TxID)
		}

		if _, ok := m.txs[*spend.TxID].utxoMap[*spend.UTXOHash]; !ok {
			return errors.NewTxNotFoundError("%v not found", spend.TxID)
		}

		// check that it is frozen, otherwise return an error
		if !m.txs[*spend.TxID].frozenMap[*spend.UTXOHash] {
			return errors.NewUtxoFrozenError("%v is not frozen", spend.TxID)
		}

		m.txs[*spend.TxID].frozenMap[*spend.UTXOHash] = false
	}

	return nil
}

// ReAssignUTXO reassigns a frozen UTXO to a new transaction output.
// The UTXO must be frozen and will become spendable after
// ReAssignedUtxoSpendableAfterBlocks blocks.
func (m *Memory) ReAssignUTXO(_ context.Context, oldUtxo *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	// check whether the utxo is frozen
	if !m.txs[*oldUtxo.TxID].frozenMap[*oldUtxo.UTXOHash] {
		return errors.NewUtxoFrozenError("%v is not frozen", oldUtxo.TxID)
	}

	// set the spendable block height of the re-assigned utxo
	m.txs[*oldUtxo.TxID].utxoSpendableIn[newUtxo.Vout] = m.blockHeight.Load() + utxo.ReAssignedUtxoSpendableAfterBlocks

	// re-assign the utxo to the new utxo
	delete(m.txs[*oldUtxo.TxID].utxoMap, *oldUtxo.UTXOHash)
	m.txs[*oldUtxo.TxID].utxoMap[*newUtxo.UTXOHash] = nil

	return nil
}

func (m *Memory) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	tx, ok := m.txs[txHash]
	if !ok {
		return nil, errors.NewTxNotFoundError("%v not found", txHash)
	}

	counterConflictingMap := make(map[chainhash.Hash]struct{})

	for _, spendingTxID := range tx.utxoMap {
		if spendingTxID != nil {
			counterConflictingMap[*spendingTxID] = struct{}{}
		}
	}

	// get all the children of the counter conflicting transactions
	for counterConflictingTx := range counterConflictingMap {
		counterConflictingChildren, err := m.GetConflictingChildren(ctx, counterConflictingTx)
		if err != nil {
			return nil, err
		}

		for _, child := range counterConflictingChildren {
			counterConflictingMap[child] = struct{}{}
		}
	}

	counterConflicting := make([]chainhash.Hash, 0, len(counterConflictingMap))

	for hash := range counterConflictingMap {
		counterConflicting = append(counterConflicting, hash)
	}

	return counterConflicting, nil
}

func (m *Memory) GetConflictingChildren(_ context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	tx, ok := m.txs[txHash]
	if !ok {
		return nil, errors.NewTxNotFoundError("%v not found", txHash)
	}

	conflicting := make([]chainhash.Hash, 0)

	conflicting = append(conflicting, tx.conflictingChildren...)

	for _, spendingTxID := range tx.utxoMap {
		if spendingTxID != nil && !slices.Contains(conflicting, *spendingTxID) {
			conflicting = append(conflicting, *spendingTxID)
		}
	}

	return conflicting, nil
}

func (m *Memory) SetConflicting(_ context.Context, txHashes []chainhash.Hash, setValue bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	var (
		affectedParentSpends = make([]*utxo.Spend, 0, len(txHashes))
		spendingTxHashes     = make([]chainhash.Hash, 0, len(txHashes))
	)

	for _, txHash := range txHashes {
		if _, ok := m.txs[txHash]; !ok {
			return nil, nil, errors.NewTxNotFoundError("%v not found", txHash)
		}

		m.txs[txHash].conflicting = setValue

		// mark this transaction as a conflicting child in all its parents
		if setValue {
			for idx, input := range m.txs[txHash].tx.Inputs {
				parentTxHash := input.PreviousTxIDChainHash()

				parentTx, ok := m.txs[*parentTxHash]
				if !ok {
					return nil, nil, errors.NewTxNotFoundError("parent tx %s of %s:%d not found", parentTxHash, txHash, idx)
				}

				if parentTx.conflictingChildren == nil {
					parentTx.conflictingChildren = make([]chainhash.Hash, 0)
				}

				if !slices.Contains(parentTx.conflictingChildren, txHash) {
					parentTx.conflictingChildren = append(parentTx.conflictingChildren, txHash)
				}
			}
		}
	}

	return affectedParentSpends, spendingTxHashes, nil
}

func (m *Memory) SetUnspendable(_ context.Context, txHashes []chainhash.Hash, setValue bool) error {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	for _, txHash := range txHashes {
		if _, ok := m.txs[txHash]; !ok {
			return errors.NewTxNotFoundError("%v not found", txHash)
		}

		m.txs[txHash].unspendable = setValue
	}

	return nil
}

// SetBlockHeight updates the current block height using atomic operations.
func (m *Memory) SetBlockHeight(height uint32) error {
	m.blockHeight.Store(height)
	return nil
}

// GetBlockHeight returns the current block height atomically.
func (m *Memory) GetBlockHeight() uint32 {
	return m.blockHeight.Load()
}

// SetMedianBlockTime updates the median block time using atomic operations.
func (m *Memory) SetMedianBlockTime(medianTime uint32) error {
	m.logger.Debugf("setting median block time to %d", medianTime)
	m.medianBlockTime.Store(medianTime)

	return nil
}

// GetMedianBlockTime returns the current median block time atomically.
func (m *Memory) GetMedianBlockTime() uint32 {
	return m.medianBlockTime.Load()
}

// GetUtxoMap returns the utxo map for a given transaction hash.
// This method is intended only for testing purposes and is not part of the Store interface.
func (m *Memory) GetUtxoMap(txHash chainhash.Hash) map[chainhash.Hash]*chainhash.Hash {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	if _, ok := m.txs[txHash]; !ok {
		return nil
	}

	return m.txs[txHash].utxoMap
}

// delete removes a transaction from the store.
// This is an internal method used for testing.
func (m *Memory) delete(hash *chainhash.Hash) error {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	delete(m.txs, *hash)

	return nil
}
