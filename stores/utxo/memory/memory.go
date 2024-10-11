package memory

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type memoryData struct {
	tx              *bt.Tx
	lockTime        uint32
	blockIDs        []uint32
	utxoMap         map[chainhash.Hash]*chainhash.Hash
	utxoSpendableIn map[uint32]uint32
	frozenMap       map[chainhash.Hash]bool
}

type Memory struct {
	logger          ulogger.Logger
	txs             map[chainhash.Hash]*memoryData
	txsMu           sync.Mutex
	blockHeight     atomic.Uint32
	medianBlockTime atomic.Uint32
}

func New(logger ulogger.Logger) *Memory {
	return &Memory{
		logger:          logger,
		txs:             make(map[chainhash.Hash]*memoryData),
		txsMu:           sync.Mutex{},
		blockHeight:     atomic.Uint32{},
		medianBlockTime: atomic.Uint32{},
	}
}

func (m *Memory) Health(_ context.Context, _ bool) (int, string, error) {
	return http.StatusOK, "Memory Store available", nil
}

func (m *Memory) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
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
		return nil, errors.NewTxAlreadyExistsError("%v already exists", txHash)
	}

	m.txs[*txHash] = &memoryData{
		tx:              tx,
		lockTime:        tx.LockTime,
		blockIDs:        make([]uint32, 0),
		utxoMap:         make(map[chainhash.Hash]*chainhash.Hash),
		utxoSpendableIn: map[uint32]uint32{},
		frozenMap:       make(map[chainhash.Hash]bool),
	}

	if len(options.BlockIDs) > 0 {
		m.txs[*txHash].blockIDs = options.BlockIDs
	}

	txMetaData, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, err
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

func (m *Memory) Get(_ context.Context, hash *chainhash.Hash, fields ...[]string) (*meta.Data, error) {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	if data, ok := m.txs[*hash]; ok {
		txMeta, err := util.TxMetaDataFromTx(data.tx)
		if err != nil {
			return nil, err
		}

		txMeta.BlockIDs = data.blockIDs

		return txMeta, nil
	}

	return nil, errors.NewTxNotFoundError("%v not found", hash)
}

func (m *Memory) GetSpend(_ context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	metaData, err := m.Get(context.Background(), spend.TxID)
	if err != nil {
		return nil, err
	}

	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	if _, ok := m.txs[*spend.TxID]; !ok {
		return nil, errors.NewTxNotFoundError("%v not found", spend.TxID)
	}

	txSpend, ok := m.txs[*spend.TxID].utxoMap[*spend.UTXOHash]
	if !ok {
		return nil, errors.NewTxNotFoundError("%v not found", spend.TxID)
	}

	utxoStatus := utxo.CalculateUtxoStatus(txSpend, metaData.LockTime, m.blockHeight.Load())

	// check if frozen
	if m.txs[*spend.TxID].frozenMap[*spend.UTXOHash] {
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

func (m *Memory) Spend(_ context.Context, spends []*utxo.Spend, blockHeight uint32) error {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	for _, spend := range spends {
		if _, ok := m.txs[*spend.TxID]; !ok {
			return errors.NewTxNotFoundError("%v not found", spend.TxID)
		}

		spendTxID, ok := m.txs[*spend.TxID].utxoMap[*spend.UTXOHash]
		if !ok {
			return errors.NewTxNotFoundError("%v not found", spend.TxID)
		}

		if spendTxID != nil {
			if *spendTxID != *spend.SpendingTxID {
				return utxo.NewErrSpent(spendTxID, spend.Vout, spend.UTXOHash, spend.UTXOHash)
			}
			// same spend tx ID, just ignore and continue
			continue
		}

		tx := m.txs[*spend.TxID]

		// check utxo is frozen
		if tx.frozenMap[*spend.UTXOHash] {
			return errors.NewFrozenError("%v is frozen", spend.TxID)
		}

		// check utxo is spendable
		if tx.utxoSpendableIn[spend.Vout] != 0 && m.txs[*spend.TxID].utxoSpendableIn[spend.Vout] > m.blockHeight.Load() {
			return errors.NewLockTimeError("%v is not spendable until %d", spend.TxID, m.txs[*spend.TxID].utxoSpendableIn[spend.Vout])
		}

		tx.utxoMap[*spend.UTXOHash] = spend.SpendingTxID
	}

	return nil
}

func (m *Memory) UnSpend(_ context.Context, spends []*utxo.Spend) error {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

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

func (m *Memory) SetMinedMulti(_ context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	for _, hash := range hashes {
		if _, ok := m.txs[*hash]; !ok {
			return errors.NewTxNotFoundError("%v not found", hash)
		}

		m.txs[*hash].blockIDs = append(m.txs[*hash].blockIDs, blockID)
	}

	return nil
}

func (m *Memory) BatchDecorate(_ context.Context, unresolvedMetaDataSlice []*utxo.UnresolvedMetaData, fields ...string) error {
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
		txMeta.LockTime = m.txs[unresolvedMetaData.Hash].lockTime

		unresolvedMetaData.Data = txMeta
	}

	return nil
}

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

func (m *Memory) FreezeUTXOs(_ context.Context, spends []*utxo.Spend) error {
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
			return errors.NewFrozenError("%v is already frozen", spend.TxID)
		}

		if m.txs[*spend.TxID].utxoMap[*spend.UTXOHash] != nil {
			return errors.NewFrozenError("%v is already spent", spend.TxID)
		}

		m.txs[*spend.TxID].frozenMap[*spend.UTXOHash] = true
	}

	return nil
}

func (m *Memory) UnFreezeUTXOs(_ context.Context, spends []*utxo.Spend) error {
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
			return errors.NewFrozenError("%v is not frozen", spend.TxID)
		}

		m.txs[*spend.TxID].frozenMap[*spend.UTXOHash] = false
	}

	return nil
}

func (m *Memory) ReAssignUTXO(_ context.Context, oldUtxo *utxo.Spend, newUtxo *utxo.Spend) error {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	// check whether the utxo is frozen
	if !m.txs[*oldUtxo.TxID].frozenMap[*oldUtxo.UTXOHash] {
		return errors.NewFrozenError("%v is not frozen", oldUtxo.TxID)
	}

	// set the spendable block height of the re-assigned utxo
	m.txs[*oldUtxo.TxID].utxoSpendableIn[newUtxo.Vout] = m.blockHeight.Load() + utxo.ReAssignedUtxoSpendableAfterBlocks

	// re-assign the utxo to the new utxo
	delete(m.txs[*oldUtxo.TxID].utxoMap, *oldUtxo.UTXOHash)
	m.txs[*oldUtxo.TxID].utxoMap[*newUtxo.UTXOHash] = nil

	return nil
}

func (m *Memory) SetBlockHeight(height uint32) error {
	m.blockHeight.Store(height)
	return nil
}

func (m *Memory) GetBlockHeight() uint32 {
	return m.blockHeight.Load()
}

func (m *Memory) SetMedianBlockTime(medianTime uint32) error {
	m.logger.Debugf("setting median block time to %d", medianTime)
	m.medianBlockTime.Store(medianTime)
	return nil
}

func (m *Memory) GetMedianBlockTime() uint32 {
	return m.medianBlockTime.Load()
}

func (m *Memory) delete(hash *chainhash.Hash) error {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	delete(m.txs, *hash)

	return nil
}
