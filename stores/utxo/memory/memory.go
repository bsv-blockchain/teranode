package memory

import (
	"context"
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
	tx       *bt.Tx
	lockTime uint32
	blockIDs []uint32
	utxoMap  map[chainhash.Hash]*chainhash.Hash
}

type Memory struct {
	logger      ulogger.Logger
	txs         map[chainhash.Hash]*memoryData
	txsMu       sync.Mutex
	blockHeight atomic.Uint32
}

func New(logger ulogger.Logger) utxo.Store {
	return &Memory{
		logger:      logger,
		txs:         make(map[chainhash.Hash]*memoryData),
		txsMu:       sync.Mutex{},
		blockHeight: atomic.Uint32{},
	}
}

func (m *Memory) Health(_ context.Context) (int, string, error) {
	return 0, "", nil
}

func (m *Memory) Create(_ context.Context, tx *bt.Tx, blockIDs ...uint32) (*meta.Data, error) {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	txHash := tx.TxIDChainHash()

	if _, ok := m.txs[*txHash]; ok {
		return nil, errors.New(errors.ERR_TX_NOT_FOUND, "%v not found", txHash)
	}

	m.txs[*txHash] = &memoryData{
		tx:       tx,
		lockTime: tx.LockTime,
		blockIDs: make([]uint32, 0),
		utxoMap:  make(map[chainhash.Hash]*chainhash.Hash),
	}

	if len(blockIDs) > 0 {
		m.txs[*txHash].blockIDs = blockIDs
	}

	txMetaData, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, err
	}

	utxoHashes, err := utxo.GetUtxoHashes(tx)
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "failed to get utxo hashes", err)
	}

	for _, utxoHash := range utxoHashes {
		m.txs[*txHash].utxoMap[utxoHash] = nil
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

	return nil, errors.New(errors.ERR_TX_NOT_FOUND, "%v not found", hash)
}

func (m *Memory) GetSpend(_ context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	metaData, err := m.Get(context.Background(), spend.TxID)
	if err != nil {
		return nil, err
	}

	if _, ok := m.txs[*spend.TxID]; !ok {
		return nil, errors.New(errors.ERR_TX_NOT_FOUND, "%v not found", spend.TxID)
	}

	txSpend, ok := m.txs[*spend.TxID].utxoMap[*spend.UTXOHash]
	if !ok {
		return nil, errors.New(errors.ERR_TX_NOT_FOUND, "%v not found", spend.TxID)
	}

	return &utxo.SpendResponse{
		Status:       int(utxo.CalculateUtxoStatus(txSpend, metaData.LockTime, m.blockHeight.Load())),
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
		return errors.New(errors.ERR_TX_NOT_FOUND, "%v not found", hash)
	}

	delete(m.txs, *hash)

	return nil
}

func (m *Memory) Spend(ctx context.Context, spends []*utxo.Spend, blockHeight uint32) error {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	for _, spend := range spends {
		if _, ok := m.txs[*spend.TxID]; !ok {
			return errors.New(errors.ERR_TX_NOT_FOUND, "%v not found", spend.TxID)
		}

		spendTxID, ok := m.txs[*spend.TxID].utxoMap[*spend.UTXOHash]
		if !ok {
			return errors.New(errors.ERR_TX_NOT_FOUND, "%v not found", spend.TxID)
		}

		if spendTxID != nil {
			if *spendTxID != *spend.SpendingTxID {
				return utxo.NewErrSpent(spendTxID, spend.Vout, spend.UTXOHash, spend.UTXOHash)
			}
			// same spend tx ID, just ignore and continue
			continue
		}

		m.txs[*spend.TxID].utxoMap[*spend.UTXOHash] = spend.SpendingTxID
	}

	return nil
}

func (m *Memory) UnSpend(_ context.Context, spends []*utxo.Spend) error {
	m.txsMu.Lock()
	defer m.txsMu.Unlock()

	for _, spend := range spends {
		if _, ok := m.txs[*spend.TxID]; !ok {
			return errors.New(errors.ERR_TX_NOT_FOUND, "%v not found", spend.TxID)
		}

		_, ok := m.txs[*spend.TxID].utxoMap[*spend.UTXOHash]
		if !ok {
			return errors.New(errors.ERR_TX_NOT_FOUND, "%v not found", spend.TxID)
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
			return errors.New(errors.ERR_TX_NOT_FOUND, "%v not found", hash)
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
			unresolvedMetaData.Err = errors.New(errors.ERR_TX_NOT_FOUND, "%v not found", unresolvedMetaData.Hash)
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
			return errors.New(errors.ERR_TX_NOT_FOUND, "previous Tx %v not found", outpoint.PreviousTxID)
		}

		if len(data.tx.Outputs) <= int(outpoint.Vout) {
			return errors.New(errors.ERR_TX_NOT_FOUND, "previous Tx %v not found", outpoint.PreviousTxID)
		}

		input := data.tx.Inputs[outpoint.Vout]
		if input == nil {
			return errors.New(errors.ERR_TX_NOT_FOUND, "previous Tx %v not found", outpoint.PreviousTxID)
		}

		outpoint.LockingScript = *input.PreviousTxScript
		outpoint.Satoshis = input.PreviousTxSatoshis
	}

	return nil
}

func (m *Memory) SetBlockHeight(height uint32) error {
	m.blockHeight.Store(height)
	return nil
}

func (m *Memory) GetBlockHeight() (uint32, error) {
	return m.blockHeight.Load(), nil
}
