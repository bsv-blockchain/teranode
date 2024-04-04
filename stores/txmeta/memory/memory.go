package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type Memory struct {
	logger          ulogger.Logger
	checkDuplicates bool
	mu              sync.Mutex
	txStatus        map[chainhash.Hash]txmeta.Data
}

func New(logger ulogger.Logger, checkDuplicates ...bool) *Memory {
	m := &Memory{
		logger:   logger,
		txStatus: make(map[chainhash.Hash]txmeta.Data),
	}

	if len(checkDuplicates) > 0 {
		m.checkDuplicates = checkDuplicates[0]
	}

	return m
}

func (m *Memory) GetMeta(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	return m.Get(ctx, hash)
}

func (m *Memory) Get(_ context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	m.mu.Lock()
	status, ok := m.txStatus[*hash]
	m.mu.Unlock()

	if !ok {
		return nil, txmeta.NewErrTxmetaNotFound(hash)
	}

	return &status, nil
}

func (m *Memory) MetaBatchDecorate(ctx context.Context, items []*txmeta.MissingTxHash, fields ...string) error {
	// TODO make this into a batch call
	for _, item := range items {
		data, err := m.Get(ctx, &item.Hash)
		if err != nil {
			if uerr, ok := err.(*errors.Error); ok {
				if uerr.Code == errors.ERR_NOT_FOUND {
					continue
				}
			}
			return err
		}
		item.Data = data
	}

	return nil
}

func (m *Memory) Create(_ context.Context, tx *bt.Tx) (*txmeta.Data, error) {

	m.mu.Lock()
	defer m.mu.Unlock()

	s, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, err
	}

	_, ok := m.txStatus[*tx.TxIDChainHash()]
	if ok {
		return s, txmeta.NewErrTxmetaAlreadyExists(tx.TxIDChainHash())
	}

	m.txStatus[*tx.TxIDChainHash()] = *s

	return s, nil
}

func (m *Memory) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) (err error) {
	for _, hash := range hashes {
		if err = m.SetMined(ctx, hash, blockID); err != nil {
			return err
		}
	}

	return nil
}

func (m *Memory) SetMined(_ context.Context, hash *chainhash.Hash, blockID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.txStatus[*hash]
	if !ok {
		s = txmeta.Data{}
	}

	if m.checkDuplicates {
		// check whether the has is already in the block hashes
		for _, b := range s.BlockIDs {
			if b == blockID {
				return fmt.Errorf("block %d already exists for tx %s", blockID, hash.String())
			}
		}
	}

	if s.BlockIDs == nil {
		s.BlockIDs = make([]uint32, 0)
	}

	s.BlockIDs = append(s.BlockIDs, blockID)

	m.txStatus[*hash] = s

	return nil
}

func (m *Memory) Delete(_ context.Context, hash *chainhash.Hash) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.txStatus, *hash)

	return nil
}
