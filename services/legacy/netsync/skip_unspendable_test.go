package netsync

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// createSpyStore records which transactions createUtxos asked the store to create, and
// which mined-block info it later stamped on the ones that already existed.
type createSpyStore struct {
	*nullstore.NullStore
	mu      sync.Mutex
	created map[chainhash.Hash]bool
	// alreadyExists makes every create report the transaction as already present, which is
	// how createUtxos is driven onto its follow-up SetMinedMulti merge path.
	alreadyExists bool
	stamps        []utxo.MinedBlockInfo
}

func (s *createSpyStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	s.mu.Lock()
	s.created[*tx.TxIDChainHash()] = true
	alreadyExists := s.alreadyExists
	s.mu.Unlock()

	if alreadyExists {
		return nil, nil, errors.NewTxExistsError("[createSpyStore] %s", tx.TxIDChainHash().String())
	}

	return s.NullStore.SpendAndCreate(ctx, tx, blockHeight, opts...)
}

func (s *createSpyStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	s.mu.Lock()
	s.stamps = append(s.stamps, info)
	s.mu.Unlock()

	out := make(map[chainhash.Hash][]uint32, len(hashes))
	for _, hash := range hashes {
		out[*hash] = []uint32{info.BlockID}
	}

	return out, nil
}

func (s *createSpyStore) SupportsOutpointOnlySpend() bool { return true }

func (s *createSpyStore) was(tx *bt.Tx) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.created[*tx.TxIDChainHash()]
}

// minedStamps returns a copy of every MinedBlockInfo the store was stamped with.
func (s *createSpyStore) minedStamps() []utxo.MinedBlockInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]utxo.MinedBlockInfo(nil), s.stamps...)
}

// twoTxMap holds one ordinary transaction and one whose only output is OP_FALSE OP_RETURN
// data, which is provably unspendable in every era.
func twoTxMap(t *testing.T) (normal, data *bt.Tx, m *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper]) {
	t.Helper()

	in := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff, PreviousTxSatoshis: 5_000}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{1}))

	normal = &bt.Tx{Version: 1, Inputs: []*bt.Input{in}, Outputs: []*bt.Output{
		{Satoshis: 1_000, LockingScript: &bscript.Script{bscript.OpDUP, bscript.OpHASH160}},
	}}

	in2 := &bt.Input{PreviousTxOutIndex: 1, SequenceNumber: 0xffffffff, PreviousTxSatoshis: 5_000}
	require.NoError(t, in2.PreviousTxIDAdd(&chainhash.Hash{2}))

	data = &bt.Tx{Version: 1, Inputs: []*bt.Input{in2}}
	require.NoError(t, data.AddOpReturnOutput([]byte("only data")))

	m = txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](2)
	m.Set(*normal.TxIDChainHash(), &TxMapWrapper{Tx: normal})
	m.Set(*data.TxIDChainHash(), &TxMapWrapper{Tx: data})

	return normal, data, m
}

// TestCreateUtxos_SkipsUnspendableTransactionsBelowTheCheckpointWhenAsked.
//
// A transaction with no spendable outputs can never be spent, so below the checkpoint on a
// node with no block persister there is nothing to store: SV Node keeps no record of it at
// all. The quick-validation path already honours blockvalidation_skipUnspendableTxStorage-
// DuringCatchup; the legacy block path, which is how mainnet receives its blocks, did not, so
// the setting changed nothing there. Its inputs are still spent in the next phase, so the
// UTXO set is unaffected.
func TestCreateUtxos_SkipsUnspendableTransactionsBelowTheCheckpointWhenAsked(t *testing.T) {
	const checkpointHeight = int32(1000)

	block := bsvutil.NewBlock(&wire.MsgBlock{Header: wire.BlockHeader{Version: 1}})
	block.SetHeight(500)

	run := func(t *testing.T, skipSetting, outpointOnly bool) (*bt.Tx, *bt.Tx, *createSpyStore) {
		t.Helper()

		tSettings, params := newOutpointOnlySettings(t, true, true, checkpointHeight)
		tSettings.BlockValidation.SkipUnspendableTxStorageDuringCatchup = skipSetting

		spy := &createSpyStore{NullStore: &nullstore.NullStore{}, created: map[chainhash.Hash]bool{}}
		sm := &SyncManager{settings: tSettings, chainParams: params, logger: ulogger.TestLogger{}, utxoStore: spy}

		normal, data, m := twoTxMap(t)
		require.NoError(t, sm.createUtxos(context.Background(), m, testBlockIdent(block), 7, outpointOnly))

		return normal, data, spy
	}

	t.Run("setting on, below checkpoint: data transaction is not stored", func(t *testing.T) {
		normal, data, spy := run(t, true, true)
		require.True(t, spy.was(normal), "a transaction with a spendable output is always stored")
		require.False(t, spy.was(data), "nothing can ever spend it and no persister needs it")
	})

	t.Run("setting off: both are stored", func(t *testing.T) {
		normal, data, spy := run(t, false, true)
		require.True(t, spy.was(normal))
		require.True(t, spy.was(data))
	})

	t.Run("above the checkpoint the setting does not apply", func(t *testing.T) {
		normal, data, spy := run(t, true, false)
		require.True(t, spy.was(normal))
		require.True(t, spy.was(data), "at the tip the mempool, the stamp and the persister may all need the row")
	})
}
