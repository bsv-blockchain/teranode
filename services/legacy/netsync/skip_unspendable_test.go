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
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
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
	// stamped is every hash SetMinedMulti was asked about, and known the hashes it holds.
	stamped []chainhash.Hash
	known   map[chainhash.Hash]bool
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
	defer s.mu.Unlock()

	s.stamps = append(s.stamps, info)

	out := make(map[chainhash.Hash][]uint32, len(hashes))
	for _, hash := range hashes {
		s.stamped = append(s.stamped, *hash)

		if s.known != nil && !s.known[*hash] {
			return nil, errors.NewTxNotFoundError("[createSpyStore] %s", hash.String())
		}

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

// A skipped transaction can still be in the store: an earlier attempt at the same block that
// failed in subtree validation stored it as unmined. On 2026-09-24 that left 352 mined data
// transactions marked unmined for good on mainnet, and the pruner named their parents for
// preservation on every block. When a create finds the block's transactions already stored,
// which is what a retry looks like, the skipped ones are marked mined too, if the store holds
// them. A first attempt pays nothing.
func TestCreateUtxos_MarksSkippedTransactionsMinedOnARetry(t *testing.T) {
	const checkpointHeight = int32(1000)

	block := bsvutil.NewBlock(&wire.MsgBlock{Header: wire.BlockHeader{Version: 1}})
	block.SetHeight(500)

	run := func(t *testing.T, retry bool) (*bt.Tx, *createSpyStore) {
		t.Helper()

		tSettings, params := newOutpointOnlySettings(t, true, true, checkpointHeight)
		tSettings.BlockValidation.SkipUnspendableTxStorageDuringCatchup = true

		normal, data, m := twoTxMap(t)

		spy := &createSpyStore{NullStore: &nullstore.NullStore{}, created: map[chainhash.Hash]bool{}, alreadyExists: retry,
			known: map[chainhash.Hash]bool{*normal.TxIDChainHash(): true, *data.TxIDChainHash(): true}}
		sm := &SyncManager{settings: tSettings, chainParams: params, logger: ulogger.TestLogger{}, utxoStore: spy, blockchainClient: bestHeaderMock()}

		require.NoError(t, sm.createUtxos(context.Background(), m, testBlockIdent(block), 7, true))

		return data, spy
	}

	t.Run("a retry marks the skipped transaction mined", func(t *testing.T) {
		data, spy := run(t, true)
		require.Contains(t, spy.stamped, *data.TxIDChainHash())
	})

	t.Run("a first attempt does not ask about it", func(t *testing.T) {
		data, spy := run(t, false)
		require.NotContains(t, spy.stamped, *data.TxIDChainHash())
	})
}

// bestHeaderMock answers the retry path's best-header question with an unrelated header.
func bestHeaderMock() *blockchain.Mock {
	best, _ := fakeBestHeader(1)
	m := &blockchain.Mock{}
	m.On("GetBestBlockHeader", mock.Anything).Return(best, &model.BlockHeaderMeta{}, nil)

	return m
}
