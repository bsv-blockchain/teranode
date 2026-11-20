package repairmissingparents

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const payAddress = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"

// fixture is a parent with two outputs: output 0 spent by a mined child, output 1
// spent by an unmined child. Both spenders are extended transactions so the store
// can compute UTXO hashes from their inputs.
type fixture struct {
	parent       *bt.Tx
	minedChild   *bt.Tx
	unminedChild *bt.Tx
	blockHash    chainhash.Hash
	blockHeight  uint32
	blockID      uint32
}

func newFixture(t *testing.T, extra ...*bt.Output) *fixture {
	t.Helper()

	parent := bt.NewTx()
	require.NoError(t, parent.From(chainhash.HashH([]byte("grandparent")).String(), 0, "51", 20000))
	require.NoError(t, parent.PayToAddress(payAddress, 9000))
	require.NoError(t, parent.PayToAddress(payAddress, 9000))

	for _, o := range extra {
		parent.AddOutput(o)
	}

	minedChild := bt.NewTx()
	require.NoError(t, minedChild.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	require.NoError(t, minedChild.PayToAddress(payAddress, 8000))

	unminedChild := bt.NewTx()
	require.NoError(t, unminedChild.From(parent.TxID(), 1, parent.Outputs[1].LockingScript.String(), parent.Outputs[1].Satoshis))
	require.NoError(t, unminedChild.PayToAddress(payAddress, 8000))

	return &fixture{
		parent:       parent,
		minedChild:   minedChild,
		unminedChild: unminedChild,
		blockHash:    chainhash.HashH([]byte("block-965720")),
		blockHeight:  965720,
		blockID:      4242,
	}
}

// peer is a configurable fake asset service.
type peer struct {
	txs   map[string][]byte
	metas map[string]any
	utxos map[string]any
}

func (f *fixture) peer(items []peerUTXO) *peer {
	return &peer{
		txs: map[string][]byte{
			f.parent.TxID():       f.parent.ExtendedBytes(),
			f.minedChild.TxID():   f.minedChild.ExtendedBytes(),
			f.unminedChild.TxID(): f.unminedChild.ExtendedBytes(),
		},
		metas: map[string]any{f.parent.TxID(): f.meta()},
		utxos: map[string]any{f.parent.TxID(): items},
	}
}

func (f *fixture) meta() map[string]any {
	return map[string]any{
		"blockHeights":   []uint32{f.blockHeight},
		"blockHashes":    []string{f.blockHash.String()},
		"subtreeIdxs":    []int{3},
		"mainChainIndex": 0,
	}
}

func (p *peer) serve(t *testing.T) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/tx/", func(w http.ResponseWriter, r *http.Request) {
		b, ok := p.txs[strings.TrimPrefix(r.URL.Path, "/api/v1/tx/")]
		if !ok {
			http.NotFound(w, r)
			return
		}

		_, _ = w.Write(b)
	})

	jsonHandler := func(prefix string, m map[string]any) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, prefix), "/json")

			v, ok := m[id]
			if !ok {
				http.NotFound(w, r)
				return
			}

			_ = json.NewEncoder(w).Encode(v)
		}
	}

	mux.HandleFunc("/api/v1/txmeta/", jsonHandler("/api/v1/txmeta/", p.metas))
	mux.HandleFunc("/api/v1/utxos/", jsonHandler("/api/v1/utxos/", p.utxos))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return srv
}

func (f *fixture) peerServer(t *testing.T, items []peerUTXO) *httptest.Server {
	t.Helper()

	return f.peer(items).serve(t)
}

func (f *fixture) spentItems() []peerUTXO {
	return []peerUTXO{
		{Vout: 0, Status: statusSpent, SpendingData: spend.NewSpendingData(f.minedChild.TxIDChainHash(), 0)},
		{Vout: 1, Status: statusSpent, SpendingData: spend.NewSpendingData(f.unminedChild.TxIDChainHash(), 0)},
	}
}

// matching is the record the store holds once the fixture parent is rebuilt correctly.
func (f *fixture) matching() *meta.Data {
	return &meta.Data{
		BlockIDs:     []uint32{f.blockID},
		BlockHeights: []uint32{f.blockHeight},
		SpendingDatas: []*spend.SpendingData{
			spend.NewSpendingData(f.minedChild.TxIDChainHash(), 0),
			spend.NewSpendingData(f.unminedChild.TxIDChainHash(), 0),
		},
	}
}

func hashPtr(h chainhash.Hash) any {
	want := h
	return mock.MatchedBy(func(p *chainhash.Hash) bool { return p != nil && p.IsEqual(&want) })
}

func txWithID(id string) any {
	return mock.MatchedBy(func(tx *bt.Tx) bool { return tx != nil && tx.TxID() == id })
}

func spendOf(h chainhash.Hash, vout uint32) any {
	want := h
	return mock.MatchedBy(func(s *utxo.Spend) bool { return s != nil && s.TxID.IsEqual(&want) && s.Vout == vout })
}

// chainFor is a blockchain client in FSM state whose one block is on the current chain.
func chainFor(state blockchain.FSMStateType, blockHash chainhash.Hash, blockID, height uint32) *blockchain.Mock {
	chain := &blockchain.Mock{}
	chain.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)
	chain.On("GetBestHeightAndTime", mock.Anything).Return(int(height+1500), 0, nil)
	chain.On("GetBlockHeader", mock.Anything, hashPtr(blockHash)).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{ID: blockID, Height: height}, nil)
	chain.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{blockID}).Return(true, nil)

	return chain
}

func idleChainFor(blockHash chainhash.Hash, blockID, height uint32) *blockchain.Mock {
	return chainFor(blockchain.FSMStateIDLE, blockHash, blockID, height)
}

func idleChain(t *testing.T, f *fixture) *blockchain.Mock {
	t.Helper()

	return idleChainFor(f.blockHash, f.blockID, f.blockHeight)
}

// unminedIterator yields the given unmined transactions once, then ends.
func unminedIterator(txs ...*bt.Tx) *utxo.MockUnminedTxIterator {
	batch := make([]*utxo.UnminedTransaction, 0, len(txs))

	for _, tx := range txs {
		inpoints, err := subtree.NewTxInpointsFromTx(tx)
		if err != nil {
			panic(err)
		}

		batch = append(batch, &utxo.UnminedTransaction{Node: &subtree.Node{Hash: *tx.TxIDChainHash()}, TxInpoints: &inpoints})
	}

	iter := new(utxo.MockUnminedTxIterator)

	if len(batch) > 0 {
		iter.On("Next", mock.Anything).Return(batch, nil).Once()
	}

	iter.On("Next", mock.Anything).Return(nil, nil)
	iter.On("Close").Return(nil)

	return iter
}

// baseStore is a store with no unmined transactions, unless unmined is given.
func baseStore(unmined ...*bt.Tx) *utxo.MockUtxostore {
	store := new(utxo.MockUtxostore)
	store.On("SetBlockHeight", mock.Anything).Return(nil)
	store.On("GetUnminedTxIterator").Return(unminedIterator(unmined...), nil)

	return store
}

// missingParentStore is a store where the fixture parent is absent and both spenders
// are held locally (minedChild mined, unminedChild unmined).
func missingParentStore(f *fixture) *utxo.MockUtxostore {
	store := baseStore()
	store.On("Get", mock.Anything, hashPtr(*f.parent.TxIDChainHash()), mock.Anything).
		Return(nil, errors.NewTxNotFoundError("not found")).Once()

	return holdSpenders(store, f)
}

// holdSpenders makes the store hold both fixture spenders: minedChild mined, unminedChild unmined.
func holdSpenders(store *utxo.MockUtxostore, f *fixture) *utxo.MockUtxostore {
	store.On("Get", mock.Anything, hashPtr(*f.minedChild.TxIDChainHash()), mock.Anything).
		Return(&meta.Data{BlockIDs: []uint32{f.blockID + 1}}, nil)
	store.On("Get", mock.Anything, hashPtr(*f.unminedChild.TxIDChainHash()), mock.Anything).
		Return(&meta.Data{}, nil)

	return store
}

func newRepairer(t *testing.T, store utxo.Store, chain blockchain.ClientI, peerURL string, opts Options) (*Repairer, *bytes.Buffer) {
	t.Helper()

	out := &bytes.Buffer{}
	opts.Stdout = out

	p := NewHTTPPeer(peerURL, nil)
	p.retryDelay = time.Millisecond

	return NewRepairer(ulogger.TestLogger{}, test.CreateBaseTestSettings(t), store, chain, p, opts), out
}

func runOne(t *testing.T, r *Repairer) Result {
	t.Helper()

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Results, 1)

	return report.Results[0]
}

func assertNoWrites(t *testing.T, store *utxo.MockUtxostore) {
	t.Helper()

	store.AssertNotCalled(t, "SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	store.AssertNotCalled(t, "DeleteComplete", mock.Anything, mock.Anything)
}

func TestRefusesWhenNodeIsNotIdle(t *testing.T) {
	f := newFixture(t)

	chain := &blockchain.Mock{}
	running := blockchain.FSMStateRUNNING
	chain.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	store := new(utxo.MockUtxostore)

	r, _ := newRepairer(t, store, chain, f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{*f.parent.TxIDChainHash()}})

	_, err := r.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "RUNNING")
	require.Contains(t, err.Error(), "setfsmstate")
	require.Contains(t, err.Error(), "--force-live")

	assertNoWrites(t, store)
	store.AssertNotCalled(t, "Get", mock.Anything, mock.Anything, mock.Anything)
}

func TestRefusesWhenFSMStateIsUnknown(t *testing.T) {
	f := newFixture(t)

	chain := &blockchain.Mock{}
	chain.On("GetFSMCurrentState", mock.Anything).Return((*blockchain.FSMStateType)(nil), nil)

	store := new(utxo.MockUtxostore)

	r, _ := newRepairer(t, store, chain, f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{*f.parent.TxIDChainHash()}})

	_, err := r.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "no FSM state")

	store.AssertNotCalled(t, "Get", mock.Anything, mock.Anything, mock.Anything)
}

func TestForceLiveOverridesGate(t *testing.T) {
	f := newFixture(t)

	chain := chainFor(blockchain.FSMStateRUNNING, f.blockHash, f.blockID, f.blockHeight)

	store := holdSpenders(baseStore(), f)
	store.On("Get", mock.Anything, hashPtr(*f.parent.TxIDChainHash()), mock.Anything).Return(f.matching(), nil)

	r, _ := newRepairer(t, store, chain, f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{*f.parent.TxIDChainHash()}, ForceLive: true})

	require.Equal(t, OutcomeAlreadyPresent, runOne(t, r).Outcome)
}

func TestAlreadyPresentMatchingPeerIsSkipped(t *testing.T) {
	f := newFixture(t)

	store := holdSpenders(baseStore(), f)
	store.On("Get", mock.Anything, hashPtr(*f.parent.TxIDChainHash()), mock.Anything).Return(f.matching(), nil)

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{*f.parent.TxIDChainHash()}})

	res := runOne(t, r)
	require.Equal(t, OutcomeAlreadyPresent, res.Outcome, res.Detail)
	assertNoWrites(t, store)
}

// A record left by an interrupted run has outputs unspent that the peer records as
// spent. The rerun must report it, not trust its presence.
func TestAlreadyPresentDifferingFromPeerIsReported(t *testing.T) {
	f := newFixture(t)

	partial := f.matching()
	partial.SpendingDatas[1] = nil

	store := holdSpenders(baseStore(), f)
	store.On("Get", mock.Anything, hashPtr(*f.parent.TxIDChainHash()), mock.Anything).Return(partial, nil)

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{*f.parent.TxIDChainHash()}})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, OutcomePresentMismatch, report.Results[0].Outcome)
	require.Contains(t, report.Results[0].Detail, "output 1 is unspent in the store")
	require.True(t, report.Failed())
	assertNoWrites(t, store)
}

func TestRepairsMissingParentWithExactSpentState(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	store := missingParentStore(f)

	var createOpts []utxo.CreateOption

	store.On("SpendAndCreate", mock.Anything, txWithID(f.parent.TxID()), f.blockHeight, mock.Anything).
		Run(func(args mock.Arguments) { createOpts = args.Get(3).([]utxo.CreateOption) }).
		Return(&meta.Data{}, nil, nil).Once()

	// Verification read after the repair. Registered after the Once above so testify
	// falls through to it once the not-found expectation is exhausted.
	store.On("Get", mock.Anything, hashPtr(parentHash), mock.Anything).Return(f.matching(), nil)

	var replayOpts [][]utxo.CreateOption

	currentHeight := f.blockHeight + 1500
	for _, child := range []*bt.Tx{f.minedChild, f.unminedChild} {
		store.On("SpendAndCreate", mock.Anything, txWithID(child.TxID()), currentHeight, mock.Anything).
			Run(func(args mock.Arguments) { replayOpts = append(replayOpts, args.Get(3).([]utxo.CreateOption)) }).
			Return(nil, []*utxo.Spend{}, nil).Once()
	}

	r, out := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Results, 1)

	res := report.Results[0]
	require.Equal(t, OutcomeRepaired, res.Outcome, res.Detail)
	require.Equal(t, []uint32{f.blockHeight}, res.BlockHeights)
	require.Equal(t, []uint32{f.blockID}, res.BlockIDs)
	require.Len(t, res.Spends, 2)
	require.False(t, report.Failed())

	co := &utxo.CreateOptions{}
	for _, o := range createOpts {
		o(co)
	}

	require.Len(t, co.MinedBlockInfos, 1)
	require.Equal(t, utxo.MinedBlockInfo{BlockID: f.blockID, BlockHeight: f.blockHeight, SubtreeIdx: 3, OnLongestChain: true}, co.MinedBlockInfos[0])
	require.NotNil(t, co.TxID)
	require.True(t, co.TxID.IsEqual(&parentHash))
	require.True(t, co.CreateOnly, "the parent's own inputs must not be re-spent")
	require.Nil(t, co.IsCoinbase, "the coinbase flag must come from the transaction, never from the peer")

	require.Len(t, replayOpts, 2)

	for _, opts := range replayOpts {
		ro := &utxo.CreateOptions{}
		for _, o := range opts {
			o(ro)
		}

		require.True(t, ro.SpendOnly)
		require.False(t, ro.IgnoreFlags.IgnoreConflicting, "the replay must keep the conflicting guard")
		require.False(t, ro.IgnoreFlags.IgnoreLocked, "the replay must keep the locked guard")
	}

	store.AssertExpectations(t)
	store.AssertNotCalled(t, "DeleteComplete", mock.Anything, mock.Anything)
	require.Contains(t, out.String(), "plan:")
	require.Contains(t, out.String(), parentHash.String())
	require.Contains(t, out.String(), string(OutcomeRepaired))
}

// The read-back is the last line of defence: a store that ends up with a spent output
// unspent must fail the record and roll it back.
func TestVerifyMismatchFailsAndRollsBack(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	store := missingParentStore(f)
	store.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&meta.Data{}, []*utxo.Spend{}, nil)

	wrong := f.matching()
	wrong.SpendingDatas[1] = nil
	store.On("Get", mock.Anything, hashPtr(parentHash), mock.Anything).Return(wrong, nil)
	store.On("DeleteComplete", mock.Anything, hashPtr(parentHash)).Return(nil).Once()

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeFailed, res.Outcome)
	require.Contains(t, res.Detail, "verify: ")
	require.Contains(t, res.Detail, "output 1 is unspent in the store")
	require.Contains(t, res.Detail, "record rolled back")
	store.AssertExpectations(t)
}

func TestVerifyRejectsWrongBlockIDs(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	store := missingParentStore(f)
	store.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&meta.Data{}, []*utxo.Spend{}, nil)

	wrong := f.matching()
	wrong.BlockIDs = []uint32{f.blockID + 9}
	store.On("Get", mock.Anything, hashPtr(parentHash), mock.Anything).Return(wrong, nil)
	store.On("DeleteComplete", mock.Anything, hashPtr(parentHash)).Return(nil).Once()

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeFailed, res.Outcome)
	require.Contains(t, res.Detail, "block IDs")
	store.AssertExpectations(t)
}

func TestReplayFailureRollsBack(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()
	currentHeight := f.blockHeight + 1500

	store := missingParentStore(f)
	store.On("SpendAndCreate", mock.Anything, txWithID(f.parent.TxID()), f.blockHeight, mock.Anything).Return(&meta.Data{}, nil, nil).Once()
	store.On("SpendAndCreate", mock.Anything, txWithID(f.minedChild.TxID()), currentHeight, mock.Anything).Return(nil, []*utxo.Spend{}, nil).Maybe()
	store.On("SpendAndCreate", mock.Anything, txWithID(f.unminedChild.TxID()), currentHeight, mock.Anything).
		Return(nil, nil, errors.NewStorageError("DEVICE_OVERLOAD")).Maybe()
	store.On("DeleteComplete", mock.Anything, hashPtr(parentHash)).Return(nil).Once()

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeFailed, res.Outcome)
	require.Contains(t, res.Detail, "DEVICE_OVERLOAD")
	require.Contains(t, res.Detail, "record rolled back")
	store.AssertExpectations(t)
}

func TestRollbackFailureIsReported(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	store := missingParentStore(f)
	store.On("SpendAndCreate", mock.Anything, txWithID(f.parent.TxID()), f.blockHeight, mock.Anything).Return(&meta.Data{}, nil, nil).Once()
	store.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.NewStorageError("timeout"))
	store.On("DeleteComplete", mock.Anything, hashPtr(parentHash)).Return(errors.NewStorageError("delete refused")).Once()

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeFailed, res.Outcome)
	require.Contains(t, res.Detail, "ROLLBACK FAILED")
	store.AssertExpectations(t)
}

func TestInconsistentSpenderIsReportedAndRolledBack(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()
	currentHeight := f.blockHeight + 1500

	store := missingParentStore(f)
	store.On("SpendAndCreate", mock.Anything, txWithID(f.parent.TxID()), f.blockHeight, mock.Anything).Return(&meta.Data{}, nil, nil).Once()
	store.On("SpendAndCreate", mock.Anything, txWithID(f.minedChild.TxID()), currentHeight, mock.Anything).Return(nil, []*utxo.Spend{}, nil).Maybe()
	store.On("SpendAndCreate", mock.Anything, txWithID(f.unminedChild.TxID()), currentHeight, mock.Anything).
		Return(nil, nil, errors.NewUtxoSpentError(parentHash, 1, chainhash.Hash{}, spend.NewSpendingData(&chainhash.Hash{0xff}, 0))).Maybe()
	store.On("DeleteComplete", mock.Anything, hashPtr(parentHash)).Return(nil).Once()

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, OutcomeInconsistentSpender, report.Results[0].Outcome)
	require.True(t, report.Failed())
	store.AssertExpectations(t)
}

func TestDryRunWritesNothing(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	store := missingParentStore(f)

	r, out := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}, DryRun: true})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Results, 1)
	require.Equal(t, OutcomePlanned, report.Results[0].Outcome)
	require.Len(t, report.Results[0].Spends, 2)
	require.False(t, report.Failed())

	assertNoWrites(t, store)
	require.Contains(t, out.String(), f.minedChild.TxID())
}

func TestPeerPrunedIsReported(t *testing.T) {
	f := newFixture(t)
	other := chainhash.HashH([]byte("unknown-to-peer"))

	store := baseStore()
	store.On("Get", mock.Anything, hashPtr(other), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found"))

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{other}})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, OutcomePeerPruned, report.Results[0].Outcome)
	require.True(t, report.Failed())
	assertNoWrites(t, store)
}

// The peer must return the transaction that was asked for.
func TestPeerReturningAnotherTxIsRejected(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	p := f.peer(f.spentItems())
	p.txs[f.parent.TxID()] = f.minedChild.ExtendedBytes()

	store := missingParentStore(f)

	r, _ := newRepairer(t, store, idleChain(t, f), p.serve(t).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeFailed, res.Outcome)
	require.Contains(t, res.Detail, "for requested "+parentHash.String())
	assertNoWrites(t, store)
}

func TestNonExtendedParentIsRejected(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	p := f.peer(f.spentItems())
	p.txs[f.parent.TxID()] = f.parent.Bytes()

	store := missingParentStore(f)

	r, _ := newRepairer(t, store, idleChain(t, f), p.serve(t).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeFailed, res.Outcome)
	require.Contains(t, res.Detail, "parent from peer is not extended")
	assertNoWrites(t, store)
}

func TestNonExtendedSpenderIsRejected(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	p := f.peer(f.spentItems())
	p.txs[f.unminedChild.TxID()] = f.unminedChild.Bytes()

	store := missingParentStore(f)

	r, _ := newRepairer(t, store, idleChain(t, f), p.serve(t).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeFailed, res.Outcome)
	require.Contains(t, res.Detail, "is not extended")
	assertNoWrites(t, store)
}

func TestChainMismatchAbortsRecord(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	tests := []struct {
		name   string
		chain  func(c *blockchain.Mock)
		meta   func(m map[string]any)
		detail string
	}{
		{
			name: "block unknown here",
			chain: func(c *blockchain.Mock) {
				c.On("GetBlockHeader", mock.Anything, hashPtr(f.blockHash)).Return(nil, nil, errors.NewNotFoundError("block not found"))
			},
			detail: "is not in our blockchain",
		},
		{
			name: "block at a different height here",
			chain: func(c *blockchain.Mock) {
				c.On("GetBlockHeader", mock.Anything, hashPtr(f.blockHash)).
					Return(&model.BlockHeader{}, &model.BlockHeaderMeta{ID: f.blockID, Height: f.blockHeight + 1}, nil)
			},
			detail: "is at height",
		},
		{
			name: "block marked invalid here",
			chain: func(c *blockchain.Mock) {
				c.On("GetBlockHeader", mock.Anything, hashPtr(f.blockHash)).
					Return(&model.BlockHeader{}, &model.BlockHeaderMeta{ID: f.blockID, Height: f.blockHeight, Invalid: true}, nil)
			},
			detail: "marked invalid",
		},
		{
			name: "block on a fork here",
			chain: func(c *blockchain.Mock) {
				c.On("GetBlockHeader", mock.Anything, hashPtr(f.blockHash)).
					Return(&model.BlockHeader{}, &model.BlockHeaderMeta{ID: f.blockID, Height: f.blockHeight}, nil)
				c.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{f.blockID}).Return(false, nil)
			},
			detail: "not on our current chain",
		},
		{
			name:   "peer predates mainChainIndex",
			meta:   func(m map[string]any) { delete(m, "mainChainIndex") },
			detail: "did not report mainChainIndex",
		},
		{
			name:   "peer could not place the tx on its main chain",
			meta:   func(m map[string]any) { m["mainChainIndex"] = -1 },
			detail: "mainChainIndex -1",
		},
		{
			name:   "subtree indexes missing",
			meta:   func(m map[string]any) { m["subtreeIdxs"] = []int{} },
			detail: "0 subtree indexes for 1 heights",
		},
		{
			name:   "negative subtree index",
			meta:   func(m map[string]any) { m["subtreeIdxs"] = []int{-1} },
			detail: "subtree index -1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chain := &blockchain.Mock{}
			idle := blockchain.FSMStateIDLE
			chain.On("GetFSMCurrentState", mock.Anything).Return(&idle, nil)
			chain.On("GetBestHeightAndTime", mock.Anything).Return(int(f.blockHeight+1500), 0, nil)

			if tt.chain != nil {
				tt.chain(chain)
			} else {
				chain.On("GetBlockHeader", mock.Anything, hashPtr(f.blockHash)).
					Return(&model.BlockHeader{}, &model.BlockHeaderMeta{ID: f.blockID, Height: f.blockHeight}, nil)
				chain.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{f.blockID}).Return(true, nil)
			}

			p := f.peer(f.spentItems())
			m := f.meta()

			if tt.meta != nil {
				tt.meta(m)
			}

			p.metas[f.parent.TxID()] = m

			store := missingParentStore(f)

			r, _ := newRepairer(t, store, chain, p.serve(t).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

			res := runOne(t, r)
			require.Equal(t, OutcomeChainMismatch, res.Outcome)
			require.Contains(t, res.Detail, tt.detail)
			assertNoWrites(t, store)
		})
	}
}

// A peer fork block we never received is dropped; only the main-chain entry is written.
func TestPeerForkBlockIsDropped(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()
	forkHash := chainhash.HashH([]byte("fork-block"))

	chain := idleChain(t, f)
	chain.On("GetBlockHeader", mock.Anything, hashPtr(forkHash)).Return(nil, nil, errors.NewNotFoundError("block not found"))

	p := f.peer(f.spentItems())
	p.metas[f.parent.TxID()] = map[string]any{
		"blockHeights":   []uint32{f.blockHeight, f.blockHeight},
		"blockHashes":    []string{forkHash.String(), f.blockHash.String()},
		"subtreeIdxs":    []int{1, 3},
		"mainChainIndex": 1,
	}

	store := missingParentStore(f)

	r, _ := newRepairer(t, store, chain, p.serve(t).URL, Options{TxIDs: []chainhash.Hash{parentHash}, DryRun: true})

	res := runOne(t, r)
	require.Equal(t, OutcomePlanned, res.Outcome, res.Detail)
	require.Equal(t, []uint32{f.blockID}, res.BlockIDs)
	require.Contains(t, res.Detail, "dropped peer block")
}

func TestPeerOutputStateMustBeUnambiguous(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()
	spent := spend.NewSpendingData(f.minedChild.TxIDChainHash(), 0)

	tests := []struct {
		name   string
		items  []peerUTXO
		detail string
	}{
		{"frozen", []peerUTXO{{Vout: 0, Status: "FROZEN", SpendingData: spent}, {Vout: 1, Status: statusOK}}, "output 0 is FROZEN"},
		{"conflicting", []peerUTXO{{Vout: 0, Status: "CONFLICTING"}, {Vout: 1, Status: statusOK}}, "output 0 is CONFLICTING"},
		{"locked", []peerUTXO{{Vout: 0, Status: "LOCKED"}, {Vout: 1, Status: statusOK}}, "output 0 is LOCKED"},
		{"not found (peer lookup failed)", []peerUTXO{{Vout: 0, Status: statusNotFound}, {Vout: 1, Status: statusOK}}, "output 0 is NOT_FOUND"},
		{"spent without spending data", []peerUTXO{{Vout: 0, Status: statusSpent}, {Vout: 1, Status: statusOK}}, "SPENT without spending data"},
		{"unknown status", []peerUTXO{{Vout: 0, Status: "WHATEVER"}, {Vout: 1, Status: statusOK}}, "output 0 is WHATEVER"},
		{"missing output", []peerUTXO{{Vout: 0, Status: statusOK}}, "state for 1 outputs, transaction has 2"},
		{"duplicate output", []peerUTXO{{Vout: 0, Status: statusOK}, {Vout: 0, Status: statusOK}}, "duplicate or out-of-range output 0"},
		{"out of range output", []peerUTXO{{Vout: 0, Status: statusOK}, {Vout: 7, Status: statusOK}}, "duplicate or out-of-range output 7"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := missingParentStore(f)

			r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, tt.items).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

			res := runOne(t, r)
			require.Equal(t, OutcomePeerStateUnusable, res.Outcome)
			require.Contains(t, res.Detail, tt.detail)
			assertNoWrites(t, store)
		})
	}
}

// An OP_FALSE OP_RETURN output is never stored as a UTXO, so NOT_FOUND is its normal state.
func TestUnspendableOutputMayBeNotFound(t *testing.T) {
	dataScript, err := bscript.NewFromASM("OP_FALSE OP_RETURN 68656c6c6f")
	require.NoError(t, err)

	f := newFixture(t, &bt.Output{LockingScript: dataScript, Satoshis: 0})
	parentHash := *f.parent.TxIDChainHash()

	items := append(f.spentItems(), peerUTXO{Vout: 2, Status: statusNotFound})

	store := missingParentStore(f)

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, items).URL, Options{TxIDs: []chainhash.Hash{parentHash}, DryRun: true})

	res := runOne(t, r)
	require.Equal(t, OutcomePlanned, res.Outcome, res.Detail)
}

func TestScanDiscoversParentsOfUnminedChildren(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	store := baseStore(f.unminedChild)
	store.On("Get", mock.Anything, hashPtr(parentHash), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found"))
	store.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&meta.Data{}, nil)

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{Scan: true, DryRun: true})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Results, 1)
	require.True(t, report.Results[0].Parent.IsEqual(&parentHash))
	require.Equal(t, OutcomePlanned, report.Results[0].Outcome)
	require.Equal(t, 1, report.Scanned)
}

// secondParent returns a further parent that child also spends, at input 1.
func secondParent(t *testing.T, child *bt.Tx) *bt.Tx {
	t.Helper()

	parent2 := bt.NewTx()
	require.NoError(t, parent2.From(chainhash.HashH([]byte("grandparent-2")).String(), 0, "51", 5000))
	require.NoError(t, parent2.PayToAddress(payAddress, 4000))

	require.NoError(t, child.From(parent2.TxID(), 0, parent2.Outputs[0].LockingScript.String(), parent2.Outputs[0].Satoshis))

	return parent2
}

func TestSpenderWithAnotherMissingParentIsQueued(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	parent2 := secondParent(t, f.unminedChild)
	parent2Hash := *parent2.TxIDChainHash()

	store := missingParentStore(f)
	store.On("Get", mock.Anything, hashPtr(parentHash), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found"))
	store.On("Get", mock.Anything, hashPtr(parent2Hash), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found"))

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}, DryRun: true})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Results, 2, "the second missing parent must be queued from the unmined spender's inputs")

	outcomes := map[chainhash.Hash]Outcome{}
	for _, res := range report.Results {
		outcomes[res.Parent] = res.Outcome
	}

	require.Equal(t, OutcomePlanned, outcomes[parentHash])
	// The peer server in this test does not know parent2, so it is reported as pruned.
	require.Equal(t, OutcomePeerPruned, outcomes[parent2Hash])
}

// A mined spender's other parent is absent because it was pruned by design; it is not a hole.
func TestMinedSpenderAbsentOtherParentIsNotQueued(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	parent2 := secondParent(t, f.minedChild)
	parent2Hash := *parent2.TxIDChainHash()

	store := missingParentStore(f)
	store.On("Get", mock.Anything, hashPtr(parentHash), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found"))
	store.On("Get", mock.Anything, hashPtr(parent2Hash), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found"))

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}, DryRun: true})

	res := runOne(t, r)
	require.Equal(t, OutcomePlanned, res.Outcome, res.Detail)
}

// The spender also spends Q:0, which is unspent here. Replaying it would mark Q:0 spent
// as a side effect, so the parent is refused.
func TestSpenderTouchingAnUnspentOutputElsewhereIsRefused(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	q := secondParent(t, f.unminedChild)
	qHash := *q.TxIDChainHash()

	store := missingParentStore(f)
	store.On("Get", mock.Anything, hashPtr(qHash), mock.Anything).Return(&meta.Data{}, nil)
	store.On("GetSpend", mock.Anything, spendOf(qHash, 0)).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeUnsafeSpender, res.Outcome)
	require.Contains(t, res.Detail, qHash.String()+":0, which is OK here")
	assertNoWrites(t, store)
}

func TestSpenderWithOtherInputSpentByAnotherTxIsRefused(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	q := secondParent(t, f.unminedChild)
	qHash := *q.TxIDChainHash()
	other := chainhash.HashH([]byte("someone-else"))

	store := missingParentStore(f)
	store.On("Get", mock.Anything, hashPtr(qHash), mock.Anything).Return(&meta.Data{}, nil)
	store.On("GetSpend", mock.Anything, spendOf(qHash, 0)).
		Return(&utxo.SpendResponse{Status: int(utxo.Status_SPENT), SpendingData: spend.NewSpendingData(&other, 0)}, nil)

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeUnsafeSpender, res.Outcome)
	assertNoWrites(t, store)
}

// A spender that is mined on our chain but was pruned here cannot be replayed when it
// also spends another parent: the store refuses the re-spend of a deleted child on a
// surviving parent and cannot find a pruned one. The parent is refused before any write.
func TestPrunedSpenderWithOtherInputIsRefused(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	q := secondParent(t, f.unminedChild)
	qHash := *q.TxIDChainHash()

	store := unheldUnminedChildStore(f)
	store.On("Get", mock.Anything, hashPtr(qHash), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found"))

	p := f.peer(f.spentItems())
	p.metas[f.unminedChild.TxID()] = f.meta()

	r, _ := newRepairer(t, store, idleChain(t, f), p.serve(t).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Results, 1, "an absent parent of a pruned spender is not queued")
	require.Equal(t, OutcomePrunedSpender, report.Results[0].Outcome)
	require.Contains(t, report.Results[0].Detail, "outside this parent")
	assertNoWrites(t, store)
}

func TestSpenderWithOtherInputAlreadySpentByItIsReplayed(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	q := secondParent(t, f.unminedChild)
	qHash := *q.TxIDChainHash()

	store := missingParentStore(f)
	store.On("Get", mock.Anything, hashPtr(qHash), mock.Anything).Return(&meta.Data{}, nil)
	store.On("GetSpend", mock.Anything, spendOf(qHash, 0)).
		Return(&utxo.SpendResponse{Status: int(utxo.Status_SPENT), SpendingData: spend.NewSpendingData(f.unminedChild.TxIDChainHash(), 1)}, nil)
	store.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&meta.Data{}, []*utxo.Spend{}, nil)
	store.On("Get", mock.Anything, hashPtr(parentHash), mock.Anything).Return(f.matching(), nil)

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeRepaired, res.Outcome, res.Detail)
}

// The peer's vin must point at the input that actually spends the output.
func TestPeerSpendingVinMustMatchSpenderInput(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	items := f.spentItems()
	items[1].SpendingData = spend.NewSpendingData(f.unminedChild.TxIDChainHash(), 1)

	store := missingParentStore(f)

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, items).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeUnsafeSpender, res.Outcome)
	assertNoWrites(t, store)
}

func TestHTTPPeerRetriesTransientNotFound(t *testing.T) {
	var calls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			http.NotFound(w, r)
			return
		}

		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)

	p := NewHTTPPeer(srv.URL, nil)
	p.retryDelay = time.Millisecond

	body, err := p.get(context.Background(), "/x")
	require.NoError(t, err)
	require.Equal(t, `{"ok":true}`, string(body))
	require.Equal(t, int32(3), calls.Load())
}

func TestHTTPPeerPersistentNotFoundIsNotFound(t *testing.T) {
	var calls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	p := NewHTTPPeer(srv.URL, nil)
	p.retryDelay = time.Millisecond

	_, err := p.get(context.Background(), "/x")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrNotFound))
	require.Equal(t, int32(peerAttempts), calls.Load())
}

func TestHTTPPeerDoesNotRetryBadRequest(t *testing.T) {
	var calls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)

	p := NewHTTPPeer(srv.URL, nil)
	p.retryDelay = time.Millisecond

	_, err := p.get(context.Background(), "/x")
	require.Error(t, err)
	require.Equal(t, int32(1), calls.Load())
}

func TestHTTPPeerOversizedResponseIsAnExplicitError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte{'x'}, 11))
	}))
	t.Cleanup(srv.Close)

	p := NewHTTPPeer(srv.URL, nil)
	p.maxBody = 10

	_, err := p.get(context.Background(), "/x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds 10 bytes")
}

// The spender also spends an output the peer reports unspent. Replaying it would spend
// that output too, so the parent is refused before any write.
func TestSpenderSpendingAnUnplannedParentOutputIsRefused(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	x := bt.NewTx()
	require.NoError(t, x.From(f.parent.TxID(), 0, f.parent.Outputs[0].LockingScript.String(), f.parent.Outputs[0].Satoshis))
	require.NoError(t, x.From(f.parent.TxID(), 1, f.parent.Outputs[1].LockingScript.String(), f.parent.Outputs[1].Satoshis))
	require.NoError(t, x.PayToAddress(payAddress, 17000))

	p := f.peer([]peerUTXO{
		{Vout: 0, Status: statusSpent, SpendingData: spend.NewSpendingData(x.TxIDChainHash(), 0)},
		{Vout: 1, Status: statusOK},
	})
	p.txs[x.TxID()] = x.ExtendedBytes()

	store := missingParentStore(f)
	store.On("Get", mock.Anything, hashPtr(*x.TxIDChainHash()), mock.Anything).Return(&meta.Data{}, nil)

	r, _ := newRepairer(t, store, idleChain(t, f), p.serve(t).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeUnsafeSpender, res.Outcome)
	require.Contains(t, res.Detail, "input 1 spends output 1")
	assertNoWrites(t, store)
}

// The peer names a spender for an output that the spender does not spend. The replay
// could never mark it spent, so the parent is refused before any write.
func TestPlannedSpendTheSpenderDoesNotMakeIsRefused(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	p := f.peer([]peerUTXO{
		{Vout: 0, Status: statusSpent, SpendingData: spend.NewSpendingData(f.minedChild.TxIDChainHash(), 0)},
		{Vout: 1, Status: statusSpent, SpendingData: spend.NewSpendingData(f.minedChild.TxIDChainHash(), 0)},
	})

	store := missingParentStore(f)

	r, _ := newRepairer(t, store, idleChain(t, f), p.serve(t).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeUnsafeSpender, res.Outcome)
	require.Contains(t, res.Detail, "peer records 2 outputs as spent by")
	assertNoWrites(t, store)
}

// The spender also spends an output the peer assigns to a different spender: a double
// spend of the parent. The parent is refused before any write.
func TestSpenderDoubleSpendingAnotherPlannedOutputIsRefused(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	x := bt.NewTx()
	require.NoError(t, x.From(f.parent.TxID(), 0, f.parent.Outputs[0].LockingScript.String(), f.parent.Outputs[0].Satoshis))
	require.NoError(t, x.From(f.parent.TxID(), 1, f.parent.Outputs[1].LockingScript.String(), f.parent.Outputs[1].Satoshis))
	require.NoError(t, x.PayToAddress(payAddress, 17000))

	p := f.peer([]peerUTXO{
		{Vout: 0, Status: statusSpent, SpendingData: spend.NewSpendingData(x.TxIDChainHash(), 0)},
		{Vout: 1, Status: statusSpent, SpendingData: spend.NewSpendingData(f.unminedChild.TxIDChainHash(), 0)},
	})
	p.txs[x.TxID()] = x.ExtendedBytes()

	store := missingParentStore(f)
	store.On("Get", mock.Anything, hashPtr(*x.TxIDChainHash()), mock.Anything).Return(&meta.Data{}, nil)

	r, _ := newRepairer(t, store, idleChain(t, f), p.serve(t).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeUnsafeSpender, res.Outcome)
	require.Contains(t, res.Detail, "input 1 spends output 1")
	assertNoWrites(t, store)
}

func TestImmatureOutputIsUnspent(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	items := f.spentItems()
	items[1] = peerUTXO{Vout: 1, Status: statusImmature}

	store := missingParentStore(f)

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, items).URL, Options{TxIDs: []chainhash.Hash{parentHash}, DryRun: true})

	res := runOne(t, r)
	require.Equal(t, OutcomePlanned, res.Outcome, res.Detail)
	require.Len(t, res.Spends, 1)
}

// localChildStore holds the fixture's unmined child in the unmined set, with the
// parent absent and both spenders held.
func localChildStore(f *fixture) *utxo.MockUtxostore {
	store := baseStore(f.unminedChild)
	store.On("Get", mock.Anything, hashPtr(*f.parent.TxIDChainHash()), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found"))

	return holdSpenders(store, f)
}

// The peer never saw the local child, so it reports the output unspent. Rebuilding it
// unspent would drop the child's spend and let any other spend of it through. This
// must hold for a --txids run too, not only for --scan.
func TestLocalChildSpendUnspentAtPeerIsRefused(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	items := f.spentItems()
	items[1] = peerUTXO{Vout: 1, Status: statusOK}

	store := localChildStore(f)

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, items).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeLocalSpendConflict, res.Outcome)
	require.Contains(t, res.Detail, "output 1 is spent here by unmined "+f.unminedChild.TxID()+" but unspent at the peer")
	assertNoWrites(t, store)
}

// The peer records a competing spender. Replaying it would write that spend directly,
// so the local child would never be marked conflicting and would stay minable.
func TestLocalChildCompetingSpenderIsRefused(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()
	competitor := chainhash.HashH([]byte("competing-spender"))

	items := f.spentItems()
	items[1].SpendingData = spend.NewSpendingData(&competitor, 0)

	store := localChildStore(f)

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, items).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeLocalSpendConflict, res.Outcome)
	require.Contains(t, res.Detail, "but by "+competitor.String()+" at the peer")
	assertNoWrites(t, store)
}

// A create error other than ErrTxExists may still have landed the write, so the record
// is rolled back, on a context that the caller's cancellation does not reach.
func TestCreateErrorRollsBack(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	ctx, cancel := context.WithCancel(context.Background())

	store := missingParentStore(f)
	store.On("SpendAndCreate", mock.Anything, txWithID(f.parent.TxID()), f.blockHeight, mock.Anything).
		Run(func(mock.Arguments) { cancel() }).
		Return(nil, nil, context.Canceled).Once()
	store.On("DeleteComplete", mock.MatchedBy(func(c context.Context) bool { return c.Err() == nil }), hashPtr(parentHash)).Return(nil).Once()

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	report, err := r.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, OutcomeFailed, report.Results[0].Outcome)
	require.Contains(t, report.Results[0].Detail, "create: ")
	require.Contains(t, report.Results[0].Detail, "record rolled back")
	store.AssertExpectations(t)
}

// ErrTxExists means something else wrote the record after it was planned; it is not
// the tool's to delete.
func TestCreateTxExistsIsNotRolledBack(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	store := missingParentStore(f)
	store.On("SpendAndCreate", mock.Anything, txWithID(f.parent.TxID()), f.blockHeight, mock.Anything).
		Return(nil, nil, errors.NewTxExistsError("exists")).Once()

	r, _ := newRepairer(t, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeFailed, res.Outcome)
	store.AssertNotCalled(t, "DeleteComplete", mock.Anything, mock.Anything)
}

// unheldUnminedChildStore holds minedChild but not unminedChild, with the parent absent.
func unheldUnminedChildStore(f *fixture) *utxo.MockUtxostore {
	store := baseStore()
	store.On("Get", mock.Anything, hashPtr(*f.parent.TxIDChainHash()), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found")).Once()
	store.On("Get", mock.Anything, hashPtr(*f.minedChild.TxIDChainHash()), mock.Anything).Return(&meta.Data{BlockIDs: []uint32{f.blockID + 1}}, nil)
	store.On("Get", mock.Anything, hashPtr(*f.unminedChild.TxIDChainHash()), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found"))

	return store
}

// The peer has a spender we never received and that is not mined anywhere: our node
// never accepted it, so the output is rebuilt unspent and the spender is not replayed.
func TestUnheldOffChainSpenderLeavesOutputUnspent(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()
	currentHeight := f.blockHeight + 1500

	p := f.peer(f.spentItems())
	p.metas[f.unminedChild.TxID()] = map[string]any{"blockHeights": []uint32{}, "blockHashes": []string{}, "subtreeIdxs": []int{}, "mainChainIndex": -1}

	store := unheldUnminedChildStore(f)
	store.On("SpendAndCreate", mock.Anything, txWithID(f.parent.TxID()), f.blockHeight, mock.Anything).Return(&meta.Data{}, nil, nil).Once()
	store.On("SpendAndCreate", mock.Anything, txWithID(f.minedChild.TxID()), currentHeight, mock.Anything).Return(nil, []*utxo.Spend{}, nil).Once()

	rebuilt := f.matching()
	rebuilt.SpendingDatas[1] = nil
	store.On("Get", mock.Anything, hashPtr(parentHash), mock.Anything).Return(rebuilt, nil)

	r, _ := newRepairer(t, store, idleChain(t, f), p.serve(t).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomeRepaired, res.Outcome, res.Detail)
	require.Len(t, res.Spends, 1)
	require.Contains(t, res.Detail, "output 1 left unspent: spender "+f.unminedChild.TxID()+" is not held here and not on our chain")
	store.AssertNotCalled(t, "SpendAndCreate", mock.Anything, txWithID(f.unminedChild.TxID()), mock.Anything, mock.Anything)
	store.AssertExpectations(t)
}

// The spender is mined in a block above our tip that we never received, which is the
// wedged block itself. Validating that block writes the spend later.
func TestUnheldSpenderInBlockUnknownHereLeavesOutputUnspent(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()
	ahead := chainhash.HashH([]byte("block-above-our-tip"))

	chain := idleChain(t, f)
	chain.On("GetBlockHeader", mock.Anything, hashPtr(ahead)).Return(nil, nil, errors.NewBlockNotFoundError("block not found"))

	p := f.peer(f.spentItems())
	p.metas[f.unminedChild.TxID()] = map[string]any{"blockHeights": []uint32{f.blockHeight + 1}, "blockHashes": []string{ahead.String()}, "subtreeIdxs": []int{0}, "mainChainIndex": 0}

	r, _ := newRepairer(t, unheldUnminedChildStore(f), chain, p.serve(t).URL, Options{TxIDs: []chainhash.Hash{parentHash}, DryRun: true})

	res := runOne(t, r)
	require.Equal(t, OutcomePlanned, res.Outcome, res.Detail)
	require.Len(t, res.Spends, 1)
	require.Contains(t, res.Detail, "output 1 left unspent")
}

// A transient failure while placing the spender must never read as "not on our chain",
// or a spent output would be rebuilt unspent.
func TestUnheldSpenderUndeterminedChainFailsTheParent(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()
	spenderBlock := chainhash.HashH([]byte("spender-block"))

	tests := []struct {
		name  string
		meta  map[string]any
		chain func(c *blockchain.Mock)
	}{
		{
			name: "block lookup fails",
			meta: map[string]any{"blockHeights": []uint32{f.blockHeight}, "blockHashes": []string{spenderBlock.String()}, "subtreeIdxs": []int{0}, "mainChainIndex": 0},
			chain: func(c *blockchain.Mock) {
				c.On("GetBlockHeader", mock.Anything, hashPtr(spenderBlock)).Return(nil, nil, errors.NewServiceError("connection refused"))
			},
		},
		{
			name: "peer cannot place it on its main chain",
			meta: map[string]any{"blockHeights": []uint32{f.blockHeight}, "blockHashes": []string{spenderBlock.String()}, "subtreeIdxs": []int{0}, "mainChainIndex": -1},
		},
		{
			name: "peer cannot resolve its main-chain block hash",
			meta: map[string]any{"blockHeights": []uint32{f.blockHeight}, "blockHashes": []string{""}, "subtreeIdxs": []int{0}, "mainChainIndex": 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chain := idleChain(t, f)
			if tt.chain != nil {
				tt.chain(chain)
			}

			p := f.peer(f.spentItems())
			p.metas[f.unminedChild.TxID()] = tt.meta

			store := unheldUnminedChildStore(f)

			r, _ := newRepairer(t, store, chain, p.serve(t).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

			res := runOne(t, r)
			require.Equal(t, OutcomeFailed, res.Outcome)
			require.Contains(t, res.Detail, f.unminedChild.TxID())
			assertNoWrites(t, store)
		})
	}
}

// A pruned spender is recorded as a deleted child, which only the Aerospike store can
// hold. On any other store the parent is refused.
func TestPrunedSpenderOnStoreWithoutDeletedChildrenIsRefused(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	p := f.peer(f.spentItems())
	p.metas[f.unminedChild.TxID()] = f.meta()

	store := unheldUnminedChildStore(f)

	r, _ := newRepairer(t, store, idleChain(t, f), p.serve(t).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	res := runOne(t, r)
	require.Equal(t, OutcomePrunedSpender, res.Outcome)
	require.Contains(t, res.Detail, "cannot record deleted children")
	assertNoWrites(t, store)
}

// A record rebuilt with an off-chain spend left unspent matches the peer on a rerun.
func TestAlreadyPresentWithOffChainSpendLeftUnspentMatches(t *testing.T) {
	f := newFixture(t)

	p := f.peer(f.spentItems())
	p.metas[f.unminedChild.TxID()] = map[string]any{"blockHeights": []uint32{}, "blockHashes": []string{}, "subtreeIdxs": []int{}, "mainChainIndex": -1}

	rebuilt := f.matching()
	rebuilt.SpendingDatas[1] = nil

	store := baseStore()
	store.On("Get", mock.Anything, hashPtr(*f.parent.TxIDChainHash()), mock.Anything).Return(rebuilt, nil)
	store.On("Get", mock.Anything, hashPtr(*f.minedChild.TxIDChainHash()), mock.Anything).Return(&meta.Data{BlockIDs: []uint32{f.blockID + 1}}, nil)
	store.On("Get", mock.Anything, hashPtr(*f.unminedChild.TxIDChainHash()), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found"))

	r, _ := newRepairer(t, store, idleChain(t, f), p.serve(t).URL, Options{TxIDs: []chainhash.Hash{*f.parent.TxIDChainHash()}})

	res := runOne(t, r)
	require.Equal(t, OutcomeAlreadyPresent, res.Outcome, res.Detail)
	assertNoWrites(t, store)
}
