package repairmissingparents

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

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

func newFixture(t *testing.T) *fixture {
	t.Helper()

	parent := bt.NewTx()
	require.NoError(t, parent.From(chainhash.HashH([]byte("grandparent")).String(), 0, "51", 20000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 9000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 9000))

	minedChild := bt.NewTx()
	require.NoError(t, minedChild.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	require.NoError(t, minedChild.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 8000))

	unminedChild := bt.NewTx()
	require.NoError(t, unminedChild.From(parent.TxID(), 1, parent.Outputs[1].LockingScript.String(), parent.Outputs[1].Satoshis))
	require.NoError(t, unminedChild.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 8000))

	return &fixture{
		parent:       parent,
		minedChild:   minedChild,
		unminedChild: unminedChild,
		blockHash:    chainhash.HashH([]byte("block-965720")),
		blockHeight:  965720,
		blockID:      4242,
	}
}

// peerServer serves the three asset endpoints the tool reads, for the fixture parent
// and its two spenders. utxoStatuses overrides the per-output status list.
func (f *fixture) peerServer(t *testing.T, utxoItems []peerUTXO) *httptest.Server {
	t.Helper()

	txs := map[string]*bt.Tx{
		f.parent.TxID():       f.parent,
		f.minedChild.TxID():   f.minedChild,
		f.unminedChild.TxID(): f.unminedChild,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/tx/", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Path[len("/api/v1/tx/"):]
		tx, ok := txs[id]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(tx.ExtendedBytes())
	})

	mux.HandleFunc("/api/v1/txmeta/"+f.parent.TxID()+"/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"blockHeights": []uint32{f.blockHeight},
			"blockHashes":  []string{f.blockHash.String()},
			"subtreeIdxs":  []int{3},
			"isCoinbase":   false,
		})
	})

	mux.HandleFunc("/api/v1/utxos/"+f.parent.TxID()+"/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(utxoItems)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return srv
}

func (f *fixture) spentItems() []peerUTXO {
	return []peerUTXO{
		{Vout: 0, Status: "SPENT", SpendingData: spend.NewSpendingData(f.minedChild.TxIDChainHash(), 0)},
		{Vout: 1, Status: "SPENT", SpendingData: spend.NewSpendingData(f.unminedChild.TxIDChainHash(), 0)},
	}
}

func hashPtr(h chainhash.Hash) any {
	want := h
	return mock.MatchedBy(func(p *chainhash.Hash) bool { return p != nil && p.IsEqual(&want) })
}

func txWithID(id string) any {
	return mock.MatchedBy(func(tx *bt.Tx) bool { return tx != nil && tx.TxID() == id })
}

func idleChain(t *testing.T, f *fixture) *blockchain.Mock {
	t.Helper()

	chain := &blockchain.Mock{}
	idle := blockchain.FSMStateIDLE
	chain.On("GetFSMCurrentState", mock.Anything).Return(&idle, nil)
	chain.On("GetBestHeightAndTime", mock.Anything).Return(int(f.blockHeight+1500), 0, nil)
	chain.On("GetBlockHeader", mock.Anything, hashPtr(f.blockHash)).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{ID: f.blockID, Height: f.blockHeight}, nil)

	return chain
}

func newRepairer(f *fixture, store utxo.Store, chain blockchain.ClientI, peerURL string, opts Options) (*Repairer, *bytes.Buffer) {
	out := &bytes.Buffer{}
	opts.Stdout = out

	return NewRepairer(ulogger.TestLogger{}, store, chain, NewHTTPPeer(peerURL, nil), opts), out
}

func TestRefusesWhenNodeIsNotIdle(t *testing.T) {
	f := newFixture(t)

	chain := &blockchain.Mock{}
	running := blockchain.FSMStateRUNNING
	chain.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	store := new(utxo.MockUtxostore)

	r, _ := newRepairer(f, store, chain, f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{*f.parent.TxIDChainHash()}})

	_, err := r.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "RUNNING")
	require.Contains(t, err.Error(), "--force-live")

	store.AssertNotCalled(t, "SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	store.AssertNotCalled(t, "Get", mock.Anything, mock.Anything, mock.Anything)
}

func TestForceLiveOverridesGate(t *testing.T) {
	f := newFixture(t)

	chain := idleChain(t, f)
	chain.ExpectedCalls = nil
	running := blockchain.FSMStateRUNNING
	chain.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	chain.On("GetBestHeightAndTime", mock.Anything).Return(int(f.blockHeight+1500), 0, nil)

	store := new(utxo.MockUtxostore)
	store.On("SetBlockHeight", mock.Anything).Return(nil)
	store.On("Get", mock.Anything, hashPtr(*f.parent.TxIDChainHash()), mock.Anything).Return(&meta.Data{}, nil)

	r, _ := newRepairer(f, store, chain, f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{*f.parent.TxIDChainHash()}, ForceLive: true})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Results, 1)
	require.Equal(t, OutcomeAlreadyPresent, report.Results[0].Outcome)
}

func TestAlreadyPresentIsSkipped(t *testing.T) {
	f := newFixture(t)

	store := new(utxo.MockUtxostore)
	store.On("SetBlockHeight", mock.Anything).Return(nil)
	store.On("Get", mock.Anything, hashPtr(*f.parent.TxIDChainHash()), mock.Anything).Return(&meta.Data{}, nil)

	r, _ := newRepairer(f, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{*f.parent.TxIDChainHash()}})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Results, 1)
	require.Equal(t, OutcomeAlreadyPresent, report.Results[0].Outcome)
	require.False(t, report.Failed())

	store.AssertNotCalled(t, "SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestRepairsMissingParentWithExactSpentState(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	store := new(utxo.MockUtxostore)
	store.On("SetBlockHeight", mock.Anything).Return(nil)

	// Absent before the repair.
	store.On("Get", mock.Anything, hashPtr(parentHash), mock.Anything).
		Return(nil, errors.NewTxNotFoundError("not found")).Once()

	var createOpts []utxo.CreateOption
	store.On("SpendAndCreate", mock.Anything, txWithID(f.parent.TxID()), f.blockHeight, mock.Anything).
		Run(func(args mock.Arguments) { createOpts = args.Get(3).([]utxo.CreateOption) }).
		Return(&meta.Data{}, nil, nil).Once()

	// Verification read after the repair. Registered after the Once above so testify
	// falls through to it once the not-found expectation is exhausted. The spenders
	// only spend this parent, so no other Get is made.
	verified := &meta.Data{
		BlockIDs:     []uint32{f.blockID},
		BlockHeights: []uint32{f.blockHeight},
		SpendingDatas: []*spend.SpendingData{
			spend.NewSpendingData(f.minedChild.TxIDChainHash(), 0),
			spend.NewSpendingData(f.unminedChild.TxIDChainHash(), 0),
		},
	}
	store.On("Get", mock.Anything, hashPtr(parentHash), mock.Anything).Return(verified, nil)

	currentHeight := f.blockHeight + 1500
	store.On("SpendAndCreate", mock.Anything, txWithID(f.minedChild.TxID()), currentHeight, mock.Anything).Return(nil, []*utxo.Spend{}, nil).Once()
	store.On("SpendAndCreate", mock.Anything, txWithID(f.unminedChild.TxID()), currentHeight, mock.Anything).Return(nil, []*utxo.Spend{}, nil).Once()

	r, out := newRepairer(f, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Results, 1)

	res := report.Results[0]
	require.Equal(t, OutcomeRepaired, res.Outcome, res.Detail)
	require.Equal(t, []uint32{f.blockHeight}, res.BlockHeights)
	require.Equal(t, []uint32{f.blockID}, res.BlockIDs)
	require.Len(t, res.Spends, 2)
	require.False(t, report.Failed())

	// Create was asked for the mined block info that maps to the local block ID.
	co := &utxo.CreateOptions{}
	for _, o := range createOpts {
		o(co)
	}
	require.Len(t, co.MinedBlockInfos, 1)
	require.Equal(t, utxo.MinedBlockInfo{BlockID: f.blockID, BlockHeight: f.blockHeight, SubtreeIdx: 3, OnLongestChain: true}, co.MinedBlockInfos[0])
	require.NotNil(t, co.TxID)
	require.True(t, co.TxID.IsEqual(&parentHash))
	require.True(t, co.CreateOnly, "the parent's own inputs must not be re-spent")

	store.AssertExpectations(t)
	require.Contains(t, out.String(), parentHash.String())
	require.Contains(t, out.String(), string(OutcomeRepaired))
}

func TestDryRunWritesNothing(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	store := new(utxo.MockUtxostore)
	store.On("SetBlockHeight", mock.Anything).Return(nil)
	store.On("Get", mock.Anything, hashPtr(parentHash), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found"))
	store.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&meta.Data{}, nil)

	r, out := newRepairer(f, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}, DryRun: true})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Results, 1)
	require.Equal(t, OutcomePlanned, report.Results[0].Outcome)
	require.Len(t, report.Results[0].Spends, 2)
	require.False(t, report.Failed())

	store.AssertNotCalled(t, "SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	require.Contains(t, out.String(), f.minedChild.TxID())
}

func TestPeerPrunedIsReported(t *testing.T) {
	f := newFixture(t)
	other := chainhash.HashH([]byte("unknown-to-peer"))

	store := new(utxo.MockUtxostore)
	store.On("SetBlockHeight", mock.Anything).Return(nil)
	store.On("Get", mock.Anything, hashPtr(other), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found"))

	r, _ := newRepairer(f, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{other}})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Results, 1)
	require.Equal(t, OutcomePeerPruned, report.Results[0].Outcome)
	require.True(t, report.Failed())

	store.AssertNotCalled(t, "SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestChainMismatchAbortsRecord(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	chain := &blockchain.Mock{}
	idle := blockchain.FSMStateIDLE
	chain.On("GetFSMCurrentState", mock.Anything).Return(&idle, nil)
	chain.On("GetBestHeightAndTime", mock.Anything).Return(int(f.blockHeight+1500), 0, nil)
	// Our chain does not have the peer's block at that height.
	chain.On("GetBlockHeader", mock.Anything, hashPtr(f.blockHash)).Return(nil, nil, errors.NewNotFoundError("block not found"))

	store := new(utxo.MockUtxostore)
	store.On("SetBlockHeight", mock.Anything).Return(nil)
	store.On("Get", mock.Anything, hashPtr(parentHash), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found"))

	r, _ := newRepairer(f, store, chain, f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, OutcomeChainMismatch, report.Results[0].Outcome)
	require.True(t, report.Failed())

	store.AssertNotCalled(t, "SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestScanDiscoversParentsOfUnminedChildren(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	inpoints, err := subtree.NewTxInpointsFromTx(f.unminedChild)
	require.NoError(t, err)

	iter := new(utxo.MockUnminedTxIterator)
	iter.On("Next", mock.Anything).Return([]*utxo.UnminedTransaction{{
		Node:       &subtree.Node{Hash: *f.unminedChild.TxIDChainHash()},
		TxInpoints: &inpoints,
	}}, nil).Once()
	iter.On("Next", mock.Anything).Return(nil, nil)
	iter.On("Close").Return(nil)

	store := new(utxo.MockUtxostore)
	store.On("SetBlockHeight", mock.Anything).Return(nil)
	store.On("GetUnminedTxIterator").Return(iter, nil)
	store.On("Get", mock.Anything, hashPtr(parentHash), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found"))
	store.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&meta.Data{}, nil)

	r, _ := newRepairer(f, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{Scan: true, DryRun: true})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Results, 1)
	require.True(t, report.Results[0].Parent.IsEqual(&parentHash))
	require.Equal(t, OutcomePlanned, report.Results[0].Outcome)
	require.Equal(t, 1, report.Scanned)
}

func TestInconsistentSpenderIsReportedNotWritten(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	store := new(utxo.MockUtxostore)
	store.On("SetBlockHeight", mock.Anything).Return(nil)
	store.On("Get", mock.Anything, hashPtr(parentHash), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found")).Once()
	store.On("SpendAndCreate", mock.Anything, txWithID(f.parent.TxID()), f.blockHeight, mock.Anything).Return(&meta.Data{}, nil, nil).Once()
	store.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&meta.Data{}, nil)

	currentHeight := f.blockHeight + 1500
	store.On("SpendAndCreate", mock.Anything, txWithID(f.minedChild.TxID()), currentHeight, mock.Anything).Return(nil, []*utxo.Spend{}, nil).Once()
	// The peer says output 1 is spent by unminedChild, but our store already records a different spender.
	store.On("SpendAndCreate", mock.Anything, txWithID(f.unminedChild.TxID()), currentHeight, mock.Anything).
		Return(nil, nil, errors.NewUtxoSpentError(parentHash, 1, chainhash.Hash{}, spend.NewSpendingData(&chainhash.Hash{0xff}, 0))).Once()

	r, _ := newRepairer(f, store, idleChain(t, f), f.peerServer(t, f.spentItems()).URL, Options{TxIDs: []chainhash.Hash{parentHash}})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, OutcomeInconsistentSpender, report.Results[0].Outcome)
	require.True(t, report.Failed())
	store.AssertExpectations(t)
}

func TestSpenderWithAnotherMissingParentIsQueued(t *testing.T) {
	f := newFixture(t)
	parentHash := *f.parent.TxIDChainHash()

	// A second parent, also missing, that the unmined child spends alongside ours.
	parent2 := bt.NewTx()
	require.NoError(t, parent2.From(chainhash.HashH([]byte("grandparent-2")).String(), 0, "51", 5000))
	require.NoError(t, parent2.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	parent2Hash := *parent2.TxIDChainHash()

	require.NoError(t, f.unminedChild.From(parent2.TxID(), 0, parent2.Outputs[0].LockingScript.String(), parent2.Outputs[0].Satoshis))

	store := new(utxo.MockUtxostore)
	store.On("SetBlockHeight", mock.Anything).Return(nil)
	store.On("Get", mock.Anything, hashPtr(parentHash), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found"))
	store.On("Get", mock.Anything, hashPtr(parent2Hash), mock.Anything).Return(nil, errors.NewTxNotFoundError("not found"))

	srv := f.peerServer(t, f.spentItems())

	r, _ := newRepairer(f, store, idleChain(t, f), srv.URL, Options{TxIDs: []chainhash.Hash{parentHash}, DryRun: true})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Results, 2, "the second missing parent must be queued from the spender's inputs")

	outcomes := map[chainhash.Hash]Outcome{}
	for _, res := range report.Results {
		outcomes[res.Parent] = res.Outcome
	}
	require.Equal(t, OutcomePlanned, outcomes[parentHash])
	// The peer server in this test does not know parent2, so it is reported as pruned.
	require.Equal(t, OutcomePeerPruned, outcomes[parent2Hash])
}
