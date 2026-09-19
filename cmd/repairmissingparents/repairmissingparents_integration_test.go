package repairmissingparents

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	aerospikeNamespace = "test"
	aerospikeSet       = "test"
)

// startAerospike starts a throwaway Aerospike container and returns a raw client and a
// teranode store on it. It skips the test when no container runtime is available.
func startAerospike(t *testing.T, tune ...func(*settings.Settings)) (*uaerospike.Client, *teranode_aerospike.Store) {
	t.Helper()
	teranode_aerospike.InitPrometheusMetrics()

	ctx := context.Background()

	container, err := aeroTest.RunContainer(ctx, aeroTest.WithTTLSupport(aerospikeNamespace))
	if err != nil {
		t.Skipf("Aerospike container not available: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	client, aeroErr := uaerospike.NewClient(host, port)
	require.NoError(t, aeroErr)
	t.Cleanup(client.Close)

	// The namespace briefly rejects writes with FAIL_FORBIDDEN after the port opens.
	probe, err := aerospike.NewKey(aerospikeNamespace, aerospikeSet, []byte("__readiness_probe__"))
	require.NoError(t, err)
	wp := aerospike.NewWritePolicy(0, 0)
	wp.RecordExistsAction = aerospike.REPLACE
	require.Eventually(t, func() bool {
		putErr := client.Put(wp, probe, aerospike.BinMap{"ready": true})
		if putErr == nil {
			_, _ = client.Delete(wp, probe)
			return true
		}
		aErr, ok := putErr.(*aerospike.AerospikeError)
		return !(ok && aErr.ResultCode == types.FAIL_FORBIDDEN)
	}, 15*time.Second, 200*time.Millisecond)

	tSettings := test.CreateBaseTestSettings(t)
	for _, fn := range tune {
		fn(tSettings)
	}

	storeURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/%s?set=%s&externalStore=file://./data/externalStore", host, port, aerospikeNamespace, aerospikeSet))
	require.NoError(t, err)

	store, err := teranode_aerospike.New(ctx, ulogger.NewErrorTestLogger(t), tSettings, storeURL)
	require.NoError(t, err)
	store.SetExternalStore(memory.New())

	return client, store
}

func readBins(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash) aerospike.BinMap {
	t.Helper()

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), hash.CloneBytes())
	require.NoError(t, err)

	rec, err := client.Get(nil, key)
	require.NoError(t, err)

	return rec.Bins
}

// TestIntegration_RepairRestoresPrunedParentExactly replays the issue-1768 damage on a
// real store: a mined parent with one mined child and one unmined child is deleted, then
// rebuilt from a peer that still holds it. The rebuilt record must equal the original in
// every bin the pruner, the validator and block assembly read.
func TestIntegration_RepairRestoresPrunedParentExactly(t *testing.T) {
	client, store := startAerospike(t)
	ctx := context.Background()
	f := newFixture(t)

	require.NoError(t, store.SetBlockHeight(f.blockHeight))

	// Parent mined at blockHeight in block blockID.
	_, _, err := store.SpendAndCreate(ctx, f.parent, f.blockHeight, utxo.WithCreateOnly(), utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID: f.blockID, BlockHeight: f.blockHeight, SubtreeIdx: 3, OnLongestChain: true,
	}))
	require.NoError(t, err)

	// Both children spend it. minedChild is then mined one block later; unminedChild stays unmined.
	_, _, err = store.SpendAndCreate(ctx, f.minedChild, f.blockHeight+1)
	require.NoError(t, err)
	_, _, err = store.SpendAndCreate(ctx, f.unminedChild, f.blockHeight+1)
	require.NoError(t, err)

	require.NoError(t, store.SetBlockHeight(f.blockHeight+1))
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{f.minedChild.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID: f.blockID + 1, BlockHeight: f.blockHeight + 1, OnLongestChain: true,
	})
	require.NoError(t, err)

	before := readBins(t, client, store, f.parent.TxIDChainHash())
	require.NotNil(t, before[fields.Utxos.String()])

	// The damage: the parent is pruned while unminedChild still references it.
	require.NoError(t, store.Delete(ctx, f.parent.TxIDChainHash()))
	_, err = store.Get(ctx, f.parent.TxIDChainHash(), fields.BlockIDs)
	require.Error(t, err)

	// The peer still holds the parent as it was.
	peer := f.peerServer(t, f.spentItems())

	chain := &blockchain.Mock{}
	idle := blockchain.FSMStateIDLE
	chain.On("GetFSMCurrentState", mock.Anything).Return(&idle, nil)
	chain.On("GetBestHeightAndTime", mock.Anything).Return(int(f.blockHeight+1500), 0, nil)
	chain.On("GetBlockHeader", mock.Anything, hashPtr(f.blockHash)).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{ID: f.blockID, Height: f.blockHeight}, nil)

	out := &bytes.Buffer{}
	r := NewRepairer(ulogger.NewErrorTestLogger(t), store, chain, NewHTTPPeer(peer.URL, nil), Options{
		Scan:   true,
		Stdout: out,
	})

	report, err := r.Run(ctx)
	require.NoError(t, err, out.String())
	require.Len(t, report.Results, 1, out.String())
	require.Equal(t, OutcomeRepaired, report.Results[0].Outcome, report.Results[0].Detail)
	require.False(t, report.Failed())
	require.Equal(t, 1, report.Scanned, "the unmined child must have been scanned")

	after := readBins(t, client, store, f.parent.TxIDChainHash())

	for _, bin := range []fields.FieldName{
		fields.Tx, fields.Utxos, fields.SpentUtxos, fields.RecordUtxos, fields.TotalExtraRecs,
		fields.BlockIDs, fields.BlockHeights, fields.SubtreeIdxs, fields.Fee, fields.SizeInBytes,
		fields.IsCoinbase, fields.Inputs, fields.External,
	} {
		require.Equal(t, before[bin.String()], after[bin.String()], "bin %s must be restored exactly", bin)
	}

	require.Nil(t, after[fields.UnminedSince.String()], "a rebuilt mined parent must not look unmined")

	// The store's own read path agrees with the peer's spent state.
	got, err := store.Get(ctx, f.parent.TxIDChainHash(), fields.Utxos, fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{f.blockID}, got.BlockIDs)
	require.Len(t, got.SpendingDatas, 2)
	require.True(t, got.SpendingDatas[0].TxID.IsEqual(f.minedChild.TxIDChainHash()))
	require.True(t, got.SpendingDatas[1].TxID.IsEqual(f.unminedChild.TxIDChainHash()))

	// Running again is a no-op.
	report, err = r.Run(ctx)
	require.NoError(t, err)
	require.Len(t, report.Results, 0, "no missing parents remain after the repair")
}

// TestIntegration_RepairRestoresPaginatedParent covers a parent whose outputs span the
// master record and extra pagination records (more outputs than utxoBatchSize). Spent
// state must land in the right record and the extra-record counters must be restored.
func TestIntegration_RepairRestoresPaginatedParent(t *testing.T) {
	const batchSize = 2

	client, store := startAerospike(t, func(s *settings.Settings) { s.UtxoStore.UtxoBatchSize = batchSize })
	ctx := context.Background()

	const height = uint32(500000)
	blockHash := chainhash.HashH([]byte("block-500000"))
	const blockID = uint32(77)

	require.NoError(t, store.SetBlockHeight(height))

	// Five outputs over three records (2 + 2 + 1).
	parent := bt.NewTx()
	require.NoError(t, parent.From(chainhash.HashH([]byte("gp-paginated")).String(), 0, "51", 60000))
	for i := 0; i < 5; i++ {
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 10000))
	}

	_, _, err := store.SpendAndCreate(ctx, parent, height, utxo.WithCreateOnly(), utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID: blockID, BlockHeight: height, OnLongestChain: true,
	}))
	require.NoError(t, err)

	// Spend outputs 1 and 4 (one in the master record, one in the last extra record).
	spenders := map[uint32]*bt.Tx{}
	for _, vout := range []uint32{1, 4} {
		child := bt.NewTx()
		require.NoError(t, child.From(parent.TxID(), vout, parent.Outputs[vout].LockingScript.String(), parent.Outputs[vout].Satoshis))
		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 9000))
		_, _, err = store.SpendAndCreate(ctx, child, height+1)
		require.NoError(t, err)
		spenders[vout] = child
	}

	masterBefore := readBins(t, client, store, parent.TxIDChainHash())
	require.Equal(t, 2, masterBefore[fields.TotalExtraRecs.String()])

	require.NoError(t, store.Delete(ctx, parent.TxIDChainHash()))

	// Peer: tx bytes for parent and spenders, mined meta, per-output state.
	txs := map[string]*bt.Tx{parent.TxID(): parent}
	for _, c := range spenders {
		txs[c.TxID()] = c
	}

	items := make([]peerUTXO, 0, 5)
	for i := uint32(0); i < 5; i++ {
		item := peerUTXO{Vout: i, Status: "OK"}
		if c, ok := spenders[i]; ok {
			item.Status = statusSpent
			item.SpendingData = spend.NewSpendingData(c.TxIDChainHash(), 0)
		}
		items = append(items, item)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/tx/", func(w http.ResponseWriter, r *http.Request) {
		tx, ok := txs[r.URL.Path[len("/api/v1/tx/"):]]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(tx.ExtendedBytes())
	})
	mux.HandleFunc("/api/v1/txmeta/"+parent.TxID()+"/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"blockHeights": []uint32{height}, "blockHashes": []string{blockHash.String()}, "subtreeIdxs": []int{0}, "isCoinbase": false,
		})
	})
	mux.HandleFunc("/api/v1/utxos/"+parent.TxID()+"/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(items)
	})
	peer := httptest.NewServer(mux)
	t.Cleanup(peer.Close)

	chain := &blockchain.Mock{}
	idle := blockchain.FSMStateIDLE
	chain.On("GetFSMCurrentState", mock.Anything).Return(&idle, nil)
	chain.On("GetBestHeightAndTime", mock.Anything).Return(int(height+10), 0, nil)
	chain.On("GetBlockHeader", mock.Anything, hashPtr(blockHash)).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{ID: blockID, Height: height}, nil)

	out := &bytes.Buffer{}
	r := NewRepairer(ulogger.NewErrorTestLogger(t), store, chain, NewHTTPPeer(peer.URL, nil), Options{
		TxIDs:  []chainhash.Hash{*parent.TxIDChainHash()},
		Stdout: out,
	})

	report, err := r.Run(ctx)
	require.NoError(t, err, out.String())
	require.Len(t, report.Results, 1)
	require.Equal(t, OutcomeRepaired, report.Results[0].Outcome, report.Results[0].Detail)
	// Delete removed only the master, so both pagination records were stale leftovers.
	require.Contains(t, report.Results[0].Detail, "2 stale pagination record(s) removed")

	masterAfter := readBins(t, client, store, parent.TxIDChainHash())
	for _, bin := range []fields.FieldName{fields.Utxos, fields.SpentUtxos, fields.RecordUtxos, fields.TotalExtraRecs, fields.SpentExtraRecs, fields.BlockIDs, fields.BlockHeights, fields.Tx} {
		require.Equal(t, masterBefore[bin.String()], masterAfter[bin.String()], "master bin %s", bin)
	}

	got, err := store.Get(ctx, parent.TxIDChainHash(), fields.Utxos)
	require.NoError(t, err)
	require.Len(t, got.SpendingDatas, 5)
	for i := uint32(0); i < 5; i++ {
		if c, ok := spenders[i]; ok {
			require.NotNil(t, got.SpendingDatas[i], "output %d must be spent", i)
			require.True(t, got.SpendingDatas[i].TxID.IsEqual(c.TxIDChainHash()), "output %d spender", i)
		} else {
			require.Nil(t, got.SpendingDatas[i], "output %d must stay unspent", i)
		}
	}
}
