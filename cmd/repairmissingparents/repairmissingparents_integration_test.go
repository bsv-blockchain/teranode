package repairmissingparents

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxologger "github.com/bsv-blockchain/teranode/stores/utxo/logger"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
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

// peerItemsFromStore builds the /utxos response the asset service would send for tx,
// from the store's own GetSpend, exactly as GetUTXOsByTXID does.
func peerItemsFromStore(t *testing.T, store utxo.Store, tx *bt.Tx) []peerUTXO {
	t.Helper()

	items := make([]peerUTXO, 0, len(tx.Outputs))

	for i, out := range tx.Outputs {
		vout := uint32(i) //nolint:gosec

		utxoHash, err := util.UTXOHash(tx.TxIDChainHash(), vout, out.LockingScript, out.Satoshis)
		require.NoError(t, err)

		item := peerUTXO{Vout: vout, Status: utxo.Status_NOT_FOUND.String()}

		res, _ := store.GetSpend(context.Background(), &utxo.Spend{TxID: tx.TxIDChainHash(), Vout: vout, UTXOHash: utxoHash})
		if res != nil && res.Status != int(utxo.Status_NOT_FOUND) {
			item.Status = utxo.Status(res.Status).String() //nolint:gosec
			item.SpendingData = res.SpendingData
		}

		items = append(items, item)
	}

	return items
}

func peerFor(txs []*bt.Tx, parent *bt.Tx, blockHash chainhash.Hash, height uint32, items []peerUTXO) *peer {
	p := &peer{
		txs:   map[string][]byte{},
		metas: map[string]any{parent.TxID(): minedMeta(blockHash, height)},
		utxos: map[string]any{parent.TxID(): items},
	}

	for _, tx := range txs {
		p.txs[tx.TxID()] = tx.ExtendedBytes()
	}

	return p
}

func servePeer(t *testing.T, txs []*bt.Tx, parent *bt.Tx, blockHash chainhash.Hash, height uint32, items []peerUTXO) *httptest.Server {
	t.Helper()

	return peerFor(txs, parent, blockHash, height, items).serve(t)
}

func childRecordExists(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, parent *chainhash.Hash, n uint32) bool {
	t.Helper()

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(parent, n))
	require.NoError(t, err)

	exists, err := client.Exists(nil, key)
	require.NoError(t, err)

	return exists
}

func newIntegrationRepairer(t *testing.T, store utxo.Store, chain blockchain.ClientI, peerURL string, opts Options) (*Repairer, *bytes.Buffer) {
	t.Helper()

	out := &bytes.Buffer{}
	opts.Stdout = out

	return NewRepairer(ulogger.NewErrorTestLogger(t), test.CreateBaseTestSettings(t), store, chain, NewHTTPPeer(peerURL, nil), opts), out
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

	// The peer still holds the parent as it was; its /utxos answer comes from the real store.
	items := peerItemsFromStore(t, store, f.parent)
	require.Equal(t, f.spentItems(), items)

	// The damage: the parent is pruned while unminedChild still references it.
	require.NoError(t, store.Delete(ctx, f.parent.TxIDChainHash()))
	_, err = store.Get(ctx, f.parent.TxIDChainHash(), fields.BlockIDs)
	require.Error(t, err)

	peer := f.peerServer(t, items)
	chain := idleChainFor(f.blockHash, f.blockID, f.blockHeight)

	r, out := newIntegrationRepairer(t, store, chain, peer.URL, Options{Scan: true})

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

	// The parent has a local unmined child, so it is preserved from the pruner.
	require.NotNil(t, after[fields.PreserveUntil.String()], "a parent with a local unmined child must be preserved")
	require.Nil(t, before[fields.PreserveUntil.String()])

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

	// Naming the repaired parent compares it with the peer and finds it matching.
	r2, _ := newIntegrationRepairer(t, store, chain, peer.URL, Options{TxIDs: []chainhash.Hash{*f.parent.TxIDChainHash()}})
	report, err = r2.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, OutcomeAlreadyPresent, report.Results[0].Outcome, report.Results[0].Detail)
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

	// Six outputs over three records (2 + 2 + 2); output 5 is OP_FALSE OP_RETURN data,
	// which the store never keeps as a UTXO.
	parent := bt.NewTx()
	require.NoError(t, parent.From(chainhash.HashH([]byte("gp-paginated")).String(), 0, "51", 60000))
	for i := 0; i < 5; i++ {
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 10000))
	}

	dataScript, err := bscript.NewFromASM("OP_FALSE OP_RETURN 68656c6c6f")
	require.NoError(t, err)
	parent.AddOutput(&bt.Output{LockingScript: dataScript, Satoshis: 0})

	_, _, err = store.SpendAndCreate(ctx, parent, height, utxo.WithCreateOnly(), utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
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

	items := peerItemsFromStore(t, store, parent)

	require.NoError(t, store.Delete(ctx, parent.TxIDChainHash()))

	txs := make([]*bt.Tx, 0, 1+len(spenders))
	txs = append(txs, parent)

	for _, c := range spenders {
		txs = append(txs, c)
	}

	peer := servePeer(t, txs, parent, blockHash, height, items)
	chain := idleChainFor(blockHash, blockID, height)

	r, out := newIntegrationRepairer(t, store, chain, peer.URL, Options{TxIDs: []chainhash.Hash{*parent.TxIDChainHash()}})

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
	require.Len(t, got.SpendingDatas, 6)
	for i := uint32(0); i < 6; i++ {
		if c, ok := spenders[i]; ok {
			require.NotNil(t, got.SpendingDatas[i], "output %d must be spent", i)
			require.True(t, got.SpendingDatas[i].TxID.IsEqual(c.TxIDChainHash()), "output %d spender", i)
		} else {
			require.Nil(t, got.SpendingDatas[i], "output %d must stay unspent", i)
		}
	}
}

// paginatedParent stores a mined five-output parent over three records (batch size 2)
// with outputs 1 and 4 spent, then deletes only its master, as a master-only prune does.
func paginatedParent(t *testing.T, store *teranode_aerospike.Store, seed string, height, blockID uint32) (*bt.Tx, []*bt.Tx, []peerUTXO) {
	t.Helper()

	ctx := context.Background()

	parent := bt.NewTx()
	require.NoError(t, parent.From(chainhash.HashH([]byte(seed)).String(), 0, "51", 60000))

	for i := 0; i < 5; i++ {
		require.NoError(t, parent.PayToAddress(payAddress, 10000))
	}

	_, _, err := store.SpendAndCreate(ctx, parent, height, utxo.WithCreateOnly(), utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID: blockID, BlockHeight: height, OnLongestChain: true,
	}))
	require.NoError(t, err)

	spenders := make([]*bt.Tx, 0, 2)

	for _, vout := range []uint32{1, 4} {
		child := bt.NewTx()
		require.NoError(t, child.From(parent.TxID(), vout, parent.Outputs[vout].LockingScript.String(), parent.Outputs[vout].Satoshis))
		require.NoError(t, child.PayToAddress(payAddress, 9000))
		_, _, err = store.SpendAndCreate(ctx, child, height+1)
		require.NoError(t, err)

		spenders = append(spenders, child)
	}

	items := peerItemsFromStore(t, store, parent)

	require.NoError(t, store.Delete(ctx, parent.TxIDChainHash()))

	return parent, spenders, items
}

// failingReplayStore fails every spend replay after a successful create.
type failingReplayStore struct {
	*teranode_aerospike.Store
}

func (s failingReplayStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	o, err := utxo.ParseCreateOptions(opts...)
	if err == nil && o.SpendOnly {
		return nil, nil, errors.NewStorageError("injected replay failure")
	}

	return s.Store.SpendAndCreate(ctx, tx, blockHeight, opts...)
}

// A replay failure after the create must leave neither the master nor any pagination
// record behind, so a rerun starts from a clean hole instead of a partial record.
func TestIntegration_ReplayFailureRollsBackPaginatedParent(t *testing.T) {
	client, store := startAerospike(t, func(s *settings.Settings) { s.UtxoStore.UtxoBatchSize = 2 })
	ctx := context.Background()

	const height = uint32(600000)
	const blockID = uint32(88)
	blockHash := chainhash.HashH([]byte("block-600000"))

	require.NoError(t, store.SetBlockHeight(height))

	parent, spenders, items := paginatedParent(t, store, "gp-rollback", height, blockID)
	parentHash := parent.TxIDChainHash()

	peer := servePeer(t, append([]*bt.Tx{parent}, spenders...), parent, blockHash, height, items)
	chain := idleChainFor(blockHash, blockID, height)

	r, out := newIntegrationRepairer(t, failingReplayStore{store}, chain, peer.URL, Options{TxIDs: []chainhash.Hash{*parentHash}})

	report, err := r.Run(ctx)
	require.NoError(t, err, out.String())
	require.Equal(t, OutcomeFailed, report.Results[0].Outcome)
	require.Contains(t, report.Results[0].Detail, "injected replay failure")
	require.Contains(t, report.Results[0].Detail, "record rolled back")

	_, err = store.Get(ctx, parentHash, fields.BlockIDs)
	require.Error(t, err, "the master must be gone after the rollback")
	require.False(t, childRecordExists(t, client, store, parentHash, 1), "pagination record 1 must be gone")
	require.False(t, childRecordExists(t, client, store, parentHash, 2), "pagination record 2 must be gone")

	// The rerun against a healthy store repairs it from scratch.
	r2, out2 := newIntegrationRepairer(t, store, chain, peer.URL, Options{TxIDs: []chainhash.Hash{*parentHash}})
	report, err = r2.Run(ctx)
	require.NoError(t, err, out2.String())
	require.Equal(t, OutcomeRepaired, report.Results[0].Outcome, report.Results[0].Detail)
}

// A dry run reports the stale pagination records but must not remove them.
func TestIntegration_DryRunLeavesStaleChildren(t *testing.T) {
	client, store := startAerospike(t, func(s *settings.Settings) { s.UtxoStore.UtxoBatchSize = 2 })
	ctx := context.Background()

	const height = uint32(610000)
	const blockID = uint32(89)
	blockHash := chainhash.HashH([]byte("block-610000"))

	require.NoError(t, store.SetBlockHeight(height))

	parent, spenders, items := paginatedParent(t, store, "gp-dryrun", height, blockID)
	parentHash := parent.TxIDChainHash()

	require.True(t, childRecordExists(t, client, store, parentHash, 1))

	peer := servePeer(t, append([]*bt.Tx{parent}, spenders...), parent, blockHash, height, items)

	r, out := newIntegrationRepairer(t, store, idleChainFor(blockHash, blockID, height), peer.URL, Options{TxIDs: []chainhash.Hash{*parentHash}, DryRun: true})

	report, err := r.Run(ctx)
	require.NoError(t, err, out.String())
	require.Equal(t, OutcomePlanned, report.Results[0].Outcome, report.Results[0].Detail)
	require.Contains(t, report.Results[0].Detail, "2 stale pagination record(s) would be removed")

	require.True(t, childRecordExists(t, client, store, parentHash, 1), "a dry run must not remove pagination record 1")
	require.True(t, childRecordExists(t, client, store, parentHash, 2), "a dry run must not remove pagination record 2")

	_, err = store.Get(ctx, parentHash, fields.BlockIDs)
	require.Error(t, err, "a dry run must not create the master")
}

// The review's scenario on a real store: the peer says P:0 is spent by X, which this
// node never received and which is not mined anywhere, and X also spends Q:0, a healthy
// unspent output here. Our node never accepted X, so P:0 is rebuilt unspent and X is
// not replayed: Q:0 stays unspent.
func TestIntegration_UnheldOffChainSpenderLeavesOutputUnspent(t *testing.T) {
	_, store := startAerospike(t)
	ctx := context.Background()

	const height = uint32(620000)
	const blockID = uint32(90)
	blockHash := chainhash.HashH([]byte("block-620000"))

	require.NoError(t, store.SetBlockHeight(height))

	mined := utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: blockID, BlockHeight: height, OnLongestChain: true})

	q := bt.NewTx()
	require.NoError(t, q.From(chainhash.HashH([]byte("gp-q")).String(), 0, "51", 10000))
	require.NoError(t, q.PayToAddress(payAddress, 9000))
	_, _, err := store.SpendAndCreate(ctx, q, height, utxo.WithCreateOnly(), mined)
	require.NoError(t, err)

	p := bt.NewTx()
	require.NoError(t, p.From(chainhash.HashH([]byte("gp-p")).String(), 0, "51", 10000))
	require.NoError(t, p.PayToAddress(payAddress, 9000))

	x := bt.NewTx()
	require.NoError(t, x.From(p.TxID(), 0, p.Outputs[0].LockingScript.String(), p.Outputs[0].Satoshis))
	require.NoError(t, x.From(q.TxID(), 0, q.Outputs[0].LockingScript.String(), q.Outputs[0].Satoshis))
	require.NoError(t, x.PayToAddress(payAddress, 17000))

	items := []peerUTXO{{Vout: 0, Status: statusSpent, SpendingData: spend.NewSpendingData(x.TxIDChainHash(), 0)}}
	pr := peerFor([]*bt.Tx{p, x}, p, blockHash, height, items)
	pr.metas[x.TxID()] = unminedMeta()

	r, out := newIntegrationRepairer(t, store, idleChainFor(blockHash, blockID, height), pr.serve(t).URL, Options{TxIDs: []chainhash.Hash{*p.TxIDChainHash()}})

	report, err := r.Run(ctx)
	require.NoError(t, err, out.String())
	require.Equal(t, OutcomeRepaired, report.Results[0].Outcome, report.Results[0].Detail)
	require.Contains(t, report.Results[0].Detail, "output 0 left unspent")

	qState := peerItemsFromStore(t, store, q)
	require.Equal(t, statusOK, qState[0].Status, "Q:0 must stay unspent")
	require.Nil(t, qState[0].SpendingData)

	pState := peerItemsFromStore(t, store, p)
	require.Equal(t, statusOK, pState[0].Status, "P:0 is rebuilt unspent")
}

// pruneSpender does what the pruner does when it deletes a mined spender: it records the
// spender in the deletedChildren bin of each record holding an output the spender spent,
// then deletes the spender.
func pruneSpender(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, spender *bt.Tx) {
	t.Helper()

	for _, in := range spender.Inputs {
		key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySource(in.PreviousTxIDChainHash(), in.PreviousTxOutIndex, store.GetUtxoBatchSize()))
		require.NoError(t, err)

		_, aErr := client.Execute(nil, key, teranode_aerospike.LuaPackage, "addDeletedChildren", aerospike.NewValue([]interface{}{spender.TxID()}))
		require.NoError(t, aErr)
	}

	require.NoError(t, store.Delete(context.Background(), spender.TxIDChainHash()))
}

// A spender that is mined on our chain and that the pruner deleted is replayed and
// recorded as a deleted child, so a re-broadcast of it is refused instead of bringing it
// back as an unmined transaction.
func TestIntegration_PrunedSpenderIsRecordedAsDeletedChild(t *testing.T) {
	client, store := startAerospike(t)
	ctx := context.Background()

	const height = uint32(670000)
	const blockID = uint32(95)
	blockHash := chainhash.HashH([]byte("block-670000"))

	require.NoError(t, store.SetBlockHeight(height))

	p := bt.NewTx()
	require.NoError(t, p.From(chainhash.HashH([]byte("gp-pruned-spender")).String(), 0, "51", 10000))
	require.NoError(t, p.PayToAddress(payAddress, 9000))
	require.NoError(t, p.PayToAddress(payAddress, 500))
	_, _, err := store.SpendAndCreate(ctx, p, height, utxo.WithCreateOnly(), utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: blockID, BlockHeight: height, OnLongestChain: true}))
	require.NoError(t, err)

	s := bt.NewTx()
	require.NoError(t, s.From(p.TxID(), 0, p.Outputs[0].LockingScript.String(), p.Outputs[0].Satoshis))
	require.NoError(t, s.PayToAddress(payAddress, 8000))
	_, _, err = store.SpendAndCreate(ctx, s, height+1)
	require.NoError(t, err)

	items := peerItemsFromStore(t, store, p)

	pruneSpender(t, client, store, s)
	require.NoError(t, store.Delete(ctx, p.TxIDChainHash()))

	pr := peerFor([]*bt.Tx{p, s}, p, blockHash, height, items)
	pr.metas[s.TxID()] = minedMeta(blockHash, height)

	r, out := newIntegrationRepairer(t, store, idleChainFor(blockHash, blockID, height), pr.serve(t).URL, Options{TxIDs: []chainhash.Hash{*p.TxIDChainHash()}})

	report, err := r.Run(ctx)
	require.NoError(t, err, out.String())
	require.Equal(t, OutcomeRepaired, report.Results[0].Outcome, report.Results[0].Detail)
	require.Contains(t, report.Results[0].Detail, "recorded as a deleted child")

	pState := peerItemsFromStore(t, store, p)
	require.Equal(t, statusSpent, pState[0].Status)
	require.True(t, pState[0].SpendingData.TxID.IsEqual(s.TxIDChainHash()), "P:0 must be spent by S")

	bins := readBins(t, client, store, p.TxIDChainHash())
	dc, _ := bins[fields.DeletedChildren.String()].(map[interface{}]interface{})
	require.Equal(t, true, dc[s.TxID()], "S must be a deleted child of P")

	// A re-broadcast of S is refused and does not bring S back.
	_, _, err = store.SpendAndCreate(ctx, s, height+2)
	require.Error(t, err, "a re-broadcast of the pruned spender must be refused")

	_, err = store.Get(ctx, s.TxIDChainHash(), fields.BlockIDs)
	require.Error(t, err, "the pruned spender must not come back")
}

// A pruned spender that also spent another parent Q cannot be replayed: on the
// surviving Q the store refuses the re-spend of a deleted child. The parent is refused
// during planning, nothing is written, and Q is unchanged.
func TestIntegration_PrunedSpenderWithOtherParentIsRefused(t *testing.T) {
	client, store := startAerospike(t)
	ctx := context.Background()

	const height = uint32(680000)
	const blockID = uint32(96)
	blockHash := chainhash.HashH([]byte("block-680000"))

	require.NoError(t, store.SetBlockHeight(height))

	mined := utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: blockID, BlockHeight: height, OnLongestChain: true})

	mk := func(seed string) *bt.Tx {
		tx := bt.NewTx()
		require.NoError(t, tx.From(chainhash.HashH([]byte(seed)).String(), 0, "51", 10000))
		require.NoError(t, tx.PayToAddress(payAddress, 9000))
		require.NoError(t, tx.PayToAddress(payAddress, 500))
		_, _, err := store.SpendAndCreate(ctx, tx, height, utxo.WithCreateOnly(), mined)
		require.NoError(t, err)

		return tx
	}

	p := mk("gp-p-pruned-two")
	q := mk("gp-q-pruned-two")

	s := bt.NewTx()
	require.NoError(t, s.From(p.TxID(), 0, p.Outputs[0].LockingScript.String(), p.Outputs[0].Satoshis))
	require.NoError(t, s.From(q.TxID(), 0, q.Outputs[0].LockingScript.String(), q.Outputs[0].Satoshis))
	require.NoError(t, s.PayToAddress(payAddress, 17000))
	_, _, err := store.SpendAndCreate(ctx, s, height+1)
	require.NoError(t, err)

	items := peerItemsFromStore(t, store, p)

	pruneSpender(t, client, store, s)
	require.NoError(t, store.Delete(ctx, p.TxIDChainHash()))

	pr := peerFor([]*bt.Tx{p, s}, p, blockHash, height, items)
	pr.metas[s.TxID()] = minedMeta(blockHash, height)

	r, out := newIntegrationRepairer(t, store, idleChainFor(blockHash, blockID, height), pr.serve(t).URL, Options{TxIDs: []chainhash.Hash{*p.TxIDChainHash()}})

	report, err := r.Run(ctx)
	require.NoError(t, err, out.String())
	require.Equal(t, OutcomePrunedSpender, report.Results[0].Outcome, report.Results[0].Detail)

	_, err = store.Get(ctx, p.TxIDChainHash(), fields.BlockIDs)
	require.Error(t, err, "P must not be written")

	qState := peerItemsFromStore(t, store, q)
	require.Equal(t, statusSpent, qState[0].Status, "Q:0 must stay spent")
	require.True(t, qState[0].SpendingData.TxID.IsEqual(s.TxIDChainHash()), "Q:0 must stay spent by S")
}

// The txid does not cover a spender's extended input data. A peer that serves X with a
// wrong value on its P input would make the replay fail the store's hash check, and the
// store's rollback of that failure would unspend X's other input Q:0. The tool must
// refuse before any write and leave Q:0 spent by X.
func TestIntegration_SpenderWithWrongInputDataIsRefused(t *testing.T) {
	_, store := startAerospike(t)
	ctx := context.Background()

	const height = uint32(630000)
	const blockID = uint32(91)
	blockHash := chainhash.HashH([]byte("block-630000"))

	require.NoError(t, store.SetBlockHeight(height))

	mined := utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: blockID, BlockHeight: height, OnLongestChain: true})

	newParent := func(seed string) *bt.Tx {
		tx := bt.NewTx()
		require.NoError(t, tx.From(chainhash.HashH([]byte(seed)).String(), 0, "51", 10000))
		require.NoError(t, tx.PayToAddress(payAddress, 9000))
		_, _, err := store.SpendAndCreate(ctx, tx, height, utxo.WithCreateOnly(), mined)
		require.NoError(t, err)

		return tx
	}

	p := newParent("gp-p-wrongdata")
	q := newParent("gp-q-wrongdata")

	x := bt.NewTx()
	require.NoError(t, x.From(p.TxID(), 0, p.Outputs[0].LockingScript.String(), p.Outputs[0].Satoshis))
	require.NoError(t, x.From(q.TxID(), 0, q.Outputs[0].LockingScript.String(), q.Outputs[0].Satoshis))
	require.NoError(t, x.PayToAddress(payAddress, 17000))
	_, _, err := store.SpendAndCreate(ctx, x, height+1)
	require.NoError(t, err)

	items := peerItemsFromStore(t, store, p)
	require.NoError(t, store.Delete(ctx, p.TxIDChainHash()))

	tampered, err := bt.NewTxFromBytes(x.ExtendedBytes())
	require.NoError(t, err)
	tampered.Inputs[0].PreviousTxSatoshis++
	require.Equal(t, x.TxID(), tampered.TxID(), "the txid must not cover the extended input data")

	pr := peerFor([]*bt.Tx{p, x}, p, blockHash, height, items)
	pr.txs[x.TxID()] = tampered.ExtendedBytes()

	r, out := newIntegrationRepairer(t, store, idleChainFor(blockHash, blockID, height), pr.serve(t).URL, Options{TxIDs: []chainhash.Hash{*p.TxIDChainHash()}})

	report, err := r.Run(ctx)
	require.NoError(t, err, out.String())
	require.Equal(t, OutcomeUnsafeSpender, report.Results[0].Outcome, report.Results[0].Detail)

	qState := peerItemsFromStore(t, store, q)
	require.Equal(t, statusSpent, qState[0].Status, "Q:0 must stay spent")
	require.True(t, qState[0].SpendingData.TxID.IsEqual(x.TxIDChainHash()), "Q:0 must stay spent by X")

	_, err = store.Get(ctx, p.TxIDChainHash(), fields.BlockIDs)
	require.Error(t, err, "P must not be written")
}

// A coinbase parent is rebuilt from its EF bytes with the coinbase flag and maturity
// height the store derives from the transaction, never from the peer.
func TestIntegration_RepairRestoresCoinbaseParent(t *testing.T) {
	client, store := startAerospike(t)
	ctx := context.Background()

	const height = uint32(640000)
	const blockID = uint32(92)
	blockHash := chainhash.HashH([]byte("block-640000"))

	coinbase, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17032dff0c2f71646c6e6b2f5e931c7f7b6199adf35e1300ffffffff01d15fa012000000001976a91417db35d440a673a218e70a5b9d07f895facf50d288ac00000000")
	require.NoError(t, err)
	require.True(t, coinbase.IsCoinbase())

	require.NoError(t, store.SetBlockHeight(height))
	_, _, err = store.SpendAndCreate(ctx, coinbase, height, utxo.WithCreateOnly(), utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID: blockID, BlockHeight: height, OnLongestChain: true,
	}))
	require.NoError(t, err)

	require.NoError(t, store.SetBlockHeight(height+1))

	before := readBins(t, client, store, coinbase.TxIDChainHash())
	require.Equal(t, true, before[fields.IsCoinbase.String()])
	require.NotNil(t, before[fields.SpendingHeight.String()], "a coinbase record carries its maturity height")

	items := peerItemsFromStore(t, store, coinbase)
	// Maturity is enforced through the SpendingHeight bin; GetSpend reports OK here.
	require.Contains(t, []string{statusOK, statusImmature}, items[0].Status)

	require.NoError(t, store.Delete(ctx, coinbase.TxIDChainHash()))

	peer := servePeer(t, []*bt.Tx{coinbase}, coinbase, blockHash, height, items)

	r, out := newIntegrationRepairer(t, store, idleChainFor(blockHash, blockID, height), peer.URL, Options{TxIDs: []chainhash.Hash{*coinbase.TxIDChainHash()}})

	report, err := r.Run(ctx)
	require.NoError(t, err, out.String())
	require.Equal(t, OutcomeRepaired, report.Results[0].Outcome, report.Results[0].Detail)

	after := readBins(t, client, store, coinbase.TxIDChainHash())
	for _, bin := range []fields.FieldName{fields.IsCoinbase, fields.SpendingHeight, fields.Utxos, fields.BlockIDs, fields.BlockHeights, fields.Tx} {
		require.Equal(t, before[bin.String()], after[bin.String()], "bin %s must be restored exactly", bin)
	}
}

// orphaningCreateStore makes the create fail after it has written the pagination
// records but not the master, the partial state a paginated create can leave.
type orphaningCreateStore struct {
	*teranode_aerospike.Store
}

func (s orphaningCreateStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	o, err := utxo.ParseCreateOptions(opts...)
	if err != nil || !o.CreateOnly {
		return s.Store.SpendAndCreate(ctx, tx, blockHeight, opts...)
	}

	if _, _, err := s.Store.SpendAndCreate(ctx, tx, blockHeight, opts...); err != nil {
		return nil, nil, err
	}

	if err := s.Delete(ctx, tx.TxIDChainHash()); err != nil {
		return nil, nil, err
	}

	return nil, nil, errors.NewStorageError("injected create failure after a partial write")
}

// A create that fails after a partial write is rolled back too, including pagination
// records whose master never landed, which DeleteComplete cannot find on its own.
func TestIntegration_CreateFailureSweepsOrphanedChildren(t *testing.T) {
	client, store := startAerospike(t, func(s *settings.Settings) { s.UtxoStore.UtxoBatchSize = 2 })
	ctx := context.Background()

	const height = uint32(650000)
	const blockID = uint32(93)
	blockHash := chainhash.HashH([]byte("block-650000"))

	require.NoError(t, store.SetBlockHeight(height))

	parent, spenders, items := paginatedParent(t, store, "gp-orphans", height, blockID)
	parentHash := parent.TxIDChainHash()

	peer := servePeer(t, append([]*bt.Tx{parent}, spenders...), parent, blockHash, height, items)

	r, out := newIntegrationRepairer(t, orphaningCreateStore{store}, idleChainFor(blockHash, blockID, height), peer.URL, Options{TxIDs: []chainhash.Hash{*parentHash}})

	report, err := r.Run(ctx)
	require.NoError(t, err, out.String())
	require.Equal(t, OutcomeFailed, report.Results[0].Outcome)
	require.Contains(t, report.Results[0].Detail, "injected create failure")
	require.Contains(t, report.Results[0].Detail, "record rolled back")

	_, err = store.Get(ctx, parentHash, fields.BlockIDs)
	require.Error(t, err, "no master may remain")
	require.False(t, childRecordExists(t, client, store, parentHash, 1), "orphaned pagination record 1 must be swept")
	require.False(t, childRecordExists(t, client, store, parentHash, 2), "orphaned pagination record 2 must be swept")
}

// ?logging=true wraps the store in the storelogger. The stale-record check must see
// through it, not skip silently.
func TestIntegration_StaleChildrenAreFoundThroughTheLoggerWrapper(t *testing.T) {
	client, store := startAerospike(t, func(s *settings.Settings) { s.UtxoStore.UtxoBatchSize = 2 })
	ctx := context.Background()

	const height = uint32(690000)
	const blockID = uint32(97)
	blockHash := chainhash.HashH([]byte("block-690000"))

	require.NoError(t, store.SetBlockHeight(height))

	parent, spenders, items := paginatedParent(t, store, "gp-wrapped", height, blockID)
	parentHash := parent.TxIDChainHash()

	peer := servePeer(t, append([]*bt.Tx{parent}, spenders...), parent, blockHash, height, items)
	wrapped := utxologger.New(ctx, ulogger.NewErrorTestLogger(t), store)

	r, out := newIntegrationRepairer(t, wrapped, idleChainFor(blockHash, blockID, height), peer.URL, Options{TxIDs: []chainhash.Hash{*parentHash}, DryRun: true})

	report, err := r.Run(ctx)
	require.NoError(t, err, out.String())
	require.Equal(t, OutcomePlanned, report.Results[0].Outcome, report.Results[0].Detail)
	require.Contains(t, report.Results[0].Detail, "2 stale pagination record(s) would be removed")
	require.True(t, childRecordExists(t, client, store, parentHash, 1))
}
