package blockassembly

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtree "github.com/bsv-blockchain/go-subtree"
	command "github.com/bsv-blockchain/teranode/cmd/recoverreplayedtransactions"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	rr "github.com/bsv-blockchain/teranode/pkg/replayrecovery"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	astore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

func replayFixtureBlock(t *testing.T, previous *model.BlockHeader, coinbase *bt.Tx, txs ...*bt.Tx) *model.Block {
	t.Helper()
	leaves := []chainhash.Hash{*coinbase.TxIDChainHash()}
	for _, tx := range txs {
		leaves = append(leaves, *tx.TxIDChainHash())
	}
	for len(leaves) > 1 {
		if len(leaves)%2 != 0 {
			leaves = append(leaves, leaves[len(leaves)-1])
		}
		next := make([]chainhash.Hash, 0, len(leaves)/2)
		for i := 0; i < len(leaves); i += 2 {
			raw := append(leaves[i].CloneBytes(), leaves[i+1][:]...)
			next = append(next, chainhash.DoubleHashH(raw))
		}
		leaves = next
	}
	header := &model.BlockHeader{Version: 1, HashPrevBlock: previous.Hash(), HashMerkleRoot: &leaves[0], Timestamp: previous.Timestamp + 600, Bits: previous.Bits, Nonce: 1}
	return &model.Block{Header: header, CoinbaseTx: coinbase, TransactionCount: uint64(len(txs) + 1), Subtrees: []*chainhash.Hash{}}
}

func TestReplayRecoveryAerospikeAssemblyEndToEnd(t *testing.T) {
	for _, paginated := range []bool{false, true} {
		t.Run(fmt.Sprintf("paginated=%v", paginated), func(t *testing.T) { runReplayRecoveryAerospikeAssemblyEndToEnd(t, paginated) })
	}
}
func runReplayRecoveryAerospikeAssemblyEndToEnd(t *testing.T, paginated bool) {
	if testing.Short() {
		t.Skip("requires real Aerospike")
	}
	initPrometheusMetrics()
	ctx, cancel := context.WithTimeout(t.Context(), 180*time.Second)
	defer cancel()
	container, err := aeroTest.RunContainer(ctx, aeroTest.WithTTLSupport("test"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(context.Background())) })
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.ServicePort(ctx)
	require.NoError(t, err)
	settings := createTestSettings(t)
	if paginated {
		settings.UtxoStore.UtxoBatchSize = 2
	}
	settings.UtxoStore.DisableDAHCleaner = true
	settings.BlockAssembly.UnminedTxDiskSortEnabled = false
	settings.BlockAssembly.OnRestartValidateParentChain = false
	logger := ulogger.NewErrorTestLogger(t)
	target, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=recovery_e2e&externalStore=memory://", host, port))
	require.NoError(t, err)
	store, err := astore.New(ctx, logger, settings, target)
	require.NoError(t, err)
	store.SetExternalStore(memory.New())
	require.NoError(t, store.SetBlockHeight(2))
	nativeBackend, err := astore.NewRecoveryBackend(store.GetClient().Client, "test", "recovery_e2e", settings.UtxoStore.UtxoBatchSize, target.Host)
	require.NoError(t, err)
	chainURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)
	chainStore, err := blockchainstore.NewStore(logger, chainURL, settings)
	require.NoError(t, err)
	chain, err := blockchain.NewLocalClient(logger, settings, chainStore, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, chainStore.Close(context.Background())) })
	archive := memory.New()
	retain := func(block *model.Block, height uint32, txs ...*bt.Tx) {
		block.Height = height
		if len(txs) == 0 {
			return
		}
		st, e := subtree.NewTree(3)
		require.NoError(t, e)
		require.NoError(t, st.AddCoinbaseNode())
		var raw bytes.Buffer
		for _, tx := range txs {
			require.NoError(t, st.AddNode(*tx.TxIDChainHash(), 0, uint64(tx.Size())))
			raw.Write(tx.Bytes())
		}
		key := st.RootHash()
		data, e := st.Serialize()
		require.NoError(t, e)
		require.NoError(t, archive.Set(ctx, key[:], fileformat.FileTypeSubtree, data))
		require.NoError(t, archive.Set(ctx, key[:], fileformat.FileTypeSubtreeData, raw.Bytes()))
		block.Subtrees = []*chainhash.Hash{key}
	}
	genesis, err := chainStore.GetBlockByID(ctx, 0)
	require.NoError(t, err)
	parent := bt.NewTx()
	require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 10000))
	for i := 0; i < 2; i++ {
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	}
	_, err = store.Create(ctx, parent, 1)
	require.NoError(t, err)
	makeChild := func(parent *bt.Tx, vout uint32, amount uint64) *bt.Tx {
		tx := bt.NewTx()
		require.NoError(t, tx.From(parent.TxID(), vout, parent.Outputs[vout].LockingScript.String(), parent.Outputs[vout].Satoshis))
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", amount))
		return tx
	}
	amount := uint64(3000)
	if paginated {
		amount = 1000
	}
	child := makeChild(parent, 0, amount)
	if paginated {
		for i := 0; i < 2; i++ {
			require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
		}
	}
	_, _, err = store.SpendAndCreate(ctx, child, 1)
	require.NoError(t, err)
	grandchild := makeChild(child, 0, 2000)
	if paginated {
		for vout := uint32(1); vout < 3; vout++ {
			require.NoError(t, grandchild.From(child.TxID(), vout, child.Outputs[vout].LockingScript.String(), child.Outputs[vout].Satoshis))
		}
	}
	_, _, err = store.SpendAndCreate(ctx, grandchild, 2)
	require.NoError(t, err)
	cb1, err := bt.NewTxFromBytes(genesis.CoinbaseTx.Bytes())
	require.NoError(t, err)
	cb1.LockTime = 1
	block1 := replayFixtureBlock(t, genesis.Header, cb1, parent, child)
	cb2, err := bt.NewTxFromBytes(genesis.CoinbaseTx.Bytes())
	require.NoError(t, err)
	cb2.LockTime = 2
	block2 := replayFixtureBlock(t, block1.Header, cb2, grandchild)
	retain(block1, 1, parent, child)
	retain(block2, 2, grandchild)
	for i, block := range []*model.Block{block1, block2} {
		require.NoError(t, chain.AddBlock(ctx, block, "", blockchainoptions.WithMinedSet(true)))
		require.NoError(t, chain.SetBlockProcessedAt(ctx, block.Hash()))
		height := uint32(i + 1)
		_, err = store.Create(ctx, block.CoinbaseTx, height)
		require.NoError(t, err)
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{block.CoinbaseTx.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: height, BlockHeight: height, OnLongestChain: true})
		require.NoError(t, err)
	}
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash(), child.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1, OnLongestChain: true})
	require.NoError(t, err)
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{grandchild.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 2, BlockHeight: 2, OnLongestChain: true})
	require.NoError(t, err)
	for page := 0; page <= (len(child.Outputs)-1)/settings.UtxoStore.UtxoBatchSize; page++ {
		childKey, err := as.NewKey("test", "recovery_e2e", uaerospike.CalculateKeySourceInternal(child.TxIDChainHash(), uint32(page)))
		require.NoError(t, err)
		_, err = store.GetClient().Delete(nil, childKey)
		require.NoError(t, err)
	}
	_, _, err = store.SpendAndCreate(ctx, child, 2)
	require.NoError(t, err)
	start := func(header *model.BlockHeader, height uint32) (*BlockAssembler, func()) {
		serviceCtx, stop := context.WithCancel(ctx)
		announcements := make(chan subtreeprocessor.NewSubtreeRequest, 100)
		go func() {
			for {
				select {
				case request := <-announcements:
					if request.ErrChan != nil {
						select {
						case request.ErrChan <- nil:
						case <-serviceCtx.Done():
							return
						}
					}
				case <-serviceCtx.Done():
					return
				}
			}
		}()
		assembler, err := NewBlockAssembler(serviceCtx, logger, settings, gocore.NewStat("replay-e2e"), store, memory.New(), chain, announcements)
		require.NoError(t, err)
		assembler.setBestBlockHeader(header, height)
		assembler.subtreeProcessor.InitCurrentBlockHeader(header)
		require.NoError(t, assembler.SetState(serviceCtx))
		require.NoError(t, assembler.Start(serviceCtx))
		var once sync.Once
		cleanup := func() {
			once.Do(func() { stop(); assembler.wg.Wait(); assembler.subtreeProcessor.Stop(context.Background()) })
		}
		t.Cleanup(cleanup)
		return assembler, cleanup
	}
	assembler, stop := start(block2.Header, 2)
	containsChild := func(b *BlockAssembler) bool {
		for _, id := range b.subtreeProcessor.GetTransactionHashes(ctx) {
			if id == *child.TxIDChainHash() {
				return true
			}
		}
		return false
	}
	require.True(t, containsChild(assembler), "actual startup retains recreated confirmed transaction")
	for _, options := range []struct{ full, inputs bool }{{false, false}, {true, false}, {false, true}} {
		done := make(chan error, 1)
		assembler.resetCh <- resetRequest{FullReset: options.full, ValidateInputs: options.inputs, ErrCh: done}
		require.NoError(t, <-done)
		if paginated && options.inputs {
			// Existing input-only reads omit external inputs. This reset drops
			// the candidate without repairing the store; startup below reloads it.
			require.False(t, containsChild(assembler))
			_, err := nativeBackend.Snapshot(ctx, child)
			require.NoError(t, err, "input validation did not repair the external replay")
		} else {
			require.Eventually(t, func() bool { return containsChild(assembler) }, 5*time.Second, 10*time.Millisecond, "reset full=%v inputs=%v retains recreated transaction", options.full, options.inputs)
		}
	}
	stop()
	assembler, stop = start(block2.Header, 2)
	require.True(t, containsChild(assembler), "second real startup also retains replay")
	stop()
	// Operators stop every writer and persist IDLE before opening recovery.
	legitimate := makeChild(parent, 1, 2500)
	_, _, err = store.SpendAndCreate(ctx, legitimate, 2)
	require.NoError(t, err)
	require.NoError(t, chain.SetState(ctx, "fsm_state", []byte("IDLE")))
	tip := rr.Tip{Hash: block2.Hash().String(), Height: 2}
	guard := func(ctx context.Context) error {
		state, e := chain.GetState(ctx, "fsm_state")
		if e != nil {
			return e
		}
		if string(state) != "IDLE" {
			return errors.NewProcessingError("not IDLE")
		}
		header, meta, e := chain.GetBestBlockHeader(ctx)
		if e != nil {
			return e
		}
		if header.Hash().String() != tip.Hash || meta.Height != tip.Height {
			return errors.NewProcessingError("tip changed")
		}
		return nil
	}
	recoveryDir := filepath.Join(t.TempDir(), "recovery")
	require.NoError(t, os.Mkdir(recoveryDir, 0700))
	manifest := filepath.Join(recoveryDir, "manifest.db")
	journal := filepath.Join(recoveryDir, "journal.db")
	_, err = rr.Inventory(ctx, nativeBackend, manifest, guard)
	require.NoError(t, err)
	rawBackend := rr.NewEvidenceBackend(nativeBackend, nil, tip, func(ctx context.Context, id string) (*bt.Tx, error) {
		h, e := chainhash.NewHashFromStr(id)
		if e != nil {
			return nil, e
		}
		return store.GetTxFromExternalStore(ctx, *h)
	})
	source, err := command.NewHistory(ctx, filepath.Join(recoveryDir, "history.db"), chainStore, archive, command.HistoryOptions{Guard: guard, TargetsPath: manifest, EndHeight: tip.Height, GenesisActivationHeight: settings.ChainCfgParams.GenesisActivationHeight, Unconfirmed: rawBackend.Transaction})
	require.NoError(t, err)
	defer source.Close()
	require.NoError(t, source.Build(ctx, tip))
	backend := rr.NewEvidenceBackend(nativeBackend, source, tip, rawBackend.Transaction)
	audit, err := rr.Discover(ctx, backend, source, manifest, guard, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, audit.FullySpent)
	_, err = rr.Apply(ctx, backend, source, manifest, journal, rr.ApplyOptions{Maintenance: true, Tip: tip, Guard: guard})
	require.NoError(t, err)
	verified, err := rr.Verify(ctx, backend, source, manifest, journal, guard)
	require.NoError(t, err)
	require.True(t, verified.Complete)
	_, err = nativeBackend.Snapshot(ctx, legitimate)
	require.NoError(t, err, "valid unmined survives repair")
	state, err := chain.GetState(ctx, "fsm_state")
	require.NoError(t, err)
	require.Equal(t, "IDLE", string(state))
	// Only the operator resumes normal services after successful verification.
	require.NoError(t, chain.SetState(ctx, "fsm_state", []byte("RUNNING")))
	assembler, stop = start(block2.Header, 2)
	require.False(t, containsChild(assembler))
	done := make(chan error, 1)
	assembler.resetCh <- resetRequest{FullReset: true, ErrCh: done}
	require.NoError(t, <-done)
	require.False(t, containsChild(assembler), "ordinary reset does not reload repaired transaction")
	stop()
	// A later canonical tip and a fresh legitimate candidate prove verification is
	// not relying on a reset acknowledgement or empty candidate alone.
	cb3, err := bt.NewTxFromBytes(genesis.CoinbaseTx.Bytes())
	require.NoError(t, err)
	cb3.LockTime = 3
	block3 := replayFixtureBlock(t, block2.Header, cb3)
	retain(block3, 3)
	require.NoError(t, chain.AddBlock(ctx, block3, "", blockchainoptions.WithMinedSet(true)))
	require.NoError(t, chain.SetBlockProcessedAt(ctx, block3.Hash()))
	_, err = store.Create(ctx, cb3, 3)
	require.NoError(t, err)
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{cb3.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 3, BlockHeight: 3, OnLongestChain: true})
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(3))
	assembler, stop = start(block3.Header, 3)
	defer stop()
	candidate, subtrees, err := assembler.GetMiningCandidate(ctx)
	require.NoError(t, err)
	require.NotNil(t, candidate)
	var candidateIDs []string
	for _, st := range subtrees {
		for _, node := range st.Nodes {
			candidateIDs = append(candidateIDs, node.Hash.String())
		}
	}
	require.Contains(t, candidateIDs, legitimate.TxID())
	require.NotContains(t, candidateIDs, child.TxID())
	_, _, err = store.SpendAndCreate(ctx, child, 3)
	require.ErrorContains(t, err, "invalid spend")
	if paginated {
		archived, err := store.GetTxFromExternalStore(ctx, *child.TxIDChainHash())
		require.NoError(t, err)
		require.Equal(t, child.TxID(), archived.TxID(), "repair preserves external transaction archive")
	}
	for page := 0; page <= (len(child.Outputs)-1)/settings.UtxoStore.UtxoBatchSize; page++ {
		absent, err := backend.Read(ctx, uaerospike.CalculateKeySourceInternal(child.TxIDChainHash(), uint32(page)))
		require.NoError(t, err)
		require.Nil(t, absent)
	}
}
