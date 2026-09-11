package recoverreplayedtransactions

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtree "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	rr "github.com/bsv-blockchain/teranode/pkg/replayrecovery"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	astore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"
)

// Exercise the same single-job orchestration as the CLI with real store and
// canonical archive reads. No blockchain RPC or storage implementation is mocked.
func TestRunJobAerospike(t *testing.T) {
	if testing.Short() {
		t.Skip("requires real Aerospike")
	}
	for _, absent := range []bool{false, true} {
		t.Run(fmt.Sprintf("absent=%v", absent), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 180*time.Second)
			defer cancel()
			container, err := aeroTest.RunContainer(ctx, aeroTest.WithTTLSupport("test"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, container.Terminate(context.Background())) })
			host, err := container.Host(ctx)
			require.NoError(t, err)
			port, err := container.ServicePort(ctx)
			require.NoError(t, err)
			s := test.CreateBaseTestSettings(t)
			s.UtxoStore.DisableDAHCleaner = true
			logger := ulogger.NewErrorTestLogger(t)
			u, err := url.Parse("sqlitememory:///")
			require.NoError(t, err)
			chain, err := blockchainsql.New(logger, u, s)
			require.NoError(t, err)
			defer chain.Close(ctx)
			archive := memory.New()
			genesis, err := chain.GetBlockByID(ctx, 0)
			require.NoError(t, err)
			parent := bt.NewTx()
			require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 10000))
			for i := 0; i < 2; i++ {
				require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
			}
			spend := func(parent *bt.Tx, vout uint32) *bt.Tx {
				tx := bt.NewTx()
				require.NoError(t, tx.From(parent.TxID(), vout, parent.Outputs[vout].LockingScript.String(), parent.Outputs[vout].Satoshis))
				require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", parent.Outputs[vout].Satoshis-100))
				return tx
			}
			child := spend(parent, 0)
			grand := spend(child, 0)
			legitimate := spend(parent, 1)
			appendBlock := func(height uint32, txs ...*bt.Tx) *model.Block {
				prev, _, e := chain.GetBestBlockHeader(ctx)
				require.NoError(t, e)
				cb, e := bt.NewTxFromBytes(genesis.CoinbaseTx.Bytes())
				require.NoError(t, e)
				cb.LockTime = height
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
				root, e := st.RootHashWithReplaceRootNode(cb.TxIDChainHash(), 0, uint64(cb.Size()))
				require.NoError(t, e)
				block := &model.Block{Header: &model.BlockHeader{Version: 1, HashPrevBlock: prev.Hash(), HashMerkleRoot: root, Timestamp: prev.Timestamp + 600, Bits: prev.Bits}, CoinbaseTx: cb, Subtrees: []*chainhash.Hash{key}, Height: height, TransactionCount: uint64(len(txs) + 1)}
				_, _, e = chain.StoreBlock(ctx, block, "test")
				require.NoError(t, e)
				return block
			}
			appendBlock(1, parent, child)
			appendBlock(2, grand)
			require.NoError(t, chain.SetState(ctx, "fsm_state", []byte("IDLE")))
			set := fmt.Sprintf("job_%v", absent)
			target, e := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=%s&externalStore=memory://", host, port, set))
			require.NoError(t, e)
			store, e := astore.New(ctx, logger, s, target)
			require.NoError(t, e)
			store.SetExternalStore(memory.New())
			require.NoError(t, store.SetBlockHeight(2))
			_, e = store.Create(ctx, parent, 1)
			require.NoError(t, e)
			_, _, e = store.SpendAndCreate(ctx, child, 1)
			require.NoError(t, e)
			_, _, e = store.SpendAndCreate(ctx, grand, 2)
			require.NoError(t, e)
			for _, tx := range []*bt.Tx{parent, child, grand} {
				height := uint32(1)
				if tx == grand {
					height = 2
				}
				_, e = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: height, BlockHeight: height, OnLongestChain: true})
				require.NoError(t, e)
			}
			key, e := as.NewKey("test", set, uaerospike.CalculateKeySourceInternal(child.TxIDChainHash(), 0))
			require.NoError(t, e)
			_, e = store.GetClient().Delete(nil, key)
			require.NoError(t, e)
			if !absent {
				_, _, e = store.SpendAndCreate(ctx, child, 2)
				require.NoError(t, e)
			}
			_, _, e = store.SpendAndCreate(ctx, legitimate, 2)
			require.NoError(t, e)
			native, e := astore.NewRecoveryBackend(store.GetClient().Client, "test", set, s.UtxoStore.UtxoBatchSize, target.Host)
			require.NoError(t, e)
			guard, e := newIdleGuard(ctx, chain)
			require.NoError(t, e)
			raw := rr.NewEvidenceBackend(native, nil, guard.tip, nil)
			run := func(options Options) (rr.Summary, error) {
				var out, progress bytes.Buffer
				e := runJob(ctx, native, raw.Transaction, chain, archive, guard, options, s.ChainCfgParams.GenesisActivationHeight, &out, &progress)
				var summary rr.Summary
				require.NoError(t, json.Unmarshal(out.Bytes(), &summary))
				return summary, e
			}
			options := Options{WorkDir: t.TempDir(), Timeout: time.Minute, Concurrency: 1}
			require.NoError(t, os.Chmod(options.WorkDir, 0700))
			before, e := native.Read(ctx, uaerospike.CalculateKeySourceInternal(parent.TxIDChainHash(), 0))
			require.NoError(t, e)
			summary, e := run(options)
			require.NoError(t, e)
			require.True(t, summary.Complete)
			after, e := native.Read(ctx, uaerospike.CalculateKeySourceInternal(parent.TxIDChainHash(), 0))
			require.NoError(t, e)
			require.Equal(t, before, after, "default audit never mutates native records")
			_, e = run(options)
			require.ErrorContains(t, e, "explicit --resume")
			options.WorkDir = t.TempDir()
			require.NoError(t, os.Chmod(options.WorkDir, 0700))
			options.Apply = true
			options.Maintenance = true
			summary, e = run(options)
			require.NoError(t, e)
			require.True(t, summary.Complete)
			require.True(t, summary.RestartRequired)
			state, e := readJobState(filepath.Join(options.WorkDir, "job.json"))
			require.NoError(t, e)
			require.Equal(t, "done", state.Phase)
			options.Resume = true
			summary, e = run(options)
			require.NoError(t, e)
			require.True(t, summary.Complete)
			_, e = native.Snapshot(ctx, legitimate)
			require.NoError(t, e)
			persisted, e := chain.GetState(ctx, "fsm_state")
			require.NoError(t, e)
			require.Equal(t, "IDLE", string(persisted))
			_, _, e = store.SpendAndCreate(ctx, child, 2)
			require.ErrorContains(t, e, "invalid spend")
			appendBlock(guard.tip.Height + 1)
			require.ErrorContains(t, guard.Check(ctx), "canonical tip changed")
			guard, e = newIdleGuard(ctx, chain)
			require.NoError(t, e)
			_, e = run(options)
			require.ErrorContains(t, e, "pinned tip mismatch")
		})
	}
}
