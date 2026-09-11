package recoverreplayedtransactions

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtree "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/pkg/replayrecovery"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestHistoryCanonicalArchiveAndGaps(t *testing.T) {
	for _, mode := range []string{"valid", "missing-data", "wrong-root", "wrong-raw", "bounded"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			u, err := url.Parse("sqlitememory:///")
			require.NoError(t, err)
			chain, err := blockchainsql.New(ulogger.TestLogger{}, u, test.CreateBaseTestSettings(t))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, chain.Close(ctx)) })
			archive := memory.New()
			raw := "0100000001" + strings.Repeat("00", 32) + "ffffffff0100ffffffff010100000000000000015100000000"
			coinbase, err := bt.NewTxFromString(raw)
			require.NoError(t, err)
			tx, err := bt.NewTxFromString(strings.Replace(raw, "ffffffff0100", "000000000100", 1))
			require.NoError(t, err)
			st, err := subtree.NewTree(1)
			require.NoError(t, err)
			require.NoError(t, st.AddCoinbaseNode())
			require.NoError(t, st.AddNode(*tx.TxIDChainHash(), 0, uint64(tx.Size())))
			storedRoot := st.RootHash()
			data, err := st.Serialize()
			require.NoError(t, err)
			require.NoError(t, archive.Set(ctx, storedRoot[:], fileformat.FileTypeSubtree, data))
			rawData := tx.Bytes()
			if mode == "wrong-raw" {
				rawData = coinbase.Bytes()
			}
			if mode != "missing-data" {
				require.NoError(t, archive.Set(ctx, storedRoot[:], fileformat.FileTypeSubtreeData, rawData))
			}
			root, err := st.RootHashWithReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))
			require.NoError(t, err)
			if mode == "wrong-root" {
				root = &chainhash.Hash{1}
			}
			prev, _, err := chain.GetBestBlockHeader(ctx)
			require.NoError(t, err)
			bits, err := model.NewNBitFromString("207fffff")
			require.NoError(t, err)
			block := &model.Block{Header: &model.BlockHeader{Version: 1, HashPrevBlock: prev.Hash(), HashMerkleRoot: root, Timestamp: 1700000001, Bits: *bits}, CoinbaseTx: coinbase, Subtrees: []*chainhash.Hash{storedRoot}, TransactionCount: 2, Height: 1}
			_, _, err = chain.StoreBlock(ctx, block, "test")
			require.NoError(t, err)
			tip := replayrecovery.Tip{Hash: block.Hash().String(), Height: 1}
			limit := 100
			if mode == "bounded" {
				limit = 1
			}
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			options := HistoryOptions{StartHeight: 1, EndHeight: 1, MaxBlobBytes: 1 << 20, MaxBlockTransactions: limit}
			if mode == "valid" {
				options.MaxBlobBytes = 0
				options.MaxBlockTransactions = 0
			}
			progressCalls := 0
			inventory, e := sql.Open("sqlite", filepath.Join(dir, "inventory.sqlite"))
			require.NoError(t, e)
			_, e = inventory.Exec("CREATE TABLE targets(id TEXT PRIMARY KEY); INSERT INTO targets VALUES(?)", tx.TxID())
			require.NoError(t, e)
			require.NoError(t, inventory.Close())
			options.TargetsPath = filepath.Join(dir, "inventory.sqlite")
			options.Guard = func(context.Context) error { return nil }
			options.Progress = func(c HistoryCoverage) {
				progressCalls++
				require.Equal(t, uint64(1), c.Scanned)
				require.Equal(t, tip, c.Tip)
			}
			history, err := NewHistory(ctx, filepath.Join(dir, "history.sqlite"), chain, archive, options)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, history.Close()) })
			require.NoError(t, history.Build(ctx, tip))
			require.GreaterOrEqual(t, progressCalls, 1)
			evidence, e := history.Check(ctx, tx.TxID(), tip)
			require.NoError(t, e)
			if mode == "valid" {
				require.Equal(t, replayrecovery.Live, evidence.Classification)
			} else {
				require.Equal(t, replayrecovery.Unknown, evidence.Classification)
			}
			inclusion, err := history.Lookup(ctx, tx.TxID(), tip)
			if mode == "valid" {
				require.NoError(t, err)
				got, e := replayrecovery.VerifyInclusion(tx.TxID(), inclusion)
				require.NoError(t, e)
				require.Equal(t, tip.Hash, got)
				require.Equal(t, tx.String(), inclusion.RawTx)
				require.Equal(t, uint32(0), history.Coverage().GapCount)
				require.NoError(t, archive.Del(ctx, storedRoot[:], fileformat.FileTypeSubtreeData))
				require.NoError(t, history.Build(ctx, tip))
				_, err = history.Lookup(ctx, tx.TxID(), tip)
				require.NoError(t, err, "second lookup/build must use index without rescan")
				require.NoError(t, history.Close())
				reopened, e := OpenHistory(filepath.Join(dir, "history.sqlite"), chain, options)
				require.NoError(t, e)
				history = reopened
				later := replayrecovery.Tip{Hash: strings.Repeat("ab", 32), Height: 2}
				again, e := history.Lookup(ctx, tx.TxID(), later)
				require.Error(t, e)
				_ = again
				require.Equal(t, uint64(1), history.Coverage().Scanned)
			} else {
				require.Error(t, err)
				require.Equal(t, uint32(1), history.Coverage().GapCount)
			}
			_, err = history.Lookup(ctx, strings.Repeat("ff", 32), tip)
			require.Error(t, err)
		})
	}
}

func TestHistoryOpenRejectsMissingIndex(t *testing.T) {
	_, err := OpenHistory(filepath.Join(t.TempDir(), "missing.sqlite"), nil, HistoryOptions{})
	require.Error(t, err)
}

func TestHistoryAuthenticatedSpendAndLargeArchive(t *testing.T) {
	for _, mode := range []string{"valid", "large", "wrong-order", "gap", "unconfirmed"} {
		t.Run(mode, func(t *testing.T) {
			large := mode == "large"
			ctx := context.Background()
			u, err := url.Parse("sqlitememory:///")
			require.NoError(t, err)
			chain, err := blockchainsql.New(ulogger.TestLogger{}, u, test.CreateBaseTestSettings(t))
			require.NoError(t, err)
			defer chain.Close(ctx)
			archive := memory.New()
			coinbase, err := bt.NewTxFromString("0100000001" + strings.Repeat("00", 32) + "ffffffff0100ffffffff010100000000000000015100000000")
			require.NoError(t, err)
			parent, err := bt.NewTxFromString("0100000001" + strings.Repeat("11", 32) + "000000000100ffffffff010100000000000000015100000000")
			require.NoError(t, err)
			childWire := append([]byte{1, 0, 0, 0, 1}, parent.TxIDChainHash().CloneBytes()...)
			suffix, err := hex.DecodeString("000000000100ffffffff010100000000000000015100000000")
			require.NoError(t, err)
			childWire = append(childWire, suffix...)
			child, err := bt.NewTxFromBytes(childWire)
			require.NoError(t, err)
			grandWire := append([]byte{}, childWire...)
			copy(grandWire[5:37], child.TxIDChainHash().CloneBytes())
			grandWire[len(grandWire)-4] = 2
			grand, err := bt.NewTxFromBytes(grandWire)
			require.NoError(t, err)
			transactions := []*bt.Tx{parent, child}
			if mode == "unconfirmed" {
				transactions = []*bt.Tx{parent}
			}
			if mode == "wrong-order" {
				transactions = []*bt.Tx{child, parent}
			}
			if large {
				for i := 0; i < 65; i++ {
					wire := append([]byte{}, childWire[:len(childWire)-6]...)
					wire[5] = 71                          // unrelated filler is authenticated but never retained
					wire = append(wire, 254, 0, 0, 16, 0) // canonical 1 MiB script length
					wire = append(wire, bytes.Repeat([]byte{81}, 1<<20)...)
					wire = append(wire, byte(i), 0, 0, 0)
					tx, e := bt.NewTxFromBytes(wire)
					require.NoError(t, e)
					transactions = append(transactions, tx)
				}
			}
			st, err := subtree.NewTree(7)
			require.NoError(t, err)
			require.NoError(t, st.AddCoinbaseNode())
			var raw bytes.Buffer
			for _, tx := range transactions {
				require.NoError(t, st.AddNode(*tx.TxIDChainHash(), 0, uint64(tx.Size())))
				raw.Write(tx.Bytes())
			}
			if large {
				require.Greater(t, raw.Len(), 64<<20)
			}
			data, err := st.Serialize()
			require.NoError(t, err)
			key := st.RootHash()
			require.NoError(t, archive.Set(ctx, key[:], fileformat.FileTypeSubtree, data))
			require.NoError(t, archive.Set(ctx, key[:], fileformat.FileTypeSubtreeData, raw.Bytes()))
			root, err := st.RootHashWithReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))
			require.NoError(t, err)
			prev, _, err := chain.GetBestBlockHeader(ctx)
			require.NoError(t, err)
			bits, err := model.NewNBitFromString("207fffff")
			require.NoError(t, err)
			block := &model.Block{Header: &model.BlockHeader{Version: 1, HashPrevBlock: prev.Hash(), HashMerkleRoot: root, Timestamp: 1700000001, Bits: *bits}, CoinbaseTx: coinbase, Subtrees: []*chainhash.Hash{key}, TransactionCount: uint64(len(transactions) + 1), Height: 1}
			_, _, err = chain.StoreBlock(ctx, block, "test")
			require.NoError(t, err)
			tip := replayrecovery.Tip{Hash: block.Hash().String(), Height: 1}
			if mode == "gap" {
				missing := chainhash.Hash{42}
				next := &model.Block{Header: &model.BlockHeader{Version: 1, HashPrevBlock: block.Hash(), HashMerkleRoot: &missing, Timestamp: 1700000002, Bits: *bits}, CoinbaseTx: coinbase, Subtrees: []*chainhash.Hash{&missing}, TransactionCount: 2, Height: 2}
				_, _, err = chain.StoreBlock(ctx, next, "test")
				require.NoError(t, err)
				tip = replayrecovery.Tip{Hash: next.Hash().String(), Height: 2}
			}

			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			path := filepath.Join(dir, "history.sqlite")
			inventory, err := sql.Open("sqlite", filepath.Join(dir, "inventory.sqlite"))
			require.NoError(t, err)
			targetID := parent.TxID()
			if mode == "unconfirmed" {
				targetID = grand.TxID()
			}
			_, err = inventory.Exec("CREATE TABLE targets(id TEXT PRIMARY KEY); INSERT INTO targets VALUES(?)", targetID)
			require.NoError(t, err)
			require.NoError(t, inventory.Close())
			options := HistoryOptions{StartHeight: 1, EndHeight: tip.Height, TargetsPath: filepath.Join(dir, "inventory.sqlite"), Guard: func(context.Context) error { return nil }}
			if mode == "unconfirmed" {
				options.StartHeight = 0
				options.Unconfirmed = func(_ context.Context, id string) (*bt.Tx, error) {
					switch id {
					case child.TxID():
						return child, nil
					case grand.TxID():
						return grand, nil
					}
					return nil, commandError("unavailable")
				}
			}
			history, err := NewHistory(ctx, path, chain, archive, options)
			require.NoError(t, err)
			require.NoError(t, history.Build(ctx, tip))
			var retained int
			require.NoError(t, history.db.QueryRow("SELECT count(*) FROM transactions").Scan(&retained))
			expectedRetained := 2
			if mode == "unconfirmed" {
				expectedRetained = 1
			}
			require.Equal(t, expectedRetained, retained)
			require.NoError(t, history.db.QueryRow("SELECT count(*) FROM blocks").Scan(&retained))
			require.Equal(t, 1, retained)
			if mode == "unconfirmed" {
				evidence, e := history.Check(ctx, grand.TxID(), tip)
				require.NoError(t, e)
				require.Equal(t, replayrecovery.Unconfirmed, evidence.Classification, evidence.Reason)
			}
			if mode == "gap" {
				require.Equal(t, uint32(1), history.Coverage().GapCount)
			} else {
				require.Zero(t, history.Coverage().GapCount)
			}
			evidence, err := history.Check(ctx, parent.TxID(), tip)
			require.NoError(t, err)
			if mode == "wrong-order" || mode == "unconfirmed" {
				require.NotEqual(t, replayrecovery.FullySpent, evidence.Classification)
			} else {
				require.Equal(t, replayrecovery.FullySpent, evidence.Classification)
			}
			require.NoError(t, history.Close())
			history, err = OpenHistory(path, chain, options)
			require.NoError(t, err)
			evidence, err = history.Check(ctx, parent.TxID(), tip)
			require.NoError(t, err)
			if mode == "wrong-order" || mode == "unconfirmed" {
				require.NotEqual(t, replayrecovery.FullySpent, evidence.Classification)
			} else {
				require.Equal(t, replayrecovery.FullySpent, evidence.Classification)
			}
			require.NoError(t, history.Close())
			db, err := sql.Open("sqlite", path)
			require.NoError(t, err)
			_, err = db.Exec("DELETE FROM targets")
			require.NoError(t, err)
			require.NoError(t, db.Close())
			_, err = OpenHistory(path, chain, options)
			require.ErrorContains(t, err, "integrity mismatch")
		})
	}
}
