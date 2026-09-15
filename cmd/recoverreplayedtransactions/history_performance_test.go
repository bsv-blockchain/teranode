package recoverreplayedtransactions

import (
	"context"
	"database/sql"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtree "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/pkg/replayrecovery"
	"github.com/stretchr/testify/require"
)

// Count real sqlitememory reads, preserving the store's canonical selection.
type countedHistoryChain struct {
	HistoryChain
	bestReads, blockReads int
}

func (c *countedHistoryChain) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.bestReads++
	return c.HistoryChain.GetBestBlockHeader(ctx)
}
func (c *countedHistoryChain) GetBlockInChainByHeightHash(ctx context.Context, height uint32, tip *chainhash.Hash) (*model.Block, bool, error) {
	c.blockReads++
	return c.HistoryChain.GetBlockInChainByHeightHash(ctx, height, tip)
}

func TestHistoryUnconfirmedDepthDoesNotMultiplyBodyScans(t *testing.T) {
	var transactions []*bt.Tx
	retained := make(map[string]*bt.Tx)
	parent := strings.Repeat("11", 32)
	for i := 0; i < 16; i++ {
		hash, err := chainhash.NewHashFromStr(parent)
		require.NoError(t, err)
		tx, err := bt.NewTxFromString("0100000001" + hex.EncodeToString(hash[:]) + "000000000100ffffffff010100000000000000015100000000")
		require.NoError(t, err)
		transactions = append(transactions, tx)
		retained[tx.TxID()] = tx
		parent = tx.TxID()
	}
	reads, scanned := make(map[string]int), 0
	options := HistoryOptions{
		Unconfirmed: func(_ context.Context, id string) (*bt.Tx, error) { reads[id]++; return retained[id], nil },
		Progress:    func(HistoryCoverage) { scanned++ },
	}
	history, chain, _, _, tip := reviewHistoryFixture(t, transactions[:10], transactions[15].TxID(), options)
	defer history.Close()
	// A normally mined census row only stops graph expansion; the final body
	// scan must still authenticate this parent before it can prove an input.
	_, err := history.db.Exec(`CREATE TABLE census.inventory(txid TEXT,master INTEGER,candidate INTEGER,reason TEXT);
 INSERT INTO census.inventory VALUES(?,1,0,'')`, transactions[9].TxID())
	require.NoError(t, err)
	counted := &countedHistoryChain{HistoryChain: chain}
	history.chain = counted
	require.NoError(t, history.Build(t.Context(), tip))
	require.LessOrEqual(t, scanned, 4, "six unconfirmed generations need at most two body passes")
	require.LessOrEqual(t, counted.blockReads, 6, "one header pass and at most two body passes")
	require.Zero(t, reads[transactions[9].TxID()], "mined hint stops expansion before reading confirmed ancestors")
	var targets int
	require.NoError(t, history.db.QueryRow("SELECT count(*) FROM targets").Scan(&targets))
	require.Equal(t, 7, targets)
	evidence, err := history.Check(t.Context(), transactions[15].TxID(), tip)
	require.NoError(t, err)
	require.Equal(t, replayrecovery.Unconfirmed, evidence.Classification, evidence.Reason)
}

func TestHistoryBuildBatchesGuardChecksAndKeepsPhaseBoundaries(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "stable", true: "tip-change"}[changed], func(t *testing.T) {
			tx, err := bt.NewTxFromString("0100000001" + strings.Repeat("11", 32) + "000000000100ffffffff010100000000000000015100000000")
			require.NoError(t, err)
			history, chain, _, _, tip := reviewHistoryFixture(t, []*bt.Tx{tx}, tx.TxID(), HistoryOptions{})
			defer history.Close()
			prior, _, err := chain.GetBlockInChainByHeightHash(t.Context(), 1, mustHistoryHash(t, tip.Hash))
			require.NoError(t, err)
			for height := uint32(2); height <= 300; height++ {
				coinbase := prior.CoinbaseTx.Clone()
				coinbase.LockTime++
				block := &model.Block{Header: &model.BlockHeader{Version: 1, HashPrevBlock: prior.Hash(), HashMerkleRoot: coinbase.TxIDChainHash(), Timestamp: prior.Header.Timestamp + 600, Bits: prior.Header.Bits}, CoinbaseTx: coinbase, TransactionCount: 1, Height: height}
				_, _, err = chain.StoreBlock(t.Context(), block, "test")
				require.NoError(t, err)
				prior = block
			}
			tip = replayrecovery.Tip{Hash: prior.Hash().String(), Height: 300}
			history.options.EndHeight = 300
			history.coverage.EndHeight = 300
			counted := &countedHistoryChain{HistoryChain: chain}
			history.chain = counted
			guards := 0
			history.options.Guard = func(context.Context) error { guards++; return nil }
			history.options.Progress = func(c HistoryCoverage) {
				if !changed || c.Scanned != 300 {
					return
				}
				coinbase := prior.CoinbaseTx.Clone()
				coinbase.LockTime++
				block := &model.Block{Header: &model.BlockHeader{Version: 1, HashPrevBlock: prior.Hash(), HashMerkleRoot: coinbase.TxIDChainHash(), Timestamp: prior.Header.Timestamp + 600, Bits: prior.Header.Bits}, CoinbaseTx: coinbase, TransactionCount: 1, Height: 301}
				_, _, e := chain.StoreBlock(t.Context(), block, "test")
				require.NoError(t, e)
			}
			err = history.Build(t.Context(), tip)
			if changed {
				require.ErrorContains(t, err, "canonical tip changed")
				require.False(t, history.built)
				var seals int
				require.NoError(t, history.db.QueryRow("SELECT count(*) FROM coverage").Scan(&seals))
				require.Zero(t, seals)
			} else {
				require.NoError(t, err)
				require.LessOrEqual(t, guards, 16, "guard RPCs must scale by bounded batches rather than every height")
				require.Equal(t, 602, counted.blockReads, "every header and block still checks pinned ancestry")
			}
		})
	}
}
func mustHistoryHash(t *testing.T, id string) *chainhash.Hash {
	t.Helper()
	hash, err := chainhash.NewHashFromStr(id)
	require.NoError(t, err)
	return hash
}

func TestHistoryExpansionDepthBoundLeavesUnknown(t *testing.T) {
	retained := make(map[string]*bt.Tx)
	parent := strings.Repeat("11", 32)
	var first, last *bt.Tx
	for i := 0; i < 270; i++ {
		hash := mustHistoryHash(t, parent)
		tx, err := bt.NewTxFromString("0100000001" + hex.EncodeToString(hash[:]) + "000000000100ffffffff010100000000000000015100000000")
		require.NoError(t, err)
		retained[tx.TxID()] = tx
		if first == nil {
			first = tx
		}
		last, parent = tx, tx.TxID()
	}
	reads, scanned := 0, 0
	history, _, _, _, tip := reviewHistoryFixture(t, []*bt.Tx{first}, last.TxID(), HistoryOptions{
		Unconfirmed: func(_ context.Context, id string) (*bt.Tx, error) { reads++; return retained[id], nil },
		Progress:    func(HistoryCoverage) { scanned++ },
	})
	defer history.Close()
	require.NoError(t, history.Build(t.Context(), tip))
	require.LessOrEqual(t, reads, 256)
	require.LessOrEqual(t, scanned, 4)
	evidence, err := history.Check(t.Context(), last.TxID(), tip)
	require.NoError(t, err)
	require.Equal(t, replayrecovery.Unknown, evidence.Classification)
}

func TestHistoryMinedExpansionHintDoesNotProveInclusion(t *testing.T) {
	first, err := bt.NewTxFromString("0100000001" + strings.Repeat("11", 32) + "000000000100ffffffff010100000000000000015100000000")
	require.NoError(t, err)
	missing, err := bt.NewTxFromString("0100000001" + hex.EncodeToString(first.TxIDChainHash()[:]) + "000000000100ffffffff010100000000000000015100000000")
	require.NoError(t, err)
	target, err := bt.NewTxFromString("0100000001" + hex.EncodeToString(missing.TxIDChainHash()[:]) + "000000000100ffffffff010100000000000000015100000000")
	require.NoError(t, err)
	retained := map[string]*bt.Tx{first.TxID(): first, missing.TxID(): missing, target.TxID(): target}
	history, _, _, _, tip := reviewHistoryFixture(t, []*bt.Tx{first}, target.TxID(), HistoryOptions{
		Unconfirmed: func(_ context.Context, id string) (*bt.Tx, error) { return retained[id], nil },
	})
	defer history.Close()
	_, err = history.db.Exec(`CREATE TABLE census.inventory(txid TEXT,master INTEGER,candidate INTEGER,reason TEXT);
 INSERT INTO census.inventory VALUES(?,1,0,'')`, missing.TxID())
	require.NoError(t, err)
	require.NoError(t, history.Build(t.Context(), tip))
	evidence, err := history.Check(t.Context(), target.TxID(), tip)
	require.NoError(t, err)
	require.Equal(t, replayrecovery.Unknown, evidence.Classification, "local mined metadata cannot replace a canonical input proof")
}

func TestHistoryBatchedScanStillChecksEveryPinnedBlock(t *testing.T) {
	tx, err := bt.NewTxFromString("0100000001" + strings.Repeat("11", 32) + "000000000100ffffffff010100000000000000015100000000")
	require.NoError(t, err)
	history, _, _, _, tip := reviewHistoryFixture(t, []*bt.Tx{tx}, tx.TxID(), HistoryOptions{})
	defer history.Close()
	history.options.Progress = func(c HistoryCoverage) {
		if c.Scanned == 1 {
			_, e := history.db.Exec("UPDATE headers SET hash=? WHERE height=1", strings.Repeat("ff", 32))
			require.NoError(t, e)
		}
	}
	require.ErrorContains(t, history.Build(t.Context(), tip), "canonical block ancestry changed")
	require.False(t, history.built)
}

func TestHistoryLookupReauthenticatesPinnedHeader(t *testing.T) {
	tx, err := bt.NewTxFromString("0100000001" + strings.Repeat("11", 32) + "000000000100ffffffff010100000000000000015100000000")
	require.NoError(t, err)
	history, _, _, _, tip := reviewHistoryFixture(t, []*bt.Tx{tx}, tx.TxID(), HistoryOptions{})
	defer history.Close()
	require.NoError(t, history.Build(t.Context(), tip))
	_, err = history.Lookup(t.Context(), tx.TxID(), tip)
	require.NoError(t, err)
	_, err = history.db.Exec("UPDATE headers SET header=zeroblob(80) WHERE height=1")
	require.NoError(t, err)
	_, err = history.Lookup(t.Context(), tx.TxID(), tip)
	require.ErrorContains(t, err, "history header differs from pinned ancestry")
}

func TestHistoryResumeRejectsChangedRetention(t *testing.T) {
	tx, err := bt.NewTxFromString("0100000001" + strings.Repeat("11", 32) + "000000000100ffffffff010100000000000000015100000000")
	require.NoError(t, err)
	history, chain, _, path, tip := reviewHistoryFixture(t, []*bt.Tx{tx}, tx.TxID(), HistoryOptions{BlockHeightRetention: 100})
	require.NoError(t, history.Build(t.Context(), tip))
	require.Equal(t, uint32(100), history.Retention())
	require.NoError(t, history.Close())
	reopened, err := OpenHistory(path, chain, HistoryOptions{BlockHeightRetention: 99, Guard: func(context.Context) error { return nil }})
	require.ErrorContains(t, err, "consensus settings changed")
	require.Nil(t, reopened)
}

func TestHistoryReuseTargetsChecksFreshCensusBeforeExpansion(t *testing.T) {
	tx, err := bt.NewTxFromString("0100000001" + strings.Repeat("11", 32) + "000000000100ffffffff010100000000000000015100000000")
	require.NoError(t, err)
	parent := tx.Inputs[0].PreviousTxIDStr()
	history, chain, _, path, tip := reviewHistoryFixture(t, []*bt.Tx{tx}, tx.TxID(), HistoryOptions{})
	_, err = history.db.Exec("INSERT INTO targets VALUES(?)", parent)
	require.NoError(t, err)
	require.NoError(t, history.Build(t.Context(), tip))
	censusPath := history.options.TargetsPath
	require.NoError(t, history.Close())
	census, err := sql.Open("sqlite", censusPath)
	require.NoError(t, err)
	defer census.Close()
	_, err = census.Exec("DELETE FROM targets; INSERT INTO targets VALUES(?)", strings.Repeat("22", 32))
	require.NoError(t, err)
	history, err = OpenHistory(path, chain, HistoryOptions{Guard: func(context.Context) error { return nil }})
	require.NoError(t, err)
	defer history.Close()
	reused, err := history.ReuseTargets(t.Context(), censusPath)
	require.NoError(t, err)
	require.False(t, reused)
	var count int
	require.NoError(t, census.QueryRow("SELECT count(*) FROM targets").Scan(&count))
	require.Equal(t, 1, count, "a missing target must not modify the census")
	_, err = census.Exec("DELETE FROM targets; INSERT INTO targets VALUES(?)", tx.TxID())
	require.NoError(t, err)
	reused, err = history.ReuseTargets(t.Context(), censusPath)
	require.NoError(t, err)
	require.True(t, reused)
	require.NoError(t, census.QueryRow("SELECT count(*) FROM targets").Scan(&count))
	require.Equal(t, 2, count, "restore previously expanded dependencies into the new census")
}

func TestHistoryEvidenceUsesLatestAuthenticatedSpendHeight(t *testing.T) {
	parent, err := bt.NewTxFromString("0100000001" + strings.Repeat("11", 32) + "000000000100ffffffff02010000000000000001510100000000000000015100000000")
	require.NoError(t, err)
	first, err := bt.NewTxFromString("0100000001" + hex.EncodeToString(parent.TxIDChainHash()[:]) + "000000000100ffffffff010100000000000000015100000000")
	require.NoError(t, err)
	second, err := bt.NewTxFromString("0100000001" + hex.EncodeToString(parent.TxIDChainHash()[:]) + "010000000100ffffffff010100000000000000015100000000")
	require.NoError(t, err)
	history, chain, archive, _, tip := reviewHistoryFixture(t, []*bt.Tx{parent}, parent.TxID(), HistoryOptions{})
	defer history.Close()
	prior, _, err := chain.GetBlockInChainByHeightHash(t.Context(), tip.Height, mustHistoryHash(t, tip.Hash))
	require.NoError(t, err)
	// Visit the later-spent output first, so overwriting rather than taking the
	// maximum would incorrectly report height 2.
	for i, tx := range []*bt.Tx{second, first} {
		coinbase := prior.CoinbaseTx.Clone()
		coinbase.LockTime++
		st, err := subtree.NewTree(2)
		require.NoError(t, err)
		require.NoError(t, st.AddCoinbaseNode())
		require.NoError(t, st.AddNode(*tx.TxIDChainHash(), 0, uint64(tx.Size())))
		data, err := st.Serialize()
		require.NoError(t, err)
		key := st.RootHash()
		require.NoError(t, archive.Set(t.Context(), key[:], fileformat.FileTypeSubtree, data))
		require.NoError(t, archive.Set(t.Context(), key[:], fileformat.FileTypeSubtreeData, tx.Bytes()))
		root, err := st.RootHashWithReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))
		require.NoError(t, err)
		block := &model.Block{Header: &model.BlockHeader{Version: 1, HashPrevBlock: prior.Hash(), HashMerkleRoot: root, Timestamp: prior.Header.Timestamp + 600, Bits: prior.Header.Bits}, CoinbaseTx: coinbase, Subtrees: []*chainhash.Hash{key}, TransactionCount: 2, Height: uint32(i + 2)}
		_, _, err = chain.StoreBlock(t.Context(), block, "test")
		require.NoError(t, err)
		prior = block
	}
	tip = replayrecovery.Tip{Hash: prior.Hash().String(), Height: 3}
	history.options.EndHeight = 3
	history.coverage.EndHeight = 3
	require.NoError(t, history.Build(t.Context(), tip))
	evidence, err := history.Check(t.Context(), parent.TxID(), tip)
	require.NoError(t, err)
	require.Equal(t, replayrecovery.FullySpent, evidence.Classification, evidence.Reason)
	require.Equal(t, uint32(1), evidence.BlockHeight)
	require.Equal(t, uint32(3), evidence.LastSpendHeight)
}

func TestHistoryExpansionSharesFanoutBudget(t *testing.T) {
	confirmed, err := bt.NewTxFromString("0100000001" + strings.Repeat("11", 32) + "000000000100ffffffff010100000000000000015100000000")
	require.NoError(t, err)
	// 10,002 distinct inputs exceed the shared 10,000-edge expansion budget.
	var raw strings.Builder
	raw.WriteString("01000000fd1227")
	for i := 0; i < 10002; i++ {
		hash := chainhash.Hash{byte(i), byte(i >> 8), 42}
		raw.WriteString(hex.EncodeToString(hash[:]))
		raw.WriteString("000000000100ffffffff")
	}
	raw.WriteString("010100000000000000015100000000")
	target, err := bt.NewTxFromString(raw.String())
	require.NoError(t, err)
	reads := 0
	history, _, _, _, tip := reviewHistoryFixture(t, []*bt.Tx{confirmed}, target.TxID(), HistoryOptions{
		Unconfirmed: func(_ context.Context, id string) (*bt.Tx, error) {
			reads++
			if id == target.TxID() {
				return target, nil
			}
			return nil, nil
		},
	})
	defer history.Close()
	require.NoError(t, history.Build(t.Context(), tip))
	var targets int
	require.NoError(t, history.db.QueryRow("SELECT count(*) FROM targets").Scan(&targets))
	require.Equal(t, 10001, targets)
	require.LessOrEqual(t, reads, 10000)
	evidence, err := history.Check(t.Context(), target.TxID(), tip)
	require.NoError(t, err)
	require.Equal(t, replayrecovery.Unknown, evidence.Classification)
}

func TestHistoryOpenHonorsCancellation(t *testing.T) {
	tx, err := bt.NewTxFromString("0100000001" + strings.Repeat("11", 32) + "000000000100ffffffff010100000000000000015100000000")
	require.NoError(t, err)
	history, chain, _, path, tip := reviewHistoryFixture(t, []*bt.Tx{tx}, tx.TxID(), HistoryOptions{})
	require.NoError(t, history.Build(t.Context(), tip))
	require.NoError(t, history.Close())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	reopened, err := openHistory(ctx, path, chain, HistoryOptions{Guard: func(context.Context) error { return nil }})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, reopened)
}
