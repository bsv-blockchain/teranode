package blockchain

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/work"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// Mined mainnet targets independently exercise UAHF, all 43 EDA increases and
// 13 periodic retargets through the last child before modern DAA activation.
func TestDifficultyHistoricalMainnet(t *testing.T) {
	const first, last = uint32(477792), uint32(504031)
	ctx := t.Context()
	params := chaincfg.MainNetParams
	s := test.CreateBaseTestSettings(t)
	s.ChainCfgParams = &params
	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, s)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	difficulty, err := NewDifficulty(store, ulogger.TestLogger{}, s)
	require.NoError(t, err)
	data, err := os.ReadFile("testdata/mainnet_headers_477792_504031.bin")
	require.NoError(t, err)
	require.Len(t, data, int(last-first+1)*80)
	require.Equal(t, "5e5536bdc70c5fa860b0737984c8cc51c9f8fbb552694253bdcc46e594ed877f", fmt.Sprintf("%x", sha256.Sum256(data)))
	pins := map[uint32]string{
		first:  "00000000000000000016ba7786309176445b838b36a16bd1ef3c3e3020473206",
		478558: "0000000000000000011865af4122fe3b144e2cbeea86142e8ff2fb4107352d43",
		last:   "0000000000000000011ebf65b60d0a3de80b8175be709d653b4c1a1beeb6ab9c",
	}

	// Seed only this header slice, avoiding a full chain/coinbase import. Every
	// calculator lookup still uses the real SQLite store and linked parent IDs.
	// Relative cumulative work suffices here; historical rules use header fields.
	tx, err := store.GetDB().BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	insert, err := tx.PrepareContext(ctx, `INSERT INTO blocks (
		id, parent_id, hash, height, version, previous_hash, merkle_root,
		block_time, n_bits, nonce, chain_work, tx_count, size_in_bytes,
		subtree_count, subtrees, coinbase_tx, peer_id, on_main_chain
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 0, 80, 0, X'', X'', 'test', TRUE)`)
	require.NoError(t, err)
	defer func() { _ = insert.Close() }()
	headers := make([]*model.BlockHeader, last-first+1)
	chainwork := new(big.Int)
	for i := range headers {
		header, err := model.NewBlockHeaderFromBytes(data[i*80 : (i+1)*80])
		require.NoError(t, err)
		height := first + uint32(i)
		var parentID any
		if i > 0 {
			require.Equal(t, headers[i-1].Hash(), header.HashPrevBlock, "height %d", height)
			parentID = i
		}
		if want, pinned := pins[height]; pinned {
			require.Equal(t, want, header.Hash().String(), "height %d", height)
		}
		require.NoError(t, header.HasMetPowLimit(&params))
		met, _, err := header.HasMetTargetDifficulty()
		require.NoError(t, err)
		require.True(t, met, "height %d", height)
		chainwork.Add(chainwork, work.CalcBlockWork(binary.LittleEndian.Uint32(header.Bits.CloneBytes())))
		_, err = insert.ExecContext(ctx, i+1, parentID, header.Hash()[:], height, header.Version,
			header.HashPrevBlock[:], header.HashMerkleRoot[:], header.Timestamp, header.Bits.CloneBytes(),
			header.Nonce, chainwork.FillBytes(make([]byte, 32)))
		require.NoError(t, err)
		headers[i] = header
	}
	require.NoError(t, tx.Commit())

	var retargets, emergencyChanges int
	// Include the child immediately before UAHF's parent-height activation.
	for height := uint32(478558); height <= last; height++ {
		parent, child := headers[height-first-1], headers[height-first]
		bits, err := difficulty.CalcNextWorkRequired(ctx, parent, height-1, int64(child.Timestamp))
		require.NoError(t, err, "height %d", height)
		require.NotNil(t, bits)
		require.Equal(t, child.Bits, *bits, "height %d", height)
		if height%2016 == 0 {
			retargets++
		} else if child.Bits != parent.Bits {
			emergencyChanges++
		}
	}
	require.Equal(t, 13, retargets)
	require.Equal(t, 43, emergencyChanges)
}
