package model

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// A received block body is only bound to its header once the merkle root over its subtrees (or,
// for a coinbase-only body, the coinbase txid) matches the header. Every check that fails on an
// unbound body must classify as corrupt, never as invalid: the same header can be replayed with
// a doctored body, and an invalid verdict would persist invalid=true for the honest hash.

// rawSubtreeStore returns the exact serialized subtree bytes it was given, keyed by root hash,
// so GetAndValidateSubtrees deserializes them on the first attempt without entering its retry.
type rawSubtreeStore struct {
	data map[chainhash.Hash][]byte
}

func (s *rawSubtreeStore) GetIoReader(_ context.Context, key []byte, _ fileformat.FileType, _ ...options.FileOption) (io.ReadCloser, error) {
	var h chainhash.Hash
	copy(h[:], key)

	b, ok := s.data[h]
	if !ok {
		return nil, errors.ErrNotFound
	}

	return io.NopCloser(bytes.NewReader(b)), nil
}

func (s *rawSubtreeStore) put(t *testing.T, st *subtreepkg.Subtree) {
	t.Helper()

	b, err := st.Serialize()
	require.NoError(t, err)

	s.data[*st.RootHash()] = b
}

func randCorruptTestHash(t *testing.T) chainhash.Hash {
	t.Helper()

	b := make([]byte, 32)
	_, err := rand.Read(b)
	require.NoError(t, err)

	h, err := chainhash.NewHash(b)
	require.NoError(t, err)

	return *h
}

func block1HeaderForTest(t *testing.T) *BlockHeader {
	t.Helper()

	blockHeaderBytes, err := hex.DecodeString(block1Header)
	require.NoError(t, err)

	blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
	require.NoError(t, err)

	return blockHeader
}

// mineTestHeader gives a modified header an easy target and grinds the nonce until it meets it,
// so the test reaches the body checks instead of failing on proof of work.
func mineTestHeader(t *testing.T, header *BlockHeader) {
	t.Helper()

	bits, err := NewNBitFromString("207fffff")
	require.NoError(t, err)

	header.Bits = *bits

	for {
		if ok, _, _ := header.HasMetTargetDifficulty(); ok {
			return
		}

		header.Nonce++
	}
}

// bindCoinbaseOnlyHeader makes header commit to a coinbase-only body: its merkle root becomes the
// coinbase txid and it is re-mined.
func bindCoinbaseOnlyHeader(t *testing.T, header *BlockHeader, coinbase *bt.Tx) *BlockHeader {
	t.Helper()

	header.HashMerkleRoot = coinbase.TxIDChainHash()
	mineTestHeader(t, header)

	return header
}

func requireCorruptNotInvalid(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "expected a corrupt-body error, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a corrupt body must never classify as invalid: %v", err)
}

func TestCheckCoinbaseOnlyBodyBound(t *testing.T) {
	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	newBody := func(merkleRoot *chainhash.Hash, subtrees []*chainhash.Hash, txCount uint64) *Block {
		return &Block{
			Header:           &BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: merkleRoot},
			CoinbaseTx:       coinbase,
			Subtrees:         subtrees,
			TransactionCount: txCount,
		}
	}

	otherRoot := chainhash.Hash{0xAB}

	t.Run("no subtrees, root is not the coinbase txid: corrupt", func(t *testing.T) {
		err := newBody(&otherRoot, nil, 1).checkCoinbaseOnlyBodyBound()
		requireCorruptNotInvalid(t, err)
		require.Contains(t, err.Error(), "the header merkle root is not the coinbase txid")
	})

	t.Run("no subtrees, root matches, but more than one transaction claimed: corrupt", func(t *testing.T) {
		err := newBody(coinbase.TxIDChainHash(), nil, 2).checkCoinbaseOnlyBodyBound()
		requireCorruptNotInvalid(t, err)
		require.Contains(t, err.Error(), "claims 2 transactions")
	})

	t.Run("genuine coinbase-only body", func(t *testing.T) {
		require.NoError(t, newBody(coinbase.TxIDChainHash(), nil, 1).checkCoinbaseOnlyBodyBound())
		require.NoError(t, newBody(coinbase.TxIDChainHash(), nil, 0).checkCoinbaseOnlyBodyBound())
	})
}

func TestGetAndValidateSubtrees_SizeMismatchIsCorrupt(t *testing.T) {
	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	st0, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st0.AddCoinbaseNode())

	for i := 0; i < 3; i++ {
		require.NoError(t, st0.AddNode(randCorruptTestHash(t), 1, 0))
	}

	// A non-final subtree shorter than the first is an illegal shape.
	st1, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		require.NoError(t, st1.AddNode(randCorruptTestHash(t), 1, 0))
	}

	st2, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		require.NoError(t, st2.AddNode(randCorruptTestHash(t), 1, 0))
	}

	store := &rawSubtreeStore{data: make(map[chainhash.Hash][]byte)}
	store.put(t, st0)
	store.put(t, st1)
	store.put(t, st2)

	b, err := NewBlock(block1HeaderForTest(t), coinbase,
		[]*chainhash.Hash{st0.RootHash(), st1.RootHash(), st2.RootHash()},
		10, 123, 0, 0)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err = b.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, store, 1)
	requireCorruptNotInvalid(t, err)
	require.Contains(t, err.Error(), "has length")
}

// TestBlockValid_UnboundBodyIsCorrupt replays a real header (mainnet block 1) with bodies that do
// not hash to its merkle root. Whatever else is wrong with such a body, the verdict must be corrupt.
func TestBlockValid_UnboundBodyIsCorrupt(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	t.Run("no subtrees, coinbase does not match the merkle root", func(t *testing.T) {
		tampered := coinbase.Clone()
		tampered.Outputs[0].Satoshis++

		b, err := NewBlock(block1HeaderForTest(t), tampered, []*chainhash.Hash{}, 1, 123, 1, 0)
		require.NoError(t, err)

		ok, err := b.Valid(context.Background(), ulogger.TestLogger{}, nil, nil, nil, nil, nil, tSettings, nil)
		require.False(t, ok)
		requireCorruptNotInvalid(t, err)
	})

	t.Run("no subtrees, unbound coinbase that is not a coinbase", func(t *testing.T) {
		notCoinbase := coinbase.Clone()
		notCoinbase.Inputs[0].PreviousTxOutIndex = 0

		b, err := NewBlock(block1HeaderForTest(t), notCoinbase, []*chainhash.Hash{}, 1, 123, 1, 0)
		require.NoError(t, err)

		ok, err := b.Valid(context.Background(), ulogger.TestLogger{}, nil, nil, nil, nil, nil, tSettings, nil)
		require.False(t, ok)
		requireCorruptNotInvalid(t, err)
	})

	t.Run("subtrees whose merkle root does not match, with a wrong coinbase height", func(t *testing.T) {
		// The coinbase height check used to run before the merkle binding, so a doctored coinbase
		// was condemned as invalid before anything bound it to the header.
		header := block1HeaderForTest(t)
		header.Version = 2
		mineTestHeader(t, header)

		st0, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)
		require.NoError(t, st0.AddCoinbaseNode())
		require.NoError(t, st0.AddNode(randCorruptTestHash(t), 1, 0))

		store := &rawSubtreeStore{data: make(map[chainhash.Hash][]byte)}
		store.put(t, st0)

		b, err := NewBlock(header, coinbase, []*chainhash.Hash{st0.RootHash()}, 2, 123, LastV1Block+100, 0)
		require.NoError(t, err)

		ok, err := b.Valid(context.Background(), ulogger.TestLogger{}, store, nil, nil, nil, nil, tSettings, nil)
		require.False(t, ok)
		requireCorruptNotInvalid(t, err)
		require.Contains(t, err.Error(), "merkle root does not match")
	})
}

// TestBlockValid_BoundBodyFailuresStayInvalid pins the other half of the rule: once the body is
// bound, a bad coinbase is the miner's own and condemns the block.
func TestBlockValid_BoundBodyFailuresStayInvalid(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	t.Run("coinbase height mismatch", func(t *testing.T) {
		header := block1HeaderForTest(t)
		header.Version = 2
		bindCoinbaseOnlyHeader(t, header, coinbase)

		b, err := NewBlock(header, coinbase, []*chainhash.Hash{}, 1, 123, LastV1Block+100, 0)
		require.NoError(t, err)

		ok, err := b.Valid(context.Background(), ulogger.TestLogger{}, nil, nil, nil, nil, nil, tSettings, nil)
		require.False(t, ok)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "a bound body with a bad coinbase height is invalid, got: %v", err)
		require.False(t, errors.IsBlockCorrupt(err))
	})

	t.Run("coinbase scriptSig too short", func(t *testing.T) {
		short := coinbase.Clone()
		short.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x01})

		header := bindCoinbaseOnlyHeader(t, block1HeaderForTest(t), short)

		b, err := NewBlock(header, short, []*chainhash.Hash{}, 1, 123, 1, 0)
		require.NoError(t, err)

		ok, err := b.Valid(context.Background(), ulogger.TestLogger{}, nil, nil, nil, nil, nil, tSettings, nil)
		require.False(t, ok)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "a bound body with a bad coinbase length is invalid, got: %v", err)
		require.Contains(t, err.Error(), "bad coinbase length")
	})
}

// TestCorruptAsInvalid pins the conversion used by callers that keep their own invalid-body
// handling (checkpoint quick validation, legacy): the result is invalid and no longer corrupt.
func TestCorruptAsInvalid(t *testing.T) {
	require.NoError(t, CorruptAsInvalid(nil))

	converted := CorruptAsInvalid(errors.NewBlockCorruptError("merkle root does not match"))
	require.True(t, errors.Is(converted, errors.ErrBlockInvalid))
	require.False(t, errors.IsBlockCorrupt(converted))
	require.Contains(t, converted.Error(), "merkle root does not match")

	other := errors.NewProcessingError("local failure")
	require.Same(t, other, CorruptAsInvalid(other))
}
