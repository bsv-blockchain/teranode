package model

import (
	"bytes"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

func readerTestTx(t *testing.T, seed byte) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	prev := chainhash.Hash{seed, 7}
	in := &bt.Input{PreviousTxOutIndex: uint32(seed), SequenceNumber: 0xffffffff}
	require.NoError(t, in.PreviousTxIDAdd(&prev))
	in.UnlockingScript = bscript.NewFromBytes([]byte{0x51, seed})
	in.PreviousTxSatoshis = 900
	in.PreviousTxScript = bscript.NewFromBytes([]byte{0x76, seed})
	tx.Inputs = append(tx.Inputs, in)
	tx.Outputs = append(tx.Outputs,
		&bt.Output{Satoshis: 800, LockingScript: bscript.NewFromBytes([]byte{0x76, 0xa9, seed})},
		&bt.Output{LockingScript: bscript.NewFromBytes(append([]byte{0x00, 0x6a}, bytes.Repeat([]byte{seed}, 500)...))},
	)

	return tx
}

// subtreeWith builds a subtree with the coinbase placeholder then txs, and the data file for it,
// writing every second transaction in extended form.
func subtreeWith(t *testing.T, txs []*bt.Tx) (*subtreepkg.Subtree, []byte) {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(8)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	var file bytes.Buffer

	for i, tx := range txs {
		require.NoError(t, st.AddNode(*tx.TxIDChainHash(), 1, uint64(tx.Size())))

		if i%2 == 1 {
			file.Write(tx.ExtendedBytes())
		} else {
			file.Write(tx.Bytes())
		}
	}

	return st, file.Bytes()
}

// The reader returns what go-subtree's does for the same file, in both forms.
func TestReadSubtreeDataMatchesGoSubtree(t *testing.T) {
	txs := []*bt.Tx{readerTestTx(t, 1), readerTestTx(t, 2), readerTestTx(t, 3), readerTestTx(t, 4)}
	st, file := subtreeWith(t, txs)

	want, err := subtreepkg.NewSubtreeDataFromReader(st, bytes.NewReader(file))
	require.NoError(t, err)

	got, err := ReadSubtreeData(st, bytes.NewReader(file))
	require.NoError(t, err)

	require.Len(t, got.Txs, len(want.Txs))
	require.Nil(t, got.Txs[0])

	for i := 1; i < len(want.Txs); i++ {
		if want.Txs[i] == nil {
			require.Nil(t, got.Txs[i], i)

			continue
		}

		require.Equal(t, want.Txs[i].ExtendedBytes(), got.Txs[i].ExtendedBytes(), i)
		require.Equal(t, want.Txs[i].IsExtended(), got.Txs[i].IsExtended(), i)
		require.Equal(t, *want.Txs[i].TxIDChainHash(), *got.Txs[i].TxIDChainHash(), i)
	}
}

// A transaction whose id is not the one its node records is refused.
func TestReadSubtreeDataRefusesAMismatchedTransaction(t *testing.T) {
	st, _ := subtreeWith(t, []*bt.Tx{readerTestTx(t, 1)})

	_, err := ReadSubtreeData(st, bytes.NewReader(readerTestTx(t, 9).Bytes()))
	require.ErrorIs(t, err, subtreepkg.ErrTxHashMismatch)
}

// More transactions than the subtree has nodes is refused.
func TestReadSubtreeDataRefusesExtraTransactions(t *testing.T) {
	txs := []*bt.Tx{readerTestTx(t, 1)}
	st, file := subtreeWith(t, txs)

	file = append(file, readerTestTx(t, 2).Bytes()...)

	_, err := ReadSubtreeData(st, bytes.NewReader(file))
	require.ErrorIs(t, err, subtreepkg.ErrTxIndexOutOfBounds)
}

// A file cut inside a transaction is an error, not a short but clean read.
func TestReadSubtreeDataRefusesATruncatedFile(t *testing.T) {
	st, file := subtreeWith(t, []*bt.Tx{readerTestTx(t, 1), readerTestTx(t, 2)})

	_, err := ReadSubtreeData(st, bytes.NewReader(file[:len(file)-2]))
	require.Error(t, err)
}
