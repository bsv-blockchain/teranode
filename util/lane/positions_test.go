package lane

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestCheckPosition(t *testing.T) {
	require.NoError(t, CheckPosition(Pack(0, 1)), "lane 0 is a real lane")
	require.NoError(t, CheckPosition(Pack(254, 1)))
	require.NoError(t, CheckPosition(Pack(255, MaxPosition)))
	require.Error(t, CheckPosition(None))
	require.Error(t, CheckPosition(Pack(7, 0)), "a lane with position 0 is not a position")
}

func TestTxPositionsCheck(t *testing.T) {
	a, b := chainhash.Hash{1}, chainhash.Hash{2}

	require.NoError(t, TxPositions{Own: Pack(3, 9)}.Check(nil))
	require.NoError(t, TxPositions{Own: Pack(3, 9), Parents: []uint64{Pack(0, 1), Pack(255, 2)}}.Check([]chainhash.Hash{a, b}))

	require.Error(t, TxPositions{}.Check(nil), "own position required")
	require.Error(t, TxPositions{Own: Pack(3, 9)}.Check([]chainhash.Hash{a}), "one position per distinct parent")
	require.Error(t, TxPositions{Own: Pack(3, 9), Parents: []uint64{1, 2}}.Check([]chainhash.Hash{a}))
	require.Error(t, TxPositions{Own: Pack(3, 9), Parents: []uint64{Pack(1, 0)}}.Check([]chainhash.Hash{a}), "zero parent position")
	require.Error(t, TxPositions{Own: Pack(3, 9), Parents: []uint64{Pack(1, 1), Pack(1, 2)}}.Check([]chainhash.Hash{a, a}), "duplicate parent")
}

func TestCheckDistinctParentsLargeList(t *testing.T) {
	hashes := make([]chainhash.Hash, 40)
	for i := range hashes {
		hashes[i] = chainhash.Hash{byte(i), 1}
	}

	require.NoError(t, CheckDistinctParents(hashes))

	hashes[39] = hashes[3]
	require.Error(t, CheckDistinctParents(hashes))
}

func TestReadPositions(t *testing.T) {
	// Lane 200 sets the top bit: the encoding is the packed value bit for bit.
	b := AppendPositions(nil, []uint64{Pack(200, 5), Pack(0, 1)})
	require.Equal(t, []byte{5, 0, 0, 0, 0, 0, 0, 200, 1, 0, 0, 0, 0, 0, 0, 0}, b)

	vs, rest, err := ReadPositions(append(b, 0xEE), 2)
	require.NoError(t, err)
	require.Equal(t, []uint64{Pack(200, 5), Pack(0, 1)}, vs)
	require.Equal(t, []byte{0xEE}, rest)

	_, _, err = ReadPositions(b, 3)
	require.Error(t, err, "truncated")

	_, _, err = ReadPositions(AppendPosition(nil, Pack(4, 0)), 1)
	require.Error(t, err, "invalid position")

	_, _, err = ReadPositions(b, -1)
	require.Error(t, err)
}
