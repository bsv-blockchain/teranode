package replayrecovery

import (
	"bytes"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestProofTransactionBounds(t *testing.T) {
	data := make([]byte, 0, 50)
	data = append(data, 1, 0, 0, 0, 1)
	data = append(data, make([]byte, 36)...)
	data = append(data, 255, 255, 255, 255, 255, 255, 255, 255, 127)
	_, err := ReadBoundedTransaction(bytes.NewReader(data))
	require.Error(t, err)
}

func TestProofOddLeafBranch(t *testing.T) {
	a, b, c := chainhash.DoubleHashH([]byte("a")), chainhash.DoubleHashH([]byte("b")), chainhash.DoubleHashH([]byte("c"))
	left := hashPair(a, b)
	root := hashPair(left, hashPair(c, c))
	header := make([]byte, 80)
	copy(header[36:68], root[:])
	inc := Inclusion{Header: header, Index: 2, MerkleBranch: []string{c.String(), left.String()}}
	got, err := VerifyInclusion(c.String(), inc)
	require.NoError(t, err)
	require.Equal(t, chainhash.DoubleHashH(header).String(), got)
	inc.MerkleBranch[0] = a.String()
	_, err = VerifyInclusion(c.String(), inc)
	require.Error(t, err)
}

func TestProofRejectsMalformedBranches(t *testing.T) {
	id := chainhash.DoubleHashH([]byte("tx"))
	header := make([]byte, 80)
	copy(header[36:68], id[:])
	for _, p := range []Inclusion{{Header: nil}, {Header: header, Index: 1}, {Header: header, MerkleBranch: []string{"bad"}}, {Header: header, MerkleBranch: make([]string, 33)}} {
		_, err := VerifyInclusion(id.String(), p)
		require.Error(t, err)
	}
}
