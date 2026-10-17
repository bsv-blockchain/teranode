package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestTier2BuildsAnUnminedRecordWithItsMarkerAndBody is icellan's review point. In the create
// race the identity read finds no row, the parent's unmined create then commits, and the UTXO
// step sends the parent to the second tier, which builds the record. That record must say the
// parent is unmined and carry its body, as the identity read does. Without the marker it read
// as mined in no block, and block assembly dropped the child as corrupt.
//
// The race is between statements, so the test runs the second tier alone on a fresh result: the
// state step 1 left behind when it ran before the create.
func TestTier2BuildsAnUnminedRecordWithItsMarkerAndBody(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	h := *parent.TxIDChainHash()
	res := newLookupResult(1)

	require.NoError(t, s.readTier2(ctx, []chainhash.Hash{h}, &res))

	got, ok := res.found[h]
	require.True(t, ok, "the second tier answers for a transaction with an identity row")
	require.Empty(t, got.BlockIDs)
	require.NotZero(t, got.UnminedSince, "an unmined parent came back as mined in no block")
	require.NotNil(t, got.Tx, "the body is in its partition, so the record carries it")
	require.Equal(t, h.String(), got.Tx.TxIDChainHash().String())
}
