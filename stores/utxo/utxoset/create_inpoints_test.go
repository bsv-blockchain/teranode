package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// Only the identity claim stores a transaction's inpoints. The block-path claim, which takes every
// create below the checkpoint, writes tx_inpoints NULL, so a create there plans none and the
// store does not build them.
func TestPlanCreatesBuildsInpointsOnlyForTheIdentityClaim(t *testing.T) {
	s, _ := newTestStore(t)

	const height = 870

	tx := mkTx(t, 1, 5_000)

	mined := &utxo.CreateOptions{}
	for _, o := range belowCheckpointOptions(height) {
		o(mined)
	}

	unmined := &utxo.CreateOptions{}

	p := s.planCreates([]*createItem{
		{tx: tx, blockHeight: height, options: mined},
		{tx: mkTx(t, 1, 6_000), blockHeight: height, options: unmined},
	})

	for k := range p.owner {
		require.NoError(t, p.errs[p.owner[k]])

		if p.minedRows[k] {
			require.Nil(t, p.inpoints[k], "a block-path create writes no inpoints")
		} else {
			require.NotNil(t, p.inpoints[k], "an identity create stores its inpoints")
		}
	}

	require.ElementsMatch(t, []bool{true, false}, p.minedRows)
}
