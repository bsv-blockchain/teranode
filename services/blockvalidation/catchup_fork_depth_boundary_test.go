package blockvalidation

// Coinbase-maturity boundary tests for validateForkDepth (issue 4592).
//
// The operator at the fork-depth gate was corrected from >= to >, and the
// reasoning travels as a comment in catchup.go: a reorg of depth d
// invalidates a coinbase at height G iff G >= H - d + 1, and that coinbase
// can only have been spent in the old chain if G + CoinbaseMaturity <= H -
// which has no solution unless d > CoinbaseMaturity. So d ==
// CoinbaseMaturity must pass and d == CoinbaseMaturity+1 must be rejected.
//
// The existing suite tests the gate from the outside at d=150 (rejected)
// and d=80 (passes) - deep inside both sides of the boundary, where >= and >
// agree. A future edit back to >= would sail through every existing test.
// These pin the two depths where the operators actually differ.

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/stretchr/testify/require"
)

func TestValidateForkDepth_CoinbaseMaturityBoundary(t *testing.T) {
	server, _, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	const maturity = 100
	server.settings.ChainCfgParams.CoinbaseMaturity = maturity

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: testhelpers.GenerateMerkleRoot(0),
		Timestamp:      uint32(time.Now().Unix() - 3600),
		Nonce:          0,
	}
	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)
	header.Bits = *nBits
	testhelpers.MineHeader(header)

	block := &model.Block{Header: header}

	cases := []struct {
		name      string
		forkDepth uint32
		wantErr   bool
		why       string
	}{
		{
			name:      "one_below_maturity_passes",
			forkDepth: maturity - 1,
			wantErr:   false,
			why:       "inside the safe side",
		},
		{
			name:      "exactly_maturity_passes",
			forkDepth: maturity,
			wantErr:   false,
			why:       "d == CoinbaseMaturity is the safe side of the proven boundary; >= would wrongly reject here",
		},
		{
			name:      "one_above_maturity_rejected",
			forkDepth: maturity + 1,
			wantErr:   true,
			why:       "the first depth at which a matured coinbase could have been spent in the reorged range",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			catchupCtx := &CatchupContext{
				blockUpTo: block,
				peerID:    "peer-boundary",
				forkDepth: tc.forkDepth,
			}

			err := server.validateForkDepth(catchupCtx)

			if tc.wantErr {
				require.Error(t, err, "depth %d must be rejected (%s)", tc.forkDepth, tc.why)
				require.Contains(t, err.Error(), "exceeds coinbase maturity")
				return
			}

			require.NoError(t, err, "depth %d must pass (%s)", tc.forkDepth, tc.why)
		})
	}
}
