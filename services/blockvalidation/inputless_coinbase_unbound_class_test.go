//go:build bvgaps

package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// requireUnboundInputlessCoinbaseClassedCorrupt asserts the class a present but inputless coinbase
// must get when the body is not bound to its header: not INCOMPLETE (BV-BODY-005 reserves that for
// an absent coinbase), and CORRUPT rather than INVALID, because an unbound body is re-fetched and
// never condemned.
func requireUnboundInputlessCoinbaseClassedCorrupt(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err)
	require.False(t, errors.Is(err, errors.ErrBlockIncomplete),
		"BV-BODY-005: ERR_BLOCK_INCOMPLETE is for an absent coinbase; a present coinbase with no inputs is malformed, got: %v", err)
	require.True(t, errors.IsBlockCorrupt(err), "an unbound malformed body must be ERR_BLOCK_CORRUPT, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "an unbound body must never be condemned invalid")
}

// TestKnownDefect_InputlessCoinbaseOnUnboundBody_IsCorruptNotIncomplete checks how a present
// coinbase with no inputs is classified when the body is not bound to the header.
//
// Spec: BV-BODY-005 and BV-BODY-006 (tracked as GAP-BV-046).
//
// Scenario: Given a block whose coinbase is present but has nil or empty inputs, and whose header
// merkle root does not commit to that coinbase (so the body is unbound). When full validation or
// quick validation runs it, Then the block is rejected as ERR_BLOCK_CORRUPT, not
// ERR_BLOCK_INCOMPLETE. INCOMPLETE is reserved for an absent coinbase.
//
// Today both paths return ERR_BLOCK_INCOMPLETE ("coinbase tx is nil or empty"), testing
// len(Inputs) == 0 together with the nil check. These cases used to be asserted as INCOMPLETE in
// TestBlockValidation_IncompleteBlockNilCoinbase and TestQuickValidateBlock_IncompleteBlockNilCoinbase,
// which now keep only the absent-coinbase case. The bound case (which must be INVALID) is proven
// on the docs/blockvalidation-spec branch by TestKnownDefect_InputlessCoinbase_IsInvalidNotIncomplete.
func TestKnownDefect_InputlessCoinbaseOnUnboundBody_IsCorruptNotIncomplete(t *testing.T) {
	inputlessCoinbases := []struct {
		name     string
		coinbase *bt.Tx
	}{
		{"nil inputs", &bt.Tx{Inputs: nil}},
		{"empty inputs", &bt.Tx{Inputs: []*bt.Input{}}},
	}

	for _, inputless := range inputlessCoinbases {
		t.Run("full validation, coinbase with "+inputless.name, func(t *testing.T) {
			initPrometheusMetrics()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			utxoStore, subtreeValidationClient, _, txStore, subtreeStore, cleanup := setup(t)
			defer cleanup()

			tSettings := test.CreateBaseTestSettings(t)

			blockchainStoreURL, err := url.Parse("sqlitememory://")
			require.NoError(t, err)

			blockchainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, blockchainStoreURL, tSettings)
			require.NoError(t, err)

			blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockchainStore, nil, nil)
			require.NoError(t, err)

			blockValidator := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

			nBits, err := model.NewNBitFromString("207fffff")
			require.NoError(t, err)

			// The all-zero merkle root commits to nothing, so the body is unbound. The header is
			// mined, so proof of work is not what rejects the block.
			header := &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  chaincfg.RegressionNetParams.GenesisHash,
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
				Bits:           *nBits,
			}

			for {
				if met, _, _ := header.HasMetTargetDifficulty(); met {
					break
				}

				header.Nonce++
			}

			block := &model.Block{
				Header:           header,
				SizeInBytes:      1000,
				TransactionCount: 1,
				CoinbaseTx:       inputless.coinbase,
				Subtrees:         []*chainhash.Hash{},
			}

			err = blockValidator.ValidateBlock(ctx, block, "test")

			requireUnboundInputlessCoinbaseClassedCorrupt(t, err)
		})

		t.Run("quick validation, coinbase with "+inputless.name, func(t *testing.T) {
			suite := NewCatchupTestSuite(t)
			defer suite.Cleanup()

			// Replacing the coinbase after the header was built leaves the header committing to the
			// old one, so the body is unbound.
			block := testhelpers.CreateTestBlocks(t, 1)[0]
			block.CoinbaseTx = inputless.coinbase

			err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test-peer", "")

			requireUnboundInputlessCoinbaseClassedCorrupt(t, err)
		})
	}
}
