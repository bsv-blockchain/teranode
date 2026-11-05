package blockvalidation

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// validateBatchHeaders must read the network's proof-of-work limit from the server's
// own settings: a header that declares an easier target than the configured network
// allows is refused even though its hash meets the target it chose.
func TestValidateBatchHeaders_NetworkPowLimit(t *testing.T) {
	server, _, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{1, 2, 3},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *bits,
	}
	target := bits.CalculateTarget()

	for ; ; header.Nonce++ {
		if hashNum := bt.ReverseBytes(header.Hash().CloneBytes()); new(big.Int).SetBytes(hashNum).Cmp(target) <= 0 {
			break
		}
	}

	headers := []*model.BlockHeader{header}

	mainnet := chaincfg.MainNetParams
	server.settings.ChainCfgParams = &mainnet
	require.Error(t, server.validateBatchHeaders(context.Background(), headers),
		"mainnet limit must refuse an easier declared target")

	regtest := chaincfg.RegressionNetParams
	server.settings.ChainCfgParams = &regtest
	require.NoError(t, server.validateBatchHeaders(context.Background(), headers))
}
