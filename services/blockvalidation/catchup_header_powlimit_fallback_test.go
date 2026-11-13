package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// A peer that answers /headers with a header declaring a target easier than the network
// limit lied about its own header; that is not a verdict on the announced block. The error
// that reaches processCatchupChItem must therefore not read as an invalid block, and the
// cached alternative peers must still be tried.
func TestProcessCatchupChItem_EasyTargetHeaderTriesAlternatives(t *testing.T) {
	prev := chainhash.HashH([]byte("prev"))
	merkle := chainhash.HashH([]byte("merkle"))
	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	easy := &model.BlockHeader{Version: 1, HashPrevBlock: &prev, HashMerkleRoot: &merkle, Timestamp: 1, Bits: *bits}

	// the chain catchup_get_block_headers.go builds for a header parse failure
	_, parseErr := catchup.ParseBlockHeaders(easy.Bytes(), &chaincfg.MainNetParams)
	require.Error(t, parseErr)
	require.True(t, errors.IsMaliciousResponseError(parseErr), "a fabricated header is still reported as the peer's malice")
	require.False(t, errors.Is(parseErr, errors.ErrBlockInvalid), "got %v", parseErr)

	chain := errors.NewNetworkPeerMaliciousError("peer sent invalid headers", parseErr)
	require.False(t, isUnvalidatablePeerError(chain), "an easy-target header must not end catch-up for the announced block")

	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)
	mockBC := &blockchain.Mock{}
	mockBC.On("ReportPeerFailure", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	u := &Server{
		settings:            tSettings,
		logger:              ulogger.TestLogger{},
		blockchainClient:    mockBC,
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		blockCatchupAttempts: ttlcache.New[chainhash.Hash, int](
			ttlcache.WithTTL[chainhash.Hash, int](10*time.Minute),
			ttlcache.WithDisableTouchOnHit[chainhash.Hash, int](),
		),
	}
	b := &model.Block{Header: &model.BlockHeader{HashPrevBlock: &prev, HashMerkleRoot: &merkle}}

	call := 0
	u.catchupFunc = func(_ context.Context, _ *model.Block, _, _ string) error {
		call++
		if call == 1 {
			return chain // the primary peer lied about its headers
		}
		return nil // the cached alternative serves the real chain
	}

	u.catchupAlternatives.Set(*b.Hash(), []processBlockCatchup{{block: b, peerID: "altpeer", baseURL: "http://alt"}}, ttlcache.DefaultTTL)
	u.processBlockNotify.Set(*b.Hash(), true, ttlcache.DefaultTTL)

	u.processCatchupChItem(context.Background(), processBlockCatchup{block: b})

	require.Equal(t, 2, call, "the primary peer, then the cached alternative")
	require.Nil(t, u.catchupAlternatives.Get(*b.Hash()), "alternatives cleared on success")
}
