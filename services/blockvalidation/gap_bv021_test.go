//go:build bvgaps

package blockvalidation

import (
	"context"
	"encoding/binary"
	"math/big"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/work"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// gap021Scenario wires a server whose local chain (tip at currentHeight) forks from a stored
// common ancestor at ancestorHeight (fork depth = currentHeight - ancestorHeight > 0), with the
// peer offering the ancestor followed by forkHeaders. The fork-depth and secret-mining gates
// pass. The mined_set surface is registered permissively so the premature clear runs without
// panicking and AssertNotCalled can observe it.
func gap021Scenario(t *testing.T, ancestorHeight, currentHeight uint32, ancestorHeader *model.BlockHeader, forkHeaders []*model.BlockHeader) (*Server, *blockchain.Mock, *model.Block, func()) {
	server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)

	mockUTXOStore.On("GetBlockHeight").Return(currentHeight)

	server.settings.ChainCfgParams.CoinbaseMaturity = 100
	server.settings.BlockValidation.SecretMiningThreshold = 1000 // keep the secret-mining gate out of the way

	nBits := ancestorHeader.Bits
	perBlockWork := work.CalcBlockWork(binary.LittleEndian.Uint32(nBits.CloneBytes()))
	ancestorChainWork := new(big.Int).Mul(perBlockWork, big.NewInt(int64(ancestorHeight)))
	localChainWork := new(big.Int).Mul(perBlockWork, big.NewInt(int64(currentHeight)))

	targetBlock := &model.Block{
		Header: forkHeaders[len(forkHeaders)-1],
		Height: currentHeight + uint32(len(forkHeaders)),
	}

	mockBlockchainClient.On("GetBlockExists", mock.Anything, ancestorHeader.Hash()).Return(true, nil).Maybe()
	mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()

	mockBlockchainClient.On("GetBlockHeader", mock.Anything, ancestorHeader.Hash()).
		Return(ancestorHeader, &model.BlockHeaderMeta{Height: ancestorHeight, ID: 1, ChainWork: ancestorChainWork.Bytes()}, nil).Maybe()
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return((*model.BlockHeader)(nil), (*model.BlockHeaderMeta)(nil), errors.NewBlockNotFoundError("block not found")).Maybe()

	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(ancestorHeader, &model.BlockHeaderMeta{Height: currentHeight, ChainWork: localChainWork.Bytes()}, nil).Maybe()
	mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{ancestorHeader.Hash()}, nil).Maybe()
	mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{ancestorHeader}, []*model.BlockHeaderMeta{{Height: ancestorHeight, ID: 1}}, nil).Maybe()
	currentState := blockchain.FSMStateRUNNING
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&currentState, nil).Maybe()

	mockBlockchainClient.On("GetBlockHeadersByHeight", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{ancestorHeader}, []*model.BlockHeaderMeta{{Height: ancestorHeight}}, nil).Maybe()
	mockBlockchainClient.On("ClearBlockMinedSet", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockBlockchainClient.On("SendNotification", mock.Anything, mock.Anything).Return(nil).Maybe()

	httpmock.ActivateNonDefault(util.HTTPClient())

	var responseHeaders []byte
	responseHeaders = append(responseHeaders, ancestorHeader.Bytes()...)
	for _, h := range forkHeaders {
		responseHeaders = append(responseHeaders, h.Bytes()...)
	}
	httpmock.RegisterResponder("GET", `=~^http://peer/headers_from_common_ancestor/.*`,
		httpmock.NewBytesResponder(200, responseHeaders))

	return server, mockBlockchainClient, targetBlock, func() {
		httpmock.DeactivateAndReset()
		cleanup()
	}
}

// TestGapBV021_NoMinedSetClearBeforeHeaderChainChecks asserts BV-CATCHUP-013 / AC-BV-CATCHUP-013.1:
// fork-side mined_set clearing (ClearBlockMinedSet + BlockMinedUnset notification) MUST NOT happen
// until every header-chain check (BV-CATCHUP-007..012, 014) has passed.
//
// The peer offers a fork of depth 10 (local tip 110, ancestor 100) whose headers pass the
// fork-depth and secret-mining gates but contain a broken previous-hash link inside the new
// headers. That is rejected by buildHeaderCache (HeaderChainCache.BuildFromHeaders -> validateChain),
// which runs AFTER step 5.5. Today catchup() has already un-stamped the local blocks above the
// ancestor, so ClearBlockMinedSet / SendNotification are called and this test fails.
//
// Code: services/blockvalidation/catchup.go catchup() step 5.5 (~287-325) vs buildHeaderCache (~1129);
// catchup/header_chain_cache.go validateChain.
func TestGapBV021_NoMinedSetClearBeforeHeaderChainChecks(t *testing.T) {
	initPrometheusMetrics()

	ancestorHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: testhelpers.GenerateMerkleRoot(0),
		Timestamp:      uint32(time.Now().Unix() - 3600),
	}
	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)
	ancestorHeader.Bits = *nBits
	testhelpers.MineHeader(ancestorHeader)

	forkHeaders := testhelpers.CreateSyntheticChainFrom(ancestorHeader, 5)

	// Break the link inside the new headers: header 3 no longer points at header 2.
	bogusPrev := chainhash.HashH([]byte("gap021-bogus-parent"))
	forkHeaders[3].HashPrevBlock = &bogusPrev
	testhelpers.MineHeader(forkHeaders[3])

	server, mockBlockchainClient, targetBlock, cleanup := gap021Scenario(t, 100, 110, ancestorHeader, forkHeaders)
	defer cleanup()

	err = server.catchup(context.Background(), targetBlock, "peer-gap021", "http://peer")

	// Sanity: the catchup is rejected for the header-chain defect, not for a setup/earlier-gate reason,
	// and it got past step 5.5 (the fork block lookup used by the clearing was reached).
	require.Error(t, err)
	require.Contains(t, err.Error(), "chain broken", "setup must fail on the broken header link (a later check)")
	mockBlockchainClient.AssertCalled(t, "GetBlockHeadersByHeight", mock.Anything, mock.Anything, mock.Anything)

	// Spec assertion: no local block was un-stamped for a fork that is rejected by a header check.
	mockBlockchainClient.AssertNotCalled(t, "ClearBlockMinedSet", mock.Anything, mock.Anything)
	mockBlockchainClient.AssertNotCalled(t, "SendNotification", mock.Anything, mock.Anything)
}
