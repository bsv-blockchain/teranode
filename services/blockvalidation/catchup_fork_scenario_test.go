package blockvalidation

import (
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

// Shared numbers for the catchup fork scenario: the local chain has its tip at
// catchupForkLocalTipHeight and forks from a stored common ancestor at catchupForkAncestorHeight
// (fork depth 10). The peer offers catchupForkHeaderCount headers on the ancestor, which out-work
// the local tip, so the offered chain passes the less-work refusal (catchup step 5.1).
const (
	catchupForkAncestorHeight = uint32(100)
	catchupForkLocalTipHeight = uint32(110)
	catchupForkHeaderCount    = 12
	catchupForkPeerURL        = "http://peer"
	catchupForkPeerID         = "catchup-fork-peer"

	// catchupForkRegtestBits is the easiest regtest difficulty, so headers mine instantly.
	catchupForkRegtestBits = "207fffff"
)

// newCatchupForkStoredHeader mines a standalone header (zero previous hash) at the regtest
// difficulty, timestamped secondsAgo in the past. merkleSeed makes headers distinct.
func newCatchupForkStoredHeader(t *testing.T, merkleSeed int, secondsAgo int64) *model.BlockHeader {
	t.Helper()

	nBits, err := model.NewNBitFromString(catchupForkRegtestBits)
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: testhelpers.GenerateMerkleRoot(merkleSeed),
		Timestamp:      uint32(time.Now().Unix() - secondsAgo),
		Bits:           *nBits,
	}
	testhelpers.MineHeader(header)

	return header
}

// newCatchupForkScenario wires a server whose local chain (tip at localTipHeight) forks from a
// stored common ancestor at ancestorHeight (fork depth = localTipHeight - ancestorHeight > 0), with
// the peer offering the ancestor followed by forkHeaders. The fork-depth and secret-mining gates
// pass. The mined_set surface is registered permissively so the clearing runs without panicking and
// AssertNotCalled can observe it. The returned target block is the last fork header.
func newCatchupForkScenario(t *testing.T, ancestorHeight, localTipHeight uint32, ancestorHeader *model.BlockHeader, forkHeaders []*model.BlockHeader) (*Server, *blockchain.Mock, *model.Block, func()) {
	const (
		coinbaseMaturity = 100
		// Large enough to keep the secret-mining gate out of the way.
		secretMiningThreshold = 1000
	)

	server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)

	mockUTXOStore.On("GetBlockHeight").Return(localTipHeight)

	server.settings.ChainCfgParams.CoinbaseMaturity = coinbaseMaturity
	server.settings.BlockValidation.SecretMiningThreshold = secretMiningThreshold

	nBits := ancestorHeader.Bits
	perBlockWork := work.CalcBlockWork(binary.LittleEndian.Uint32(nBits.CloneBytes()))
	ancestorChainWork := new(big.Int).Mul(perBlockWork, big.NewInt(int64(ancestorHeight)))
	localChainWork := new(big.Int).Mul(perBlockWork, big.NewInt(int64(localTipHeight)))

	targetBlock := &model.Block{
		Header: forkHeaders[len(forkHeaders)-1],
		Height: localTipHeight + uint32(len(forkHeaders)),
	}

	mockBlockchainClient.On("GetBlockExists", mock.Anything, ancestorHeader.Hash()).Return(true, nil).Maybe()
	mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()

	mockBlockchainClient.On("GetBlockHeader", mock.Anything, ancestorHeader.Hash()).
		Return(ancestorHeader, &model.BlockHeaderMeta{Height: ancestorHeight, ID: 1, ChainWork: ancestorChainWork.Bytes()}, nil).Maybe()
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return((*model.BlockHeader)(nil), (*model.BlockHeaderMeta)(nil), errors.NewBlockNotFoundError("block not found")).Maybe()

	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(ancestorHeader, &model.BlockHeaderMeta{Height: localTipHeight, ChainWork: localChainWork.Bytes()}, nil).Maybe()
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
	for _, forkHeader := range forkHeaders {
		responseHeaders = append(responseHeaders, forkHeader.Bytes()...)
	}
	httpmock.RegisterResponder("GET", `=~^http://peer/headers_from_common_ancestor/.*`,
		httpmock.NewBytesResponder(200, responseHeaders))

	return server, mockBlockchainClient, targetBlock, func() {
		httpmock.DeactivateAndReset()
		cleanup()
	}
}
