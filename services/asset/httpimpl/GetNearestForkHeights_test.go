package httpimpl

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// buildForkChain builds block headers and metas for a chain with forks at the
// specified heights. All fork blocks share the same parent as the main chain
// block at that height (a true fork).
func buildForkChain(startHeight, count uint32, forkHeights map[uint32]bool) ([]*model.BlockHeader, []*model.BlockHeaderMeta) {
	var headers []*model.BlockHeader
	var metas []*model.BlockHeaderMeta

	prevHash := &chainhash.Hash{}

	for h := startHeight; h < startHeight+count; h++ {
		hdr := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1000 + h,
			Nonce:          h,
		}
		meta := &model.BlockHeaderMeta{
			Height: h,
		}

		headers = append(headers, hdr)
		metas = append(metas, meta)

		if forkHeights[h] {
			forkHdr := &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  prevHash,
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      2000 + h,
				Nonce:          h + 10000,
			}
			forkMeta := &model.BlockHeaderMeta{
				Height: h,
			}

			headers = append(headers, forkHdr)
			metas = append(metas, forkMeta)
		}

		prevHash = hdr.Hash()
	}

	return headers, metas
}

func TestGetNearestForkHeights(t *testing.T) {
	initPrometheusMetrics()

	// Block at height 10 on a chain from 0..20, forks at 5, 12, 18
	rootBlockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      500000,
		Nonce:          99,
	}
	rootMeta := &model.BlockHeaderMeta{
		Height: 10,
	}

	forkHeights := map[uint32]bool{5: true, 12: true, 18: true}

	t.Run("finds prev and next fork", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		chainHeaders, chainMetas := buildForkChain(0, 21, forkHeights)

		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(rootBlockHeader, rootMeta, nil).Once()
		mockRepo.On("GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything).Return(chainHeaders, chainMetas, nil).Once()

		echoContext.SetPath("/block/:hash/nearestforks")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(rootBlockHeader.String())

		err := httpServer.GetNearestForkHeights(echoContext)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		var resp nearestForksResponse
		require.NoError(t, json.Unmarshal(responseRecorder.Body.Bytes(), &resp))

		assert.Equal(t, uint32(10), resp.CurrentHeight)
		require.NotNil(t, resp.PrevFork, "expected prev fork")
		assert.Equal(t, uint32(5), resp.PrevFork.Height)
		require.NotNil(t, resp.NextFork, "expected next fork")
		assert.Equal(t, uint32(12), resp.NextFork.Height)
	})

	t.Run("no forks in range", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		chainHeaders, chainMetas := buildForkChain(0, 21, map[uint32]bool{})

		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(rootBlockHeader, rootMeta, nil).Once()
		mockRepo.On("GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything).Return(chainHeaders, chainMetas, nil).Once()

		echoContext.SetPath("/block/:hash/nearestforks")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(rootBlockHeader.String())

		err := httpServer.GetNearestForkHeights(echoContext)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		var resp nearestForksResponse
		require.NoError(t, json.Unmarshal(responseRecorder.Body.Bytes(), &resp))

		assert.Nil(t, resp.PrevFork)
		assert.Nil(t, resp.NextFork)
	})

	t.Run("visible fork skipped for next", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Block at height 7, forks at 8 and 15. The fork at 8 has the current
		// block as parent so it is the visible fork. Next should skip it.
		parentHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      700000,
			Nonce:          7,
		}
		parentMeta := &model.BlockHeaderMeta{Height: 7}

		// Build a chain where fork at height 8 has parentHeader.Hash() as prev
		var headers []*model.BlockHeader
		var metas []*model.BlockHeaderMeta

		prevHash := &chainhash.Hash{}

		for h := uint32(0); h <= 20; h++ {
			var hdr *model.BlockHeader
			if h == 8 {
				// Main chain block at 8 uses parentHeader hash as prev
				hdr = &model.BlockHeader{
					Version:        1,
					HashPrevBlock:  parentHeader.Hash(),
					HashMerkleRoot: &chainhash.Hash{},
					Timestamp:      1000 + h,
					Nonce:          h,
				}
			} else {
				hdr = &model.BlockHeader{
					Version:        1,
					HashPrevBlock:  prevHash,
					HashMerkleRoot: &chainhash.Hash{},
					Timestamp:      1000 + h,
					Nonce:          h,
				}
			}
			meta := &model.BlockHeaderMeta{Height: h}

			headers = append(headers, hdr)
			metas = append(metas, meta)

			if h == 8 {
				// Fork block also pointing to parentHeader
				forkHdr := &model.BlockHeader{
					Version:        1,
					HashPrevBlock:  parentHeader.Hash(),
					HashMerkleRoot: &chainhash.Hash{},
					Timestamp:      2000 + h,
					Nonce:          h + 10000,
				}

				headers = append(headers, forkHdr)
				metas = append(metas, &model.BlockHeaderMeta{Height: h})
			}

			if h == 15 {
				forkHdr := &model.BlockHeader{
					Version:        1,
					HashPrevBlock:  prevHash,
					HashMerkleRoot: &chainhash.Hash{},
					Timestamp:      2000 + h,
					Nonce:          h + 10000,
				}

				headers = append(headers, forkHdr)
				metas = append(metas, &model.BlockHeaderMeta{Height: h})
			}

			prevHash = hdr.Hash()
		}

		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(parentHeader, parentMeta, nil).Once()
		mockRepo.On("GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything).Return(headers, metas, nil).Once()

		echoContext.SetPath("/block/:hash/nearestforks")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(parentHeader.String())

		err := httpServer.GetNearestForkHeights(echoContext)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		var resp nearestForksResponse
		require.NoError(t, json.Unmarshal(responseRecorder.Body.Bytes(), &resp))

		assert.Equal(t, uint32(7), resp.CurrentHeight)
		// Next should skip the visible fork at 8 and find 15
		require.NotNil(t, resp.NextFork)
		assert.Equal(t, uint32(15), resp.NextFork.Height)
	})

	t.Run("block at height 0", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		genesisHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      100000,
			Nonce:          0,
		}
		genesisMeta := &model.BlockHeaderMeta{Height: 0}

		chainHeaders, chainMetas := buildForkChain(0, 10, map[uint32]bool{3: true})

		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(genesisHeader, genesisMeta, nil).Once()
		mockRepo.On("GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything).Return(chainHeaders, chainMetas, nil).Once()

		echoContext.SetPath("/block/:hash/nearestforks")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(genesisHeader.String())

		err := httpServer.GetNearestForkHeights(echoContext)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		var resp nearestForksResponse
		require.NoError(t, json.Unmarshal(responseRecorder.Body.Bytes(), &resp))

		assert.Equal(t, uint32(0), resp.CurrentHeight)
		assert.Nil(t, resp.PrevFork)
		require.NotNil(t, resp.NextFork)
		assert.Equal(t, uint32(3), resp.NextFork.Height)
	})

	t.Run("invalid hash", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		echoContext.SetPath("/block/:hash/nearestforks")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("short")

		err := httpServer.GetNearestForkHeights(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
	})

	t.Run("invalid range param", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		echoContext.SetPath("/block/:hash/nearestforks")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(rootBlockHeader.String())
		echoContext.QueryParams().Set("range", "abc")

		err := httpServer.GetNearestForkHeights(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
	})

	t.Run("repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.NewStorageError("db error"))

		echoContext.SetPath("/block/:hash/nearestforks")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		err := httpServer.GetNearestForkHeights(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)
	})
}
