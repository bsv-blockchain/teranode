package httpimpl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type statusOutpoint struct {
	TxID chainhash.Hash
	Vout uint32
}

func statusBody(ops ...statusOutpoint) *bytes.Reader {
	recs := make([]struct {
		TxID chainhash.Hash
		Vout uint32
	}, len(ops))
	for i, o := range ops {
		recs[i].TxID, recs[i].Vout = o.TxID, o.Vout
	}

	return bytes.NewReader(buildUTXOsRequest(recs))
}

// mockStatusHTTP is GetMockHTTP with chain parameters, which the handler reads.
func mockStatusHTTP(t testing.TB, body io.Reader) (*HTTP, *repository.Mock, echo.Context, *httptest.ResponseRecorder) {
	t.Helper()

	h, repo, ec, rec := GetMockHTTP(t, body)
	params := chaincfg.MainNetParams
	h.settings.ChainCfgParams = &params

	return h, repo, ec, rec
}

func statusHash(b byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = b

	return h
}

// fundingTx returns a transaction whose output 0 is worth 1234 sat.
func fundingTx(t *testing.T) (*bt.Tx, string) {
	t.Helper()

	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)

	tx := bt.NewTx()
	tx.AddOutput(&bt.Output{Satoshis: 1234, LockingScript: script})

	sum := sha256.Sum256(*script)

	return tx, hex.EncodeToString(sum[:])
}

func bestBlockMocks(mockRepo *repository.Mock, tip uint32) {
	mockRepo.On("GetBestBlockHeader").Return(testBlockHeader, &model.BlockHeaderMeta{Height: tip}, nil)
}

func callStatus(t *testing.T, httpServer *HTTP, ec echo.Context, rec *httptest.ResponseRecorder) UTXOStatusResponse {
	t.Helper()

	ec.Request().Method = http.MethodPost
	require.NoError(t, httpServer.GetUTXOStatuses()(ec))

	var out UTXOStatusResponse

	res := rec.Result()
	defer res.Body.Close()

	require.Equal(t, http.StatusOK, res.StatusCode)
	require.NoError(t, json.NewDecoder(res.Body).Decode(&out))

	return out
}

func TestGetUTXOStatuses(t *testing.T) {
	initPrometheusMetrics()

	fundID := statusHash(1)
	spenderID := statusHash(2)
	tx, wantScriptHash := fundingTx(t)

	blockWithHeader := &model.Block{ID: 7, Header: testBlockHeader}

	t.Run("unspent and mined, with value and best block", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, BlockIDs: []uint32{7}, BlockHeights: []uint32{850}}, nil)

		bc := &blockchain.Mock{}
		bc.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{7}).Return(true, nil)
		mockRepo.On("GetBlockchainClient").Return(bc)
		mockRepo.On("GetBlockByHeight", uint32(850)).Return(blockWithHeader, nil)

		out := callStatus(t, httpServer, ec, rec)

		require.Equal(t, testBlockHeader.Hash().String(), out.BestBlock.Hash)
		require.Equal(t, uint32(900), out.BestBlock.Height)
		require.False(t, out.BestBlockChanged)
		require.Len(t, out.Results, 1)

		r := out.Results[0]
		require.Equal(t, utxoStatusUnspent, r.Status)
		require.Empty(t, r.Reason)
		require.NotNil(t, r.Satoshis)
		require.Equal(t, uint64(1234), *r.Satoshis)
		require.Equal(t, wantScriptHash, r.LockingScriptSha256)
		require.NotNil(t, r.Funding)
		require.True(t, r.Funding.Mined)
		require.False(t, r.Funding.Unconfirmed)
		require.Equal(t, uint32(850), r.Funding.BlockHeight)
		require.Equal(t, testBlockHeader.Hash().String(), r.Funding.BlockHash)
		require.Nil(t, r.Spender)
	})

	t.Run("spent reports the spender and its height", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{
			Status:       int(utxo.Status_SPENT),
			SpendingData: spendpkg.NewSpendingData(&spenderID, 3),
		}, nil)
		mockRepo.On("GetTxFundingRecord", &fundID).Return(&meta.Data{Tx: tx, BlockIDs: []uint32{7}, BlockHeights: []uint32{850}}, nil)
		mockRepo.On("GetTxBlockRefs", &spenderID).Return(&meta.Data{BlockIDs: []uint32{8}, BlockHeights: []uint32{870}}, nil)

		bc := &blockchain.Mock{}
		bc.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil)
		mockRepo.On("GetBlockchainClient").Return(bc)
		mockRepo.On("GetBlockByHeight", uint32(850)).Return(blockWithHeader, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusSpent, r.Status)
		require.NotNil(t, r.Spender)
		require.Equal(t, spenderID.String(), r.Spender.TxID)
		require.Equal(t, 3, r.Spender.Vin)
		require.True(t, r.Spender.Mined)
		require.Equal(t, uint32(870), r.Spender.BlockHeight)
		// the spender path reads only the block references, never the full record (no transaction body)
		mockRepo.AssertCalled(t, "GetTxBlockRefs", &spenderID)
		mockRepo.AssertNotCalled(t, "GetTxFundingRecord", &spenderID)
	})

	t.Run("served records count on the route's own counter, not on POST /api/v1/utxos", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}, statusOutpoint{fundID, 1}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, BlockIDs: []uint32{7}, BlockHeights: []uint32{850}}, nil)

		bc := &blockchain.Mock{}
		bc.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil)
		mockRepo.On("GetBlockchainClient").Return(bc)
		mockRepo.On("GetBlockByHeight", uint32(850)).Return(blockWithHeader, nil)

		statusesBefore := testutil.ToFloat64(prometheusAssetHTTPGetUTXOStatuses.WithLabelValues("OK", "200"))
		utxosBefore := testutil.ToFloat64(prometheusAssetHTTPGetUTXOs.WithLabelValues("OK", "200"))

		out := callStatus(t, httpServer, ec, rec)
		require.Len(t, out.Results, 2)

		require.InDelta(t, statusesBefore+2, testutil.ToFloat64(prometheusAssetHTTPGetUTXOStatuses.WithLabelValues("OK", "200")), 0)
		require.InDelta(t, utxosBefore, testutil.ToFloat64(prometheusAssetHTTPGetUTXOs.WithLabelValues("OK", "200")), 0)
	})

	t.Run("unconfirmed funding transaction is flagged", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 899}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnspent, r.Status)
		require.NotNil(t, r.Funding)
		require.False(t, r.Funding.Mined)
		require.True(t, r.Funding.Unconfirmed)
		require.Empty(t, r.Funding.BlockHash)
	})

	t.Run("reorged out funding block is not reported as mined", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, BlockIDs: []uint32{7}, BlockHeights: []uint32{850}}, nil)

		bc := &blockchain.Mock{}
		bc.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{7}).Return(false, nil)
		mockRepo.On("GetBlockchainClient").Return(bc)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.NotNil(t, r.Funding)
		require.False(t, r.Funding.Mined)
		require.True(t, r.Funding.Unconfirmed)
		require.Zero(t, r.Funding.BlockHeight)
	})

	// Aerospike-style: no spend-height hold is reported by the store itself.
	t.Run("coinbase output inside maturity is immature even when the store says OK", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, IsCoinbase: true, BlockIDs: []uint32{7}, BlockHeights: []uint32{850}}, nil)

		bc := &blockchain.Mock{}
		bc.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil)
		mockRepo.On("GetBlockchainClient").Return(bc)
		mockRepo.On("GetBlockByHeight", uint32(850)).Return(blockWithHeader, nil)

		httpServer.settings.ChainCfgParams.CoinbaseMaturity = 100

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnspent, r.Status)
		require.True(t, r.Immature)
	})

	t.Run("coinbase output past maturity is not immature", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, IsCoinbase: true, BlockIDs: []uint32{7}, BlockHeights: []uint32{700}}, nil)

		bc := &blockchain.Mock{}
		bc.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil)
		mockRepo.On("GetBlockchainClient").Return(bc)
		mockRepo.On("GetBlockByHeight", uint32(700)).Return(blockWithHeader, nil)

		httpServer.settings.ChainCfgParams.CoinbaseMaturity = 100

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnspent, r.Status)
		require.False(t, r.Immature)
	})

	// Funding height 850 with maturity 100: the output is immature up to best height 949 and mature from 950,
	// the same boundary the SQL store's GetSpend applies (coinbase spending height > store height).
	for _, tc := range []struct {
		bestHeight   uint32
		wantImmature bool
	}{
		{bestHeight: 949, wantImmature: true},
		{bestHeight: 950, wantImmature: false},
	} {
		t.Run(fmt.Sprintf("coinbase maturity boundary at best height %d", tc.bestHeight), func(t *testing.T) {
			httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

			bestBlockMocks(mockRepo, tc.bestHeight)
			mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
			mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, IsCoinbase: true, BlockIDs: []uint32{7}, BlockHeights: []uint32{850}}, nil)

			bc := &blockchain.Mock{}
			bc.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil)
			mockRepo.On("GetBlockchainClient").Return(bc)
			mockRepo.On("GetBlockByHeight", uint32(850)).Return(blockWithHeader, nil)

			httpServer.settings.ChainCfgParams.CoinbaseMaturity = 100

			r := callStatus(t, httpServer, ec, rec).Results[0]

			require.Equal(t, utxoStatusUnspent, r.Status)
			require.Equal(t, tc.wantImmature, r.Immature)
		})
	}

	// A coinbase output whose funding block cannot be placed on the main chain has no maturity answer,
	// so it is never reported as unspent.
	for _, tc := range []struct {
		name    string
		onChain bool
		chkErr  error
		blockID uint32
	}{
		{name: "the main-chain check fails", chkErr: errors.NewServiceUnavailableError("down"), blockID: 7},
		{name: "the block at that height has another ID", onChain: true, blockID: 99},
		{name: "the funding block was reorged out", onChain: false, blockID: 7},
	} {
		t.Run("coinbase maturity is unknown when "+tc.name, func(t *testing.T) {
			httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

			bestBlockMocks(mockRepo, 900)
			mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
			mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, IsCoinbase: true, BlockIDs: []uint32{7}, BlockHeights: []uint32{850}}, nil)

			bc := &blockchain.Mock{}
			bc.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(tc.onChain, tc.chkErr)
			mockRepo.On("GetBlockchainClient").Return(bc)
			mockRepo.On("GetBlockByHeight", uint32(850)).Return(&model.Block{ID: tc.blockID, Header: testBlockHeader}, nil)

			httpServer.settings.ChainCfgParams.CoinbaseMaturity = 100

			r := callStatus(t, httpServer, ec, rec).Results[0]

			require.Equal(t, utxoStatusUnknown, r.Status)
			require.Equal(t, utxoReasonMaturity, r.Reason)
			require.False(t, r.Immature)
		})
	}

	t.Run("store IMMATURE status is reported as immature", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_IMMATURE)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 899}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnspent, r.Status)
		require.True(t, r.Immature)
	})

	// A spend-height hold replaces the output's own state on some stores: Aerospike sets IMMATURE over
	// SPENT, SQL and pebble set it after frozen, locked and conflicting.
	t.Run("a hold over a spent output is unknown, not unspent", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{
			Status:       int(utxo.Status_IMMATURE),
			SpendingData: spendpkg.NewSpendingData(&spenderID, 3),
		}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 899}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonHeld, r.Reason)
		require.Nil(t, r.Spender)
	})

	t.Run("a hold over a frozen output (the frozen marker as spending data) is unknown", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{
			Status:       int(utxo.Status_IMMATURE),
			SpendingData: spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, 0),
		}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 899}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonHeld, r.Reason)
	})

	t.Run("a hold over a conflicting or locked transaction is unknown with that reason", func(t *testing.T) {
		for _, tc := range []struct {
			data   *meta.Data
			reason string
		}{
			{data: &meta.Data{Tx: tx, UnminedSince: 899, Conflicting: true}, reason: utxoReasonConflict},
			{data: &meta.Data{Tx: tx, UnminedSince: 899, Locked: true}, reason: utxoReasonLocked},
		} {
			httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

			bestBlockMocks(mockRepo, 900)
			mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_IMMATURE)}, nil)
			mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(tc.data, nil)

			r := callStatus(t, httpServer, ec, rec).Results[0]

			require.Equal(t, utxoStatusUnknown, r.Status)
			require.Equal(t, tc.reason, r.Reason)
		}
	})

	t.Run("OP_FALSE OP_RETURN output is unknown even when the store says OK", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		script := bscript.NewFromBytes([]byte{bscript.OpFALSE, bscript.OpRETURN, 0x01, 0xaa})
		dataTx := bt.NewTx()
		dataTx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: script})

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: dataTx, UnminedSince: 899}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonUnspend, r.Reason)
	})

	t.Run("a failed main-chain check leaves funding unknown, not unconfirmed", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, BlockIDs: []uint32{7}, BlockHeights: []uint32{850}}, nil)

		bc := &blockchain.Mock{}
		bc.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(false, errors.NewServiceUnavailableError("down"))
		mockRepo.On("GetBlockchainClient").Return(bc)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnspent, r.Status)
		require.Nil(t, r.Funding)
	})

	t.Run("no block evidence and not marked unmined leaves funding unknown", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnspent, r.Status)
		require.Nil(t, r.Funding)
	})

	t.Run("a seeded record with block ID 0 at a later height is not reported as mined", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, BlockIDs: []uint32{0}, BlockHeights: []uint32{850}}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnspent, r.Status)
		require.Nil(t, r.Funding)
	})

	t.Run("a block whose ID differs from the one at that height is not reported as mined", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, BlockIDs: []uint32{7}, BlockHeights: []uint32{850}}, nil)

		bc := &blockchain.Mock{}
		bc.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil)
		mockRepo.On("GetBlockchainClient").Return(bc)
		mockRepo.On("GetBlockByHeight", uint32(850)).Return(&model.Block{ID: 99, Header: testBlockHeader}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Nil(t, r.Funding)
	})

	t.Run("blocks are fetched once per height for many outputs", func(t *testing.T) {
		a, b := statusHash(11), statusHash(12)
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{a, 0}, statusOutpoint{b, 0}, statusOutpoint{a, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, BlockIDs: []uint32{7}, BlockHeights: []uint32{850}}, nil)

		bc := &blockchain.Mock{}
		bc.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil)
		mockRepo.On("GetBlockchainClient").Return(bc)
		mockRepo.On("GetBlockByHeight", uint32(850)).Return(blockWithHeader, nil)

		out := callStatus(t, httpServer, ec, rec)

		require.Len(t, out.Results, 3)
		mockRepo.AssertNumberOfCalls(t, "GetBlockByHeight", 1)
	})

	t.Run("main-chain cache path", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bc := &blockchain.Mock{}
		bc.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{7}).Return(true, nil)
		httpServer.mainChainCache = newMainChainCache(bc, ulogger.TestLogger{}, 0)

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, BlockIDs: []uint32{7}, BlockHeights: []uint32{850}}, nil)
		mockRepo.On("GetBlockByHeight", uint32(850)).Return(blockWithHeader, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.NotNil(t, r.Funding)
		require.True(t, r.Funding.Mined)
		require.Equal(t, uint32(850), r.Funding.BlockHeight)
	})

	t.Run("output not found but body held is output_not_found", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 899}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonNoOutput, r.Reason)
	})

	t.Run("output not found and meta store error is store_unavailable", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(nil, errors.NewStorageError("backend down"))

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonStore, r.Reason)
	})

	// The store holds the output, but without its funding record neither the script check nor the
	// coinbase maturity check can run.
	t.Run("a held output whose funding record cannot be read is store_unavailable", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(nil, errors.NewStorageError("backend down"))

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonStore, r.Reason)
	})

	t.Run("a held output whose funding record has gone is not_found_may_be_pruned", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("gone"))

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonPruned, r.Reason)
	})

	t.Run("spent with nil spending data is unknown", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_SPENT)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 899}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonOther, r.Reason)
	})

	t.Run("failure on the second best-block read is a 503", func(t *testing.T) {
		httpServer, mockRepo, ec, _ := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		mockRepo.On("GetBestBlockHeader").Return(testBlockHeader, &model.BlockHeaderMeta{Height: 900}, nil).Once()
		mockRepo.On("GetBestBlockHeader").Return(nil, nil, errors.NewServiceUnavailableError("down")).Once()
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("x"))

		ec.Request().Method = http.MethodPost

		var he *echo.HTTPError

		require.ErrorAs(t, httpServer.GetUTXOStatuses()(ec), &he)
		require.Equal(t, http.StatusServiceUnavailable, he.Code)
	})

	t.Run("a cancelled request context fails the request", func(t *testing.T) {
		httpServer, mockRepo, ec, _ := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		ctx, cancel := context.WithCancel(context.Background())
		ec.SetRequest(ec.Request().WithContext(ctx))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Run(func(mock.Arguments) { cancel() }).Return(nil, errors.NewStorageError("cancelled"))

		ec.Request().Method = http.MethodPost

		require.Error(t, httpServer.GetUTXOStatuses()(ec))
	})

	t.Run("a configured response-byte cap applies", func(t *testing.T) {
		httpServer, _, ec, _ := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))
		httpServer.settings.Asset.MaxBatchResponseBytes = 10
		ec.Request().Method = http.MethodPost

		var he *echo.HTTPError

		require.ErrorAs(t, httpServer.GetUTXOStatuses()(ec), &he)
		require.Equal(t, http.StatusRequestEntityTooLarge, he.Code)
	})

	t.Run("not found is unknown and never spent or unspent", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("gone"))

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonPruned, r.Reason)
		require.Nil(t, r.Satoshis)
		require.Nil(t, r.Funding)
		require.Nil(t, r.Spender)
	})

	t.Run("pruned: store ErrNotFound on the output is also unknown", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(nil, errors.NewTxNotFoundError("pruned"))
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("pruned"))

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonPruned, r.Reason)
	})

	t.Run("output index beyond the transaction is unknown", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 5}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonVout, r.Reason)
	})

	t.Run("store failure on one record is unknown, others still answered", func(t *testing.T) {
		other := statusHash(9)
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}, statusOutpoint{other, 0}))

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.MatchedBy(func(s *utxo.Spend) bool { return *s.TxID == fundID })).
			Return(nil, errors.NewStorageError("backend down"))
		mockRepo.On("GetUtxo", mock.MatchedBy(func(s *utxo.Spend) bool { return *s.TxID == other })).
			Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 899}, nil)

		out := callStatus(t, httpServer, ec, rec)

		require.Len(t, out.Results, 2)
		require.Equal(t, utxoStatusUnknown, out.Results[0].Status)
		require.Equal(t, utxoReasonStore, out.Results[0].Reason)
		require.Equal(t, utxoStatusUnspent, out.Results[1].Status)
	})

	t.Run("frozen, locked and conflicting are unknown with a reason", func(t *testing.T) {
		for st, reason := range map[utxo.Status]string{
			utxo.Status_FROZEN:      utxoReasonFrozen,
			utxo.Status_LOCKED:      utxoReasonLocked,
			utxo.Status_CONFLICTING: utxoReasonConflict,
		} {
			httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

			bestBlockMocks(mockRepo, 900)
			mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(st)}, nil)
			mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 1}, nil)

			r := callStatus(t, httpServer, ec, rec).Results[0]

			require.Equal(t, utxoStatusUnknown, r.Status)
			require.Equal(t, reason, r.Reason)
		}
	})

	t.Run("best block moving during the read is reported", func(t *testing.T) {
		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		mockRepo.On("GetBestBlockHeader").Return(testBlockHeader, &model.BlockHeaderMeta{Height: 900}, nil).Once()
		mockRepo.On("GetBestBlockHeader").Return(testBlockHeader, &model.BlockHeaderMeta{Height: 901}, nil).Once()
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("x"))

		out := callStatus(t, httpServer, ec, rec)

		require.Equal(t, uint32(900), out.BestBlock.Height)
		require.True(t, out.BestBlockChanged)
	})

	t.Run("best block unavailable is a 503", func(t *testing.T) {
		httpServer, mockRepo, ec, _ := mockStatusHTTP(t, statusBody(statusOutpoint{fundID, 0}))

		mockRepo.On("GetBestBlockHeader").Return(nil, nil, errors.NewServiceUnavailableError("down"))

		ec.Request().Method = http.MethodPost
		err := httpServer.GetUTXOStatuses()(ec)

		var he *echo.HTTPError
		require.ErrorAs(t, err, &he)
		require.Equal(t, http.StatusServiceUnavailable, he.Code)
	})

	t.Run("batch bounds and malformed bodies", func(t *testing.T) {
		codeOf := func(body *bytes.Reader) int {
			httpServer, _, ec, _ := mockStatusHTTP(t, body)
			ec.Request().Method = http.MethodPost

			var he *echo.HTTPError

			require.ErrorAs(t, httpServer.GetUTXOStatuses()(ec), &he)

			return he.Code
		}

		require.Equal(t, http.StatusBadRequest, codeOf(bytes.NewReader(nil)), "empty")
		require.Equal(t, http.StatusBadRequest, codeOf(bytes.NewReader(make([]byte, 35))), "not a multiple of 36")
		require.Equal(t, http.StatusRequestEntityTooLarge, codeOf(bytes.NewReader(make([]byte, 36*(utxoStatusMaxRecords+1)))), "over the cap")

		// A lower configured asset_maxBatchRecords wins over the route cap.
		httpServer, _, ec, _ := mockStatusHTTP(t, bytes.NewReader(make([]byte, 36*3)))
		httpServer.settings.Asset.MaxBatchRecords = 2
		ec.Request().Method = http.MethodPost

		var he *echo.HTTPError

		require.ErrorAs(t, httpServer.GetUTXOStatuses()(ec), &he)
		require.Equal(t, http.StatusRequestEntityTooLarge, he.Code)
	})
}

func TestGetUTXOStatusesFundingReads(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()

	t.Run("a funding read keeps only the requested outputs and drops the transaction body", func(t *testing.T) {
		id := statusHash(21)
		httpServer, mockRepo, _, _ := mockStatusHTTP(t, statusBody(statusOutpoint{id, 0}))

		big := bt.NewTx()

		for i := 1; i <= 3; i++ {
			script, err := bscript.NewFromHexString(fmt.Sprintf("76a914%040x88ac", i))
			require.NoError(t, err)

			big.AddOutput(&bt.Output{Satoshis: uint64(100 * i), LockingScript: script})
		}

		mockRepo.On("GetTxFundingRecord", &id).Return(&meta.Data{Tx: big, BlockIDs: []uint32{7}, BlockHeights: []uint32{850}}, nil)

		lk := &utxoStatusLookup{
			h:        httpServer,
			bestH:    900,
			wanted:   map[chainhash.Hash][]uint32{id: {2, 0, 2, 9}},
			bodySem:  make(chan struct{}, utxoStatusBodyReads),
			fundings: make(map[chainhash.Hash]*utxoStatusFundingEntry),
			refs:     make(map[chainhash.Hash]*utxoStatusMetaEntry),
			blocks:   make(map[uint32]*utxoStatusBlockEntry),
		}

		fr, err := lk.fundingRecord(ctx, &id)
		require.NoError(t, err)
		require.NotNil(t, fr)

		require.Nil(t, fr.meta.Tx, "the transaction body must not be kept")
		require.True(t, fr.hasBody)
		require.Equal(t, 3, fr.numOutputs)
		require.Len(t, fr.outputs, 2, "only the requested outputs that exist are kept")
		require.Equal(t, uint64(100), fr.outputs[0].satoshis)
		require.Equal(t, uint64(300), fr.outputs[2].satoshis)

		sum := sha256.Sum256(*big.Outputs[2].LockingScript)
		require.Equal(t, hex.EncodeToString(sum[:]), fr.outputs[2].scriptHash)
		require.True(t, fr.outputs[2].storable)
		require.Equal(t, []uint32{850}, fr.meta.BlockHeights)

		again, err := lk.fundingRecord(ctx, &id)
		require.NoError(t, err)
		require.Same(t, fr, again)
		mockRepo.AssertNumberOfCalls(t, "GetTxFundingRecord", 1)
	})

	t.Run("at most utxoStatusBodyReads funding reads run at once", func(t *testing.T) {
		const n = 40

		ops := make([]statusOutpoint, n)
		for i := range ops {
			ops[i] = statusOutpoint{statusHash(byte(100 + i)), 0}
		}

		httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(ops...))
		tx, _ := fundingTx(t)

		var inFlight, peak atomic.Int32

		bestBlockMocks(mockRepo, 900)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything).Run(func(mock.Arguments) {
			cur := inFlight.Add(1)

			for {
				p := peak.Load()
				if cur <= p || peak.CompareAndSwap(p, cur) {
					break
				}
			}

			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
		}).Return(&meta.Data{Tx: tx, UnminedSince: 899}, nil)

		out := callStatus(t, httpServer, ec, rec)

		require.Len(t, out.Results, n)
		require.Equal(t, 8, utxoStatusBodyReads, "asset_reference.md documents at most 8 funding reads at once")
		require.LessOrEqual(t, peak.Load(), int32(utxoStatusBodyReads))
	})
}
