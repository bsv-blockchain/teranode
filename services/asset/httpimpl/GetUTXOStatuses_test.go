package httpimpl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
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
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
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

const (
	// statusChainTip is the height of the last main-chain block in the test chain.
	statusChainTip = 160
	// statusForkHeight is where the test chain has a second block that is not on the main chain.
	statusForkHeight = 50
)

// statusChain is a real blockchain store (sqlitememory) for the handler tests: a main chain of
// statusChainTip blocks on top of genesis, and one competing block at statusForkHeight that is not
// on the main chain. It is built once per test function and only read.
type statusChain struct {
	client blockchain.ClientI
	ids    map[uint32]uint32
	hashes map[uint32]string
	forkID uint32
}

func statusChainBlock(t *testing.T, height uint32, prev *chainhash.Hash, bits model.NBit, variant byte) *model.Block {
	t.Helper()

	cb := bt.NewTx()

	input := &bt.Input{PreviousTxOutIndex: 0xFFFFFFFF, SequenceNumber: 0xFFFFFFFF}
	require.NoError(t, input.PreviousTxIDAdd(&chainhash.Hash{}))

	input.UnlockingScript = bscript.NewFromBytes([]byte{4, byte(height), byte(height >> 8), byte(height >> 16), variant})
	cb.Inputs = append(cb.Inputs, input)

	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)

	cb.AddOutput(&bt.Output{Satoshis: 5000000000, LockingScript: script})

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        2,
			Timestamp:      1600000000 + height*600 + uint32(variant),
			Nonce:          height + uint32(variant)<<24,
			HashPrevBlock:  prev,
			HashMerkleRoot: cb.TxIDChainHash(),
			Bits:           bits,
		},
		Height:           height,
		CoinbaseTx:       cb,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
}

func newStatusChain(t *testing.T) *statusChain {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	store, err := blockchain_store.NewStore(logger, storeURL, tSettings)
	require.NoError(t, err)

	client, err := blockchain.NewLocalClient(logger, tSettings, store, nil, nil)
	require.NoError(t, err)

	genesis, _, err := store.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	ch := &statusChain{client: client, ids: make(map[uint32]uint32), hashes: make(map[uint32]string)}

	prev := genesis.Hash()

	var forkParent *chainhash.Hash

	for h := uint32(1); h <= statusChainTip; h++ {
		if h == statusForkHeight {
			forkParent = prev
		}

		b := statusChainBlock(t, h, prev, *bits, 0)

		id, _, err := store.StoreBlock(ctx, b, "test")
		require.NoError(t, err, "block at height %d", h)

		ch.ids[h], ch.hashes[h] = uint32(id), b.Hash().String() //nolint:gosec
		prev = b.Hash()
	}

	fork := statusChainBlock(t, statusForkHeight, forkParent, *bits, 1)

	id, _, err := store.StoreBlock(ctx, fork, "test")
	require.NoError(t, err)

	ch.forkID = uint32(id) //nolint:gosec

	onChain, err := client.CheckBlockIsInCurrentChain(ctx, []uint32{ch.forkID})
	require.NoError(t, err)
	require.False(t, onChain, "the fork block must not be on the main chain")

	return ch
}

// statusChainClient is the real chain client with two test hooks: a main-chain check that fails,
// and a count of header lookups.
type statusChainClient struct {
	blockchain.ClientI
	checkErr    error
	headerCalls atomic.Int32
}

func (c *statusChainClient) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	if c.checkErr != nil {
		return false, c.checkErr
	}

	return c.ClientI.CheckBlockIsInCurrentChain(ctx, blockIDs)
}

func (c *statusChainClient) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	c.headerCalls.Add(1)

	return c.ClientI.GetBlockHeadersByHeight(ctx, startHeight, endHeight)
}

// statusHTTP is mockStatusHTTP with the repository's blockchain client served by the test chain.
func statusHTTP(t *testing.T, ch *statusChain, body io.Reader) (*HTTP, *repository.Mock, echo.Context, *httptest.ResponseRecorder, *statusChainClient) {
	t.Helper()

	h, repo, ec, rec := mockStatusHTTP(t, body)
	cc := &statusChainClient{ClientI: ch.client}
	repo.On("GetBlockchainClient").Return(cc)

	return h, repo, ec, rec, cc
}

// minedAt is a record of tx mined in the main-chain block at height.
func (ch *statusChain) minedAt(tx *bt.Tx, height uint32) *meta.Data {
	return &meta.Data{Tx: tx, BlockIDs: []uint32{ch.ids[height]}, BlockHeights: []uint32{height}}
}

func statusHash(b byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = b

	return h
}

// fundingTx returns a complete transaction (one input) whose output 0 is worth 1234 sat. Request it by
// its own txid: the handler takes a value only from the complete requested transaction.
func fundingTx(t *testing.T) (*bt.Tx, string) {
	t.Helper()

	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)

	tx := bt.NewTx()
	require.NoError(t, tx.From(strings.Repeat("ab", 32), 0, "76a914000000000000000000000000000000000000000088ac", 5000))
	tx.AddOutput(&bt.Output{Satoshis: 1234, LockingScript: script})

	sum := sha256.Sum256(*script)

	return tx, hex.EncodeToString(sum[:])
}

// wideTx returns a complete transaction with n outputs.
func wideTx(t *testing.T, n int) *bt.Tx {
	t.Helper()

	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)

	tx := bt.NewTx()
	require.NoError(t, tx.From(strings.Repeat("ef", 32), 0, "76a914000000000000000000000000000000000000000088ac", 1_000_000))

	for i := 0; i < n; i++ {
		tx.AddOutput(&bt.Output{Satoshis: uint64(1000 + i), LockingScript: script}) //nolint:gosec
	}

	return tx
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

// blockingFundingRepo answers GetTxFundingRecord like Aerospike's get(): it returns as soon as the
// context it is given is cancelled, or when release is closed.
type blockingFundingRepo struct {
	*repository.Mock
	entered chan struct{}
	release chan struct{}
	data    *meta.Data
}

func (r *blockingFundingRepo) GetTxFundingRecord(ctx context.Context, _ *chainhash.Hash) (*meta.Data, error) {
	close(r.entered)

	select {
	case <-ctx.Done():
	case <-r.release:
	}

	return r.data, nil
}

// countingLogger counts Warnf calls.
type countingLogger struct {
	ulogger.Logger
	warns atomic.Int32
}

func (l *countingLogger) Warnf(format string, args ...interface{}) {
	l.warns.Add(1)
	l.Logger.Warnf(format, args...)
}

func TestGetUTXOStatuses(t *testing.T) {
	initPrometheusMetrics()

	ch := newStatusChain(t)

	tx, wantScriptHash := fundingTx(t)
	fundID := *tx.TxIDChainHash()
	spenderID := statusHash(2)

	wantUTXOHash, err := util.UTXOHashFromOutput(&fundID, tx.Outputs[0], 0)
	require.NoError(t, err)

	t.Run("unspent and mined, with value and best block", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(ch.minedAt(tx, 50), nil)

		out := callStatus(t, httpServer, ec, rec)

		require.Equal(t, testBlockHeader.Hash().String(), out.BestBlock.Hash)
		require.Equal(t, uint32(statusChainTip), out.BestBlock.Height)
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
		require.Equal(t, uint32(50), r.Funding.BlockHeight)
		require.Equal(t, ch.hashes[50], r.Funding.BlockHash)
		require.Nil(t, r.Spender)

		// The store's commitment is checked against the UTXO hash computed from the body.
		mockRepo.AssertCalled(t, "GetUtxo", mock.MatchedBy(func(s *utxo.Spend) bool {
			return s.UTXOHash != nil && s.UTXOHash.IsEqual(wantUTXOHash)
		}))
	})

	t.Run("a stored commitment that differs from the body's output is reassigned, with no value", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(nil, errors.NewUtxoHashMismatchError("utxo hash mismatch"))
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(ch.minedAt(tx, 50), nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonReassign, r.Reason)
		require.Nil(t, r.Satoshis)
		require.Empty(t, r.LockingScriptSha256)
	})

	t.Run("a mismatch error when no hash was sent is a store failure, not a reassignment", func(t *testing.T) {
		other := statusHash(52)
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{other, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(nil, errors.NewUtxoHashMismatchError("utxo hash mismatch"))
		mockRepo.On("GetTxFundingRecord", mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 150}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonStore, r.Reason)
	})

	t.Run("spent reports the spender and its height", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{
			Status:       int(utxo.Status_SPENT),
			SpendingData: spendpkg.NewSpendingData(&spenderID, 3),
		}, nil)
		mockRepo.On("GetTxFundingRecord", &fundID).Return(ch.minedAt(tx, 50), nil)
		mockRepo.On("GetTxBlockRefs", &spenderID).Return(&meta.Data{BlockIDs: []uint32{ch.ids[70]}, BlockHeights: []uint32{70}}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusSpent, r.Status)
		require.NotNil(t, r.Spender)
		require.Equal(t, spenderID.String(), r.Spender.TxID)
		require.Equal(t, 3, r.Spender.Vin)
		require.NotNil(t, r.Spender.Mined)
		require.True(t, *r.Spender.Mined)
		require.Equal(t, uint32(70), r.Spender.BlockHeight)
		// the spender path reads only the block references, never the full record (no transaction body)
		mockRepo.AssertCalled(t, "GetTxBlockRefs", &spenderID)
		mockRepo.AssertNotCalled(t, "GetTxFundingRecord", &spenderID)
	})

	// mined is true only when placed, false only when known unmined, and absent otherwise.
	for _, tc := range []struct {
		name       string
		refs       *meta.Data
		refsErr    error
		checkErr   error
		wantMined  *bool
		wantHeight uint32
	}{
		{name: "an unmined spender is mined=false", refs: &meta.Data{UnminedSince: 150}, wantMined: new(bool)},
		{name: "a spender only in a block off the main chain is mined=false", refs: &meta.Data{BlockIDs: []uint32{ch.forkID}, BlockHeights: []uint32{statusForkHeight}}, wantMined: new(bool)},
		{name: "a spender whose block ID is not the block at that height has no mined field", refs: &meta.Data{BlockIDs: []uint32{ch.ids[30]}, BlockHeights: []uint32{70}}},
		{name: "a spender whose main-chain check fails has no mined field", refs: &meta.Data{BlockIDs: []uint32{ch.ids[70]}, BlockHeights: []uint32{70}}, checkErr: errors.NewServiceUnavailableError("down")},
		{name: "a spender whose record is not held has no mined field", refsErr: errors.NewTxNotFoundError("gone")},
		{name: "a spender whose record cannot be read has no mined field", refsErr: errors.NewStorageError("backend down")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			httpServer, mockRepo, ec, rec, cc := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

			bestBlockMocks(mockRepo, statusChainTip)
			mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{
				Status:       int(utxo.Status_SPENT),
				SpendingData: spendpkg.NewSpendingData(&spenderID, 1),
			}, nil)
			mockRepo.On("GetTxFundingRecord", &fundID).Return(&meta.Data{Tx: tx, UnminedSince: 150}, nil)
			mockRepo.On("GetTxBlockRefs", &spenderID).Return(tc.refs, tc.refsErr)

			cc.checkErr = tc.checkErr

			r := callStatus(t, httpServer, ec, rec).Results[0]

			require.Equal(t, utxoStatusSpent, r.Status)
			require.NotNil(t, r.Spender)
			require.Equal(t, tc.wantMined, r.Spender.Mined)
			require.Equal(t, tc.wantHeight, r.Spender.BlockHeight)
		})
	}

	t.Run("an absent spender mined field is left out of the JSON", func(t *testing.T) {
		b, err := json.Marshal(UTXOStatusSpender{TxID: spenderID.String(), Vin: 1})
		require.NoError(t, err)
		require.NotContains(t, string(b), "mined")

		mined := false
		b, err = json.Marshal(UTXOStatusSpender{TxID: spenderID.String(), Vin: 1, Mined: &mined})
		require.NoError(t, err)
		require.Contains(t, string(b), `"mined":false`)
	})

	t.Run("served records count on the route's own counter, not on POST /api/v1/utxos", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}, statusOutpoint{fundID, 1}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(ch.minedAt(tx, 50), nil)

		statusesBefore := testutil.ToFloat64(prometheusAssetHTTPGetUTXOStatuses.WithLabelValues("OK", "200"))
		utxosBefore := testutil.ToFloat64(prometheusAssetHTTPGetUTXOs.WithLabelValues("OK", "200"))

		out := callStatus(t, httpServer, ec, rec)
		require.Len(t, out.Results, 2)

		require.InDelta(t, statusesBefore+2, testutil.ToFloat64(prometheusAssetHTTPGetUTXOStatuses.WithLabelValues("OK", "200")), 0)
		require.InDelta(t, utxosBefore, testutil.ToFloat64(prometheusAssetHTTPGetUTXOs.WithLabelValues("OK", "200")), 0)
	})

	t.Run("unconfirmed funding transaction is flagged", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 150}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnspent, r.Status)
		require.NotNil(t, r.Funding)
		require.False(t, r.Funding.Mined)
		require.True(t, r.Funding.Unconfirmed)
		require.Empty(t, r.Funding.BlockHash)
	})

	t.Run("reorged out funding block is not reported as mined", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, BlockIDs: []uint32{ch.forkID}, BlockHeights: []uint32{statusForkHeight}}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.NotNil(t, r.Funding)
		require.False(t, r.Funding.Mined)
		require.True(t, r.Funding.Unconfirmed)
		require.Zero(t, r.Funding.BlockHeight)
	})

	// Aerospike-style: no spend-height hold is reported by the store itself.
	t.Run("coinbase output inside maturity is immature even when the store says OK", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, 120)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)

		data := ch.minedAt(tx, 50)
		data.IsCoinbase = true
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(data, nil)

		httpServer.settings.ChainCfgParams.CoinbaseMaturity = 100

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnspent, r.Status)
		require.True(t, r.Immature)
	})

	t.Run("coinbase output past maturity is not immature", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)

		data := ch.minedAt(tx, 30)
		data.IsCoinbase = true
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(data, nil)

		httpServer.settings.ChainCfgParams.CoinbaseMaturity = 100

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnspent, r.Status)
		require.False(t, r.Immature)
	})

	// Funding height 50 with maturity 100: the output is immature up to best height 149 and mature from 150,
	// the same boundary the SQL store's GetSpend applies (coinbase spending height > store height).
	for _, tc := range []struct {
		bestHeight   uint32
		wantImmature bool
	}{
		{bestHeight: 149, wantImmature: true},
		{bestHeight: 150, wantImmature: false},
	} {
		t.Run(fmt.Sprintf("coinbase maturity boundary at best height %d", tc.bestHeight), func(t *testing.T) {
			httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

			bestBlockMocks(mockRepo, tc.bestHeight)
			mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)

			data := ch.minedAt(tx, 50)
			data.IsCoinbase = true
			mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(data, nil)

			httpServer.settings.ChainCfgParams.CoinbaseMaturity = 100

			r := callStatus(t, httpServer, ec, rec).Results[0]

			require.Equal(t, utxoStatusUnspent, r.Status)
			require.Equal(t, tc.wantImmature, r.Immature)
		})
	}

	// A coinbase output whose funding block cannot be placed on the main chain has no maturity answer,
	// so it is never reported as unspent.
	for _, tc := range []struct {
		name     string
		checkErr error
		id       uint32
	}{
		{name: "the main-chain check fails", checkErr: errors.NewServiceUnavailableError("down"), id: ch.ids[50]},
		{name: "the block at that height has another ID", id: ch.ids[30]},
		{name: "the funding block was reorged out", id: ch.forkID},
	} {
		t.Run("coinbase maturity is unknown when "+tc.name, func(t *testing.T) {
			httpServer, mockRepo, ec, rec, cc := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

			bestBlockMocks(mockRepo, statusChainTip)
			mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
			mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, IsCoinbase: true, BlockIDs: []uint32{tc.id}, BlockHeights: []uint32{50}}, nil)

			cc.checkErr = tc.checkErr
			httpServer.settings.ChainCfgParams.CoinbaseMaturity = 100

			r := callStatus(t, httpServer, ec, rec).Results[0]

			require.Equal(t, utxoStatusUnknown, r.Status)
			require.Equal(t, utxoReasonMaturity, r.Reason)
			require.False(t, r.Immature)
		})
	}

	t.Run("store IMMATURE status is reported as immature", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_IMMATURE)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 150}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnspent, r.Status)
		require.True(t, r.Immature)
	})

	// A spend-height hold replaces the output's own state on some stores: Aerospike sets IMMATURE over
	// SPENT, SQL and pebble set it after frozen, locked and conflicting.
	t.Run("a hold over a spent output is unknown, not unspent", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{
			Status:       int(utxo.Status_IMMATURE),
			SpendingData: spendpkg.NewSpendingData(&spenderID, 3),
		}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 150}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonHeld, r.Reason)
		require.Nil(t, r.Spender)
	})

	t.Run("a hold over a frozen output (the frozen marker as spending data) is unknown", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{
			Status:       int(utxo.Status_IMMATURE),
			SpendingData: spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, 0),
		}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 150}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonHeld, r.Reason)
	})

	t.Run("a hold over a conflicting or locked transaction is unknown with that reason", func(t *testing.T) {
		for _, tc := range []struct {
			data   *meta.Data
			reason string
		}{
			{data: &meta.Data{Tx: tx, UnminedSince: 150, Conflicting: true}, reason: utxoReasonConflict},
			{data: &meta.Data{Tx: tx, UnminedSince: 150, Locked: true}, reason: utxoReasonLocked},
		} {
			httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

			bestBlockMocks(mockRepo, statusChainTip)
			mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_IMMATURE)}, nil)
			mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(tc.data, nil)

			r := callStatus(t, httpServer, ec, rec).Results[0]

			require.Equal(t, utxoStatusUnknown, r.Status)
			require.Equal(t, tc.reason, r.Reason)
		}
	})

	// On Aerospike the conflicting flag sits on the transaction's master record only: an output on a
	// child page (vout >= utxoBatchSize, 128 by default) reads OK. The transaction-level flags are
	// checked on the OK path too.
	t.Run("an OK output of a conflicting or locked transaction past the batch size is unknown", func(t *testing.T) {
		wide := wideTx(t, 130)
		wideID := *wide.TxIDChainHash()

		for _, tc := range []struct {
			conflicting, locked bool
			reason              string
		}{
			{conflicting: true, reason: utxoReasonConflict},
			{locked: true, reason: utxoReasonLocked},
		} {
			httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{wideID, 129}))

			data := ch.minedAt(wide, 50)
			data.Conflicting, data.Locked = tc.conflicting, tc.locked

			bestBlockMocks(mockRepo, statusChainTip)
			mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
			mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(data, nil)

			r := callStatus(t, httpServer, ec, rec).Results[0]

			require.Equal(t, utxoStatusUnknown, r.Status)
			require.Equal(t, tc.reason, r.Reason)
			require.Equal(t, uint32(129), r.Vout)
		}
	})

	// The flags are on Aerospike's master record only: a spent output on a child page reads SPENT with a
	// spender, where SQL and pebble answer conflicting.
	t.Run("a spent output of a conflicting or locked transaction past the batch size is unknown", func(t *testing.T) {
		wide := wideTx(t, 130)
		wideID := *wide.TxIDChainHash()

		for _, tc := range []struct {
			conflicting, locked bool
			reason              string
		}{
			{conflicting: true, reason: utxoReasonConflict},
			{locked: true, reason: utxoReasonLocked},
		} {
			httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{wideID, 129}))

			data := ch.minedAt(wide, 50)
			data.Conflicting, data.Locked = tc.conflicting, tc.locked

			bestBlockMocks(mockRepo, statusChainTip)
			mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{
				Status:       int(utxo.Status_SPENT),
				SpendingData: spendpkg.NewSpendingData(&spenderID, 3),
			}, nil)
			mockRepo.On("GetTxBlockRefs", mock.Anything).Return(nil, errors.NewTxNotFoundError("x"))
			mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(data, nil)

			r := callStatus(t, httpServer, ec, rec).Results[0]

			require.Equal(t, utxoStatusUnknown, r.Status)
			require.Equal(t, tc.reason, r.Reason)
			require.Nil(t, r.Spender)
		}
	})

	t.Run("OP_FALSE OP_RETURN output is unknown even when the store says OK", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		script := bscript.NewFromBytes([]byte{bscript.OpFALSE, bscript.OpRETURN, 0x01, 0xaa})
		dataTx := bt.NewTx()
		dataTx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: script})

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: dataTx, UnminedSince: 150}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonUnspend, r.Reason)
	})

	// A bare OP_RETURN output is unspendable before Genesis and spendable after it, so its answer depends
	// on where the funding block is. The era comes from the main-chain placement, never from a recorded
	// height alone.
	for _, tc := range []struct {
		name       string
		genesis    uint32
		data       func(tx *bt.Tx) *meta.Data
		checkErr   error
		wantStatus string
		wantReason string
	}{
		{name: "mined before Genesis is unspendable", genesis: 100, data: func(tx *bt.Tx) *meta.Data { return ch.minedAt(tx, 50) }, wantStatus: utxoStatusUnknown, wantReason: utxoReasonUnspend},
		{name: "mined after Genesis is unspent", genesis: 40, data: func(tx *bt.Tx) *meta.Data { return ch.minedAt(tx, 50) }, wantStatus: utxoStatusUnspent},
		{name: "unmined, Genesis at the next block, is unspent", genesis: statusChainTip + 1, data: func(tx *bt.Tx) *meta.Data { return &meta.Data{Tx: tx, UnminedSince: 150} }, wantStatus: utxoStatusUnspent},
		{name: "unmined, Genesis after the next block, is unspendable", genesis: statusChainTip + 2, data: func(tx *bt.Tx) *meta.Data { return &meta.Data{Tx: tx, UnminedSince: 150} }, wantStatus: utxoStatusUnknown, wantReason: utxoReasonUnspend},
		{name: "a recorded block off the main chain is placed as unconfirmed", genesis: 100, data: func(tx *bt.Tx) *meta.Data {
			return &meta.Data{Tx: tx, BlockIDs: []uint32{ch.forkID}, BlockHeights: []uint32{statusForkHeight}}
		}, wantStatus: utxoStatusUnspent},
		{name: "an unplaced funding block leaves the script rules unknown", genesis: 100, data: func(tx *bt.Tx) *meta.Data { return ch.minedAt(tx, 50) }, checkErr: errors.NewServiceUnavailableError("down"), wantStatus: utxoStatusUnknown, wantReason: utxoReasonScript},
	} {
		t.Run("bare OP_RETURN output: "+tc.name, func(t *testing.T) {
			retTx := bt.NewTx()
			require.NoError(t, retTx.From(strings.Repeat("12", 32), 0, "76a914000000000000000000000000000000000000000088ac", 5000))
			retTx.AddOutput(&bt.Output{Satoshis: 1, LockingScript: bscript.NewFromBytes([]byte{bscript.OpRETURN, 0x01, 0xaa})})
			retID := *retTx.TxIDChainHash()

			httpServer, mockRepo, ec, rec, cc := statusHTTP(t, ch, statusBody(statusOutpoint{retID, 0}))

			bestBlockMocks(mockRepo, statusChainTip)
			mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
			mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(tc.data(retTx), nil)

			cc.checkErr = tc.checkErr
			httpServer.settings.ChainCfgParams.GenesisActivationHeight = tc.genesis

			r := callStatus(t, httpServer, ec, rec).Results[0]

			require.Equal(t, tc.wantStatus, r.Status)
			require.Equal(t, tc.wantReason, r.Reason)
		})
	}

	t.Run("a failed main-chain check leaves funding unknown, not unconfirmed", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, cc := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(ch.minedAt(tx, 50), nil)

		cc.checkErr = errors.NewServiceUnavailableError("down")

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnspent, r.Status)
		require.Nil(t, r.Funding)
	})

	t.Run("block IDs and heights of different lengths leave funding unknown, not unconfirmed", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		// The one paired block is off the main chain; the second ID has no height.
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{
			Tx:           tx,
			BlockIDs:     []uint32{ch.forkID, ch.ids[50]},
			BlockHeights: []uint32{statusForkHeight},
		}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnspent, r.Status)
		require.Nil(t, r.Funding)
	})

	t.Run("no block evidence and not marked unmined leaves funding unknown", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnspent, r.Status)
		require.Nil(t, r.Funding)
	})

	t.Run("a seeded record with block ID 0 at a later height is not reported as mined", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, BlockIDs: []uint32{0}, BlockHeights: []uint32{50}}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnspent, r.Status)
		require.Nil(t, r.Funding)
	})

	t.Run("a block whose ID differs from the one at that height is not reported as mined", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		// The block of height 30 is on the main chain, so the chain check passes; it is not the block at 50.
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, BlockIDs: []uint32{ch.ids[30]}, BlockHeights: []uint32{50}}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Nil(t, r.Funding)
	})

	t.Run("headers are looked up once per height for many outputs", func(t *testing.T) {
		a, b := statusHash(11), statusHash(12)
		httpServer, mockRepo, ec, rec, cc := statusHTTP(t, ch, statusBody(statusOutpoint{a, 0}, statusOutpoint{b, 0}, statusOutpoint{a, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(ch.minedAt(tx, 50), nil)

		out := callStatus(t, httpServer, ec, rec)

		require.Len(t, out.Results, 3)
		require.Equal(t, int32(1), cc.headerCalls.Load())
	})

	t.Run("main-chain cache path", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, cc := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		httpServer.mainChainCache = newMainChainCache(cc, ulogger.TestLogger{}, 0)

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(ch.minedAt(tx, 50), nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.NotNil(t, r.Funding)
		require.True(t, r.Funding.Mined)
		require.Equal(t, uint32(50), r.Funding.BlockHeight)
		require.Equal(t, ch.hashes[50], r.Funding.BlockHash)
	})

	// A record seeded from a UTXO snapshot holds only the outputs that were unspent then: a nil hole at
	// each spent one, trailing spent outputs cut off, and no inputs (so it is not the requested txid). On
	// Aerospike a hole is a nil UTXO slot, and GetSpend reads that as OK.
	t.Run("a seeded record: a hole is output_not_found even when the store says OK", func(t *testing.T) {
		script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
		require.NoError(t, err)

		seededID := statusHash(31)
		seeded := &bt.Tx{Outputs: []*bt.Output{{Satoshis: 700, LockingScript: script}, nil, {Satoshis: 900, LockingScript: script}}}

		for _, tc := range []struct {
			name       string
			vout       uint32
			status     utxo.Status
			wantStatus string
			wantReason string
		}{
			{name: "hole, store OK", vout: 1, status: utxo.Status_OK, wantStatus: utxoStatusUnknown, wantReason: utxoReasonNoOutput},
			{name: "hole, store NOT_FOUND", vout: 1, status: utxo.Status_NOT_FOUND, wantStatus: utxoStatusUnknown, wantReason: utxoReasonNoOutput},
			{name: "trailing spent output, store NOT_FOUND", vout: 3, status: utxo.Status_NOT_FOUND, wantStatus: utxoStatusUnknown, wantReason: utxoReasonNoOutput},
			{name: "present output, store OK", vout: 0, status: utxo.Status_OK, wantStatus: utxoStatusUnspent},
		} {
			t.Run(tc.name, func(t *testing.T) {
				httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{seededID, tc.vout}))

				bestBlockMocks(mockRepo, statusChainTip)
				mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(tc.status)}, nil)
				mockRepo.On("GetTxFundingRecord", mock.Anything).Return(&meta.Data{Tx: seeded, UnminedSince: 150}, nil)

				r := callStatus(t, httpServer, ec, rec).Results[0]

				require.Equal(t, tc.wantStatus, r.Status)
				require.Equal(t, tc.wantReason, r.Reason)
				// No value or script hash is taken from a body that is not the requested transaction.
				require.Nil(t, r.Satoshis)
				require.Empty(t, r.LockingScriptSha256)
			})
		}
	})

	t.Run("a complete transaction under another txid gives no value and no hash to check", func(t *testing.T) {
		other := statusHash(51)
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{other, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 150}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnspent, r.Status)
		require.Nil(t, r.Satoshis)
		require.Empty(t, r.LockingScriptSha256)
		mockRepo.AssertCalled(t, "GetUtxo", mock.MatchedBy(func(s *utxo.Spend) bool { return s.UTXOHash == nil }))
	})

	t.Run("output not found but body held is output_not_found", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 150}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonNoOutput, r.Reason)
		// No commitment was checked for an output the store does not hold, so no value is given.
		require.Nil(t, r.Satoshis)
		require.Empty(t, r.LockingScriptSha256)
	})

	// not_found_may_be_pruned means the store holds no record; a record held without its body is held.
	t.Run("output not found while the record is held without a body is output_not_found", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{UnminedSince: 150}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonNoOutput, r.Reason)
	})

	t.Run("output not found and meta store error is store_unavailable", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(nil, errors.NewStorageError("backend down"))

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonStore, r.Reason)
	})

	// The store holds the output, but without its funding record neither the script check nor the
	// coinbase maturity check can run.
	t.Run("a held output whose funding record cannot be read is store_unavailable", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(nil, errors.NewStorageError("backend down"))

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonStore, r.Reason)
	})

	// The funding record is read first, so absent-then-held means it appeared between the two reads.
	t.Run("a held output whose funding record was absent when read first is changed_during_read", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("gone"))

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, "changed_during_read", r.Reason)
	})

	t.Run("spent with nil spending data is unknown", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_SPENT)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 150}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonOther, r.Reason)
	})

	t.Run("failure on the second best-block read is a 503", func(t *testing.T) {
		httpServer, mockRepo, ec, _, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		mockRepo.On("GetBestBlockHeader").Return(testBlockHeader, &model.BlockHeaderMeta{Height: statusChainTip}, nil).Once()
		mockRepo.On("GetBestBlockHeader").Return(nil, nil, errors.NewServiceUnavailableError("down")).Once()
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("x"))

		ec.Request().Method = http.MethodPost

		var he *echo.HTTPError

		require.ErrorAs(t, httpServer.GetUTXOStatuses()(ec), &he)
		require.Equal(t, http.StatusServiceUnavailable, he.Code)
	})

	t.Run("a cancelled request context fails the request", func(t *testing.T) {
		httpServer, mockRepo, ec, _, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		ctx, cancel := context.WithCancel(context.Background())
		ec.SetRequest(ec.Request().WithContext(ctx))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("x"))
		mockRepo.On("GetUtxo", mock.Anything).Run(func(mock.Arguments) { cancel() }).Return(nil, errors.NewStorageError("cancelled"))

		ec.Request().Method = http.MethodPost

		require.Error(t, httpServer.GetUTXOStatuses()(ec))
	})

	// After a client cancels, records that have not started a funding read must not start one: the read
	// is detached from cancellation, so a started read would run to completion for nobody. When a slot is
	// free and the context is done, both cases of a select are ready, so each record is tried.
	t.Run("a cancelled request starts no further funding reads", func(t *testing.T) {
		httpServer, mockRepo, _, _, _ := statusHTTP(t, ch, statusBody())

		mockRepo.On("GetTxFundingRecord", mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 150}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		lk := newUTXOStatusLookup(httpServer, statusChainTip, nil)

		for i := 0; i < 200; i++ {
			hash := chainhash.HashH([]byte{byte(i), byte(i >> 8)})

			_, err := lk.fundingRecord(ctx, &hash)
			require.ErrorIs(t, err, context.Canceled)
		}

		mockRepo.AssertNotCalled(t, "GetTxFundingRecord", mock.Anything)
		require.Equal(t, 0, len(utxoStatusBodySem))
	})

	t.Run("a configured response-byte cap applies", func(t *testing.T) {
		httpServer, _, ec, _, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))
		httpServer.settings.Asset.MaxBatchResponseBytes = 10
		ec.Request().Method = http.MethodPost

		var he *echo.HTTPError

		require.ErrorAs(t, httpServer.GetUTXOStatuses()(ec), &he)
		require.Equal(t, http.StatusRequestEntityTooLarge, he.Code)
	})

	t.Run("not found is unknown and never spent or unspent", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
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
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(nil, errors.NewTxNotFoundError("pruned"))
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("pruned"))

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonPruned, r.Reason)
	})

	t.Run("output index beyond the transaction is unknown", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 5}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx}, nil)

		r := callStatus(t, httpServer, ec, rec).Results[0]

		require.Equal(t, utxoStatusUnknown, r.Status)
		require.Equal(t, utxoReasonVout, r.Reason)
	})

	t.Run("store failure on one record is unknown, others still answered", func(t *testing.T) {
		other := statusHash(9)
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}, statusOutpoint{other, 0}))

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.MatchedBy(func(s *utxo.Spend) bool { return *s.TxID == fundID })).
			Return(nil, errors.NewStorageError("backend down"))
		mockRepo.On("GetUtxo", mock.MatchedBy(func(s *utxo.Spend) bool { return *s.TxID == other })).
			Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 150}, nil)

		out := callStatus(t, httpServer, ec, rec)

		require.Len(t, out.Results, 2)
		require.Equal(t, utxoStatusUnknown, out.Results[0].Status)
		require.Equal(t, utxoReasonStore, out.Results[0].Reason)
		require.Equal(t, utxoStatusUnspent, out.Results[1].Status)
	})

	t.Run("store failures are logged once per request, not once per record", func(t *testing.T) {
		ops := []statusOutpoint{{statusHash(61), 0}, {statusHash(62), 0}, {statusHash(63), 0}}
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(ops...))

		logger := &countingLogger{Logger: ulogger.TestLogger{}}
		httpServer.logger = logger

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(nil, errors.NewStorageError("backend down"))
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(nil, errors.NewStorageError("backend down"))

		out := callStatus(t, httpServer, ec, rec)

		for _, r := range out.Results {
			require.Equal(t, utxoReasonStore, r.Reason)
		}

		require.Equal(t, int32(1), logger.warns.Load())
	})

	t.Run("frozen, locked and conflicting are unknown with a reason", func(t *testing.T) {
		for st, reason := range map[utxo.Status]string{
			utxo.Status_FROZEN:      utxoReasonFrozen,
			utxo.Status_LOCKED:      utxoReasonLocked,
			utxo.Status_CONFLICTING: utxoReasonConflict,
		} {
			httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

			bestBlockMocks(mockRepo, statusChainTip)
			mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(st)}, nil)
			mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(&meta.Data{Tx: tx, UnminedSince: 1}, nil)

			r := callStatus(t, httpServer, ec, rec).Results[0]

			require.Equal(t, utxoStatusUnknown, r.Status)
			require.Equal(t, reason, r.Reason)
		}
	})

	t.Run("best block moving during the read is reported", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		mockRepo.On("GetBestBlockHeader").Return(testBlockHeader, &model.BlockHeaderMeta{Height: statusChainTip}, nil).Once()
		mockRepo.On("GetBestBlockHeader").Return(testBlockHeader, &model.BlockHeaderMeta{Height: statusChainTip + 1}, nil).Once()
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("x"))

		out := callStatus(t, httpServer, ec, rec)

		require.Equal(t, uint32(statusChainTip), out.BestBlock.Height)
		require.True(t, out.BestBlockChanged)
	})

	t.Run("best block unavailable is a 503", func(t *testing.T) {
		httpServer, mockRepo, ec, _, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

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

	t.Run("one record under ?pretty stays within the per-record byte charge", func(t *testing.T) {
		httpServer, mockRepo, ec, rec, _ := statusHTTP(t, ch, statusBody(statusOutpoint{fundID, 0}))

		req := httptest.NewRequest(http.MethodPost, "/?pretty", statusBody(statusOutpoint{fundID, 0}))
		ec.SetRequest(req)

		bestBlockMocks(mockRepo, statusChainTip)
		mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
		mockRepo.On("GetTxFundingRecord", mock.Anything, mock.Anything).Return(ch.minedAt(tx, 50), nil)

		require.NoError(t, httpServer.GetUTXOStatuses()(ec))
		require.Contains(t, rec.Body.String(), "\n  ", "the response is indented")
		require.LessOrEqual(t, rec.Body.Len(), utxoStatusRecordMaxBytes)
	})
}

// utxoStatusRecordMaxBytes is charged per record before any lookup runs, so it must bound the largest
// record the route can write, indented as ?pretty writes it (two spaces per level, the record two
// levels deep), with every field at its longest at once.
func TestGetUTXOStatusesRecordMaxBytes(t *testing.T) {
	longest := ""
	for _, r := range []string{
		utxoReasonPruned, utxoReasonStore, utxoReasonVout, utxoReasonFrozen, utxoReasonConflict,
		utxoReasonLocked, utxoReasonOther, utxoReasonNoOutput, utxoReasonUnspend, utxoReasonMaturity,
		utxoReasonHeld, utxoReasonReassign, utxoReasonScript,
	} {
		if len(r) > len(longest) {
			longest = r
		}
	}

	hash64 := strings.Repeat("f", 64)
	sats := uint64(math.MaxUint64)
	mined := true

	record := UTXOStatusRecord{
		TxID:                hash64,
		Vout:                math.MaxUint32,
		Status:              utxoStatusUnspent,
		Reason:              longest,
		Satoshis:            &sats,
		LockingScriptSha256: hash64,
		Immature:            true,
		Funding:             &UTXOStatusFunding{Mined: true, Unconfirmed: true, BlockHeight: math.MaxUint32, BlockHash: hash64},
		Spender:             &UTXOStatusSpender{TxID: hash64, Vin: math.MinInt64, Mined: &mined, BlockHeight: math.MaxUint32},
	}

	size := func(results []UTXOStatusRecord) int {
		var buf bytes.Buffer

		enc := json.NewEncoder(&buf)
		enc.SetIndent("", "  ")
		require.NoError(t, enc.Encode(UTXOStatusResponse{BestBlock: UTXOStatusBlockRef{Hash: hash64, Height: math.MaxUint32}, Results: results}))

		return buf.Len()
	}

	one := size([]UTXOStatusRecord{record})
	two := size([]UTXOStatusRecord{record, record})
	perRecord := two - one

	t.Logf("largest indented record: %d bytes (one-record response %d bytes)", perRecord, one)

	require.LessOrEqual(t, perRecord, utxoStatusRecordMaxBytes)
	require.LessOrEqual(t, one, utxoStatusRecordMaxBytes, "a one-record response, envelope included, fits the charge")
}

func TestGetUTXOStatusesFundingReads(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()

	t.Run("a funding read keeps only the requested outputs and drops the transaction body", func(t *testing.T) {
		big := bt.NewTx()
		require.NoError(t, big.From(strings.Repeat("cd", 32), 1, "76a914000000000000000000000000000000000000000088ac", 9000))

		for i := 1; i <= 3; i++ {
			script, err := bscript.NewFromHexString(fmt.Sprintf("76a914%040x88ac", i))
			require.NoError(t, err)

			big.AddOutput(&bt.Output{Satoshis: uint64(100 * i), LockingScript: script})
		}

		id := *big.TxIDChainHash()
		httpServer, mockRepo, _, _ := mockStatusHTTP(t, statusBody(statusOutpoint{id, 0}))

		mockRepo.On("GetTxFundingRecord", &id).Return(&meta.Data{Tx: big, BlockIDs: []uint32{7}, BlockHeights: []uint32{850}}, nil)

		lk := newUTXOStatusLookup(httpServer, 900, map[chainhash.Hash][]uint32{id: {2, 0, 2, 9}})

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
		require.True(t, fr.outputs[2].storablePre)
		require.True(t, fr.outputs[2].storablePost)

		wantHash, err := util.UTXOHashFromOutput(&id, big.Outputs[2], 2)
		require.NoError(t, err)
		require.True(t, fr.outputs[2].verified)
		require.Equal(t, *wantHash, fr.outputs[2].utxoHash)
		require.Equal(t, []uint32{850}, fr.meta.BlockHeights)

		again, err := lk.fundingRecord(ctx, &id)
		require.NoError(t, err)
		require.Same(t, fr, again)
		mockRepo.AssertNumberOfCalls(t, "GetTxFundingRecord", 1)
	})

	t.Run("the script rules are kept for both eras", func(t *testing.T) {
		retTx := bt.NewTx()
		require.NoError(t, retTx.From(strings.Repeat("34", 32), 0, "76a914000000000000000000000000000000000000000088ac", 5000))
		retTx.AddOutput(&bt.Output{Satoshis: 1, LockingScript: bscript.NewFromBytes([]byte{bscript.OpRETURN, 0x01, 0xaa})})
		retTx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: bscript.NewFromBytes([]byte{bscript.OpFALSE, bscript.OpRETURN, 0x01, 0xaa})})

		id := *retTx.TxIDChainHash()
		httpServer, _, _, _ := mockStatusHTTP(t, statusBody(statusOutpoint{id, 0}))

		lk := newUTXOStatusLookup(httpServer, 900, nil)
		fr := lk.summarise(&id, &meta.Data{Tx: retTx}, []uint32{0, 1})

		require.False(t, fr.outputs[0].storablePre, "a bare OP_RETURN is unspendable before Genesis")
		require.True(t, fr.outputs[0].storablePost, "and spendable after it")
		require.False(t, fr.outputs[1].storablePre)
		require.False(t, fr.outputs[1].storablePost, "OP_FALSE OP_RETURN is unspendable in every era")
	})

	t.Run("at most utxoStatusBodyReads funding reads run at once across concurrent requests", func(t *testing.T) {
		const (
			perRequest = 40
			requests   = 3
		)

		tx, _ := fundingTx(t)

		var inFlight, peak atomic.Int32

		slowRead := func(mock.Arguments) {
			cur := inFlight.Add(1)

			for {
				p := peak.Load()
				if cur <= p || peak.CompareAndSwap(p, cur) {
					break
				}
			}

			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
		}

		var wg sync.WaitGroup

		for r := 0; r < requests; r++ {
			ops := make([]statusOutpoint, perRequest)
			for i := range ops {
				ops[i] = statusOutpoint{statusHash(byte(r*perRequest + i)), 0}
			}

			httpServer, mockRepo, ec, rec := mockStatusHTTP(t, statusBody(ops...))

			bestBlockMocks(mockRepo, 900)
			mockRepo.On("GetUtxo", mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)
			mockRepo.On("GetTxFundingRecord", mock.Anything).Run(slowRead).Return(&meta.Data{Tx: tx, UnminedSince: 899}, nil)

			wg.Add(1)

			go func() {
				defer wg.Done()

				ec.Request().Method = http.MethodPost
				if err := httpServer.GetUTXOStatuses()(ec); err != nil {
					t.Errorf("request failed: %v", err)
				}

				if rec.Code != http.StatusOK {
					t.Errorf("status %d", rec.Code)
				}
			}()
		}

		wg.Wait()

		require.Equal(t, 32, utxoStatusBodyReads, "asset_reference.md documents at most 32 funding reads at once")
		require.LessOrEqual(t, peak.Load(), int32(utxoStatusBodyReads))
		require.Greater(t, peak.Load(), int32(1), "the reads do run concurrently")
	})
}

// summarise must not dereference a missing output or script: either is a hole, never a panic.
func TestGetUTXOStatusesSummariseHoles(t *testing.T) {
	httpServer, _, _, _ := mockStatusHTTP(t, statusBody(statusOutpoint{statusHash(41), 0}))

	lk := newUTXOStatusLookup(httpServer, 900, nil)
	id := statusHash(41)
	partial := &bt.Tx{Outputs: []*bt.Output{nil, {Satoshis: 5}}}

	var fr *utxoStatusFundingRecord

	require.NotPanics(t, func() { fr = lk.summarise(&id, &meta.Data{Tx: partial}, []uint32{0, 1, 7}) })
	require.True(t, fr.partial)
	require.False(t, fr.hasBody)
	require.True(t, fr.holes[0])
	require.True(t, fr.holes[1])
	require.Empty(t, fr.outputs)
}
