package httpimpl

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// buildUTXOsRequest concatenates (txid, vout) records in the on-wire format
// the POST /api/v1/utxos endpoint expects.
func buildUTXOsRequest(records []struct {
	TxID chainhash.Hash
	Vout uint32
}) []byte {
	buf := make([]byte, 0, len(records)*utxosRequestRecordSize)

	for _, r := range records {
		buf = append(buf, r.TxID[:]...)

		var voutBytes [4]byte
		binary.LittleEndian.PutUint32(voutBytes[:], r.Vout)
		buf = append(buf, voutBytes[:]...)
	}

	return buf
}

// parseUTXOsResponse decodes the fixed-size 48-byte records returned by
// POST /api/v1/utxos into SpendResponse structs.
func parseUTXOsResponse(t *testing.T, b []byte) []*utxo.SpendResponse {
	t.Helper()

	require.Equal(t, 0, len(b)%utxosResponseRecordSize, "response length must be a multiple of %d", utxosResponseRecordSize)

	out := make([]*utxo.SpendResponse, 0, len(b)/utxosResponseRecordSize)

	for off := 0; off < len(b); off += utxosResponseRecordSize {
		rec := b[off : off+utxosResponseRecordSize]

		status := int(binary.LittleEndian.Uint64(rec[0:8]))
		lockTime := binary.LittleEndian.Uint32(rec[8:12])
		vin := binary.LittleEndian.Uint32(rec[12:16])

		resp := &utxo.SpendResponse{
			Status:   status,
			LockTime: lockTime,
		}

		var spendTxID chainhash.Hash
		copy(spendTxID[:], rec[16:48])

		if spendTxID != (chainhash.Hash{}) {
			resp.SpendingData = spendpkg.NewSpendingData(&spendTxID, int(vin))
		}

		out = append(out, resp)
	}

	return out
}

func TestGetUTXOs(t *testing.T) {
	initPrometheusMetrics()

	mkTxID := func(b byte) chainhash.Hash {
		var h chainhash.Hash
		h[0] = b
		return h
	}

	t.Run("empty body returns 200 with empty payload", func(t *testing.T) {
		httpServer, _, echoContext, rec := GetMockHTTP(t, bytes.NewReader(nil))
		echoContext.Request().Method = http.MethodPost

		require.NoError(t, httpServer.GetUTXOs(BINARY_STREAM)(echoContext))
		require.Equal(t, http.StatusOK, rec.Code)
		require.Empty(t, rec.Body.Bytes())
	})

	t.Run("malformed body length is rejected with 400", func(t *testing.T) {
		// 37 bytes — not a multiple of 36.
		body := bytes.Repeat([]byte{0x01}, utxosRequestRecordSize+1)
		httpServer, _, echoContext, _ := GetMockHTTP(t, bytes.NewReader(body))
		echoContext.Request().Method = http.MethodPost

		err := httpServer.GetUTXOs(BINARY_STREAM)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Contains(t, echoErr.Message.(string), "not a multiple of 36")
	})

	t.Run("mixed found/unspent/spent/not-found records preserve order", func(t *testing.T) {
		httpServer, mockRepo, echoContext, rec := GetMockHTTP(t, nil)

		txA := mkTxID(0x0a)
		txB := mkTxID(0x0b)
		txC := mkTxID(0x0c)
		spendingTx := mkTxID(0xee)

		// Per-record mock responses keyed on the Spend struct's TxID.
		mockRepo.On("GetUtxo", mock.MatchedBy(func(s *utxo.Spend) bool {
			return s.TxID != nil && *s.TxID == txA
		})).Return(&utxo.SpendResponse{
			Status:   int(utxo.Status_OK),
			LockTime: 1234567,
		}, nil)

		mockRepo.On("GetUtxo", mock.MatchedBy(func(s *utxo.Spend) bool {
			return s.TxID != nil && *s.TxID == txB
		})).Return(&utxo.SpendResponse{
			Status:       int(utxo.Status_SPENT),
			LockTime:     0,
			SpendingData: spendpkg.NewSpendingData(&spendingTx, 7),
		}, nil)

		mockRepo.On("GetUtxo", mock.MatchedBy(func(s *utxo.Spend) bool {
			return s.TxID != nil && *s.TxID == txC
		})).Return(&utxo.SpendResponse{
			Status: int(utxo.Status_NOT_FOUND),
		}, nil)

		body := buildUTXOsRequest([]struct {
			TxID chainhash.Hash
			Vout uint32
		}{
			{TxID: txA, Vout: 0},
			{TxID: txB, Vout: 1},
			{TxID: txC, Vout: 2},
		})

		echoContext.Request().Method = http.MethodPost
		echoContext.Request().Body = mustBody(body)

		require.NoError(t, httpServer.GetUTXOs(BINARY_STREAM)(echoContext))
		require.Equal(t, http.StatusOK, rec.Code)

		got := parseUTXOsResponse(t, rec.Body.Bytes())
		require.Len(t, got, 3)

		// Order is preserved.
		assert.Equal(t, int(utxo.Status_OK), got[0].Status)
		assert.Equal(t, uint32(1234567), got[0].LockTime)
		assert.Nil(t, got[0].SpendingData)

		assert.Equal(t, int(utxo.Status_SPENT), got[1].Status)
		require.NotNil(t, got[1].SpendingData)
		assert.Equal(t, spendingTx, *got[1].SpendingData.TxID)
		assert.Equal(t, 7, got[1].SpendingData.Vin)

		assert.Equal(t, int(utxo.Status_NOT_FOUND), got[2].Status)
		assert.Nil(t, got[2].SpendingData)
	})

	t.Run("UTXOHash is nil on every store call", func(t *testing.T) {
		httpServer, mockRepo, echoContext, rec := GetMockHTTP(t, nil)

		mockRepo.On("GetUtxo", mock.MatchedBy(func(s *utxo.Spend) bool {
			return s.UTXOHash == nil
		})).Return(&utxo.SpendResponse{
			Status: int(utxo.Status_OK),
		}, nil)

		txA := mkTxID(0x42)

		body := buildUTXOsRequest([]struct {
			TxID chainhash.Hash
			Vout uint32
		}{
			{TxID: txA, Vout: 0},
		})

		echoContext.Request().Method = http.MethodPost
		echoContext.Request().Body = mustBody(body)

		require.NoError(t, httpServer.GetUTXOs(BINARY_STREAM)(echoContext))
		require.Equal(t, http.StatusOK, rec.Code)

		mockRepo.AssertCalled(t, "GetUtxo", mock.MatchedBy(func(s *utxo.Spend) bool {
			return s.UTXOHash == nil
		}))
		mockRepo.AssertNotCalled(t, "GetTransaction", mock.Anything, mock.Anything)
	})

	t.Run("not-found error from store maps to per-record NOT_FOUND", func(t *testing.T) {
		httpServer, mockRepo, echoContext, rec := GetMockHTTP(t, nil)

		// One record returns OK, the other returns a not-found error from
		// the store. The whole request must still succeed (200) with the
		// erroring record reported as NOT_FOUND.
		txOK := mkTxID(0xaa)
		txMissing := mkTxID(0xbb)

		mockRepo.On("GetUtxo", mock.MatchedBy(func(s *utxo.Spend) bool {
			return s.TxID != nil && *s.TxID == txOK
		})).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)

		mockRepo.On("GetUtxo", mock.MatchedBy(func(s *utxo.Spend) bool {
			return s.TxID != nil && *s.TxID == txMissing
		})).Return(nil, errors.NewNotFoundError("UTXO not found"))

		body := buildUTXOsRequest([]struct {
			TxID chainhash.Hash
			Vout uint32
		}{
			{TxID: txOK, Vout: 0},
			{TxID: txMissing, Vout: 0},
		})

		echoContext.Request().Method = http.MethodPost
		echoContext.Request().Body = mustBody(body)

		require.NoError(t, httpServer.GetUTXOs(BINARY_STREAM)(echoContext))
		require.Equal(t, http.StatusOK, rec.Code)

		got := parseUTXOsResponse(t, rec.Body.Bytes())
		require.Len(t, got, 2)
		assert.Equal(t, int(utxo.Status_OK), got[0].Status)
		assert.Equal(t, int(utxo.Status_NOT_FOUND), got[1].Status)
	})

	t.Run("processing error from store fails the request", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetUtxo", mock.Anything).Return(nil, errors.NewProcessingError("aerospike unavailable"))

		txA := mkTxID(0x01)

		body := buildUTXOsRequest([]struct {
			TxID chainhash.Hash
			Vout uint32
		}{
			{TxID: txA, Vout: 0},
		})

		echoContext.Request().Method = http.MethodPost
		echoContext.Request().Body = mustBody(body)

		err := httpServer.GetUTXOs(BINARY_STREAM)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)
	})

	t.Run("panic in goroutine becomes 500 instead of crashing the process", func(t *testing.T) {
		// Regression guard for the issue identified on PR #950: echo's
		// middleware.Recover does NOT cover errgroup goroutines. Without our
		// per-goroutine recover, a panic deep in the store driver (e.g. the
		// pre-existing index-out-of-range in stores/utxo/aerospike/get.go when
		// vout exceeds the actual output count) would crash the asset process.
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetUtxo", mock.Anything).Run(func(args mock.Arguments) {
			panic("simulated store panic")
		}).Return(nil, nil)

		body := buildUTXOsRequest([]struct {
			TxID chainhash.Hash
			Vout uint32
		}{
			{TxID: mkTxID(0x01), Vout: 0},
		})

		echoContext.Request().Method = http.MethodPost
		echoContext.Request().Body = mustBody(body)

		err := httpServer.GetUTXOs(BINARY_STREAM)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr), "expected echo.HTTPError, got %T: %v", err, err)
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)
	})

	t.Run("HEX mode returns hex-encoded binary payload", func(t *testing.T) {
		httpServer, mockRepo, echoContext, rec := GetMockHTTP(t, nil)

		txA := mkTxID(0xa1)
		spendingTx := mkTxID(0xee)

		mockRepo.On("GetUtxo", mock.MatchedBy(func(s *utxo.Spend) bool {
			return s.TxID != nil && *s.TxID == txA
		})).Return(&utxo.SpendResponse{
			Status:       int(utxo.Status_SPENT),
			LockTime:     99,
			SpendingData: spendpkg.NewSpendingData(&spendingTx, 5),
		}, nil)

		body := buildUTXOsRequest([]struct {
			TxID chainhash.Hash
			Vout uint32
		}{
			{TxID: txA, Vout: 0},
		})

		echoContext.Request().Method = http.MethodPost
		echoContext.Request().Body = mustBody(body)

		require.NoError(t, httpServer.GetUTXOs(HEX)(echoContext))
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "text/plain; charset=UTF-8", rec.Header().Get("Content-Type"))

		decoded, err := hex.DecodeString(rec.Body.String())
		require.NoError(t, err)

		got := parseUTXOsResponse(t, decoded)
		require.Len(t, got, 1)
		assert.Equal(t, int(utxo.Status_SPENT), got[0].Status)
		assert.Equal(t, uint32(99), got[0].LockTime)
		require.NotNil(t, got[0].SpendingData)
		assert.Equal(t, spendingTx, *got[0].SpendingData.TxID)
		assert.Equal(t, 5, got[0].SpendingData.Vin)
	})

	t.Run("JSON mode returns array of SpendResponse objects in input order", func(t *testing.T) {
		httpServer, mockRepo, echoContext, rec := GetMockHTTP(t, nil)

		txA := mkTxID(0xa1)
		txB := mkTxID(0xb2)
		txC := mkTxID(0xc3)
		spendingTx := mkTxID(0xee)

		mockRepo.On("GetUtxo", mock.MatchedBy(func(s *utxo.Spend) bool {
			return s.TxID != nil && *s.TxID == txA
		})).Return(&utxo.SpendResponse{
			Status:   int(utxo.Status_OK),
			LockTime: 1234567,
		}, nil)

		mockRepo.On("GetUtxo", mock.MatchedBy(func(s *utxo.Spend) bool {
			return s.TxID != nil && *s.TxID == txB
		})).Return(&utxo.SpendResponse{
			Status:       int(utxo.Status_SPENT),
			SpendingData: spendpkg.NewSpendingData(&spendingTx, 11),
		}, nil)

		mockRepo.On("GetUtxo", mock.MatchedBy(func(s *utxo.Spend) bool {
			return s.TxID != nil && *s.TxID == txC
		})).Return(&utxo.SpendResponse{
			Status: int(utxo.Status_NOT_FOUND),
		}, nil)

		body := buildUTXOsRequest([]struct {
			TxID chainhash.Hash
			Vout uint32
		}{
			{TxID: txA, Vout: 0},
			{TxID: txB, Vout: 1},
			{TxID: txC, Vout: 2},
		})

		echoContext.Request().Method = http.MethodPost
		echoContext.Request().Body = mustBody(body)

		require.NoError(t, httpServer.GetUTXOs(JSON)(echoContext))
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

		var got []*utxo.SpendResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		require.Len(t, got, 3)

		assert.Equal(t, int(utxo.Status_OK), got[0].Status)
		assert.Equal(t, uint32(1234567), got[0].LockTime)
		assert.Nil(t, got[0].SpendingData)

		assert.Equal(t, int(utxo.Status_SPENT), got[1].Status)
		require.NotNil(t, got[1].SpendingData)
		assert.Equal(t, spendingTx, *got[1].SpendingData.TxID)
		assert.Equal(t, 11, got[1].SpendingData.Vin)

		assert.Equal(t, int(utxo.Status_NOT_FOUND), got[2].Status)
		assert.Nil(t, got[2].SpendingData)
	})

	t.Run("JSON mode on empty body returns empty array", func(t *testing.T) {
		httpServer, _, echoContext, rec := GetMockHTTP(t, bytes.NewReader(nil))
		echoContext.Request().Method = http.MethodPost

		require.NoError(t, httpServer.GetUTXOs(JSON)(echoContext))
		require.Equal(t, http.StatusOK, rec.Code)
		assert.JSONEq(t, "[]", rec.Body.String())
	})
}

func mustBody(b []byte) *closingReader {
	return &closingReader{Reader: bytes.NewReader(b)}
}

// closingReader is a tiny io.ReadCloser around bytes.Reader so we can plug it
// into echo.Context.Request().Body without pulling in net/http internals.
type closingReader struct {
	*bytes.Reader
}

func (c *closingReader) Close() error { return nil }
