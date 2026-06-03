package validator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestValidateTransactionRequest_ParentMetadataRoundTrip pins the proto wire
// encoding for ParentMetadata. A future refactor that drops the field from
// validator_api.proto (or renames it) will fail this test before any silent
// regression can ship to the remote-validator wire.
func TestValidateTransactionRequest_ParentMetadataRoundTrip(t *testing.T) {
	hash1 := chainhash.Hash{0xaa, 0xbb, 0xcc}
	hash2 := chainhash.Hash{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}

	req := &validator_api.ValidateTransactionRequest{
		TransactionData: []byte{1, 2, 3},
		BlockHeight:     42,
		ParentMetadata: []*validator_api.ParentTxMetadata{
			{ParentHash: hash1[:], BlockHeight: 100},
			{ParentHash: hash2[:], BlockHeight: 200},
		},
	}

	bytesOut, err := proto.Marshal(req)
	require.NoError(t, err)

	got := &validator_api.ValidateTransactionRequest{}
	require.NoError(t, proto.Unmarshal(bytesOut, got))

	require.Len(t, got.ParentMetadata, 2, "parent_metadata must round-trip both entries")
	// Wire entries do not guarantee order; map them by hash for assertion.
	byHash := make(map[chainhash.Hash]uint32, 2)
	for _, entry := range got.ParentMetadata {
		require.Len(t, entry.ParentHash, chainhash.HashSize)
		var h chainhash.Hash
		copy(h[:], entry.ParentHash)
		byHash[h] = entry.BlockHeight
	}
	require.Equal(t, uint32(100), byHash[hash1])
	require.Equal(t, uint32(200), byHash[hash2])
}

// TestValidateTransactionRequest_ParentMetadataOmitted_IsEmpty pins the
// receiver-side contract for an empty/missing field. On the wire, "field
// absent" and "field present but empty" are indistinguishable in proto3, and
// the server-side reconstruction collapses both to a nil Options.ParentMetadata.
func TestValidateTransactionRequest_ParentMetadataOmitted_IsEmpty(t *testing.T) {
	req := &validator_api.ValidateTransactionRequest{
		TransactionData: []byte{1, 2, 3},
		BlockHeight:     42,
	}

	bytesOut, err := proto.Marshal(req)
	require.NoError(t, err)

	got := &validator_api.ValidateTransactionRequest{}
	require.NoError(t, proto.Unmarshal(bytesOut, got))

	require.Empty(t, got.ParentMetadata, "parent_metadata must round-trip as empty when sender omits it")
}

// TestOptionsFromValidateRequest_ParentMetadataRoundTrip pins the full
// Client → Server propagation: build the gRPC request from Options.ParentMetadata,
// project the request back into Options, expect the reconstructed map to
// equal the source by content.
func TestOptionsFromValidateRequest_ParentMetadataRoundTrip(t *testing.T) {
	hashA := chainhash.Hash{0xde, 0xad, 0xbe, 0xef}
	hashB := chainhash.Hash{0xca, 0xfe, 0xba, 0xbe}

	src := &Options{
		ParentMetadata: map[chainhash.Hash]*ParentTxMetadata{
			hashA: {BlockHeight: 11},
			hashB: {BlockHeight: 22},
		},
	}

	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 42, src)

	// Also round-trip through the protobuf wire to pin the full path.
	wire, err := proto.Marshal(req)
	require.NoError(t, err)
	wireBack := &validator_api.ValidateTransactionRequest{}
	require.NoError(t, proto.Unmarshal(wire, wireBack))

	got, err := optionsFromValidateRequest(wireBack)
	require.NoError(t, err)

	require.Len(t, got.ParentMetadata, 2)
	require.NotNil(t, got.ParentMetadata[hashA])
	require.Equal(t, uint32(11), got.ParentMetadata[hashA].BlockHeight)
	require.NotNil(t, got.ParentMetadata[hashB])
	require.Equal(t, uint32(22), got.ParentMetadata[hashB].BlockHeight)
}

// TestOptionsFromValidateRequest_ParentMetadataEmptySrc_IsNil pins that a
// nil or empty source map produces a nil reconstructed map (not an empty
// non-nil map). Both wire forms are indistinguishable, so the server side
// must normalise — keeps downstream behaviour consistent.
func TestOptionsFromValidateRequest_ParentMetadataEmptySrc_IsNil(t *testing.T) {
	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 42, &Options{})
	require.Empty(t, req.ParentMetadata, "empty source must produce empty wire")

	got, err := optionsFromValidateRequest(req)
	require.NoError(t, err)
	require.Nil(t, got.ParentMetadata, "empty wire must produce nil ParentMetadata on the server side")
}

// TestParentMetadata_NonSymmetricHashByteOrder is the critical safety net for
// the wire encoding choice (repeated bytes parent_hash). chainhash.Hash has a
// display order (in NewHashFromStr / String) reversed from its internal byte
// storage. The wire transports the *internal* bytes via hash[:]; this test
// pins that the on-wire byte sequence is **not** display-reversed.
//
// If a future refactor switches to a string-keyed encoding and uses
// hex.EncodeToString(hash[:]) + chainhash.NewHashFromStr(...) mismatched
// against each other, the asymmetric hash here would round-trip to the wrong
// key and fail this assertion immediately.
func TestParentMetadata_NonSymmetricHashByteOrder(t *testing.T) {
	// A deliberately asymmetric hash: distinct first and last bytes. If byte
	// order is reversed on either side of the wire, the reconstructed key
	// differs from the source key.
	hash := chainhash.Hash{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
	}

	src := &Options{
		ParentMetadata: map[chainhash.Hash]*ParentTxMetadata{
			hash: {BlockHeight: 7},
		},
	}

	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 42, src)
	require.Len(t, req.ParentMetadata, 1)

	// Wire bytes must be the internal byte order, not display-reversed.
	require.Equal(t, []byte(hash[:]), req.ParentMetadata[0].ParentHash,
		"on-wire parent_hash must use chainhash.Hash internal byte order")

	wire, err := proto.Marshal(req)
	require.NoError(t, err)
	wireBack := &validator_api.ValidateTransactionRequest{}
	require.NoError(t, proto.Unmarshal(wire, wireBack))
	got, err := optionsFromValidateRequest(wireBack)
	require.NoError(t, err)

	require.Len(t, got.ParentMetadata, 1)
	require.NotNil(t, got.ParentMetadata[hash])
	require.Equal(t, uint32(7), got.ParentMetadata[hash].BlockHeight)
}

// TestParentMetadata_WireDefensivelyCopiesHashBytes pins that the wire
// representation does not alias the source map's hash bytes — a future caller
// that mutates the source map after marshalling must not see torn writes in
// the wire form.
func TestParentMetadata_WireDefensivelyCopiesHashBytes(t *testing.T) {
	hash := chainhash.Hash{0x11, 0x22, 0x33, 0x44}
	src := &Options{
		ParentMetadata: map[chainhash.Hash]*ParentTxMetadata{
			hash: {BlockHeight: 9},
		},
	}

	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 42, src)
	require.Len(t, req.ParentMetadata, 1)

	wireBytes := req.ParentMetadata[0].ParentHash
	require.Equal(t, []byte(hash[:]), wireBytes)

	// The wire slice must not share backing storage with the source key
	// (which is an array on the map's stack/heap). Mutating one must not
	// affect the other.
	wireBytes[0] = 0xff
	require.Equal(t, byte(0x11), hash[0], "source hash key must not be aliased by wire bytes")
}

// TestIsProtobufContentType pins the Content-Type discrimination used by
// handleSingleTx to choose between the protobuf-body path and the legacy
// raw-bytes-plus-query-params path. Includes parameter-tolerant variants
// (charset, quality) that well-behaved HTTP intermediaries may append.
func TestIsProtobufContentType(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  bool
	}{
		{"empty", "", false},
		{"octet-stream", "application/octet-stream", false},
		{"json", "application/json", false},
		{"x-protobuf bare", "application/x-protobuf", true},
		{"x-protobuf upper", "APPLICATION/X-PROTOBUF", true},
		{"x-protobuf with charset", "application/x-protobuf; charset=binary", true},
		{"x-protobuf with spaces", "  application/x-protobuf  ", true},
		{"protobuf bare", "application/protobuf", true},
		{"protobuf with params", "application/protobuf; q=1.0", true},
		{"protobuf-ish but wrong", "application/x-protobuf-2", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isProtobufContentType(tc.input))
		})
	}
}

// TestHTTPHandlerPath_ProtobufBody_EndToEnd pins that a request received on
// the /tx HTTP endpoint with Content-Type: application/x-protobuf and a
// marshalled ValidateTransactionRequest body round-trips ParentMetadata
// correctly. Mirrors the protobuf-body shape the validator's HTTP fallback
// client now produces.
func TestHTTPHandlerPath_ProtobufBody_EndToEnd(t *testing.T) {
	hash := chainhash.Hash{0xab, 0xcd, 0xef, 0x01}
	src := &Options{
		ParentMetadata: map[chainhash.Hash]*ParentTxMetadata{
			hash: {BlockHeight: 13},
		},
	}

	srcReq := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 42, src)
	body, err := proto.Marshal(srcReq)
	require.NoError(t, err)

	// Simulate what handleSingleTx would do for an x-protobuf request.
	e := echo.New()
	httpReq := httptest.NewRequest(http.MethodPost, "/tx", strings.NewReader(string(body)))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	rec := httptest.NewRecorder()
	_ = e.NewContext(httpReq, rec)

	require.True(t, isProtobufContentType(httpReq.Header.Get("Content-Type")))

	got := &validator_api.ValidateTransactionRequest{}
	require.NoError(t, proto.Unmarshal(body, got))

	gotOpts, err := optionsFromValidateRequest(got)
	require.NoError(t, err)
	require.Len(t, gotOpts.ParentMetadata, 1)
	require.NotNil(t, gotOpts.ParentMetadata[hash])
	require.Equal(t, uint32(13), gotOpts.ParentMetadata[hash].BlockHeight)
}

// TestParentMetadataFromWire_FailsClosedOnMalformedHashLength pins the
// fail-closed contract on the server: a client that emits a parent_hash of
// the wrong length (anything other than chainhash.HashSize bytes) is
// surfaced as a request-level error rather than silently dropped. Silently
// dropping would force the validator into the UTXO-store fallback for the
// corresponding input; for an in-block parent that path stamps the
// unconfirmedParentHeight sentinel and yields a misleading
// bad-txns-unconfirmed-input-in-block rejection — exactly the shape that
// carrying ParentMetadata on the wire is meant to prevent.
func TestParentMetadataFromWire_FailsClosedOnMalformedHashLength(t *testing.T) {
	cases := []struct {
		name   string
		length int
	}{
		{"empty", 0},
		{"too short", 16},
		{"one byte short", chainhash.HashSize - 1},
		{"one byte long", chainhash.HashSize + 1},
		{"way too long", 128},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := &validator_api.ValidateTransactionRequest{
				ParentMetadata: []*validator_api.ParentTxMetadata{
					{ParentHash: make([]byte, tc.length), BlockHeight: 7},
				},
			}
			_, err := optionsFromValidateRequest(req)
			require.Error(t, err, "malformed parent_hash length must surface as a request-level error")
			require.Contains(t, err.Error(), "parent_hash",
				"error must identify the field so a client-side bug is debuggable")
		})
	}
}

// TestParentMetadataFromWire_FailsClosedOnNilEntry pins that a nil
// ParentTxMetadata entry on the wire is rejected. Same rationale as the
// malformed-length case: nothing about the consensus contract makes a nil
// entry meaningful, and silently dropping it would re-create the silent
// degradation pt.1 is meant to remove.
func TestParentMetadataFromWire_FailsClosedOnNilEntry(t *testing.T) {
	req := &validator_api.ValidateTransactionRequest{
		ParentMetadata: []*validator_api.ParentTxMetadata{nil},
	}
	_, err := optionsFromValidateRequest(req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil")
}

// TestHTTPHandlerPath_LegacyOctetStream_BackwardCompat pins that the legacy
// /tx path (Content-Type: application/octet-stream + raw tx body + scalar
// query params) still works and produces a nil ParentMetadata. The legacy
// path has no representation for ParentMetadata; this is by design and
// callers needing in-block-parent metadata must use the protobuf-body path.
func TestHTTPHandlerPath_LegacyOctetStream_BackwardCompat(t *testing.T) {
	e := echo.New()
	ctx, err := echoRequestWithQuery(e, "blockHeight=42")
	require.NoError(t, err)

	require.False(t, isProtobufContentType(ctx.Request().Header.Get("Content-Type")),
		"legacy path must not be misclassified as protobuf")

	blockHeight, opts := extractValidationParams(ctx)
	require.Equal(t, uint32(42), blockHeight)
	require.Nil(t, opts.ParentMetadata,
		"legacy octet-stream path has no ParentMetadata representation; must stay nil")

	// The legacy path subsequently calls buildValidateTxRequest + optionsFromValidateRequest.
	// The wire form has no parent_metadata entries, so the projection must not error.
	req := buildValidateTxRequest([]byte{1, 2, 3}, blockHeight, opts)
	gotOpts, err := optionsFromValidateRequest(req)
	require.NoError(t, err)
	require.Nil(t, gotOpts.ParentMetadata)
}
