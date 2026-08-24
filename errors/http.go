package errors

import (
	"encoding/base64"
	"net/http"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
)

// HTTPErrorHeader carries a failure's public code and message across an internal
// HTTP hop, the way WrapGRPCPublic's TError detail carries them across a gRPC one.
//
// A plain-text HTTP body cannot be classified. A service that receives one has to
// either re-parse the peer's rendered error string or wrap the whole body in an
// error of its own choosing, and the second is what the validator's callers did:
// a policy rejection came back as SERVICE_ERROR, which is off publicCauseCodes, so
// the verdict collapsed to a retryable 500 and the peer's entire internal chain —
// file, line, function — was spliced into a client-facing message. A permanently
// invalid transaction was reported as "try again later".
//
// This header closes that gap without touching the response body or status, so a
// peer that does not set it (an older build) behaves exactly as before and a peer
// that does not read it is unaffected. Only PublicError's projection travels:
// code and message, never the chain, the file/line/function, or the error data.
const HTTPErrorHeader = "X-Teranode-Error"

// maxHTTPErrorHeaderBytes bounds the encoded header value. Servers commonly cap a
// single header at 8KB and drop the whole request or response when it is exceeded,
// so the message is truncated to fit rather than risking the header being
// discarded. Public messages are a line long; this is a backstop, not a budget.
const maxHTTPErrorHeaderBytes = 4096

// truncationSuffix marks a message shortened to fit the header.
const truncationSuffix = "…[truncated]"

// AttachHTTPError writes err's public code and message to h under
// HTTPErrorHeader, so an HTTP peer can reconstruct the verdict with
// HTTPErrorFrom instead of guessing from the response body.
//
// It is a no-op for a nil error or header. The projection is PublicError's, so
// this is safe to call on any error, including one that wraps node-internal
// detail: a cause that is not on publicCauseCodes collapses to the outermost
// code and message exactly as it would on any other public surface.
func AttachHTTPError(h http.Header, err error) {
	if h == nil || err == nil {
		return
	}

	publicErr := PublicError(err)
	if publicErr == nil {
		return
	}

	// Sanitize for valid UTF-8: proto rejects invalid UTF-8 in a string field,
	// and the header value must survive the round trip intact.
	encoded, ok := encodeHTTPError(publicErr.code, RemoveInvalidUTF8(publicErr.message))
	if !ok {
		return
	}

	h.Set(HTTPErrorHeader, encoded)
}

// encodeHTTPError renders code and message as a base64 TError, truncating the
// message if the result would exceed maxHTTPErrorHeaderBytes. The code is the
// part a caller classifies on, so it is kept even when the message will not fit.
func encodeHTTPError(code ERR, message string) (string, bool) {
	encoded, ok := marshalHTTPError(code, message)
	if ok && len(encoded) <= maxHTTPErrorHeaderBytes {
		return encoded, true
	}

	if !ok {
		return "", false
	}

	// base64 costs 4 bytes per 3, and the proto adds a small fixed overhead;
	// half of the budget leaves ample room for both.
	budget := maxHTTPErrorHeaderBytes/2 - len(truncationSuffix)
	if budget < 0 || len(message) <= budget {
		// Nothing left to trim — emit the code with no message rather than
		// dropping the header entirely.
		return marshalHTTPError(code, "")
	}

	return marshalHTTPError(code, truncateRunes(message, budget)+truncationSuffix)
}

// truncateRunes cuts s to at most n bytes without splitting a rune — a half rune
// is invalid UTF-8, which proto.Marshal rejects outright.
func truncateRunes(s string, n int) string {
	if len(s) <= n {
		return s
	}

	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}

	return s[:n]
}

func marshalHTTPError(code ERR, message string) (string, bool) {
	raw, err := proto.Marshal(&TError{Code: code, Message: message})
	if err != nil {
		return "", false
	}

	return base64.StdEncoding.EncodeToString(raw), true
}

// HTTPErrorFrom reconstructs the code and message a peer attached with
// AttachHTTPError, or nil when the header is absent, oversized, or unparseable.
//
// Only code and message are meaningful on the returned error — it carries no
// chain, no file/line/function and no data, and a caller must not present it as
// though it did. A nil return is the "peer told us nothing" case, not an error:
// callers fall back to whatever they did before the header existed.
func HTTPErrorFrom(h http.Header) *Error {
	if h == nil {
		return nil
	}

	value := h.Get(HTTPErrorHeader)
	if value == "" || len(value) > maxHTTPErrorHeaderBytes {
		return nil
	}

	raw, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil
	}

	var detail TError
	if err = proto.Unmarshal(raw, &detail); err != nil {
		return nil
	}

	// Reject a code this build does not know rather than manufacturing an error
	// whose String() would be a bare number.
	if _, ok := ERR_name[int32(detail.Code)]; !ok {
		return nil
	}

	return &Error{
		code:    detail.Code,
		message: detail.Message,
	}
}
