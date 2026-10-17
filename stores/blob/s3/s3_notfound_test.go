package s3

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/stretchr/testify/require"
)

// missingKeyErrors are the ways a missing object surfaces from the AWS SDK: the
// two typed errors, plus the generic API error reported in
// aws/aws-sdk-go-v2#2084, where a 404 is rendered as NotFound rather than
// NoSuchKey.
//
// Only Exists carried that workaround; the read paths matched the literal
// string "NoSuchKey" and so missed both other shapes. That gap matters because
// callers distinguish "this object is not here" from "the store failed":
// stores/utxo/aerospike falls back to the .outputs blob only on a genuine miss,
// so an unrecognised miss reads as a storage failure and aborts an operation that
// should have succeeded.
//
// A bodyless 404 — the shape with no error code at all — cannot reach us as an
// uncoded error: the SDK's S3 deserializers pass UseStatusCode, so s3shared
// derives the code "NotFound" from the status text before building either the
// typed error (HeadObject) or the generic one (GetObject). Every miss therefore
// carries a code, which is why matching on ErrorCode() is sufficient and a
// blanket status-404 check is not needed.
func missingKeyErrors() []struct {
	name string
	err  error
} {
	return []struct {
		name string
		err  error
	}{
		{name: "typed NoSuchKey", err: &types.NoSuchKey{}},
		{name: "typed NotFound", err: &types.NotFound{}},
		{
			// The shape reported in aws/aws-sdk-go-v2#2084: a generic API error
			// whose code is NotFound and whose message never says NoSuchKey.
			name: "generic NotFound api error",
			err:  &smithy.GenericAPIError{Code: "NotFound", Message: "Not Found"},
		},
	}
}

func TestS3_GetIoReader_MissingKeyMapsToErrNotFound(t *testing.T) {
	for _, tt := range missingKeyErrors() {
		t.Run(tt.name, func(t *testing.T) {
			s3Store, mock := setupTestS3(t)
			mock.SetGetObjectMissError(tt.err)

			_, err := s3Store.GetIoReader(context.Background(), []byte("missing"), fileformat.FileTypeTx)

			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrNotFound),
				"a missing object must map to ErrNotFound so callers can tell a miss from a store failure, got %v", err)
		})
	}
}

func TestS3_Get_MissingKeyMapsToErrNotFound(t *testing.T) {
	for _, tt := range missingKeyErrors() {
		t.Run(tt.name, func(t *testing.T) {
			s3Store, mock := setupTestS3(t)
			mock.SetGetObjectMissError(tt.err)

			_, err := s3Store.Get(context.Background(), []byte("missing"), fileformat.FileTypeTx)

			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrNotFound),
				"a missing object must map to ErrNotFound so callers can tell a miss from a store failure, got %v", err)
		})
	}
}

// TestS3_Get_MissingKeyFromTransferManagerMapsToErrNotFound is the shape that
// matters most in production, and the one the mock's Download-delegates-to-GetObject
// wiring cannot reach on its own.
//
// The real client does not implement Download in terms of GetObject: s3client.go
// calls transfermanager.GetObject, which issues its own HeadObject on the
// concurrent/ranged path and returns the SDK error unwrapped. So a genuine miss
// arrives at S3.Get as *types.NotFound, whose message never contains "NoSuchKey" —
// exactly what the previous strings.Contains check missed. Injecting at the
// Download boundary exercises that path rather than a delegation artefact.
func TestS3_Get_MissingKeyFromTransferManagerMapsToErrNotFound(t *testing.T) {
	for _, tt := range missingKeyErrors() {
		t.Run(tt.name, func(t *testing.T) {
			s3Store, mock := setupTestS3(t)
			mock.SetDownloadError(tt.err)

			_, err := s3Store.Get(context.Background(), []byte("missing"), fileformat.FileTypeTx)

			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrNotFound),
				"a miss surfaced by the transfer manager must map to ErrNotFound, got %v", err)
		})
	}

	t.Run("a real failure from the transfer manager is not a miss", func(t *testing.T) {
		s3Store, mock := setupTestS3(t)
		mock.SetDownloadError(&smithy.GenericAPIError{Code: "AccessDenied", Message: "Access Denied"})

		_, err := s3Store.Get(context.Background(), []byte("missing"), fileformat.FileTypeTx)

		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrNotFound),
			"AccessDenied must stay a failure — S3 returns it for a missing key when the "+
				"principal lacks s3:ListBucket, and guessing would mask a real permission fault")
	})
}

// TestS3_Exists_MissingKeyIsNotAnError covers the path that carried the
// aws/aws-sdk-go-v2#2084 workaround before this change. Exists is the most
// behaviourally sensitive of the three: it reports a miss as (false, nil), so a
// miss shape it fails to recognise becomes a hard error at every caller that only
// wanted to know whether the blob is there.
func TestS3_Exists_MissingKeyIsNotAnError(t *testing.T) {
	for _, tt := range missingKeyErrors() {
		t.Run(tt.name, func(t *testing.T) {
			s3Store, mock := setupTestS3(t)
			mock.SetHeadObjectError(tt.err)

			exists, err := s3Store.Exists(context.Background(), []byte("missing"), fileformat.FileTypeTx)

			require.NoError(t, err, "a missing object is not a failure to look, got %v", err)
			require.False(t, exists)
		})
	}
}

// TestS3_Exists_RealFailureIsNotAMiss pins the other direction. NoSuchBucket is
// also a 404, so widening the miss check to "any 404" would turn a bucket
// misconfiguration into a silent "not stored" — worse than the bug being fixed,
// because the aerospike caller would then take the degraded .outputs path for
// every transaction in the store.
func TestS3_Exists_RealFailureIsNotAMiss(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{name: "NoSuchBucket", err: &types.NoSuchBucket{}},
		{name: "AccessDenied", err: &smithy.GenericAPIError{Code: "AccessDenied", Message: "Access Denied"}},
		{name: "message merely mentions NotFound", err: errors.NewError("dial tcp: lookup s3.example: NotFound in DNS")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s3Store, mock := setupTestS3(t)
			mock.SetHeadObjectError(tt.err)

			exists, err := s3Store.Exists(context.Background(), []byte("missing"), fileformat.FileTypeTx)

			require.Error(t, err, "a store failure must not be reported as a miss")
			require.False(t, exists)
		})
	}
}

// TestS3_GetIoReader_RealFailureIsNotAMiss is the other half of the contract: a
// genuine store failure must NOT be reported as a miss, or a caller that treats
// not-found as "fall back to the degraded path" will silently mask an outage.
func TestS3_GetIoReader_RealFailureIsNotAMiss(t *testing.T) {
	s3Store, mock := setupTestS3(t)
	mock.SetGetObjectMissError(&smithy.GenericAPIError{Code: "InternalError", Message: "We encountered an internal error"})

	_, err := s3Store.GetIoReader(context.Background(), []byte("missing"), fileformat.FileTypeTx)

	require.Error(t, err)
	require.False(t, errors.Is(err, errors.ErrNotFound),
		"an internal store error must not be reported as a missing object")
}

// asDelivered wraps err the way the SDK actually delivers it to a caller: the
// typed or coded error sits inside an *awshttp.ResponseError, inside a
// *smithy.OperationError. Rendered, that is
//
//	operation error S3: GetObject, https response error StatusCode: 404,
//	RequestID: REQ, api error NotFound: Not Found
//
// which is the shape that defeated the previous check — the message never contains
// "NoSuchKey", so a substring match on it refused a genuine miss.
func asDelivered(operation string, status int, err error) error {
	return &smithy.OperationError{
		ServiceID:     "S3",
		OperationName: operation,
		Err: &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
				Err:      err,
			},
			RequestID: "REQ",
		},
	}
}

// TestS3_MissingKeyIsRecognisedThroughSDKWrappers is the case the other tests in
// this file cannot reach: they inject bare typed errors, but no caller ever sees
// one bare. isMissingObject relies on errors.As unwrapping two SDK layers to find
// the typed error or the parsed code, and nothing here proved that it does.
//
// It is worth proving rather than assuming, because the check this replaces failed
// for exactly this reason: it assumed a message shape, and the wrapping changed it.
func TestS3_MissingKeyIsRecognisedThroughSDKWrappers(t *testing.T) {
	for _, tt := range missingKeyErrors() {
		t.Run(tt.name+" via GetObject", func(t *testing.T) {
			s3Store, mock := setupTestS3(t)
			mock.SetGetObjectMissError(asDelivered("GetObject", 404, tt.err))

			_, err := s3Store.Get(context.Background(), []byte("missing"), fileformat.FileTypeTx)

			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrNotFound),
				"a miss must survive the SDK's OperationError/ResponseError wrapping, got %v", err)
		})

		t.Run(tt.name+" via HeadObject", func(t *testing.T) {
			s3Store, mock := setupTestS3(t)
			mock.SetHeadObjectError(asDelivered("HeadObject", 404, tt.err))

			exists, err := s3Store.Exists(context.Background(), []byte("missing"), fileformat.FileTypeTx)

			require.NoError(t, err, "a wrapped miss is not a failure to look, got %v", err)
			require.False(t, exists)
		})
	}

	t.Run("a wrapped real failure is still not a miss", func(t *testing.T) {
		s3Store, mock := setupTestS3(t)
		mock.SetGetObjectMissError(asDelivered("GetObject", 403,
			&smithy.GenericAPIError{Code: "AccessDenied", Message: "Access Denied"}))

		_, err := s3Store.Get(context.Background(), []byte("missing"), fileformat.FileTypeTx)

		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrNotFound),
			"the wrapping must not turn a permission fault into a miss")
	})
}

// TestS3_ErrorMessagesRenderTheObjectKey covers the paths whose only change here
// was `%s` being handed a `*string`.
//
// They are all failure branches, so none of them ran in any existing test — which
// is how the bug survived: the messages are only built once something has already
// gone wrong, and that is precisely when a key rendered as `%!s(*string=0x...)`
// costs the most to whoever is reading the log.
//
// Each case asserts the key is present and the pointer rendering is not, rather
// than asserting on the whole message, so rewording a message does not break them.
func TestS3_ErrorMessagesRenderTheObjectKey(t *testing.T) {
	const badRender = "%!s("

	key := []byte("render-me")

	t.Run("SetFromReader on an existing object", func(t *testing.T) {
		s3Store, _ := setupTestS3(t)
		require.NoError(t, s3Store.Set(context.Background(), key, fileformat.FileTypeTx, []byte("v")))

		err := s3Store.SetFromReader(context.Background(), key, fileformat.FileTypeTx,
			io.NopCloser(bytes.NewReader([]byte("v"))))

		require.Error(t, err)
		require.NotContains(t, err.Error(), badRender, "the object key must render as text")
	})

	t.Run("SetFromReader when the upload fails", func(t *testing.T) {
		s3Store, mock := setupTestS3(t)
		mock.SetUploadError(errors.NewError("upload refused"))

		err := s3Store.SetFromReader(context.Background(), key, fileformat.FileTypeTx,
			io.NopCloser(bytes.NewReader([]byte("v"))))

		require.Error(t, err)
		require.NotContains(t, err.Error(), badRender)
	})

	t.Run("Set on an existing object", func(t *testing.T) {
		s3Store, _ := setupTestS3(t)
		require.NoError(t, s3Store.Set(context.Background(), key, fileformat.FileTypeTx, []byte("v")))

		err := s3Store.Set(context.Background(), key, fileformat.FileTypeTx, []byte("v"))

		require.Error(t, err)
		require.NotContains(t, err.Error(), badRender)
	})

	t.Run("Set when the upload fails", func(t *testing.T) {
		s3Store, mock := setupTestS3(t)
		mock.SetUploadError(errors.NewError("upload refused"))

		err := s3Store.Set(context.Background(), key, fileformat.FileTypeTx, []byte("v"))

		require.Error(t, err)
		require.NotContains(t, err.Error(), badRender)
	})

	t.Run("Del when the delete fails", func(t *testing.T) {
		s3Store, mock := setupTestS3(t)
		mock.SetDeleteError(errors.NewError("delete refused"))

		err := s3Store.Del(context.Background(), key, fileformat.FileTypeTx)

		require.Error(t, err)
		require.NotContains(t, err.Error(), badRender)
	})
}

// TestS3_MalformedStoredObjectIsAStorageFault covers the header branches. A blob
// that is present but unreadable is neither a miss nor a success: it must not be
// mapped to ErrNotFound, because the aerospike caller treats a miss as licence to
// fall back to a different representation, and a corrupt blob is evidence that this
// node's stored copy is wrong rather than absent.
func TestS3_MalformedStoredObjectIsAStorageFault(t *testing.T) {
	key := []byte("malformed")

	// storeRaw puts bytes under the key with no valid fileformat header.
	storeRaw := func(t *testing.T, mock *mockS3Client, s3Store *S3, raw []byte) {
		t.Helper()
		_, err := mock.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String("test-bucket"),
			Key:    s3Store.getObjectKey(key, fileformat.FileTypeTx, s3Store.options),
			Body:   bytes.NewReader(raw),
		})
		require.NoError(t, err)
	}

	t.Run("GetIoReader on a blob with no valid header", func(t *testing.T) {
		s3Store, mock := setupTestS3(t)
		storeRaw(t, mock, s3Store, []byte("not a header"))

		_, err := s3Store.GetIoReader(context.Background(), key, fileformat.FileTypeTx)

		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrNotFound),
			"a present but unreadable blob is not a miss — calling it one invites the caller's fallback")
		require.NotContains(t, err.Error(), "%!s(")
	})

	t.Run("Get on a blob with no valid header", func(t *testing.T) {
		s3Store, mock := setupTestS3(t)
		storeRaw(t, mock, s3Store, []byte("not a header"))

		_, err := s3Store.Get(context.Background(), key, fileformat.FileTypeTx)

		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrNotFound))
		require.NotContains(t, err.Error(), "%!s(")
	})

	t.Run("GetIoReader on a blob whose header is a different file type", func(t *testing.T) {
		s3Store, mock := setupTestS3(t)

		// A well-formed blob, stored under the .tx key but carrying a subtree header.
		header := fileformat.NewHeader(fileformat.FileTypeSubtree)
		storeRaw(t, mock, s3Store, append(header.Bytes(), []byte("body")...))

		_, err := s3Store.GetIoReader(context.Background(), key, fileformat.FileTypeTx)

		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrNotFound))
		require.NotContains(t, err.Error(), "%!s(")
	})
}
