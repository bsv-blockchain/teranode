package s3

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
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
