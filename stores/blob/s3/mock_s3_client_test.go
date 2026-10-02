package s3

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/bsv-blockchain/teranode/errors"
)

type mockS3Client struct {
	mu    sync.RWMutex
	store map[string][]byte

	headObjectErr error

	// getObjectMissErr overrides the error returned for a key that is not in the
	// store, so a test can reproduce how the real SDK renders a 404 in the
	// configurations affected by aws/aws-sdk-go-v2#2084 (as NotFound rather than
	// NoSuchKey).
	getObjectMissErr error

	// downloadErr overrides Download directly, bypassing the GetObject delegation
	// below. The real client does not implement Download in terms of GetObject: it
	// calls transfermanager.GetObject (s3client.go), which issues its own
	// HeadObject on the concurrent/ranged path and so surfaces a miss as
	// *types.NotFound rather than *types.NoSuchKey. Injecting at this boundary is
	// the only way to exercise that shape, since the delegation cannot produce it.
	downloadErr error
}

func newMockS3Client() S3Client {
	return &mockS3Client{
		store: make(map[string][]byte),
	}
}

// Implement the S3Client interface - raw S3 operations only
func (m *mockS3Client) PutObject(ctx context.Context, input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := aws.ToString(input.Key)

	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}

	m.store[key] = data

	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3Client) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := aws.ToString(input.Key)

	data, ok := m.store[key]
	if !ok {
		if m.getObjectMissErr != nil {
			return nil, m.getObjectMissErr
		}

		return nil, &types.NoSuchKey{}
	}

	reader := io.NopCloser(bytes.NewReader(data))

	if input.Range != nil {
		rangeStr := aws.ToString(input.Range)

		start, end, err := parseRange(rangeStr, len(data))
		if err != nil {
			return nil, err
		}

		reader = io.NopCloser(bytes.NewReader(data[start : end+1]))
	}

	return &s3.GetObjectOutput{
		Body:          reader,
		ContentLength: aws.Int64(int64(len(data))),
	}, nil
}

func (m *mockS3Client) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.headObjectErr != nil {
		return nil, m.headObjectErr
	}

	key := aws.ToString(input.Key)
	data, ok := m.store[key]

	if !ok {
		return nil, &types.NoSuchKey{}
	}

	// Return a HeadObjectOutput with realistic metadata
	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String("application/octet-stream"),
		ETag:          aws.String("\"mock-etag\""),
		LastModified:  aws.Time(time.Now()),
		AcceptRanges:  aws.String("bytes"),
		VersionId:     aws.String("mock-version"),
	}, nil
}

func (m *mockS3Client) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := aws.ToString(input.Key)
	delete(m.store, key)

	return &s3.DeleteObjectOutput{}, nil
}

func (m *mockS3Client) CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{
		Bucket:   input.Bucket,
		Key:      input.Key,
		UploadId: aws.String("mock-upload-id"),
	}, nil
}

func (m *mockS3Client) UploadPart(ctx context.Context, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	return &s3.UploadPartOutput{
		ETag: aws.String("mock-etag"),
	}, nil
}

func (m *mockS3Client) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error) {
	return &s3.CompleteMultipartUploadOutput{
		Bucket: input.Bucket,
		Key:    input.Key,
	}, nil
}

func (m *mockS3Client) AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error) {
	return &s3.AbortMultipartUploadOutput{}, nil
}

func (m *mockS3Client) Upload(ctx context.Context, input *s3.PutObjectInput) error {
	_, err := m.PutObject(ctx, input)
	return err
}

func (m *mockS3Client) Download(ctx context.Context, input *s3.GetObjectInput) ([]byte, error) {
	m.mu.RLock()
	downloadErr := m.downloadErr
	m.mu.RUnlock()

	if downloadErr != nil {
		return nil, downloadErr
	}

	output, err := m.GetObject(ctx, input)
	if err != nil {
		return nil, err
	}
	defer output.Body.Close()

	return io.ReadAll(output.Body)
}

func parseRange(rangeStr string, size int) (start, end int, err error) {
	if len(rangeStr) < 7 || rangeStr[:6] != "bytes=" {
		return 0, 0, errors.NewError("invalid range format")
	}

	parts := strings.Split(rangeStr[6:], "-")
	if len(parts) != 2 {
		return 0, 0, errors.NewError("invalid range format")
	}

	start, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, err
	}

	end = size - 1
	if parts[1] != "" {
		end, err = strconv.Atoi(parts[1])
		if err != nil {
			return 0, 0, err
		}
	}

	if end >= size {
		end = size - 1
	}

	if start > end {
		return 0, 0, errors.NewError("invalid range")
	}

	return start, end, nil
}

func (m *mockS3Client) SetHeadObjectError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.headObjectErr = err
}

// SetGetObjectMissError sets the error GetObject returns for a key that is not in
// the store. Written under the same lock the read path takes, so a test that
// injects from one goroutine and reads from another stays race-free.
func (m *mockS3Client) SetGetObjectMissError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getObjectMissErr = err
}

// SetDownloadError makes Download fail directly, without going through GetObject.
func (m *mockS3Client) SetDownloadError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.downloadErr = err
}
