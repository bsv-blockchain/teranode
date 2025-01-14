package s3

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Client defines the interface for S3 operations we need
type S3Client interface {
	// Core operations
	PutObject(ctx context.Context, input *s3.PutObjectInput) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)

	// Upload operations
	CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, input *s3.UploadPartInput) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error)

	// Uploader/Downloader operations
	Upload(ctx context.Context, input *s3.PutObjectInput) (*manager.UploadOutput, error)
	Download(ctx context.Context, w io.WriterAt, input *s3.GetObjectInput) (n int64, err error)
}

// realS3Client wraps the actual AWS S3 client
type realS3Client struct {
	client     *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
}

// NewRealS3Client creates a new S3 client using AWS SDK
func NewRealS3Client(cfg aws.Config) S3Client {
	client := s3.NewFromConfig(cfg)

	return &realS3Client{
		client:     client,
		uploader:   manager.NewUploader(client),
		downloader: manager.NewDownloader(client),
	}
}

// Implement all methods for realS3Client...
func (c *realS3Client) PutObject(ctx context.Context, input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	return c.client.PutObject(ctx, input)
}

func (c *realS3Client) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return c.client.GetObject(ctx, input)
}

func (c *realS3Client) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	return c.client.HeadObject(ctx, input)
}

func (c *realS3Client) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	return c.client.DeleteObject(ctx, input)
}

func (c *realS3Client) CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error) {
	return c.client.CreateMultipartUpload(ctx, input)
}

func (c *realS3Client) UploadPart(ctx context.Context, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	return c.client.UploadPart(ctx, input)
}

func (c *realS3Client) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error) {
	return c.client.CompleteMultipartUpload(ctx, input)
}

func (c *realS3Client) AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error) {
	return c.client.AbortMultipartUpload(ctx, input)
}

func (c *realS3Client) Upload(ctx context.Context, input *s3.PutObjectInput) (*manager.UploadOutput, error) {
	return c.uploader.Upload(ctx, input)
}

func (c *realS3Client) Download(ctx context.Context, w io.WriterAt, input *s3.GetObjectInput) (n int64, err error) {
	return c.downloader.Download(ctx, w, input)
}
