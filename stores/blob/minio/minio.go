package minio

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Minio struct {
	client     *minio.Client
	bucketName string
	tempTTL    bool
	logger     ulogger.Logger
}

func New(logger ulogger.Logger, minioURL *url.URL) (*Minio, error) {
	logger = logger.New("minio")

	useSSL := minioURL.Scheme == "minios"
	secretAccessKey, _ := minioURL.User.Password()
	client, err := minio.New(minioURL.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(minioURL.User.Username(), secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, err
	}

	bucketName := minioURL.Path[1:]
	location := "us-east-1"
	if minioURL.Query().Get("location") != "" {
		location = minioURL.Query().Get("location")
	}

	objectLocking := false
	if minioURL.Query().Get("objectLocking") != "" {
		objectLocking = true
	}

	tempTTL := false
	if minioURL.Query().Get("tempTTL") != "" {
		tempTTL = true
	}

	err = client.MakeBucket(context.Background(), bucketName, minio.MakeBucketOptions{
		Region:        location,
		ObjectLocking: objectLocking,
	})
	if err != nil {
		exists, errBucketExists := client.BucketExists(context.Background(), bucketName)
		if errBucketExists == nil && exists {
			// log.Printf("We already own %s\n", bucketName)
		} else {
			return nil, fmt.Errorf("error creating bucket %s: %v", bucketName, err)
		}
	}

	// Define a Lifecycle configuration with a rule to expire (TTL) objects with a specific prefix after 30 days
	//lc := lifecycle.NewConfiguration()
	//lc.Rules = []lifecycle.Rule{{
	//	ID:     "expire-rule",
	//	Status: "Enabled",
	//	Prefix: "temp/", // Objects with the prefix 'temp/' will have this rule applied
	//	Expiration: lifecycle.Expiration{
	//		Days: lifecycle.ExpirationDays(1), // TTL set for 1 day
	//	},
	//}}
	//
	//// Set the lifecycle configuration on the bucket
	//err = client.SetBucketLifecycle(context.Background(), bucketName, lc)
	//if err != nil {
	//	return nil, fmt.Errorf("error setting bucket lifecycle: %v", err)
	//}

	return &Minio{
		client:     client,
		bucketName: bucketName,
		tempTTL:    tempTTL,
		logger:     logger,
	}, nil
}

func (m *Minio) Health(ctx context.Context) (int, string, error) {
	_, err := m.Exists(ctx, []byte("Health"))
	if err != nil {
		return -1, "Minio Store", err
	}

	return 0, "Minio Store", nil
}

func (m *Minio) Close(ctx context.Context) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_minio", true).NewStat("Close").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "minio:Close")
	defer traceSpan.Finish()

	return nil //m.client.Close()
}

func (m *Minio) SetFromReader(ctx context.Context, key []byte, reader io.ReadCloser, opts ...options.Options) error {
	defer reader.Close()

	b, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read data from reader: %w", err)
	}

	return m.Set(ctx, key, b, opts...)
}

func (m *Minio) Set(ctx context.Context, hash []byte, value []byte, opts ...options.Options) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_minio", true).NewStat("Set").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "minio:Set")
	defer traceSpan.Finish()

	objectName := utils.ReverseAndHexEncodeSlice(hash)
	bufReader := bytes.NewReader(value)

	if m.tempTTL {
		objectName = "temp/" + objectName
	}

	// Set the object lock mode and retention period
	objectOptions := minio.PutObjectOptions{
		ContentType:     "application/octet-stream",
		Mode:            minio.Governance,                  // or "COMPLIANCE". Governance allows for deletion/edit, compliance does not
		RetainUntilDate: time.Now().Add(120 * time.Minute), // todo fix this to be read from the opts or passed in signature
	}

	setOptions := options.NewSetOptions(opts...)
	if setOptions.TTL > 0 {
		objectOptions.RetainUntilDate = time.Now().Add(setOptions.TTL)
	}

	_, err := m.client.PutObject(ctx, m.bucketName, objectName, bufReader, int64(len(value)), objectOptions)
	if err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to set minio data: %w", err)
	}

	return nil
}

func (m *Minio) SetTTL(ctx context.Context, hash []byte, ttl time.Duration) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_minio", true).NewStat("SetTTL").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "minio:SetTTL")
	defer traceSpan.Finish()

	objectName := utils.ReverseAndHexEncodeSlice(hash)
	objectOptions := minio.PutObjectRetentionOptions{
		RetainUntilDate: nil,
	}

	if m.tempTTL {
		if _, err := m.client.CopyObject(ctx, minio.CopyDestOptions{
			Bucket: m.bucketName,
			Object: objectName,
		}, minio.CopySrcOptions{
			Bucket: m.bucketName,
			Object: "temp/" + objectName,
		}); err != nil {
			traceSpan.RecordError(err)
			return fmt.Errorf("failed to copy minio data: %w", err)
		}
	} else {
		if ttl > 0 {
			retention := time.Now().Add(ttl)
			objectOptions.RetainUntilDate = &retention
		}

		err := m.client.PutObjectRetention(ctx, m.bucketName, objectName, objectOptions)
		if err != nil {
			traceSpan.RecordError(err)
			return fmt.Errorf("failed to set minio retention options: %w", err)
		}
	}

	return nil
}

func (m *Minio) GetIoReader(ctx context.Context, key []byte) (io.ReadCloser, error) {
	b, err := m.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	return options.ReaderWrapper{Reader: bytes.NewBuffer(b), Closer: options.ReaderCloser{}}, nil
}

func (m *Minio) Get(ctx context.Context, hash []byte) ([]byte, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_minio", true).NewStat("Get").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "minio:Get")
	defer traceSpan.Finish()

	objectName := utils.ReverseAndHexEncodeSlice(hash)
	object, err := m.client.GetObject(ctx, m.bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		traceSpan.RecordError(err)
		return nil, fmt.Errorf("failed to get minio data: %w", err)
	}
	defer object.Close()

	var b []byte
	b, err = io.ReadAll(object)
	if err != nil && err != io.EOF {
		errResponse := minio.ToErrorResponse(err)
		if m.tempTTL && errResponse.Code == "NoSuchKey" {
			// look for the temp object
			object, err = m.client.GetObject(ctx, m.bucketName, "temp/"+objectName, minio.GetObjectOptions{})
			if err != nil {
				traceSpan.RecordError(err)
				return nil, fmt.Errorf("failed to get minio temp data: %w", err)
			}
			defer object.Close()

			b, err = io.ReadAll(object)
			if err != nil && err != io.EOF {
				traceSpan.RecordError(err)
				return nil, fmt.Errorf("failed to read minio temp data: %w", err)
			}
		} else {
			traceSpan.RecordError(err)
			return nil, fmt.Errorf("failed to read minio data: %w", err)
		}
	}

	return b, err
}

func (m *Minio) Exists(ctx context.Context, hash []byte) (bool, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_minio", true).NewStat("Exists").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "minio:Exists")
	defer traceSpan.Finish()

	objectName := utils.ReverseAndHexEncodeSlice(hash)
	_, err := m.client.StatObject(ctx, m.bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			_, err = m.client.StatObject(ctx, m.bucketName, "temp/"+objectName, minio.GetObjectOptions{})
			if err != nil {
				errResponse = minio.ToErrorResponse(err)
				if errResponse.Code == "NoSuchKey" {
					return false, nil
				}
				traceSpan.RecordError(err)
				return false, fmt.Errorf("failed to get minio data: %w", err)
			} else {
				return true, nil
			}
		}
		traceSpan.RecordError(err)
		return false, fmt.Errorf("failed to get minio data: %w", err)
	}

	return true, nil
}

func (m *Minio) Del(ctx context.Context, hash []byte) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_minio", true).NewStat("Del").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "minio:Del")
	defer traceSpan.Finish()

	objectName := utils.ReverseAndHexEncodeSlice(hash)
	err := m.client.RemoveObject(ctx, m.bucketName, objectName, minio.RemoveObjectOptions{
		GovernanceBypass: true,
	})
	if err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to del minio data: %w", err)
	}

	return nil
}
