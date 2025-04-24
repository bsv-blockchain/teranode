package batcher

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"io"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
)

type Batcher struct {
	logger           ulogger.Logger
	blobStore        blobStoreSetter
	sizeInBytes      int
	writeKeys        bool
	queue            *util.LockFreeQ[BatchItem]
	queueCtx         context.Context
	queueCancel      context.CancelFunc
	currentBatch     []byte
	currentBatchKeys []byte
}

type BatchItem struct {
	hash  chainhash.Hash
	value []byte
	// next  atomic.Pointer[BatchItem]
}

type blobStoreSetter interface {
	Health(ctx context.Context, checkLiveness bool) (int, string, error)
	Set(ctx context.Context, key []byte, value []byte, opts ...options.FileOption) error
}

func New(logger ulogger.Logger, blobStore blobStoreSetter, sizeInBytes int, writeKeys bool) *Batcher {
	ctx, cancel := context.WithCancel(context.Background())
	b := &Batcher{
		logger:           logger,
		blobStore:        blobStore,
		sizeInBytes:      sizeInBytes,
		writeKeys:        writeKeys,
		queue:            util.NewLockFreeQ[BatchItem](),
		queueCtx:         ctx,
		queueCancel:      cancel,
		currentBatch:     make([]byte, 0, sizeInBytes),
		currentBatchKeys: make([]byte, 0, sizeInBytes),
	}

	go func() {
		var (
			batchItem *BatchItem
			err       error
		)

		for {
			select {
			case <-b.queueCtx.Done():
				// Process remaining items before exiting
				for {
					batchItem = b.queue.Dequeue()
					if batchItem == nil {
						break
					}

					if err = b.processBatchItem(batchItem); err != nil {
						b.logger.Errorf("error processing batch item during shutdown: %v", err)
					}
				}
				// Write final batch if needed
				if len(b.currentBatch) > 0 {
					if err = b.writeBatch(b.currentBatch, b.currentBatchKeys); err != nil {
						b.logger.Errorf("error writing final batch during shutdown: %v", err)
					}
				}
				return
			default:
				batchItem = b.queue.Dequeue()
				if batchItem == nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				err = b.processBatchItem(batchItem)
				if err != nil {
					b.logger.Errorf("error processing batch item: %v", err)
				}
			}
		}
	}()

	return b
}

func (b *Batcher) processBatchItem(batchItem *BatchItem) error {
	// check whether our batch would overflow the size limit, or is zero, which means we have 1 big transaction
	currentPos := len(b.currentBatch)
	dataSize := len(batchItem.value)

	if currentPos+dataSize > b.sizeInBytes && currentPos > 0 {
		if err := b.writeBatch(b.currentBatch, b.currentBatchKeys); err != nil {
			return errors.NewStorageError("error writing batch", err)
		}

		b.currentBatch = make([]byte, 0, b.sizeInBytes)
		b.currentBatchKeys = make([]byte, 0, b.sizeInBytes)
	}

	// add to batch
	b.currentBatch = append(b.currentBatch, batchItem.value...)

	if b.writeKeys {
		// keys are written as a separate batch, with the position and size as the first 8 bytes
		// followed by the key and a carriage return
		hashLength := len(batchItem.hash)
		key := make([]byte, hashLength+8)

		copy(key[:hashLength], batchItem.hash[:])

		currentPosUint32, err := util.SafeIntToUint32(currentPos)
		if err != nil {
			return err
		}

		dataSizeUint32, err := util.SafeIntToUint32(dataSize)
		if err != nil {
			return err
		}

		binary.BigEndian.PutUint32(key[hashLength:hashLength+4], currentPosUint32)
		binary.BigEndian.PutUint32(key[hashLength+4:hashLength+8], dataSizeUint32)

		hexKey := hex.EncodeToString(key)
		hexKey += "\n"

		// append the bytes of the key
		b.currentBatchKeys = append(b.currentBatchKeys, []byte(hexKey)...)
	}

	return nil
}

func (b *Batcher) writeBatch(currentBatch []byte, batchKeys []byte) error {
	batchKey := make([]byte, 4)

	timeUint32, err := util.SafeIntToUint32(int(time.Now().Unix()))
	if err != nil {
		return err
	}

	// add the current time as the first bytes
	binary.BigEndian.PutUint32(batchKey, timeUint32)
	// add a random string as the next bytes, to prevent conflicting filenames from other pods
	randBytes := make([]byte, 4)
	_, _ = rand.Read(randBytes)
	batchKey = append(batchKey, randBytes...)

	g, gCtx := errgroup.WithContext(context.Background())

	// flush current batch
	g.Go(func() error {
		b.logger.Debugf("flushing batch of %d bytes", len(currentBatch))
		// we need to reverse the bytes of the key, since this is not a transaction ID
		if err := b.blobStore.Set(gCtx, utils.ReverseSlice(batchKey), currentBatch, options.WithFileExtension("data")); err != nil {
			return errors.NewStorageError("error putting batch", err)
		}

		return nil
	})

	if b.writeKeys {
		// flush current batch keys
		g.Go(func() error {
			// we need to reverse the bytes of the key, since this is not a transaction ID, but a batch ID
			if err := b.blobStore.Set(gCtx, utils.ReverseSlice(batchKey), batchKeys, options.WithFileExtension("keys")); err != nil {
				return errors.NewStorageError("error putting batch keys", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (b *Batcher) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	// just pass the health of the underlying blob store
	return b.blobStore.Health(ctx, checkLiveness)
}

func (b *Batcher) Close(_ context.Context) error {
	// Signal the background goroutine to stop
	b.queueCancel()

	// Wait a bit to ensure the goroutine has time to process remaining items
	time.Sleep(100 * time.Millisecond)

	return nil
}

func (b *Batcher) SetFromReader(ctx context.Context, key []byte, reader io.ReadCloser, opts ...options.FileOption) error {
	defer reader.Close()

	bb, err := io.ReadAll(reader)
	if err != nil {
		return errors.NewStorageError("failed to read data from reader", err)
	}

	return b.Set(ctx, key, bb, opts...)
}

func (b *Batcher) Set(_ context.Context, hash []byte, value []byte, opts ...options.FileOption) error {
	b.queue.Enqueue(BatchItem{
		hash:  chainhash.Hash(hash),
		value: value,
	})

	return nil
}

func (b *Batcher) SetDAH(_ context.Context, _ []byte, _ uint32, opts ...options.FileOption) error {
	return errors.NewProcessingError("DAH is not supported in a batcher store")
}

func (b *Batcher) GetDAH(_ context.Context, _ []byte, opts ...options.FileOption) (uint32, error) {
	return 0, errors.NewProcessingError("DAH is not supported in a batcher store")
}

func (b *Batcher) GetIoReader(_ context.Context, _ []byte, opts ...options.FileOption) (io.ReadCloser, error) {
	return nil, errors.NewStorageError("getIoReader is not supported in a batcher store")
}

func (b *Batcher) Get(_ context.Context, _ []byte, opts ...options.FileOption) ([]byte, error) {
	return nil, errors.NewStorageError("get is not supported in a batcher store")
}

func (b *Batcher) GetHead(_ context.Context, _ []byte, _ int, opts ...options.FileOption) ([]byte, error) {
	return nil, errors.NewStorageError("get head is not supported in a batcher store")
}

func (b *Batcher) Exists(_ context.Context, hash []byte, opts ...options.FileOption) (bool, error) {
	return false, errors.NewStorageError("exists is not supported in a batcher store")
}

func (b *Batcher) Del(_ context.Context, hash []byte, opts ...options.FileOption) error {
	return errors.NewStorageError("del is not supported in a batcher store")
}

func (b *Batcher) GetHeader(_ context.Context, _ []byte, _ ...options.FileOption) ([]byte, error) {
	return nil, errors.NewStorageError("get header is not supported in a batcher store")
}

func (b *Batcher) GetFooterMetaData(_ context.Context, _ []byte, _ ...options.FileOption) ([]byte, error) {
	return nil, errors.NewStorageError("get meta data is not supported in a batcher store")
}
