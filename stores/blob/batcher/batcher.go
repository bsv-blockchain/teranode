package batcher

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
)

type Batcher struct {
	logger           ulogger.Logger
	blobStore        BlobStore
	sizeInBytes      int
	writeKeys        bool
	queue            *util.LockFreeQ[BatchItem]
	queueCtx         context.Context
	currentBatch     []byte
	currentBatchKeys []byte
}

type BatchItem struct {
	hash  chainhash.Hash
	value []byte
	next  atomic.Pointer[BatchItem]
}

type BlobStore interface {
	Health(ctx context.Context) (int, string, error)
	Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error
}

func New(logger ulogger.Logger, blobStore BlobStore, sizeInBytes int, writeKeys bool) *Batcher {
	b := &Batcher{
		logger:           logger,
		blobStore:        blobStore,
		sizeInBytes:      sizeInBytes,
		writeKeys:        writeKeys,
		queue:            util.NewLockFreeQ[BatchItem](),
		queueCtx:         context.Background(),
		currentBatch:     make([]byte, 0, sizeInBytes),
		currentBatchKeys: make([]byte, 0, sizeInBytes),
	}

	go func() {
		var batchItem *BatchItem
		var err error
		for {
			select {
			case <-b.queueCtx.Done():
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
			return fmt.Errorf("error writing batch: %v", err)
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

		binary.BigEndian.PutUint32(key[hashLength:hashLength+4], uint32(currentPos))
		binary.BigEndian.PutUint32(key[hashLength+4:hashLength+8], uint32(dataSize))

		hexKey := hex.EncodeToString(key)
		hexKey += "\n"

		// append the bytes of the key
		b.currentBatchKeys = append(b.currentBatchKeys, []byte(hexKey)...)
	}

	return nil
}

func (b *Batcher) writeBatch(currentBatch []byte, batchKeys []byte) error {
	batchKey := make([]byte, 4)
	// add the current time as the first bytes
	binary.BigEndian.PutUint32(batchKey, uint32(time.Now().Unix()))
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
			return fmt.Errorf("error putting batch: %v", err)
		}
		return nil
	})

	if b.writeKeys {
		// flush current batch keys
		g.Go(func() error {
			// we need to reverse the bytes of the key, since this is not a transaction ID, but a batch ID
			if err := b.blobStore.Set(gCtx, utils.ReverseSlice(batchKey), batchKeys, options.WithFileExtension("keys")); err != nil {
				return fmt.Errorf("error putting batch keys: %v", err)
			}
			return nil
		})

	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (b *Batcher) Health(ctx context.Context) (int, string, error) {
	// just pass the health of the underlying blob store
	return b.blobStore.Health(ctx)
}

func (b *Batcher) Close(_ context.Context) error {
	// close the context, no more additions to the queue
	b.queueCtx.Done()
	time.Sleep(10 * time.Millisecond)

	// dequeue all items
	for {
		batchItem := b.queue.Dequeue()
		if batchItem == nil {
			break
		}
		if err := b.processBatchItem(batchItem); err != nil {
			b.logger.Errorf("error processing batch item in Close: %v", err)
		}
	}

	// save the last batch we have in RAM
	if len(b.currentBatch) > 0 {
		if err := b.writeBatch(b.currentBatch, b.currentBatchKeys); err != nil {
			b.logger.Errorf("error writing final batch in Close: %v", err)
		}
	}

	return nil
}

func (b *Batcher) SetFromReader(ctx context.Context, key []byte, reader io.ReadCloser, opts ...options.Options) error {
	defer reader.Close()

	bb, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read data from reader: %w", err)
	}

	return b.Set(ctx, key, bb, opts...)
}

func (b *Batcher) Set(_ context.Context, hash []byte, value []byte, opts ...options.Options) error {
	b.queue.Enqueue(BatchItem{
		hash:  chainhash.Hash(hash),
		value: value,
	})

	return nil
}

func (b *Batcher) SetTTL(_ context.Context, _ []byte, _ time.Duration) error {
	return errors.New("TTL is not supported in a batcher store")
}

func (b *Batcher) GetIoReader(_ context.Context, _ []byte) (io.ReadCloser, error) {
	return nil, fmt.Errorf("getIoReader is not supported in a batcher store")
}

func (b *Batcher) Get(_ context.Context, _ []byte) ([]byte, error) {
	return nil, fmt.Errorf("get is not supported in a batcher store")
}

func (b *Batcher) GetHead(_ context.Context, _ []byte, _ int) ([]byte, error) {
	return nil, fmt.Errorf("get head is not supported in a batcher store")
}

func (b *Batcher) Exists(_ context.Context, hash []byte) (bool, error) {
	return false, fmt.Errorf("exists is not supported in a batcher store")
}

func (b *Batcher) Del(_ context.Context, hash []byte) error {
	return fmt.Errorf("del is not supported in a batcher store")
}
