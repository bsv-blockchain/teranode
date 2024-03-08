package subtreevalidation

import (
	"context"
	"errors"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

var (
	once sync.Once
)

func (u *SubtreeValidation) subtreeHandler(msg util.KafkaMessage) {
	if msg.Message != nil {
		startTime := time.Now()
		defer func() {
			prometheusSubtreeValidationValidateSubtreeHandler.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
		}()

		if len(msg.Message.Value) < 32 {
			u.logger.Errorf("Received subtree message of %d bytes", len(msg.Message.Value))
			return
		}

		hash, err := chainhash.NewHash(msg.Message.Value[:32])
		if err != nil {
			u.logger.Errorf("Failed to parse subtree hash from message: %v", err)
			return
		}

		var baseUrl string
		if len(msg.Message.Value) > 32 {
			baseUrl = string(msg.Message.Value[32:])
		}

		u.logger.Infof("Received subtree message for %s from %s", hash.String(), baseUrl)

		gotLock, err := tryLockIfNotExists(context.Background(), u.subtreeStore, hash)
		if err != nil {
			u.logger.Infof("error getting lock for Subtree %s", hash.String())
			return
		}

		if !gotLock {
			u.logger.Infof("Subtree %s already exists", hash.String())
			return
		}

		v := ValidateSubtree{
			SubtreeHash:   *hash,
			BaseUrl:       baseUrl,
			SubtreeHashes: nil,
			AllowFailFast: false,
		}

		// Call the validateSubtreeInternal method
		if err = u.validateSubtreeInternal(context.Background(), v); err != nil {
			u.logger.Errorf("Failed to validate subtree %s: %v", hash.String(), err)
		}
	}

}

type Exister interface {
	Exists(ctx context.Context, key []byte, opts ...options.Options) (bool, error)
}

func tryLockIfNotExists(ctx context.Context, exister Exister, hash *chainhash.Hash) (bool, error) {
	b, err := exister.Exists(ctx, hash[:])
	if err != nil {
		return false, err
	}
	if b {
		return false, nil
	}

	quorumPath, _ := gocore.Config().Get("subtree_quorum_path", "")

	if quorumPath == "" {
		return true, nil // Return true if no quorum path is set to tell upstream to process the subtree as if it were locked
	}

	once.Do(func() {
		if err := os.MkdirAll(quorumPath, 0755); err != nil {
			log.Fatalf("Failed to create quorum path: %v", err)
		}
	})

	lockPath := path.Join(quorumPath, hash.String()) + ".lock"

	// Attempt to acquire lock by atomically creating the lock file
	// The O_CREATE|O_EXCL|O_WRONLY flags ensure the file is created only if it does not already exist
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			// Failed to acquire lock (file already exists or other error)
			return false, nil
		}

		return false, err
	}
	_ = file.Close() // Close the file immediately after creating it

	// Release the lock after 30s or when context is cancelled
	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(30 * time.Second):
		}
		_ = os.Remove(lockPath)
	}()

	return true, nil
}
