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

func (u *Server) subtreeHandler(msg util.KafkaMessage) {
	if msg.Message != nil {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

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

		gotLock, _, err := tryLockIfNotExists(ctx, u.subtreeStore, hash)
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
			AllowFailFast: true, // allow subtrees to fail fast, when getting from the network, will be retried if in a block
		}

		// Call the validateSubtreeInternal method
		if err = u.validateSubtreeInternal(ctx, v, util.GenesisActivationHeight); err != nil {
			u.logger.Errorf("Failed to validate subtree %s: %v", hash.String(), err)
		}
	}
}

type Exister interface {
	Exists(ctx context.Context, key []byte, opts ...options.Options) (bool, error)
}

func tryLockIfNotExists(ctx context.Context, exister Exister, hash *chainhash.Hash) (bool, bool, error) { // First bool is if the lock was acquired, second is if the subtree exists
	b, err := exister.Exists(ctx, hash[:])
	if err != nil {
		return false, false, err
	}
	if b {
		return false, true, nil
	}

	quorumPath, _ := gocore.Config().Get("subtree_quorum_path", "")
	quorumTimeout, _, _ := gocore.Config().GetDuration("subtree_quorum_timeout", 30*time.Second)

	if quorumPath == "" {
		return true, false, nil // Return true if no quorum path is set to tell upstream to process the subtree as if it were locked
	}

	once.Do(func() {
		log.Printf("creating subtree quorum path: %s", quorumPath)
		if err := os.MkdirAll(quorumPath, 0755); err != nil {
			log.Fatalf("Failed to create subtree quorum path: %v", err)
		}
	})

	lockFile := path.Join(quorumPath, hash.String()) + ".lock"

	// If the lock file already exists, the subtree is being processed by another node. However, the lock may be stale.
	// If the lock file is older than the quorum timeout, it is considered stale and can be removed.
	if info, err := os.Stat(lockFile); err == nil {
		if time.Since(info.ModTime()) > quorumTimeout {
			if err := os.Remove(lockFile); err != nil {
				log.Printf("ERROR: failed to remove stale lock file %q: %v", lockFile, err)
			}
		}
	}

	// Attempt to acquire lock by atomically creating the lock file
	// The O_CREATE|O_EXCL|O_WRONLY flags ensure the file is created only if it does not already exist
	file, err := os.OpenFile(lockFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			// Failed to acquire lock (file already exists or other error)
			return false, false, nil
		}

		return false, false, err
	}

	// Close the file immediately after creating it
	if err := file.Close(); err != nil {
		log.Printf("ERROR: failed to close lock file %q: %v", lockFile, err)
	}

	go func() {
		// Release the lock after 30s or when context is cancelled
		select {
		case <-ctx.Done():
		case <-time.After(quorumTimeout):
		}

		if err := os.Remove(lockFile); err != nil {
			log.Printf("ERROR: failed to remove lock file %q: %v", lockFile, err)
		}
	}()

	return true, false, nil
}
