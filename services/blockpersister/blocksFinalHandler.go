package blockpersister

import (
	"bufio"
	"context"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockpersister/utxo"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

var (
	once sync.Once
)

func (u *Server) blocksFinalHandler(msg util.KafkaMessage) {
	var err error

	defer func() {
		if msg.Message != nil && err == nil {
			msg.Session.MarkMessage(msg.Message, "")
			msg.Session.Commit()
		}
	}()

	if msg.Message != nil {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ctx, _, deferFn := tracing.StartTracing(ctx, "blocksFinalHandler",
			tracing.WithHistogram(prometheusBlockPersisterValidateSubtreeHandler),
			tracing.WithLogMessage(u.logger, "[blocksFinalHandler] called for block %s", utils.ReverseAndHexEncodeSlice(msg.Message.Key)),
		)
		defer deferFn()

		if len(msg.Message.Key) != 32 {
			u.logger.Errorf("Received blocksFinal message key %d bytes", len(msg.Message.Value))
			return
		}

		var hash *chainhash.Hash

		hash, err = chainhash.NewHash(msg.Message.Key[:])
		if err != nil {
			u.logger.Errorf("Failed to parse block hash from message: %v", err)
			return
		}

		var gotLock bool
		var exists bool

		gotLock, exists, err = tryLockIfNotExists(ctx, u.logger, hash, u.blockStore, options.WithFileExtension("block"))
		if err != nil {
			u.logger.Infof("error getting lock for Subtree %s: %v", hash.String(), err)
			return
		}

		if exists {
			u.logger.Infof("Block %s already exists", hash.String())
			return
		}

		if !gotLock {
			u.logger.Infof("Block %s already being persisted", hash.String())
			return
		}

		if err = u.persistBlock(ctx, hash, msg.Message.Value); err != nil {
			if errors.Is(err, errors.ErrNotFound) {
				u.logger.Warnf("PreviousBlock %s not found, so UTXOSet not processed", hash.String())
				err = nil
			} else {
				u.logger.Errorf("Error persisting block %s: %v", hash.String(), err)
			}
			return
		}
	}
}

func (u *Server) persistBlock(ctx context.Context, hash *chainhash.Hash, blockBytes []byte) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "persistBlock",
		tracing.WithHistogram(prometheusBlockPersisterPersistBlock),
		tracing.WithLogMessage(u.logger, "[persistBlock] called for block %s", hash.String()),
	)
	defer deferFn()

	block, err := model.NewBlockFromBytes(blockBytes)
	if err != nil {
		return errors.NewProcessingError("error creating block from bytes", err)
	}

	u.logger.Infof("[BlockPersister] Processing block %s (%d subtrees)...", block.Header.Hash().String(), len(block.Subtrees))

	concurrency, _ := gocore.Config().GetInt("blockpersister_concurrency", 8)
	u.logger.Infof("[BlockPersister] Processing subtrees with concurrency %d", concurrency)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	// Create a new UTXO diff
	utxoDiff, err := utxo.NewUTXODiff(ctx, u.logger, u.blockStore, block.Header.Hash())
	if err != nil {
		return errors.NewProcessingError("error creating utxo diff", err)
	}

	// Add coinbase utxos to the utxo diff
	utxoDiff.ProcessTx(block.CoinbaseTx)

	for i, subtreeHash := range block.Subtrees {
		subtreeHash := subtreeHash
		i := i

		g.Go(func() error {
			u.logger.Infof("[BlockPersister] processing subtree %d / %d [%s]", i+1, len(block.Subtrees), subtreeHash.String())

			return u.ProcessSubtree(gCtx, *subtreeHash, block.CoinbaseTx, utxoDiff)
		})
	}

	u.logger.Infof("[BlockPersister] writing UTXODiff for block %s", block.Header.Hash().String())

	if err = g.Wait(); err != nil {
		return errors.NewProcessingError("error processing subtrees", err)
	}

	// At this point, we have a complete UTXODiff for this block.
	if err = utxoDiff.Close(); err != nil {
		return errors.NewStorageError("error closing utxo diff", err)
	}

	// Now, write the block file
	u.logger.Infof("[BlockPersister] Writing block %s to disk", block.Header.Hash().String())

	reader, writer := io.Pipe()

	bufferedWriter := bufio.NewWriter(writer)

	go func() {
		defer func() {
			// Flush the buffer and close the writer with error handling
			if err := bufferedWriter.Flush(); err != nil {
				u.logger.Errorf("Error flushing writer: %v", err)
			}

			if err := writer.CloseWithError(nil); err != nil {
				u.logger.Errorf("Error closing writer: %v", err)
			}
		}()

		// Write 80 byte block header
		_, err := bufferedWriter.Write(blockBytes)
		if err != nil {
			u.logger.Errorf("Error writing block: %v", err)
			writer.CloseWithError(err)
			return
		}
	}()

	// Items with TTL get written to base folder, so we need to set the TTL here and will remove it when the file is written.
	// With the lustre store, removing the TTL will move the file to the S3 folder which tells lustre to move it to an S3 bucket on AWS.
	if err = u.blockStore.SetFromReader(ctx, hash[:], reader, options.WithFileExtension("block"), options.WithTTL(24*time.Hour)); err != nil {
		return errors.NewStorageError("[BlockPersister] error persisting block", err)
	}

	if err = u.blockStore.SetTTL(ctx, hash[:], 0, options.WithFileExtension("block")); err != nil {
		return errors.NewStorageError("[BlockPersister] error persisting block", err)
	}

	if gocore.Config().GetBool("blockPersister_processUTXOSets", false) {
		u.logger.Infof("[BlockPersister] Processing UTXOSet for block %s", block.Header.Hash().String())

		// 1. Load the deletions file for this block in to a set
		// 2. Open the previous UTXOSet for the previous block
		// 3. Open a new UTXOSet for this block
		// 4. Stream each record and write to new UTXOSet if not in the deletions set
		// 5. Open the additions file for this block and stream each record to the new UTXOSet if not in the deletions set
		// 6. Close the new UTXOSet and write to disk

	}

	return nil
}

type Exister interface {
	Exists(ctx context.Context, key []byte, opts ...options.Options) (bool, error)
}

func tryLockIfNotExists(ctx context.Context, logger ulogger.Logger, hash *chainhash.Hash, exister Exister, opts ...options.Options) (bool, bool, error) { // First bool is if the lock was acquired, second is if the item already exists
	b, err := exister.Exists(ctx, hash[:], opts...)
	if err != nil {
		return false, false, err
	}
	if b {
		return false, true, nil
	}

	quorumPath, _ := gocore.Config().Get("block_quorum_path", "")
	if quorumPath == "" {
		return true, false, nil // Return true if no quorum path is set to tell upstream to process the subtree as if it were locked
	}

	once.Do(func() {
		logger.Infof("Creating block quorum path %s", quorumPath)
		if err := os.MkdirAll(quorumPath, 0755); err != nil {
			logger.Fatalf("Failed to create block quorum path: %v", err)
		}
	})

	lockFile := path.Join(quorumPath, hash.String()) + ".lock"

	// If the lock file already exists, the block is being processed by another node. However, the lock may be stale.
	// If the lock file mtime is more than 10 seconds old it is considered stale and can be removed.
	if info, err := os.Stat(lockFile); err == nil {
		if time.Since(info.ModTime()) > 10*time.Second {
			if err := os.Remove(lockFile); err != nil {
				logger.Warnf("failed to remove stale lock file %q: %v", lockFile, err)
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
		logger.Warnf("failed to close lock file %q: %v", lockFile, err)
	}

	go func() {
		// Initialize ticker to update the lock file every 5 seconds
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				if err := os.Remove(lockFile); err != nil {
					logger.Warnf("failed to remove lock file %q: %v", lockFile, err)
				}
				return
			case <-ticker.C:
				// Touch the lock file by updating its access and modification times to the current time
				now := time.Now()
				if err := os.Chtimes(lockFile, now, now); err != nil {
					logger.Warnf("failed to update lock file %q: %v", lockFile, err)
				}
			}
		}
	}()

	return true, false, nil
}
