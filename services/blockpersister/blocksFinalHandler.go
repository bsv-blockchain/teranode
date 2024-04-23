package blockpersister

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	utxo_model "github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

var (
	once sync.Once
)

func (u *Server) blocksFinalHandler(msg util.KafkaMessage) {
	if msg.Message != nil {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		startTime := time.Now()
		defer func() {
			prometheusBlockPersisterValidateSubtreeHandler.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
		}()

		if len(msg.Message.Key) != 32 {
			u.logger.Errorf("Received blocksFinal message key %d bytes", len(msg.Message.Value))
			return
		}

		hash, err := chainhash.NewHash(msg.Message.Key[:])
		if err != nil {
			u.logger.Errorf("Failed to parse block hash from message: %v", err)
			return
		}

		gotLock, _, err := tryLockIfNotExists(ctx, u.blockStore, hash)
		if err != nil {
			u.logger.Infof("error getting lock for Subtree %s", hash.String())
			return
		}

		if !gotLock {
			u.logger.Infof("Block %s already being persisted", hash.String())
			return
		}

		if err := u.persistBlock(ctx, hash, msg.Message.Value); err != nil {
			if errors.Is(err, errors.ErrNotFound) {
				u.logger.Warnf("PreviousBlock %s not found, so UTXOSet not processed", hash.String())
			} else {
				u.logger.Errorf("Error persisting block %s: %v", hash.String(), err)
			}
			return
		}
	}
}

func (u *Server) persistBlock(ctx context.Context, hash *chainhash.Hash, blockBytes []byte) error {
	block, err := model.NewBlockFromBytes(blockBytes)
	if err != nil {
		return fmt.Errorf("error creating block from bytes: %w", err)
	}

	u.logger.Infof("[BlockPersister] Processing block %s (%d subtrees)...", block.Header.Hash().String(), len(block.Subtrees))

	reader, writer := io.Pipe()

	bufferedWriter := bufio.NewWriter(writer)

	// Create a new UTXO diff
	utxoDiff := utxo_model.NewUTXODiff(u.logger, block.Header.Hash())

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
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
		n, err := bufferedWriter.Write(block.Header.Bytes())
		if err != nil {
			writer.CloseWithError(err)
			return fmt.Errorf("error writing block header: %w", err)
		}
		if n != 80 {
			err := fmt.Errorf("error writing block header: wrote %d bytes, expected 80", n)
			writer.CloseWithError(err)
			return err
		}

		// Write varint of number of tx's
		if err = wire.WriteVarInt(bufferedWriter, 0, block.TransactionCount); err != nil {
			writer.CloseWithError(err)
			return fmt.Errorf("error writing transaction count: %w", err)
		}

		if _, err = bufferedWriter.Write(block.CoinbaseTx.Bytes()); err != nil {
			writer.CloseWithError(err)
			return fmt.Errorf("error writing coinbase tx: %w", err)
		}

		// Add coinbase utxos to the utxo diff
		utxoDiff.ProcessTx(block.CoinbaseTx)

		for _, subtreeHash := range block.Subtrees {
			// Call the validateSubtreeInternal method
			if err = u.processSubtree(gCtx, *subtreeHash, bufferedWriter, utxoDiff); err != nil {
				writer.CloseWithError(err)
				return fmt.Errorf(" subtree %s: %v", hash.String(), err)
			}
		}

		return nil
	})

	g.Go(func() error {
		// Write the block to the block store
		return u.blockStore.SetFromReader(ctx, hash[:], reader, options.WithFileExtension("block"), options.WithPrefixDirectory(10))
	})

	if err := g.Wait(); err != nil {
		return fmt.Errorf("error processing block %s: %w", hash.String(), err)
	}

	// At this point, we have a complete UTXODiff for this block.
	if err := utxoDiff.Persist(ctx, u.blockStore); err != nil {
		return fmt.Errorf("error persisting utxo diff: %w", err)
	}

	// 2. Now we need to apply this UTXODiff to the UTXOSet for the previous block
	previousUTXOSet, found := utxo_model.UTXOSetCache.Get(*block.Header.HashPrevBlock)
	if !found {
		// Load the UTXOSet from disk
		previousUTXOSet, err = utxo_model.LoadUTXOSet(u.blockStore, *block.Header.HashPrevBlock)
		if err != nil {
			return fmt.Errorf("OSet %s: %w", *block.Header.HashPrevBlock, err)
		}
	}

	// 1. Create a new UTXOSet for this block from the previous UTXOSet
	utxoSet := utxo_model.NewUTXOSetFromPrevious(block.Header.Hash(), previousUTXOSet)

	// 2. Remove all spent UTXOs
	utxoDiff.Removed.Iter(func(uk utxo_model.UTXOKey, uv *utxo_model.UTXOValue) (stop bool) {
		utxoSet.Delete(uk)
		return
	})

	// 3. Add all new UTXOs
	utxoDiff.Added.Iter(func(uk utxo_model.UTXOKey, uv *utxo_model.UTXOValue) (stop bool) {
		utxoSet.Add(uk, uv)

		return
	})

	if err := utxoSet.Persist(ctx, u.blockStore); err != nil {
		return fmt.Errorf("error persisting utxo set: %w", err)
	}

	utxo_model.UTXOSetCache.Put(*block.Header.Hash(), previousUTXOSet)

	return nil
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

	quorumPath, _ := gocore.Config().Get("block_quorum_path", "")
	quorumTimeout, _, _ := gocore.Config().GetDuration("block_quorum_timeout", 30*time.Second)

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
