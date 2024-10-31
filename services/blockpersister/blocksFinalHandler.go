package blockpersister

import (
	"context"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/utxopersister"
	"github.com/bitcoin-sv/ubsv/services/utxopersister/filestorer"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util/kafka"
	"github.com/bitcoin-sv/ubsv/util/quorum"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

var (
	once sync.Once
	q    *quorum.Quorum
)

func (u *Server) consumerMessageHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error {
	return func(msg *kafka.KafkaMessage) error {
		// this does manual commit so we need to implement error handling and differentiate between errors
		errCh := make(chan error, 1)
		go func() {
			errCh <- u.blocksFinalHandler(msg)
		}()

		select {
		case err := <-errCh:
			// if err is nil, it means function is successfully executed, return nil.
			if err == nil {
				return nil
			}

			if errors.Is(err, errors.ErrBlockExists) {
				// if block exists, it is not an error, so return nil to mark message as committed.
				return nil
			}

			// currently, the following cases are considered recoverable:
			// ERR_STORAGE_ERROR, ERR_SERVICE_ERROR
			// all other cases, including but not limited to, are considered as unrecoverable:
			// ERR_PROCESSING, ERR_BLOCK_EXISTS, ERR_INVALID_ARGUMENT

			// If error is not nil, check if the error is a recoverable error.
			// If it is a recoverable error, then return the error, so that it kafka message is not marked as committed.
			// So the message will be consumed again.
			if errors.Is(err, errors.ErrStorageError) || errors.Is(err, errors.ErrServiceError) {
				u.logger.Errorf("blocksFinalHandler failed: %v", err)
				return err
			}

			// error is not nil and not recoverable, so it is unrecoverable error, and it should not be tried again
			// kafka message should be committed, so return nil to mark message.
			u.logger.Errorf("Unrecoverable error (%v) processing kafka message %v for block persister block handler, marking Kafka message as completed.\n", msg, err)

			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (u *Server) blocksFinalHandler(msg *kafka.KafkaMessage) error {
	var err error

	once.Do(func() {
		quorumPath, _ := gocore.Config().Get("block_quorum_path", "")
		if quorumPath == "" {
			err = errors.NewConfigurationError("No block_quorum_path specified")
			return
		}

		var quorumTimeout time.Duration

		quorumTimeout, err, _ = gocore.Config().GetDuration("block_quorum_timeout", 10*time.Second)
		if err != nil {
			err = errors.NewConfigurationError("Bad block_quorum_timeout specified", err)
			return
		}

		q, err = quorum.New(
			u.logger,
			u.blockStore,
			quorumPath,
			quorum.WithTimeout(quorumTimeout),
			quorum.WithExtension("block"),
		)
	})

	if err != nil {
		return err
	}

	if msg != nil {
		ctx, _, deferFn := tracing.StartTracing(u.ctx, "blocksFinalHandler",
			tracing.WithHistogram(prometheusBlockPersisterValidateSubtreeHandler),
			tracing.WithLogMessage(u.logger, "[blocksFinalHandler] called for block %s", utils.ReverseAndHexEncodeSlice(msg.Key)),
		)
		defer deferFn()

		if len(msg.Key) != 32 {
			u.logger.Errorf("Received blocksFinal message key %d bytes", len(msg.Value))
			return errors.New(errors.ERR_INVALID_ARGUMENT, "Received subtree message of %d bytes", len(msg.Value))
		}

		var hash *chainhash.Hash

		hash, err = chainhash.NewHash(msg.Key)
		if err != nil {
			u.logger.Errorf("Failed to parse block hash from message: %v", err)
			return errors.New(errors.ERR_INVALID_ARGUMENT, "Failed to parse block hash from message", err)
		}

		var (
			gotLock   bool
			exists    bool
			releaseFn func()
		)

		// Create a new context with cancel function for the lock
		lockCtx, cancelLock := context.WithCancel(ctx)
		defer cancelLock() // Ensure the lock is released when persistBlock finishes

		gotLock, exists, releaseFn, err = q.TryLockIfNotExists(lockCtx, hash)
		if err != nil {
			u.logger.Infof("error getting lock for block %s: %v", hash.String(), err)
			return errors.NewProcessingError("error getting lock for block %s", hash.String(), err)
		}
		defer releaseFn()

		if exists {
			u.logger.Infof("Block %s already exists", hash.String())
			return errors.NewBlockExistsError("Block %s already exists", hash.String())
		}

		if !gotLock {
			u.logger.Infof("Block %s already being persisted", hash.String())
			return errors.NewBlockExistsError("Block %s already being persisted", hash.String())
		}

		if err = u.persistBlock(ctx, hash, msg.Value); err != nil {
			if errors.Is(err, errors.ErrNotFound) {
				u.logger.Warnf("PreviousBlock %s not found, so UTXOSet not processed", hash.String())

				err = nil
			} else {
				// don't wrap the error again, persistBlock should return the error in correct format
				u.logger.Errorf("Error persisting block %s: %v", hash.String(), err)
			}

			return err
		}
	}

	return nil
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

	// Create a new UTXO diff
	utxoDiff, err := utxopersister.NewUTXOSet(ctx, u.logger, u.blockStore, block.Header.Hash(), block.Height)
	if err != nil {
		return errors.NewProcessingError("error creating utxo diff", err)
	}

	if len(block.Subtrees) == 0 {
		// No subtrees to process, just write the coinbase UTXO to the diff and continue
		if err := utxoDiff.ProcessTx(block.CoinbaseTx); err != nil {
			return errors.NewProcessingError("error processing coinbase tx", err)
		}
	} else {
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(concurrency)

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
			// Don't wrap the error again, ProcessSubtree should return the error in correct format
			return err
		}
	}

	// At this point, we have a complete UTXODiff for this block.
	if err = utxoDiff.Close(); err != nil {
		return errors.NewStorageError("error closing utxo diff", err)
	}

	// Now, write the block file
	u.logger.Infof("[BlockPersister] Writing block %s to disk", block.Header.Hash().String())

	storer := filestorer.NewFileStorer(ctx, u.logger, u.blockStore, hash[:], "block")

	if _, err = storer.Write(blockBytes); err != nil {
		return errors.NewStorageError("error writing block to disk", err)
	}

	if err = storer.Close(ctx); err != nil {
		return errors.NewStorageError("error closing block file", err)
	}

	return nil
}
