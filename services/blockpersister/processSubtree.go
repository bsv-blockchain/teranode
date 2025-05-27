// Package blockpersister provides comprehensive functionality for persisting blockchain blocks and their associated data.
// It handles block persistence, transaction processing, and UTXO set management across multiple storage backends.
package blockpersister

import (
	"context"
	"encoding/binary"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/utxopersister"
	"github.com/bitcoin-sv/teranode/services/utxopersister/filestorer"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// ProcessSubtree processes a subtree of transactions, validating and storing them.
//
// A subtree represents a hierarchical structure containing transaction references that make up part of a block.
// This method retrieves a subtree from the subtree store, processes all the transactions it contains,
// and writes them to the block store while updating the UTXO set differences.
//
// The process follows these key steps:
//  1. Retrieve the subtree from the subtree store using its hash
//  2. Deserialize the subtree structure to extract transaction hashes
//  3. Load transaction metadata from the UTXO store, either in batch mode or individually
//  4. Create a file storer for writing transactions to persistent storage
//  5. Write all transactions and process their UTXO changes
//
// Transaction metadata retrieval can use batching if configured, which optimizes performance
// for high transaction volumes by reducing the number of individual store requests.
//
// Parameters:
//   - pCtx: Parent context for the operation, used for cancellation and tracing
//   - subtreeHash: Hash identifier of the subtree to process
//   - coinbaseTx: The coinbase transaction for the block containing this subtree
//   - utxoDiff: UTXO set difference tracker to record all changes resulting from processing
//
// Returns an error if any part of the subtree processing fails. Errors are wrapped with
// appropriate context to identify the specific failure point (storage, processing, etc.).
//
// Note: Processing is not atomic across multiple subtrees - each subtree is processed individually,
// allowing partial block processing to succeed even if some subtrees fail.
func (u *Server) ProcessSubtree(pCtx context.Context, subtreeHash chainhash.Hash, coinbaseTx *bt.Tx, utxoDiff *utxopersister.UTXOSet) error {
	ctx, _, deferFn := tracing.StartTracing(pCtx, "ProcessSubtree",
		tracing.WithHistogram(prometheusBlockPersisterValidateSubtree),
		tracing.WithDebugLogMessage(u.logger, "[ProcessSubtree] called for subtree %s", subtreeHash.String()),
	)
	defer deferFn()

	// 1. get the subtree data from the subtree store
	subtreeData, err := u.readSubtreeData(ctx, subtreeHash)
	if err != nil {
		return err
	}

	txHashes := make([]chainhash.Hash, len(subtreeData.Txs))

	for i := 0; i < len(subtreeData.Txs); i++ {
		if subtreeData.Txs[i] != nil {
			txHashes[i] = *subtreeData.Txs[i].TxIDChainHash()

			continue
		}

		if i == 0 {
			txHashes[i] = util.CoinbasePlaceholderHashValue
		}
	}

	// txMetaSlice will be populated with the txMeta data for each txHash
	txMetaSlice := make([]*meta.Data, len(txHashes))

	if txHashes[0].Equal(util.CoinbasePlaceholderHashValue) {
		txMetaSlice[0] = &meta.Data{Tx: coinbaseTx}
	}

	batched := u.settings.Block.BatchMissingTransactions

	// 2. ...then attempt to load the txMeta from the store (i.e - aerospike in production)
	missed, err := u.processTxMetaUsingStore(ctx, txHashes, txMetaSlice, batched)
	if err != nil {
		return errors.NewServiceError("[ValidateSubtreeInternal][%s] failed to get tx meta from store", subtreeHash.String(), err)
	}

	if missed > 0 {
		for i := 0; i < len(txHashes); i++ {
			u.logger.Errorf("[ValidateSubtreeInternal][%s] failed to get tx meta from store for tx %s", subtreeHash.String(), txHashes[i].String())
		}

		return errors.NewServiceError("[ValidateSubtreeInternal][%s] failed to get %d of %d tx meta from store", subtreeHash.String(), missed, len(txHashes))
	}

	storer, err := filestorer.NewFileStorer(context.Background(), u.logger, u.settings, u.blockStore, subtreeHash[:], options.SubtreeDataFileExtension)
	if err != nil {
		return errors.NewStorageError("error creating subtree file", err)
	}
	defer storer.Close(context.Background())

	if err := WriteTxs(context.Background(), u.logger, storer, txMetaSlice, utxoDiff); err != nil {
		return errors.NewProcessingError("[BlockPersister] error writing txs", err)
	}

	return nil
}

func (u *Server) readSubtreeData(ctx context.Context, subtreeHash chainhash.Hash) (*util.SubtreeData, error) {
	// 1. get the subtree from the subtree store
	subtreeReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes(), options.WithFileExtension(options.SubtreeFileExtension))
	if err != nil {
		return nil, errors.NewStorageError("[BlockPersister] failed to get subtree from store", err)
	}
	defer subtreeReader.Close()

	subtree := &util.Subtree{}
	if err := subtree.DeserializeFromReader(subtreeReader); err != nil {
		return nil, errors.NewProcessingError("[BlockPersister] failed to deserialize subtree", err)
	}

	// 2 get the subtree data from the subtree store
	subtreeDataReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes(), options.WithFileExtension(options.SubtreeDataFileExtension))
	if err != nil {
		return nil, errors.NewStorageError("[BlockPersister] error getting subtree data for %s from store", subtreeHash.String(), err)
	}

	defer subtreeDataReader.Close()

	subtreeData, err := util.NewSubtreeDataFromReader(subtree, subtreeDataReader)
	if err != nil {
		return nil, errors.NewProcessingError("[BlockPersister] error deserializing subtree data", err)
	}

	return subtreeData, nil
}

// WriteTxs writes a series of transactions to storage and processes their UTXO changes.
//
// This function handles the final persistence of transaction data to storage and optionally
// processes UTXO set changes. It's a critical component in the block persistence pipeline
// that ensures transactions are properly serialized and stored.
//
// The function performs the following steps:
//  1. Write the number of transactions as a 32-bit integer header
//  2. For each transaction in the provided slice:
//     a. Write the raw transaction bytes to storage
//     b. If a UTXO diff is provided, process the transaction's UTXO changes
//  3. Report any errors or validation issues encountered
//
// The function includes safety checks to handle nil transaction metadata or transactions,
// logging errors but continuing processing when possible to maximize resilience.
//
// Parameters:
//   - ctx: Context for the operation, enabling cancellation and tracing
//   - logger: Logger for recording operations, errors, and warnings
//   - writer: FileStorer destination for writing serialized transaction data
//   - txMetaSlice: Slice of transaction metadata objects containing the transactions to write
//   - utxoDiff: UTXO set difference tracker (optional, can be nil if UTXO tracking not needed)
//
// Returns an error if writing fails at any point. Specific error conditions include:
//   - Failure to write the transaction count header
//   - Failure to write individual transaction data
//   - Errors during UTXO processing for transactions
//
// The operation is not fully atomic - some transactions may be written successfully even if
// others fail. The caller should handle partial success scenarios appropriately.
func WriteTxs(ctx context.Context, logger ulogger.Logger, writer *filestorer.FileStorer, txMetaSlice []*meta.Data, utxoDiff *utxopersister.UTXOSet) error {
	// Write the number of txs in the subtree
	//      this makes it impossible to stream directly from S3 to the client
	if err := binary.Write(writer, binary.LittleEndian, uint32(len(txMetaSlice))); err != nil {
		return errors.NewProcessingError("error writing number of txs", err)
	}

	for i := 0; i < len(txMetaSlice); i++ {
		txMeta := txMetaSlice[i]
		if txMeta == nil {
			logger.Errorf("[WriteTxs] txMeta is nil at index %d", i)
			continue
		}

		if txMeta.Tx == nil {
			logger.Errorf("[WriteTxs] txMeta.Tx is nil at index %d", i)
			continue
		}

		if _, err := writer.Write(txMeta.Tx.Bytes()); err != nil {
			return errors.NewProcessingError("error writing tx", err)
		}

		if utxoDiff != nil {
			// Process the utxo diff...
			if err := utxoDiff.ProcessTx(txMetaSlice[i].Tx); err != nil {
				return errors.NewProcessingError("error processing tx", err)
			}
		}
	}

	return nil
}
