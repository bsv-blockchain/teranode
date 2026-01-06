// Package blockpersister provides comprehensive functionality for persisting blockchain blocks and their associated data.
// It handles block persistence, transaction processing, and UTXO set management across multiple storage backends.
package blockpersister

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/services/utxopersister/filestorer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/tracing"
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
	ctx, _, deferFn := tracing.Tracer("blockpersister").Start(pCtx, "ProcessSubtree",
		tracing.WithHistogram(prometheusBlockPersisterValidateSubtree),
		tracing.WithDebugLogMessage(u.logger, "[ProcessSubtree] called for subtree %s", subtreeHash.String()),
	)
	defer deferFn()

	// check whether the subtreeData already exists in the store
	subtreeDataExists, err := u.subtreeStore.Exists(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData)
	if err != nil {
		return errors.NewStorageError("[BlockPersister] error checking if subtree data exists for %s", subtreeHash.String(), err)
	}

	var subtreeData *subtreepkg.Data

	if subtreeDataExists {
		// Update DAH (Delete-At-Height) to persist the file
		err = u.subtreeStore.SetDAH(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData, 0)
		if err != nil {
			return errors.NewStorageError("[BlockPersister] error setting subtree data DAH for %s", subtreeHash.String(), err)
		}

		if utxoDiff != nil {
			// Subtree data already exists, load it to process UTXOs
			u.logger.Debugf("[BlockPersister] Subtree data for %s already exists, loading for UTXO processing", subtreeHash.String())

			subtreeData, err = u.readSubtreeData(ctx, subtreeHash)
			if err != nil {
				return err
			}
		}
	} else {
		// Subtree data doesn't exist, create it
		u.logger.Debugf("[BlockPersister] Subtree data for %s does not exist, creating", subtreeHash.String())

		// 1. get the subtree from the subtree store
		subtree, err := u.readSubtree(ctx, subtreeHash)
		if err != nil {
			return err
		}

		subtreeData = subtreepkg.NewSubtreeData(subtree)
		if subtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			if err = subtreeData.AddTx(coinbaseTx, 0); err != nil {
				return errors.NewProcessingError("[BlockPersister] error adding coinbase tx to subtree data", err)
			}
		}

		// 2. ...then attempt to load the txMeta from the store (i.e - aerospike in production)
		if err = u.processTxMetaUsingStore(ctx, subtree, subtreeData); err != nil {
			return errors.NewServiceError("[ValidateSubtreeInternal][%s] failed to get tx meta from store", subtreeHash.String(), err)
		}

		// add support for writing the subtree data from a reader
		subtreeDataBytes, err := subtreeData.Serialize()
		if err != nil {
			return errors.NewProcessingError("[BlockPersister] error serializing subtree data for %s", subtreeHash.String(), err)
		}

		if err = u.subtreeStore.Set(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData, subtreeDataBytes); err != nil {
			return errors.NewStorageError("[BlockPersister] error storing subtree data for %s", subtreeHash.String(), err)
		}
	}

	// 3. Process all transactions through UTXO diff to track additions and deletions
	// This always happens regardless of whether subtreeData already existed
	// Only process UTXO changes if utxoDiff is provided (i.e., UTXO file processing is enabled)
	if utxoDiff != nil {
		for _, tx := range subtreeData.Txs {
			if tx != nil {
				if err = utxoDiff.ProcessTx(tx); err != nil {
					return errors.NewProcessingError("error processing tx for UTXO", err)
				}
			}
		}
	}

	return nil
}

// readSubtreeData retrieves and deserializes subtree data from the subtree store.
//
// This internal method handles the two-stage process of loading subtree information:
// first retrieving the subtree structure itself, then loading the associated subtree data
// that contains the actual transaction references and metadata.
//
// The function performs these operations:
//  1. Retrieves the subtree structure from the subtree store using the provided hash
//  2. Deserializes the subtree to understand its structure and transaction organization
//  3. Retrieves the corresponding subtree data file containing transaction references
//  4. Deserializes the subtree data into a usable format for transaction processing
//
// Both the subtree and subtree data are stored as separate files in the blob store,
// with the subtree containing the hierarchical structure and the subtree data containing
// the actual transaction hashes and references needed for processing.
//
// Parameters:
//   - ctx: Context for the operation, enabling cancellation and timeout handling
//   - subtreeHash: Hash identifier of the subtree to retrieve and deserialize
//
// Returns:
//   - *util.SubtreeData: The deserialized subtree data ready for transaction processing
//   - error: Any error encountered during retrieval or deserialization
//
// Possible errors include storage access failures, file corruption, or deserialization
// issues. All errors are wrapped with appropriate context for debugging.
func (u *Server) readSubtreeData(ctx context.Context, subtreeHash chainhash.Hash) (*subtreepkg.Data, error) {
	// 1. get the subtree from the subtree store
	subtree, err := u.readSubtree(ctx, subtreeHash)
	if err != nil {
		return nil, errors.NewProcessingError("[BlockPersister] error reading subtree", err)
	}

	// 2 get the subtree data from the subtree store
	subtreeDataReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData)
	if err != nil {
		return nil, errors.NewStorageError("[BlockPersister] error getting subtree data for %s from store", subtreeHash.String(), err)
	}

	defer subtreeDataReader.Close()

	subtreeData, err := subtreepkg.NewSubtreeDataFromReader(subtree, subtreeDataReader)
	if err != nil {
		return nil, errors.NewProcessingError("[BlockPersister] error deserializing subtree data", err)
	}

	return subtreeData, nil
}

// readSubtree retrieves a subtree from the subtree store and deserializes it.
//
// This function is responsible for loading a subtree structure from persistent storage,
// which contains the hierarchical organization of transactions within a block.
// It retrieves the subtree file using the provided hash and deserializes it into a
// usable subtree object.
// The process includes:
//  1. Attempting to read the subtree from the store using the provided hash
//  2. If the primary read fails, it attempts to read from a secondary location (e.g., a backup store)
//  3. Deserializing the retrieved subtree data into a subtree object
//
// Parameters:
//   - ctx: Context for the operation, enabling cancellation and timeout handling
//   - subtreeHash: Hash identifier of the subtree to retrieve and deserialize
//
// Returns:
//   - *subtreepkg.Subtree: The deserialized subtree object ready for further processing
//   - error: Any error encountered during retrieval or deserialization
//
// Possible errors include storage access failures, file not found errors, or deserialization issues.
// All errors are wrapped with appropriate context for debugging.
func (u *Server) readSubtree(ctx context.Context, subtreeHash chainhash.Hash) (*subtreepkg.Subtree, error) {
	subtreeReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtree)
	if err != nil {
		subtreeReader, err = u.subtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeToCheck)
		if err != nil {
			return nil, errors.NewStorageError("[BlockPersister] failed to get subtree from store", err)
		}
	}

	defer subtreeReader.Close()

	subtree := &subtreepkg.Subtree{}
	if err = subtree.DeserializeFromReader(subtreeReader); err != nil {
		return nil, errors.NewProcessingError("[BlockPersister] failed to deserialize subtree", err)
	}

	return subtree, nil
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
func WriteTxs(_ context.Context, logger ulogger.Logger, writer *filestorer.FileStorer, txs []*bt.Tx, utxoDiff *utxopersister.UTXOSet) error {
	for i, tx := range txs {
		if tx == nil {
			logger.Errorf("[WriteTxs] txMeta.Tx is nil at index %d", i)
			continue
		}

		// always write the non-extended normal bytes to the subtree data file !
		// our peer node should extend the transactions if needed
		if _, err := writer.Write(tx.Bytes()); err != nil {
			return errors.NewProcessingError("error writing tx", err)
		}

		if utxoDiff != nil {
			// Process the utxo diff...
			if err := utxoDiff.ProcessTx(tx); err != nil {
				return errors.NewProcessingError("error processing tx", err)
			}
		}
	}

	return nil
}
