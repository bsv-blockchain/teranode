// Package utxopersister creates and maintains up-to-date Unspent Transaction Output (UTXO) file sets
// for each block in the Teranode blockchain. Its primary function is to process the output of the
// Block Persister service (utxo-additions and utxo-deletions) and generate complete UTXO set files.
// The resulting UTXO set files can be exported and used to initialize the UTXO store in new Teranode instances.

package utxopersister

import (
	"context"
	"io"
	"sort"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
)

// headerIfc defines the interface for retrieving block headers.
// This interface abstracts access to blockchain data, allowing the consolidator to work
// with different implementations of blockchain storage or client services.
type headerIfc interface {
	GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
}

// consolidator manages the consolidation of UTXO additions and deletions.
// It processes additions and deletions across a range of blocks to create an accurate UTXO set.
// This type is responsible for resolving conflicts when outputs are both created and spent within
// the consolidated range, ensuring the final UTXO set is correct.
type consolidator struct {
	// logger provides logging functionality
	logger ulogger.Logger

	// settings contains configuration settings
	settings *settings.Settings

	// blockchainStore provides access to blockchain storage
	blockchainStore headerIfc

	// blockchainClient provides access to blockchain client
	blockchainClient headerIfc

	// blockStore provides access to block storage
	blockStore blob.Store

	// insertCounter tracks the order of additions
	insertCounter uint64

	// additions maps deletions to their corresponding additions
	additions map[UTXODeletion]*Addition

	// deletions stores the set of deletions
	deletions map[UTXODeletion]struct{}

	// firstBlockHeight stores the height of the first block in range - The first block height in the range (startHeight)
	firstBlockHeight uint32

	// firstPreviousBlockHash stores the hash of the block before the first block
	// This is the hash of the previous block of the first block in the range (will be used to copy the last UTXOSet)
	firstPreviousBlockHash *chainhash.Hash

	// lastBlockHeight stores the height of the last block
	lastBlockHeight uint32

	// lastBlockHash stores the hash of the last block
	lastBlockHash *chainhash.Hash

	// previousBlockHash stores the hash of the previous block
	previousBlockHash *chainhash.Hash
}

// Addition represents a UTXO addition.
// It contains information about a new unspent output, including its insertion order,
// value, height, script, and whether it's from a coinbase transaction.
// This structure is used during consolidation to track new outputs.
type Addition struct {
	// Order represents the insertion order
	Order uint64

	// Value represents the UTXO value
	Value uint64

	// Height represents the block height
	Height uint32

	// Script contains the locking script
	Script []byte

	// Coinbase indicates if this is a coinbase transaction
	Coinbase bool
}

// NewConsolidator creates a new consolidator instance.
// It initializes the consolidator with the provided dependencies and empty data structures.
// The consolidator needs access to blockchain data and storage capabilities to perform its operations.
// Returns a pointer to the initialized consolidator.
func NewConsolidator(logger ulogger.Logger, tSettings *settings.Settings, blockchainStore headerIfc, blockchainClient headerIfc, blockStore blob.Store, previousBlockHash *chainhash.Hash) *consolidator {
	return &consolidator{
		logger:                 logger,
		settings:               tSettings,
		blockchainStore:        blockchainStore,
		blockchainClient:       blockchainClient,
		blockStore:             blockStore,
		deletions:              make(map[UTXODeletion]struct{}),
		additions:              make(map[UTXODeletion]*Addition),
		firstPreviousBlockHash: previousBlockHash,
	}
}

// ConsolidateBlockRange consolidates UTXOs for the specified block range.
// It processes all blocks from startBlock to endBlock, retrieving their headers,
// and then processes the UTXO additions and deletions from each block.
// This builds up a comprehensive view of all UTXOs across the range.
// Returns an error if consolidation fails.
func (c *consolidator) ConsolidateBlockRange(ctx context.Context, startBlock, endBlock uint32) error {
	var (
		headers []*model.BlockHeader
		metas   []*model.BlockHeaderMeta
		err     error
	)

	if c.blockchainStore != nil {
		headers, metas, err = c.blockchainStore.GetBlockHeadersByHeight(ctx, startBlock, endBlock)
	} else {
		headers, metas, err = c.blockchainClient.GetBlockHeadersByHeight(ctx, startBlock, endBlock)
	}

	if err != nil {
		return err
	}

	c.firstBlockHeight = startBlock

	for i := 0; i < len(headers); i++ {
		header := headers[i]
		meta := metas[i]

		hash := header.Hash()
		previousHash := header.HashPrevBlock
		height := meta.Height

		c.logger.Infof("Processing header at height %d", height)

		if hash.String() == c.settings.ChainCfgParams.GenesisHash.String() {
			continue
		}

		// Get the last 2 block headers from this last processed height
		us, _, err := GetUTXOSetWithExistCheck(ctx, c.logger, c.settings, c.blockStore, hash)
		if err != nil {
			return err
		}

		r, err := us.GetUTXODeletionsReader(ctx)
		if err != nil {
			return err
		}

		if err := c.processDeletionsFromReader(ctx, r); err != nil {
			return err
		}

		r, err = us.GetUTXOAdditionsReader(ctx)
		if err != nil {
			return err
		}

		if err := c.processAdditionsFromReader(ctx, r); err != nil {
			return err
		}

		c.lastBlockHeight = height
		c.lastBlockHash = hash
		c.previousBlockHash = previousHash
	}

	c.logger.Infof("Finished processing headers at height %d", c.lastBlockHeight)

	return nil
}

// processDeletionsFromReader processes UTXO deletions from a reader.
// It reads serialized deletion records and updates the consolidator's internal state accordingly.
// When a deletion is encountered, it either removes a previously added output or records the deletion
// for future conflict resolution.
// Returns an error if processing fails.
func (c *consolidator) processDeletionsFromReader(ctx context.Context, r io.Reader) error {
	if err := checkMagic(r, "U-D-1.0"); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			outpoint, err := NewUTXODeletionFromReader(r)
			if err != nil {
				if err == io.EOF {
					return nil
				}

				return err
			}

			if _, exists := c.additions[*outpoint]; exists {
				delete(c.additions, *outpoint)
			} else {
				c.deletions[*outpoint] = struct{}{}
			}
		}
	}
}

// processAdditionsFromReader processes UTXO additions from a reader.
// It reads serialized UTXO addition records and updates the consolidator's internal state.
// Each addition is tracked unless it has already been marked as spent in the deletions map.
// Returns an error if processing fails.
func (c *consolidator) processAdditionsFromReader(ctx context.Context, r io.ReadCloser) error {
	if err := checkMagic(r, "U-A-1.0"); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			wrapper, err := NewUTXOWrapperFromReader(ctx, r)
			if err != nil {
				if err == io.EOF {
					return nil
				}

				return err
			}

			txid := &wrapper.TxID

			// For each UTXO in the wrapper, check if it exists in the deletions map
			for _, utxo := range wrapper.UTXOs {
				key := UTXODeletion{
					TxID:  *txid,
					Index: utxo.Index,
				}

				// Check if the addition is in the deletions map
				if _, exists := c.deletions[key]; exists {
					delete(c.deletions, key)
				} else {
					c.additions[key] = &Addition{
						Height:   wrapper.Height,
						Coinbase: wrapper.Coinbase,
						Order:    c.insertCounter,
						Value:    utxo.Value,
						Script:   utxo.Script,
					}

					c.insertCounter++
				}
			}
		}
	}
}

type keyAndVal struct {
	Key   UTXODeletion
	Value *Addition
}

// getSortedUTXOWrappers returns a sorted slice of UTXO wrappers.
// It converts the consolidator's internal map of additions into a sorted slice of UTXOWrappers.
// The sorting is based on transaction ID, which ensures deterministic output order.
// Returns the sorted slice of UTXOWrappers, ready to be written to the UTXO set.
func (c *consolidator) getSortedUTXOWrappers() []*UTXOWrapper {
	sortedAdditions := make([]*keyAndVal, 0, len(c.additions))

	for key, val := range c.additions {
		sortedAdditions = append(sortedAdditions, &keyAndVal{
			Key:   key,
			Value: val,
		})
	}

	sort.Slice(sortedAdditions, func(i, j int) bool {
		return sortedAdditions[i].Value.Order < sortedAdditions[j].Value.Order
	})

	wrappers := make([]*UTXOWrapper, 0, len(sortedAdditions))

	var (
		wrapper *UTXOWrapper
	)

	for _, addition := range sortedAdditions {
		if wrapper == nil || wrapper.TxID != addition.Key.TxID {
			if wrapper != nil {
				sort.Slice(wrapper.UTXOs, func(i, j int) bool {
					return wrapper.UTXOs[i].Index < wrapper.UTXOs[j].Index
				})

				wrappers = append(wrappers, wrapper)
			}

			wrapper = &UTXOWrapper{
				TxID:     addition.Key.TxID,
				Height:   addition.Value.Height,
				Coinbase: addition.Value.Coinbase,
				UTXOs: []*UTXO{
					{
						Index:  addition.Key.Index,
						Value:  addition.Value.Value,
						Script: addition.Value.Script,
					},
				},
			}
		} else {
			wrapper.UTXOs = append(wrapper.UTXOs, &UTXO{
				Index:  addition.Key.Index,
				Value:  addition.Value.Value,
				Script: addition.Value.Script,
			})
		}
	}

	if wrapper != nil {
		sort.Slice(wrapper.UTXOs, func(i, j int) bool {
			return wrapper.UTXOs[i].Index < wrapper.UTXOs[j].Index
		})

		wrappers = append(wrappers, wrapper)
	}

	return wrappers
}
