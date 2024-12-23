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

type headerIfc interface {
	GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
}

type consolidator struct {
	logger           ulogger.Logger
	settings         *settings.Settings
	blockchainStore  headerIfc
	blockchainClient headerIfc
	blockStore       blob.Store
	insertCounter    uint64 // Used to order the additions
	additions        map[UTXODeletion]*Addition
	deletions        map[UTXODeletion]struct{}
	firstBlockHeight uint32 // The first block height in the range (startHeight)
	// firstBlockHash    *chainhash.Hash
	firstPreviousBlockHash *chainhash.Hash // This is the hash of the previous block of the first block in the range (will be used to copy the last UTXOSet)
	lastBlockHeight        uint32
	lastBlockHash          *chainhash.Hash // Used to name the new UTXOSet
	previousBlockHash      *chainhash.Hash // Used to put in the header of the new UTXOSet
}

type Addition struct {
	Order    uint64
	Value    uint64
	Height   uint32
	Script   []byte
	Coinbase bool
}

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

// Helper function to get sorted additions
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
