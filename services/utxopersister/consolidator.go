package utxopersister

import (
	"context"
	"io"
	"sort"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

type headerIfc interface {
	GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
}

type consolidator struct {
	logger           ulogger.Logger
	blockchainStore  headerIfc
	blockchainClient headerIfc
	blockStore       blob.Store
	insertCounter    uint64
	deletions        map[*UTXODeletion]struct{}
	additions        map[*UTXODeletion]*Addition
}

type Addition struct {
	Height   uint32
	Coinbase bool
	Order    uint64
	Value    uint64
	Script   []byte
}

func NewConsolidator(logger ulogger.Logger, blockchainStore headerIfc, blockchainClient headerIfc, blockStore blob.Store) *consolidator {
	return &consolidator{
		logger:           logger,
		blockchainStore:  blockchainStore,
		blockchainClient: blockchainClient,
		blockStore:       blockStore,
		deletions:        make(map[*UTXODeletion]struct{}),
		additions:        make(map[*UTXODeletion]*Addition),
	}
}

func (c *consolidator) ConsolidateBlockRange(ctx context.Context, startBlock, endBlock uint32) error {
	var (
		headers []*model.BlockHeader
		err     error
	)

	if c.blockchainStore != nil {
		headers, _, err = c.blockchainStore.GetBlockHeadersFromHeight(ctx, endBlock, endBlock-startBlock)
	} else {
		headers, _, err = c.blockchainClient.GetBlockHeadersFromHeight(ctx, endBlock, endBlock-startBlock)
	}

	if err != nil {
		return err
	}

	for i := len(headers) - 1; i >= 0; i-- {
		header := headers[i]
		hash := header.Hash()

		// Get the last 2 block headers from this last processed height
		us, err := GetUTXOSetWithExistCheck(ctx, c.logger, c.blockStore, hash)
		if err != nil {
			return err
		}

		if i != len(headers)-1 {
			r, err := us.GetUTXODeletionsReader(ctx)
			if err != nil {
				return err
			}

			magic, blockHash, blockHeight, err := GetHeaderFromReader(r)
			if err != nil {
				return err
			}

			c.logger.Infof("magic: %s, blockHash: %s, blockHeight: %d\n", magic, blockHash, blockHeight)

			// Ignore deletions for the first iteration
			if err := c.processDeletions(ctx, r); err != nil {
				return err
			}
		}

		r, err := us.GetUTXOAdditionsReader(ctx)
		if err != nil {
			return err
		}

		magic, blockHash, blockHeight, err := GetHeaderFromReader(r)
		if err != nil {
			return err
		}

		c.logger.Infof("magic: %s, blockHash: %s, blockHeight: %d\n", magic, blockHash, blockHeight)

		if err := c.processAdditions(ctx, r); err != nil {
			return err
		}
	}

	return nil
}

func (c *consolidator) processDeletions(ctx context.Context, r io.ReadCloser) error {
	for {
		ud, err := NewUTXODeletionFromReader(r)
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		// Check if the deletion is in the additions map
		if _, exists := c.additions[ud]; exists {
			delete(c.additions, ud)
		} else {
			c.deletions[ud] = struct{}{}
		}
	}

	return nil
}

func (c *consolidator) processAdditions(ctx context.Context, r io.ReadCloser) error {
	for {
		wrapper, err := NewUTXOWrapperFromReader(ctx, r)
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		txid := &wrapper.TxID

		// For each UTXO in the wrapper, check if it exists in the deletions map
		for _, utxo := range wrapper.UTXOs {
			key := &UTXODeletion{
				TxID:  txid,
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

	return nil
}

// Helper function to get sorted additions
func GetSortedAdditions(additions map[[36]byte]Addition) []Addition {
	sortedAdditions := make([]Addition, 0, len(additions))
	for _, addition := range additions {
		sortedAdditions = append(sortedAdditions, addition)
	}

	sort.Slice(sortedAdditions, func(i, j int) bool {
		return sortedAdditions[i].Order < sortedAdditions[j].Order
	})

	return sortedAdditions
}
