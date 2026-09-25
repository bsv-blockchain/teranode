// Package clientfetch adapts the blockchain client's parent-link header fetch to the
// chainancestry builder's seam. It is a package of its own so that chainancestry imports nothing
// from model: the UTXO stores import chainancestry through their pruner interface, and model's
// tests import the UTXO stores, so a model import here would be an import cycle in test.
package clientfetch

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/chainancestry"
)

// HeaderClient is the one blockchain call the builder needs, in the shape the blockchain
// client and store both offer. It is an interface here so this package imports neither.
type HeaderClient interface {
	GetBlockHeadersByParentLinks(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
}

// ClientFetcher adapts a HeaderClient to the Fetcher seam.
type ClientFetcher struct {
	Client HeaderClient
}

// Headers walks parent links through the client and keeps only what the builder proves with.
func (f ClientFetcher) Headers(ctx context.Context, from chainhash.Hash, n uint32) ([]chainancestry.Row, error) {
	headers, metas, err := f.Client.GetBlockHeadersByParentLinks(ctx, &from, uint64(n))
	if err != nil {
		return nil, err
	}

	if len(metas) != len(headers) {
		return nil, errors.NewProcessingError("[chainancestry] %d headers with %d metas", len(headers), len(metas))
	}

	rows := make([]chainancestry.Row, 0, len(headers))

	for i, h := range headers {
		if h == nil || metas[i] == nil || h.HashPrevBlock == nil {
			return nil, errors.NewProcessingError("[chainancestry] nil header or meta at position %d", i)
		}

		rows = append(rows, chainancestry.Row{
			Hash:     *h.Hash(),
			PrevHash: *h.HashPrevBlock,
			Height:   metas[i].Height,
			ID:       metas[i].ID,
			MinedSet: metas[i].MinedSet,
		})
	}

	return rows, nil
}
