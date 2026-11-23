package subtreeprocessor

import (
	"context"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"golang.org/x/sync/errgroup"
)

// reorgDeltaNode is a node of a lost block that no winner block has, with where it sits.
type reorgDeltaNode struct {
	Hash chainhash.Hash
	// Block, Subtree and Node index the lost blocks as given, the block's subtrees and the
	// subtree's nodes
	Block, Subtree, Node int
}

// reorgDelta is the exact difference between the lost blocks and the winner blocks,
// without the coinbase placeholder on either side.
type reorgDelta struct {
	// D1 is the lost nodes the winners do not have, in the lost blocks' order: block as
	// given, then subtree, then node. A move-back inserts them in this order.
	D1 []reorgDeltaNode
	// D2 is the winner hashes no lost node matched, each once, ascending.
	D2 []chainhash.Hash
	// Hits is the number of lost nodes the winners have.
	Hits uint64
}

// buildUnionSet builds one frozen set of every leaf of the blocks' subtrees, with the
// reading and bucketing of CreateTransactionMap. The coinbase placeholder is in it once
// per block, so with several blocks the set has duplicates; Find and IterPositions
// treat them as one hash.
func (stp *SubtreeProcessor) buildUnionSet(ctx context.Context, blocks []*model.Block) (*SortedHashSet, error) {
	subtrees := make(map[chainhash.Hash]int)

	var txCount uint64

	for _, block := range blocks {
		txCount += block.TransactionCount

		for _, st := range block.Subtrees {
			// a subtree two blocks share holds the same hashes, and the set is a set
			if _, ok := subtrees[*st]; !ok {
				subtrees[*st] = len(subtrees)
			}
		}
	}

	set, _, err := stp.CreateTransactionMap(ctx, subtrees, len(subtrees), txCount)
	if err != nil {
		return nil, err
	}

	return set, nil
}

// computeReorgDelta probes every node of the lost blocks against the frozen winners
// set, a subtree at a time on up to workers goroutines, and returns both differences
// exactly. A hit sets one bit of a bitmap over the set's positions; D2 is the
// positions left unset. An unreadable lost subtree fails the whole call: a partial
// delta would silently drop transactions.
func (stp *SubtreeProcessor) computeReorgDelta(ctx context.Context, lost []*model.Block, winners *SortedHashSet, workers int) (*reorgDelta, error) {
	type job struct{ block, subtree int }

	var jobs []job

	for b, block := range lost {
		for s := range block.Subtrees {
			jobs = append(jobs, job{b, s})
		}
	}

	var (
		bitmap  = make([]atomic.Uint64, (winners.Positions()+63)/64)
		d1      = make([][]reorgDeltaNode, len(jobs))
		hits    = make([]uint64, len(jobs))
		maxSize = stp.settings.SubtreeValidation.MaxIncomingSubtreeBytes
	)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(max(1, workers))

	for j, jb := range jobs {
		g.Go(func() error {
			if err := gCtx.Err(); err != nil {
				return err
			}

			root := lost[jb.block].Subtrees[jb.subtree]

			reader, err := stp.subtreeStore.GetIoReader(gCtx, root[:], fileformat.FileTypeSubtree)
			if err != nil {
				reader, err = stp.subtreeStore.GetIoReader(gCtx, root[:], fileformat.FileTypeSubtreeToCheck)
				if err != nil {
					return errors.NewServiceError("error getting lost subtree: %s", root.String(), err)
				}
			}

			defer func() { _ = reader.Close() }()

			// One bucket keeps the leaves in node order. The subtree's own leaf count is only
			// read inside the deserializer, so size by the block's average (subtrees are full
			// but the last), bounded by what a subtree may hold, to skip the append doublings.
			hint := min(lost[jb.block].TransactionCount/uint64(len(lost[jb.block].Subtrees)), uint64(maxSize)/subtreeLeafBytes)
			leaves := map[uint16][]chainhash.Hash{0: make([]chainhash.Hash, 0, hint)}

			var conflicting []chainhash.Hash

			if err = deserializeHashesIntoBuckets(reader, func(chainhash.Hash) uint16 { return 0 }, maxSize, &leaves, &conflicting); err != nil {
				return errors.NewProcessingError("error deserializing lost subtree: %s", root.String(), err)
			}

			var jobHits uint64

			for n, h := range leaves[0] {
				if h == subtreepkg.CoinbasePlaceholderHashValue {
					continue
				}

				pos, ok := winners.Find(h)
				if !ok {
					d1[j] = append(d1[j], reorgDeltaNode{Hash: h, Block: jb.block, Subtree: jb.subtree, Node: n})
					continue
				}

				jobHits++

				// load first: most winners are hit once and the word is shared by 64 of them
				word, bit := &bitmap[pos/64], uint64(1)<<(pos%64)
				if word.Load()&bit == 0 {
					word.Or(bit)
				}
			}

			hits[j] = jobHits

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, errors.NewProcessingError("error reading the lost subtrees", err)
	}

	delta := &reorgDelta{}

	nD1 := 0
	for j := range jobs {
		nD1 += len(d1[j])
		delta.Hits += hits[j]
	}

	delta.D1 = make([]reorgDeltaNode, 0, nD1)
	for j := range jobs {
		delta.D1 = append(delta.D1, d1[j]...)
		d1[j] = nil // not held twice while the rest are copied
	}

	// each partition is walked by one goroutine, and partition p holds the hashes below
	// those of p+1, so the per-partition lists join in order
	d2 := make([][]chainhash.Hash, winners.Partitions())

	winners.IterPositions(workers, func(part int, pos uint64, h *chainhash.Hash) {
		if bitmap[pos/64].Load()&(1<<(pos%64)) == 0 && *h != subtreepkg.CoinbasePlaceholderHashValue {
			d2[part] = append(d2[part], *h)
		}
	})

	for _, p := range d2 {
		delta.D2 = append(delta.D2, p...)
	}

	return delta, nil
}
