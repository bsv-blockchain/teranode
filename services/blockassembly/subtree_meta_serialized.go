package blockassembly

import (
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
)

// serializedInpointsMap is a parent tx map that keeps inpoints serialized (the
// in-memory SplitTxInpointsMap). It copies a record into dst while holding its
// own lock, so the bytes stay valid whatever the owner does to the map next.
type serializedInpointsMap interface {
	// AppendSerialized appends TxInpoints.Serialize's output for hash.
	AppendSerialized(dst []byte, hash chainhash.Hash) (out []byte, found bool, err error)
	// AppendMetaInpoints is AppendSerialized, refusing inpoints whose parent
	// hashes were never set, as Meta.Serialize does for every node but the first.
	AppendMetaInpoints(dst []byte, hash chainhash.Hash) (out []byte, found bool, err error)
}

var emptyInpointsWire = [4]byte{} // TxInpoints.Serialize of inpoints without parents

// buildSubtreeMetaBytes returns what subtreepkg.Meta.Serialize returns for the
// subtree with the inpoints of every node, without decoding the inpoints and
// encoding them again: it appends the stored wire bytes of each node to the
// output. Meta.Serialize's layout is the root hash, the node count, then the
// inpoints of every node in order; TestBuildSubtreeMetaBytes_MatchesSubtreeMeta
// pins this to its output.
//
// A node the map does not have is looked up in deletedTxs, the backup of the
// txs removed while the subtree was being stored. It fails where building the
// Meta and serializing it fails: a node with no inpoints anywhere, inpoints
// that cannot be serialized, or parent hashes that were never set on any node
// but the first.
func buildSubtreeMetaBytes(subtree *subtreepkg.Subtree, parents serializedInpointsMap, deletedTxs *txmap.SyncedMap[chainhash.Hash, subtreepkg.TxInpoints]) ([]byte, error) {
	nodes := subtree.Nodes

	// root hash, node count, and for a typical tx one parent with a vout or two
	out := make([]byte, 0, chainhash.HashSize+4+len(nodes)*(4+chainhash.HashSize+8))
	out = append(out, subtree.RootHash()[:]...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(nodes))) //nolint:gosec // bounded by the subtree size

	for idx, node := range nodes {
		if node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			if idx != 0 {
				return nil, errors.NewProcessingError("parent tx hashes are not set for the coinbase placeholder at node %d", idx)
			}

			out = append(out, emptyInpointsWire[:]...)

			continue
		}

		var (
			found bool
			err   error
		)

		if idx == 0 {
			out, found, err = parents.AppendSerialized(out, node.Hash)
		} else {
			out, found, err = parents.AppendMetaInpoints(out, node.Hash)
		}

		if err != nil {
			return nil, errors.NewProcessingError("inpoints of node %s cannot be serialized", node.Hash.String(), err)
		}

		if found {
			continue
		}

		if deletedTxs == nil {
			return nil, errors.NewProcessingError("inpoints of node %s not found", node.Hash.String())
		}

		deleted, found := deletedTxs.Get(node.Hash)
		if !found {
			return nil, errors.NewProcessingError("inpoints of node %s not found in the parent tx map or the deleted txs", node.Hash.String())
		}

		if idx != 0 && deleted.ParentTxHashes == nil {
			return nil, errors.NewProcessingError("parent tx hashes are not set for node %s", node.Hash.String())
		}

		out, err = deleted.AppendSerialized(out)
		if err != nil {
			return nil, errors.NewProcessingError("inpoints of node %s cannot be serialized", node.Hash.String(), err)
		}
	}

	return out, nil
}
