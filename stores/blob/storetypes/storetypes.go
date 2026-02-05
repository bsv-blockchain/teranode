// Package storetypes defines blob store type constants.
// This package has no dependencies and can be safely imported by any package
// to avoid import cycles when referencing store types.
package storetypes

// BlobStoreType identifies the type of blob store.
// These values correspond to the BlobStoreType enum in blockchain_api.proto.
type BlobStoreType int32

const (
	TXSTORE             BlobStoreType = 0
	SUBTREESTORE        BlobStoreType = 1
	BLOCKSTORE          BlobStoreType = 2
	TEMPSTORE           BlobStoreType = 3
	BLOCKPERSISTERSTORE BlobStoreType = 4
)

// String returns the string representation of the store type.
func (t BlobStoreType) String() string {
	switch t {
	case TXSTORE:
		return "TXSTORE"
	case SUBTREESTORE:
		return "SUBTREESTORE"
	case BLOCKSTORE:
		return "BLOCKSTORE"
	case TEMPSTORE:
		return "TEMPSTORE"
	case BLOCKPERSISTERSTORE:
		return "BLOCKPERSISTERSTORE"
	default:
		return "UNKNOWN"
	}
}
