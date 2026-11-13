package model

import (
	subtreepkg "github.com/bsv-blockchain/go-subtree"
)

// ParentSpendsMap is the interface for tracking spent inpoints during block validation.
// DiskParentSpendsMap (disk-backed, block_diskMapDirs) implements it. Without
// disk map directories duplicate inputs are found by the batched check in
// inpoint_dedup.go instead.
type ParentSpendsMap interface {
	// SetIfNotExists records the inpoint. inserted=true means newly recorded;
	// inserted=false means it was already present (a duplicate spend). A non-nil
	// error indicates a storage/capacity failure and MUST halt validation — the
	// caller must not treat an error as either "new" or "duplicate".
	SetIfNotExists(inpoint subtreepkg.Inpoint) (inserted bool, err error)
}
