// Package blockvalidation defines interfaces for block validation operations
package blockvalidation

import "context"

// InvalidBlockHandler defines the interface for handling invalid blocks
// This interface is used to report when a block has failed validation
// so that appropriate action can be taken (e.g., banning peers)
type InvalidBlockHandler interface {
	// ReportInvalidBlock is called when a block fails validation
	// blockHash is the hash of the invalid block
	// reason describes why the block is invalid
	ReportInvalidBlock(ctx context.Context, blockHash string, reason string) error
}
