// Package blockvalidation defines interfaces for block validation operations
package blockvalidation

import "context"

// InvalidBlockHandler defines the interface for handling invalid blocks
// This interface is used to report when a block has failed validation
// so that appropriate action can be taken (e.g., banning peers)
type InvalidBlockHandler interface {
	// ReportInvalidBlock is called when a block fails validation
	// blockHash is the hash of the invalid block
	// peerURL is the DataHub URL the block was fetched from (may be empty);
	// it serves as attribution fallback when the announcement record is gone
	// reason describes why the block is invalid
	ReportInvalidBlock(ctx context.Context, blockHash string, peerURL string, reason string) error
}
