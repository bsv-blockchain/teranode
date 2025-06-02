// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"
)

// InvalidBlockHandler defines the interface for handling invalid block notifications.
// This interface allows the block validation service to notify the P2P service
// when a block is found to be invalid, so the P2P service can add ban score to
// the peer that sent the invalid block.
type InvalidBlockHandler interface {
	// ReportInvalidBlock is called when a block is found to be invalid.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Hash of the invalid block
	//   - reason: Reason for the block being invalid
	//
	// Returns an error if the peer cannot be found or the ban score cannot be added.
	ReportInvalidBlock(ctx context.Context, blockHash string, reason string) error
}
