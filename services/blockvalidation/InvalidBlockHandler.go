// Package blockvalidation implements block validation for Bitcoin SV nodes in Teranode.
package blockvalidation

import (
	"context"
	"sync"

	"github.com/bitcoin-sv/teranode/errors"
)

// InvalidBlockHandler defines an interface for handling invalid block notifications.
// This interface allows the block validation service to notify other services
// (like the P2P service) when a block is found to be invalid.
type InvalidBlockHandler interface {
	// ReportInvalidBlock is called when a block is found to be invalid.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Hash of the invalid block
	//   - reason: Reason for the block being invalid
	//
	// Returns an error if the notification cannot be processed.
	ReportInvalidBlock(ctx context.Context, blockHash string, reason string) error
}

var (
	// Global server instance
	serverInstance *Server
	serverMutex    sync.Mutex

	// ErrServerNotInitialized is returned when the server instance is not initialized
	ErrServerNotInitialized = errors.NewServiceError("block validation server not initialized")
)

// SetServer sets the global server instance for the blockvalidation package.
// This allows other packages to register handlers with the block validation service.
func SetServer(server *Server) {
	serverMutex.Lock()
	defer serverMutex.Unlock()

	serverInstance = server
}

// GetServer returns the global server instance for the blockvalidation package.
// This allows other packages to register handlers with the block validation service.
func GetServer() (*Server, error) {
	serverMutex.Lock()
	defer serverMutex.Unlock()

	if serverInstance == nil {
		return nil, ErrServerNotInitialized
	}

	return serverInstance, nil
}

// RegisterInvalidBlockHandler registers an InvalidBlockHandler with the block validation server.
// This allows the block validation service to notify other services when a block is found to be invalid.
func (s *Server) RegisterInvalidBlockHandler(handler InvalidBlockHandler) {
	// Pass the handler to the block validation instance
	if s.blockValidation != nil && s.blockValidation.invalidBlockHandler == nil {
		s.blockValidation.invalidBlockHandler = handler
		s.logger.Infof("[RegisterInvalidBlockHandler] registered invalid block handler")
	}
}
