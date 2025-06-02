// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"

	"github.com/bitcoin-sv/teranode/errors"
	blockvalidationiface "github.com/bitcoin-sv/teranode/interfaces/blockvalidation"
	"github.com/bitcoin-sv/teranode/services/blockvalidation"
	"github.com/bitcoin-sv/teranode/ulogger"
)

// RegisterWithBlockValidation registers the P2P server as an InvalidBlockHandler with the block validation service.
// This allows the block validation service to notify the P2P server when a block is found to be invalid,
// so the P2P server can add ban score to the peer that sent the invalid block.
//
// The server parameter should be an instance of blockvalidation.Server that has been properly initialized.
func RegisterWithBlockValidation(ctx context.Context, logger ulogger.Logger, server *blockvalidation.Server, handler blockvalidationiface.InvalidBlockHandler) error {
	if server == nil {
		err := errors.New(errors.ERR_INVALID_ARGUMENT, "block validation server cannot be nil")
		logger.Errorf("[RegisterWithBlockValidation] %v", err)

		return errors.NewServiceError("failed to register invalid block handler", err)
	}

	// Register the InvalidBlockHandler with the block validation server
	server.RegisterInvalidBlockHandler(handler)
	logger.Infof("[RegisterWithBlockValidation] successfully registered P2P server as InvalidBlockHandler")

	return nil
}
