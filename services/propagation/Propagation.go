package propagation

import (
	"context"
	"net"

	"github.com/TAAL-GmbH/ubsv/services/validator"
	"github.com/TAAL-GmbH/ubsv/stores/blob"
	"github.com/ordishs/go-utils"
)

// The plan.

// 1. Connect to reg test mode P2P interface
// 2. Listen for new transaction or block announcements (INV)
// 3. Request full transaction or block when new transaction or block notification is received (GET DATA)
// 4. Validate transaction or block
// 5. Store transaction or block
// 6. Announce transaction or block to other peers (INV)
// 7. Repeat

type Server struct {
	logger          utils.Logger
	validatorClient *validator.Client
}

func NewServer(logger utils.Logger, txStore blob.Store, blockStore blob.Store, validatorClient *validator.Client) *Server {
	return &Server{
		logger:          logger,
		validatorClient: validatorClient,
	}
}

func (s *Server) Start(ctx context.Context) error {
	listen := "localhost:8833"
	s.logger.Infof("Listening on %s", listen)
	conn, err := net.Listen("tcp", listen)
	if err != nil {
		s.logger.Fatalf("Error listening: %v", err)
	}

	// TODO improve this listener
	go func() {
		var c net.Conn
		for {
			// Listen for an incoming connection.
			select {
			case <-ctx.Done():
				return
			default:
				c, err = conn.Accept()
				if err != nil {
					s.logger.Fatalf("Error accepting: %v", err)
				}

				s.logger.Infof("Received incoming connection from %s", c.RemoteAddr().String())

			}
		}
	}()

	<-ctx.Done()

	return nil
}

func (s *Server) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	s.validatorClient.Stop()
}
