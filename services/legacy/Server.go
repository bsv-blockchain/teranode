package legacy

import (
	"context"

	"github.com/bitcoin-sv/ubsv/services/legacy/external"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

type Server struct {
	logger ulogger.Logger
}

func New(logger ulogger.Logger) *Server {
	return &Server{
		logger: logger,
	}

}

func (ps *Server) Health(_ context.Context) (int, string, error) {
	return 0, "", nil
}

func (ps *Server) Init(_ context.Context) (err error) {
	return nil
}

func (ps *Server) Start(ctx context.Context) (err error) {
	external.Start()
	return nil
}

func (ps *Server) Stop(ctx context.Context) (err error) {
	return nil
}
