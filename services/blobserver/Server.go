package blobserver

import (
	"context"
	"errors"

	"github.com/TAAL-GmbH/ubsv/services/blobserver/dao"
	"github.com/TAAL-GmbH/ubsv/services/blobserver/grpc_impl"
	"github.com/TAAL-GmbH/ubsv/services/blobserver/http_impl"
	"github.com/ordishs/gocore"
)

// Server type carries the logger within it
type Server struct {
	grpcServer *grpc_impl.GRPC
	httpServer *http_impl.HTTP
}

func Enabled() bool {
	_, grpcOk := gocore.Config().Get("blobserver_grpcAddress")
	_, httpOk := gocore.Config().Get("blobserver_httpAddress")
	return grpcOk || httpOk
}

// NewServer will return a server instance with the logger stored within it
func NewServer() (*Server, error) {
	_, grpcOk := gocore.Config().Get("blobserver_grpcAddress")
	_, httpOk := gocore.Config().Get("blobserver_httpAddress")

	if !grpcOk && !httpOk {
		return nil, errors.New("no blobserver_grpcAddress or blobserver_httpAddress setting found")
	}

	db, err := dao.NewDAO()
	if err != nil {
		panic(err)
	}

	s := &Server{}

	if grpcOk {
		s.grpcServer, err = grpc_impl.New(db)
		if err != nil {
			return nil, err
		}
	}

	if httpOk {
		s.httpServer, err = http_impl.New(db)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

// Start function
func (v *Server) Start() error {
	// TODO add address
	if v.grpcServer != nil {
		v.grpcServer.Start("")
	}

	if v.httpServer != nil {
		v.httpServer.Start("")
	}

	return nil
}

func (v *Server) Stop(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if v.grpcServer != nil {
		v.grpcServer.Stop(ctx)
	}

	if v.httpServer != nil {
		v.httpServer.Stop(ctx)
	}
}
