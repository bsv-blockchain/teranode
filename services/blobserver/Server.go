package blobserver

import (
	"context"
	"errors"

	"github.com/TAAL-GmbH/ubsv/services/blobserver/dao"
	"github.com/TAAL-GmbH/ubsv/services/blobserver/grpc_impl"
	"github.com/TAAL-GmbH/ubsv/services/blobserver/http_impl"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

// Server type carries the logger within it
type Server struct {
	logger     utils.Logger
	grpcAddr   string
	httpAddr   string
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
	grpcAddr, grpcOk := gocore.Config().Get("blobserver_grpcAddress")
	httpAddr, httpOk := gocore.Config().Get("blobserver_httpAddress")

	if !grpcOk && !httpOk {
		return nil, errors.New("no blobserver_grpcAddress or blobserver_httpAddress setting found")
	}

	db, err := dao.NewDAO()
	if err != nil {
		panic(err)
	}

	s := &Server{
		logger:   gocore.Log("blob"),
		grpcAddr: grpcAddr,
		httpAddr: httpAddr,
	}

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
	if v.grpcServer != nil {
		if err := v.grpcServer.Start(v.grpcAddr); err != nil {
			return err
		}
	}

	if v.httpServer != nil {
		if err := v.httpServer.Start(v.httpAddr); err != nil {
			return err
		}
	}

	return nil
}

func (v *Server) Stop(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if v.grpcServer != nil {
		if err := v.grpcServer.Stop(ctx); err != nil {
			v.logger.Errorf("error stopping grpc server", "error", err)
		}
	}

	if v.httpServer != nil {
		if err := v.httpServer.Stop(ctx); err != nil {
			v.logger.Errorf("error stopping http server", "error", err)
		}
	}
}
