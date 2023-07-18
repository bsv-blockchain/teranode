package blockvalidation

import (
	"context"

	blockvalidation_api "github.com/TAAL-GmbH/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Store struct {
	client blockvalidation_api.BlockValidationAPIClient
}

func NewClient() *Store {
	ctx := context.Background()

	blockValidationGrpcAddress, ok := gocore.Config().Get("blockvalidation_grpcAddress")
	if !ok {
		panic("no blockvalidation_grpcAddress setting found")
	}
	baConn, err := utils.GetGRPCClient(ctx, blockValidationGrpcAddress, &utils.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		panic(err)
	}

	return &Store{
		client: blockvalidation_api.NewBlockValidationAPIClient(baConn),
	}
}
