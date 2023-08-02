package coinbasetracker

import (
	"context"

	coinbasetracker_api "github.com/TAAL-GmbH/ubsv/services/coinbasetracker/coinbasetracker_api"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Store struct {
	client coinbasetracker_api.CoinbasetrackerAPIClient
}

func NewClient() *Store {
	ctx := context.Background()

	coinbaseTrackerGrpcAddress, ok := gocore.Config().Get("coinbasetracker_grpcAddress")
	if !ok {
		panic("no blockvalidation_grpcAddress setting found")
	}
	baConn, err := utils.GetGRPCClient(ctx, coinbaseTrackerGrpcAddress, &utils.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		panic(err)
	}

	return &Store{
		client: coinbasetracker_api.NewCoinbasetrackerAPIClient(baConn),
	}
}
