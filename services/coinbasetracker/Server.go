package coinbasetracker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/TAAL-GmbH/ubsv/db/model"
	networkModel "github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blobserver/blobserver_api"

	"github.com/libsv/go-p2p/chaincfg/chainhash"

	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	coinbasetracker_api "github.com/TAAL-GmbH/ubsv/services/coinbasetracker/coinbasetracker_api"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CoinbaseTrackerServer type carries the logger within it
type CoinbaseTrackerServer struct {
	coinbasetracker_api.UnimplementedCoinbasetrackerAPIServer
	logger           utils.Logger
	grpcServer       *grpc.Server
	blockchainClient blockchain.ClientI
	coinbaseTracker  *CoinbaseTracker
	testnet          bool
}

func Enabled() bool {
	_, found := gocore.Config().Get("coinbasetracker_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger) *CoinbaseTrackerServer {
	con := &CoinbaseTrackerServer{
		logger:  logger,
		testnet: gocore.Config().GetBool("network", true),
	}

	return con
}

func (u *CoinbaseTrackerServer) Init(ctx context.Context) (err error) {
	u.blockchainClient, err = blockchain.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("error creating blockchain client: %s", err)
	}

	u.coinbaseTracker = NewCoinbaseTracker(u.logger, u.blockchainClient)

	return nil
}

// Start function
func (u *CoinbaseTrackerServer) Start(ctx context.Context) error {

	address, ok := gocore.Config().Get("coinbasetracker_grpcAddress")
	if !ok {
		return errors.New("no coinbasetracker_grpcAddress setting found")
	}

	var err error
	u.grpcServer, err = utils.GetGRPCServer(&utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
	})
	if err != nil {
		return fmt.Errorf("could not create GRPC server [%w]", err)
	}

	gocore.SetAddress(address)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	coinbasetracker_api.RegisterCoinbasetrackerAPIServer(u.grpcServer, u)

	// Register reflection service on gRPC server.
	reflection.Register(u.grpcServer)

	u.logger.Infof("coinbaseTracker GRPC service listening on %s", address)

	// get best block from node
	// get best block from db
	// if different fill in the gaps
	// subscribe to new blocks through the blob server
	blobserverAddr, ok := gocore.Config().Get("blobserver_grpcAddress")
	if !ok {
		return errors.New("no blobserver_grpcAddress setting found")
	}

	conn, err := utils.GetGRPCClient(ctx, blobserverAddr, &utils.ConnectionOptions{})
	if err != nil {
		return err
	}

	blobServerClient := blobserver_api.NewBlobServerAPIClient(conn)

	// define here to prevent malloc
	var stream blobserver_api.BlobServerAPI_SubscribeClient
	var resp *blobserver_api.Notification
	var hash *chainhash.Hash

	go func() {

		for {
			u.logger.Infof("starting new subscription to blobserver: %v", blobserverAddr)
			stream, err = blobServerClient.Subscribe(ctx, &blobserver_api.SubscribeRequest{
				Source: "coinbaseTracker",
			})
			if err != nil {
				u.logger.Errorf("could not subscribe to blobserver: %v", err)
				time.Sleep(10 * time.Second)
				continue
			}

			var b []byte
			var newBlock *networkModel.Block
			var bestBlock *model.Block
			var newBlockHeight uint32
			for {
				resp, err = stream.Recv()
				if err != nil {
					u.logger.Errorf("could not receive from blobserver: %v", err)
					_ = stream.CloseSend()
					time.Sleep(10 * time.Second)
					break
				}

				if resp.Type == blobserver_api.Type_Block {
					hash, err = chainhash.NewHash(resp.Hash)
					if err != nil {
						u.logger.Errorf("could not create hash from bytes", "err", err)
						continue
					}
					u.logger.Debugf("Received BLOCK notification: %s", hash.String())
					// get the block
					b, err = doHTTPRequest(ctx, fmt.Sprintf("%s/block/%s", resp.BaseUrl, hash.String()))
					if err != nil {
						continue
					}
					newBlock, err = networkModel.NewBlockFromBytes(b)
					if err != nil {
						u.logger.Errorf("could not get block from network %+v", err)
						break
					}
					// get the best block from the db.
					// if the block is not the best block, then we need to fill in the gaps
					bestBlock, err = u.coinbaseTracker.GetBestBlockFromDb(ctx)
					if err != nil {
						u.logger.Infof("No best block in db %+v", err)
						// add the block to the db
						newBlockHeight, err = newBlock.ExtractCoinbaseHeight()
						if err != nil {
							u.logger.Errorf("could not extract block height", err)
							break
						}

						err = u.coinbaseTracker.AddBlock(ctx, &model.Block{
							Height:        uint64(newBlockHeight),
							BlockHash:     newBlock.Hash().String(),
							PrevBlockHash: newBlock.Header.HashPrevBlock.String(),
						})
						if err != nil {
							u.logger.Errorf("could not add block to db %+v", err)
							break
						}
						// add coinbase utxos
						u.saveCoinbaseUtxos(ctx, newBlock)

						break
					}

					// if the block is not the best block, then we need to fill in the gaps
					if newBlock.Header.HashPrevBlock.String() != bestBlock.BlockHash {
						newBlockHeight, err = newBlock.ExtractCoinbaseHeight()
						if err != nil {
							u.logger.Errorf("could not extract block height", err)
							break
						}
						missingBlocks := make([]*networkModel.Block, 0, newBlockHeight-uint32(bestBlock.Height))
						missingBlocks = append(missingBlocks, newBlock)
						// get the previous block until the previous block hash is equal to the bestBlock hash
						for newBlock.Header.HashPrevBlock.String() != bestBlock.BlockHash {
							b, err = doHTTPRequest(ctx, fmt.Sprintf("%s/block/%s", resp.BaseUrl, newBlock.Header.HashPrevBlock.String()))
							if err != nil {
								continue
							}
							newBlock, err = networkModel.NewBlockFromBytes(b)
							if err != nil {
								u.logger.Errorf("could not get block from network %+v", err)
								break
							}
							missingBlocks = append(missingBlocks, newBlock)
						}
						// add the blocks to the db
						var height uint32
						for _, block := range missingBlocks {
							height, err = block.ExtractCoinbaseHeight()
							if err != nil {
								u.logger.Errorf("could not extract block height", err)
								break
							}
							err = u.coinbaseTracker.AddBlock(ctx, &model.Block{
								Height:        uint64(height),
								BlockHash:     block.Hash().String(),
								PrevBlockHash: block.Header.HashPrevBlock.String(),
							})
							if err != nil {
								u.logger.Errorf("could not add block to db %+v", err)
								break
							}
							// add coinbase utxos
							u.saveCoinbaseUtxos(ctx, block)
						}
					}
				}
			}
		}
	}()

	if err = u.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (u *CoinbaseTrackerServer) saveCoinbaseUtxos(ctx context.Context, newBlock *networkModel.Block) {
	for i, o := range newBlock.CoinbaseTx.Outputs {
		addr, err := o.LockingScript.Addresses()
		if err != nil {
			u.logger.Errorf("could not get address from script %+v", err)
			break
		}
		err = u.coinbaseTracker.AddUtxo(ctx, &model.UTXO{
			Txid:          newBlock.CoinbaseTx.TxID(),
			Vout:          uint32(i),
			LockingScript: o.LockingScript.String(),
			Satoshis:      o.Satoshis,
			Address:       addr[0],
		})
		if err != nil {
			u.logger.Errorf("could not add utxo to db %+v", err)
			break
		}
	}
}

func (u *CoinbaseTrackerServer) Stop(ctx context.Context) error {
	_, cancel := context.WithCancel(ctx)
	defer cancel()
	_ = u.coinbaseTracker.Stop()
	u.grpcServer.GracefulStop()

	return nil
}

func (u *CoinbaseTrackerServer) Health(_ context.Context, _ *emptypb.Empty) (*coinbasetracker_api.HealthResponse, error) {
	return &coinbasetracker_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *CoinbaseTrackerServer) GetUtxos(ctx context.Context, req *coinbasetracker_api.GetUtxoRequest) (*coinbasetracker_api.GetUtxoResponse, error) {

	utxos, err := u.coinbaseTracker.GetUtxos(ctx, req.Address, req.Amount)
	if err != nil {
		return nil, err
	}

	respUtxos := make([]*coinbasetracker_api.Utxo, len(utxos))
	for i, utxo := range utxos {
		respUtxos[i] = &coinbasetracker_api.Utxo{
			TxId:     utxo.TxID,
			Vout:     utxo.Vout,
			Script:   *utxo.LockingScript,
			Satoshis: utxo.Satoshis,
		}
	}
	resp := &coinbasetracker_api.GetUtxoResponse{}
	resp.Utxos = respUtxos
	return resp, nil
}

func (u *CoinbaseTrackerServer) SubmitTransaction(ctx context.Context, req *coinbasetracker_api.SubmitTransactionRequest) (*emptypb.Empty, error) {

	err := u.coinbaseTracker.SubmitTransaction(ctx, req.Tx)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
func doHTTPRequest(ctx context.Context, url string) ([]byte, error) {
	httpClient := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request [%s]", err.Error())
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to do http request [%s]", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http request [%s] returned status code [%d]", url, resp.StatusCode)
	}

	blockBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read http response body [%s]", err.Error())
	}

	return blockBytes, nil
}
