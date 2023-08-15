package coinbasetracker

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/TAAL-GmbH/ubsv/db"
	"github.com/TAAL-GmbH/ubsv/db/base"
	"github.com/TAAL-GmbH/ubsv/db/model"
	networkModel "github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blobserver/blobserver_api"
	"github.com/TAAL-GmbH/ubsv/util"

	//"golang.org/x/time/rate"

	"github.com/libsv/go-p2p/chaincfg/chainhash"

	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	coinbasetracker_api "github.com/TAAL-GmbH/ubsv/services/coinbasetracker/coinbasetracker_api"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/go-utils/lockfree"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CoinbaseTrackerServer type carries the logger within it
type CoinbaseTrackerServer struct {
	coinbasetracker_api.UnimplementedCoinbasetrackerAPIServer
	logger utils.Logger
	// grpcServer       *grpc.Server
	blockchainClient blockchain.ClientI
	coinbaseTracker  *CoinbaseTracker
	testnet          bool
	running          bool
}

type BlockUtxo struct {
	block *model.Block
	utxos []*model.UTXO
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

	// DevNote: subscription and grpc routines each should use its own
	// unique db connection. The subscription routine must not
	// call the grpc connection via the tracker instance.
	qu := lockfree.NewQueue[*BlockUtxo]()

	go func(q *lockfree.Queue[*BlockUtxo]) {
		store, ok := gocore.Config().Get("coinbasetracker_store")
		if !ok {
			u.logger.Warnf("coinbasetracker_store is not set. Using sqlite.")
			store = "sqlite"
		}

		store_config, ok := gocore.Config().Get("coinbasetracker_store_config")
		if !ok {
			u.logger.Warnf("coinbasetracker_store_config is not set. Using sqlite in-mem.")
			store_config = "file::memory:?cache=shared"
		}
		dbm := db.Create(store, store_config)
		blockutxos := []*BlockUtxo{}

		queue_wait, _ := gocore.Config().GetInt("coinbasetracker_queue_wait", 100)

		for {
			t := time.NewTimer(time.Duration(queue_wait) * time.Millisecond)
			<-t.C
			for {
				bu, _ := q.DequeueAsType()
				if bu == nil {
					break
				}
				blockutxos = append(blockutxos, bu)
			}
			if len(blockutxos) == 0 {
				continue
			}
			err := addBlockUtxos(dbm, u.logger, blockutxos)
			if err != nil {
				u.logger.Errorf("failed to add blocks: %s", err.Error())
			}
			blockutxos = []*BlockUtxo{}
		}
	}(qu)

	go func() {
		<-ctx.Done()
		u.logger.Infof("[CoinbaseTracker] context done, closing client")
		u.running = false
		err = conn.Close()
		if err != nil {
			u.logger.Errorf("[CoinbaseTracker] failed to close connection", err)
		}
	}()

	u.running = true
	go func(q *lockfree.Queue[*BlockUtxo]) {
		for u.running {
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
			//var bestBlockDB *model.Block
			var newBlockHeight uint32
			for u.running {
				resp, err = stream.Recv()
				if err != nil {
					if !strings.Contains(err.Error(), context.Canceled.Error()) {
						u.logger.Errorf("could not receive from blobserver: %v", err)
					}
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
					u.logger.Debugf("\U0001F532 Received BLOCK notification: %s", hash.String())
					// get the block
					b, err = util.DoHTTPRequest(ctx, fmt.Sprintf("%s/block/%s", resp.BaseUrl, hash.String()))
					if err != nil {
						continue
					}
					newBlock, err = networkModel.NewBlockFromBytes(b)
					if err != nil {
						u.logger.Errorf("could not get block from network %+v", err)
						continue
					}

					if err == nil {
						// add the block to the db
						newBlockHeight, err = newBlock.ExtractCoinbaseHeight()
						if err != nil {
							u.logger.Errorf("could not extract block height", err)
							continue
						}
						blockutxo := &BlockUtxo{
							block: &model.Block{
								Height:        uint64(newBlockHeight),
								BlockHash:     newBlock.Hash().String(),
								PrevBlockHash: newBlock.Header.HashPrevBlock.String(),
							},
						}

						for i, o := range newBlock.CoinbaseTx.Outputs {
							addr, err := o.LockingScript.Addresses()
							if err != nil {
								u.logger.Errorf("could not get address from script %s", err.Error())
								panic("could not get address from script")
							}
							blockutxo.utxos = append(blockutxo.utxos,
								&model.UTXO{
									BlockHash:     newBlock.Hash().String(),
									Txid:          newBlock.CoinbaseTx.TxID(),
									Vout:          uint32(i),
									LockingScript: o.LockingScript.String(),
									Satoshis:      o.Satoshis,
									Address:       addr[0],
								},
							)
						}
						q.Enqueue(blockutxo)
						continue
					}

					// if the block is not the best block, then we need to fill in the gaps
					// if newBlock.Header.HashPrevBlock.String() != bestBlockDB.BlockHash {
					// 	newBlockHeight, err = newBlock.ExtractCoinbaseHeight()
					// 	if err != nil {
					// 		u.logger.Errorf("could not extract block height", err)
					// 		break
					// 	}
					// 	missingBlocks := make([]*networkModel.Block, 0, newBlockHeight-uint32(bestBlockDB.Height))
					// 	missingBlocks = append(missingBlocks, newBlock)
					// 	// get the previous block until the previous block hash is equal to the bestBlock hash
					// 	genesisHash := chainhash.Hash{}
					// 	for newBlock.Header.HashPrevBlock.String() != bestBlockDB.BlockHash {
					// 		if newBlock.Header.HashPrevBlock.String() == genesisHash.String() {
					// 			//reached genesis block
					// 			break
					// 		}
					// 		b, err = util.DoHTTPRequest(ctx, fmt.Sprintf("%s/block/%s", resp.BaseUrl, newBlock.Header.HashPrevBlock.String()))
					// 		if err != nil {
					// 			continue
					// 		}
					// 		newBlock, err = networkModel.NewBlockFromBytes(b)
					// 		if err != nil {
					// 			u.logger.Errorf("could not get block from network %+v", err)
					// 			break
					// 		}
					// 		missingBlocks = append(missingBlocks, newBlock)
					// 	}
					// 	// add the blocks to the db
					// 	var height uint32
					// 	// make this operaton a lower priority by rate limiting it to 250ms
					// 	limiter := rate.NewLimiter(rate.Every(1*time.Second/4), 5)
					// 	for _, block := range missingBlocks {
					// 		height, err = block.ExtractCoinbaseHeight()
					// 		if err != nil {
					// 			u.logger.Errorf("could not extract block height", err)
					// 			break
					// 		}
					// 		u.logger.Debugf("catchup> best block from network hash: %s has %d tx", block.Hash().String(), block.TransactionCount)
					// 		limiter.Wait(context.Background())
					// 		err = AddBlock(dbm, u.logger,
					// 			&model.Block{
					// 				Height:        uint64(height),
					// 				BlockHash:     block.Hash().String(),
					// 				PrevBlockHash: block.Header.HashPrevBlock.String(),
					// 			})
					// 		if err != nil {
					// 			e, ok := err.(sqlerr.Error)
					// 			if ok {
					// 				switch e.ExtendedCode {
					// 				case 1555:
					// 					u.logger.Warnf("unique contraint on add block with hash %s: %s - code %d\n",
					// 						block.Hash().String(),
					// 						e.Error(),
					// 						e.ExtendedCode,
					// 					)
					// 				case 2067:
					// 					u.logger.Warnf("unique contraint on add block with hash %s: %s - code %d\n",
					// 						block.Hash().String(),
					// 						e.Error(),
					// 						e.ExtendedCode,
					// 					)
					// 				case 5:
					// 					u.logger.Errorf("failed to add block with hash %s: %s - code %d\n",
					// 						block.Hash().String(),
					// 						e.Error(),
					// 						e.ExtendedCode,
					// 					)
					// 				default:
					// 					u.logger.Errorf("failed to add block with hash %s: %s - code %d\n",
					// 						block.Hash().String(),
					// 						e.Error(),
					// 						e.ExtendedCode,
					// 					)
					// 				}
					// 			} else {
					// 				u.logger.Errorf("could not add block to db %+v", err)
					// 			}
					// 			break
					// 		}
					// 		u.logger.Debugf("Added catchup BLOCK with height: %d | hash: %s", newBlockHeight, newBlock.Hash().String())
					// 		// add coinbase utxos
					// 		SaveCoinbaseUtxos(dbm, u.logger, block)
					// 	}
					// }
				}
			}
		}
	}(qu)

	// this will block
	if err = util.StartGRPCServer(ctx, u.logger, "coinbasetracker", func(server *grpc.Server) {
		coinbasetracker_api.RegisterCoinbasetrackerAPIServer(server, u)
	}); err != nil {
		return err
	}

	return nil
}

func (u *CoinbaseTrackerServer) Stop(ctx context.Context) error {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	_ = u.coinbaseTracker.Stop()

	return nil
}

func (u *CoinbaseTrackerServer) Health(_ context.Context, _ *emptypb.Empty) (*coinbasetracker_api.HealthResponse, error) {
	return &coinbasetracker_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *CoinbaseTrackerServer) GetUtxo(ctx context.Context, req *coinbasetracker_api.GetUtxoRequest) (*coinbasetracker_api.Utxo, error) {

	utxo, err := u.coinbaseTracker.GetUtxo(ctx, req.Address)
	if err != nil {
		return nil, err
	}

	return &coinbasetracker_api.Utxo{
		TxId:     utxo.TxID,
		Vout:     utxo.Vout,
		Script:   *utxo.LockingScript,
		Satoshis: utxo.Satoshis,
	}, nil
}

func (u *CoinbaseTrackerServer) MarkUtxoSpent(ctx context.Context, req *coinbasetracker_api.MarkUtxoSpentRequest) (*emptypb.Empty, error) {
	err := u.coinbaseTracker.SubmitTransaction(ctx,
		&model.UTXO{
			Txid: hex.EncodeToString(req.TxId),
			Vout: req.Vout,
		},
	)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func addBlockUtxos(dbm base.DbManager, log utils.Logger, blockutxos []*BlockUtxo) error {
	blocks := []*model.Block{}
	utxos := []*model.UTXO{}
	for _, bu := range blockutxos {
		blocks = append(blocks, bu.block)
		utxos = append(utxos, bu.utxos...)
	}
	err := AddBlockUtxos(dbm, log, blocks, utxos)
	if err != nil {
		log.Errorf("error adding blocks and utxos: %s", err.Error())
	}
	return err
}
