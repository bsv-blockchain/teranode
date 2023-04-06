package propagation

import (
	"context"

	"github.com/TAAL-GmbH/ubsv/services/propagation/store/file"
	"github.com/TAAL-GmbH/ubsv/services/validator"
	"github.com/libsv/go-p2p"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/libsv/go-p2p/wire"
	"github.com/ordishs/go-bitcoin"
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
	peerHandler     p2p.PeerHandlerI
	validatorClient *validator.Client
}

func NewServer(logger utils.Logger) *Server {
	txStore, err := file.New("./data/txStore")
	if err != nil {
		logger.Fatalf("error creating transaction store: %v", err)
	}

	blockStore, err := file.New("./data/blockStore")
	if err != nil {
		logger.Fatalf("error creating block store: %v", err)
	}

	validatorClient, err := validator.NewClient()
	if err != nil {
		logger.Fatalf("error creating validator client: %v", err)
	}

	return &Server{
		logger:          logger,
		peerHandler:     NewPeerHandler(txStore, blockStore, validatorClient),
		validatorClient: validatorClient,
	}
}

func (s *Server) Start(ctx context.Context) error {
	pm := p2p.NewPeerManager(s.logger, wire.TestNet)

	peer, err := p2p.NewPeer(s.logger, "localhost:18333", s.peerHandler, wire.TestNet)
	if err != nil {
		s.logger.Fatalf("error creating peer %s: %v", "localhost:18333", err)
	}

	if err = pm.AddPeer(peer); err != nil {
		s.logger.Fatalf("error adding peer %s: %v", "localhost:18333", err)
	}

	// wait for all blocks to be downloaded
	// this is only in testing on Regtest and should be removed in production
	s.ibd(pm)

	<-ctx.Done()

	return nil
}

func (s *Server) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	s.validatorClient.Stop()
}

func (s *Server) ibd(pm p2p.PeerManagerI) error {
	// initial block download
	s.logger.Infof("Starting Initial Block Download")

	btc, err := bitcoin.New("localhost", 18332, "bitcoin", "bitcoin", false)
	if err != nil {
		panic(err)
	}

	info, err := btc.GetBlockchainInfo()
	if err != nil {
		panic(err)
	}

	nextBlockHash := info.BestBlockHash

	blockHeaders := make([]string, 0)
	blockHeaders = append(blockHeaders, nextBlockHash)

	s.logger.Infof("Getting Block headers from Bitcoin node")
	var blockHeader *bitcoin.BlockHeader
	for {
		blockHeader, err = btc.GetBlockHeader(nextBlockHash)
		if err != nil {
			panic(err)
		}

		blockHeaders = append(blockHeaders, blockHeader.PreviousBlockHash)
		nextBlockHash = blockHeader.PreviousBlockHash

		if blockHeader.Height == 0 {
			break
		}
	}

	// reverse the order in the block headers slice
	for i, j := 0, len(blockHeaders)-1; i < j; i, j = i+1, j-1 {
		blockHeaders[i], blockHeaders[j] = blockHeaders[j], blockHeaders[i]
	}

	s.logger.Infof("Requesting Blocks through p2p")
	var blockHash *chainhash.Hash
	for _, blockHeaderHash := range blockHeaders {
		blockHash, err = chainhash.NewHashFromStr(blockHeaderHash)
		if err != nil {
			return err
		}
		pm.RequestBlock(blockHash)
	}

	return nil
}
