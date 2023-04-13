package propagation

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/TAAL-GmbH/ubsv/services/propagation/store"
	"github.com/TAAL-GmbH/ubsv/services/propagation/store/badger"
	"github.com/TAAL-GmbH/ubsv/services/validator"
	"github.com/libsv/go-p2p"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/libsv/go-p2p/wire"
	"github.com/ordishs/go-bitcoin"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
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

func NewServer(logger utils.Logger, txStore store.TransactionStore, validatorClient *validator.Client) *Server {
	blockStore, err := badger.New("./data/blockStore")
	if err != nil {
		logger.Fatalf("error creating block store: %v", err)
	}

	return &Server{
		logger:          logger,
		peerHandler:     NewPeerHandler(txStore, blockStore, validatorClient),
		validatorClient: validatorClient,
	}
}

func (s *Server) Start(ctx context.Context) error {
	pm := p2p.NewPeerManager(s.logger, wire.TestNet)

	peerCount, ok := gocore.Config().GetInt("peerCount")
	if ok {
		for i := 1; i <= peerCount; i++ {
			p2pURL, err, found := gocore.Config().GetURL(fmt.Sprintf("peer_%d_p2p", i))
			if !found {
				s.logger.Fatalf("peer_%d_p2p must be set", i)
			}
			if err != nil {
				s.logger.Fatalf("error reading peer_%d_p2p: %v", i, err)
			}

			peer, err := p2p.NewPeer(s.logger, p2pURL.Host, s.peerHandler, wire.TestNet)
			if err != nil {
				s.logger.Fatalf("error creating peer %s: %v", p2pURL.Host, err)
			}

			if err = pm.AddPeer(peer); err != nil {
				s.logger.Fatalf("error adding peer %s: %v", p2pURL.Host, err)
			}
		}
	}

	doRegtestSync := gocore.Config().GetBool("regtestSync")
	if doRegtestSync {
		// wait for all blocks to be downloaded from Regtest
		// this is only in testing on Regtest and should be removed in production
		err := s.regtestSync(pm)
		if err != nil {
			s.logger.Fatalf("error during regtest sync: %v", err)
		}
	}

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

				peer, err := p2p.NewPeer(s.logger, c.RemoteAddr().String(), s.peerHandler, wire.TestNet, p2p.WithIncomingConnection(c))
				if err != nil {
					s.logger.Errorf("Error creating peer: %v", err)
				}

				if err = pm.AddPeer(peer); err != nil {
					s.logger.Errorf("Error adding peer: %v", err)
				}
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

func (s *Server) regtestSync(pm p2p.PeerManagerI) error {
	s.logger.Infof("Starting Regtest sync (not Initial Block Download)")

	p2pURL, err, found := gocore.Config().GetURL(fmt.Sprintf("peer_%d_rpc", 1))
	if !found {
		s.logger.Fatalf("peer_%d_rpc must be set", 1)
	}
	if err != nil {
		s.logger.Fatalf("error reading peer_%d_rpc: %v", 1, err)
	}

	port, err := strconv.Atoi(p2pURL.Port())
	if err != nil {
		panic(err)
	}
	password, _ := p2pURL.User.Password()
	btc, err := bitcoin.New(p2pURL.Hostname(), port, p2pURL.User.Username(), password, false)
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
