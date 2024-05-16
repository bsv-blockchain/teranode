package legacy

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/legacy/chaincfg"
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

var (
	params   = &chaincfg.MainNetParams
	lastHash = params.GenesisHash
	count    int
)

type Server struct {
	logger ulogger.Logger
	stats  *gocore.Stat
	config *peer.Config
	peer   *peer.Peer
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger) *Server {
	// initPrometheusMetrics()

	return &Server{
		logger: logger,
		stats:  gocore.NewStat("legacy"),
	}
}

func (s *Server) Health(_ context.Context) (int, string, error) {
	return 0, "", nil
}

func (s *Server) Init(ctx context.Context) error {
	// Create a new Bitcoin peer configuration
	s.config = &peer.Config{
		UserAgentName:    "headers-sync",
		UserAgentVersion: "0.0.1",
		ChainParams:      params,
		Listeners: peer.MessageListeners{

			OnHeaders: func(p *peer.Peer, msg *wire.MsgHeaders) {
				fmt.Printf("Received %d headers\n", len(msg.Headers))

				err := verifyHeadersChain(msg.Headers, lastHash)
				if err != nil {
					log.Fatalf("Header chain verification failed: %v", err)
				}
				fmt.Println("Header chain is valid")

				// Now get each block in turn and process it
				for _, header := range msg.Headers {
					blockHash := header.BlockHash()
					getDataMsg := wire.NewMsgGetData()
					getDataMsg.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &blockHash))

					done := make(chan struct{})
					s.peer.QueueMessage(getDataMsg, done)

					<-done
				}

				h := msg.Headers[len(msg.Headers)-1].BlockHash()
				lastHash = &h

				invMsg := wire.NewMsgGetHeaders()
				invMsg.AddBlockLocatorHash(lastHash)
				invMsg.HashStop = chainhash.Hash{}

				// Send the getheaders message
				fmt.Printf("Requesting headers starting from %s\n", lastHash)

				s.peer.QueueMessage(invMsg, nil)
			},

			OnBlock: func(p *peer.Peer, msg *wire.MsgBlock, buf []byte) {
				count++
				fmt.Printf("Received block with %d txns %s (Height: %d)\n", len(msg.Transactions), msg.BlockHash(), count)
			},
		},
	}

	return nil
}

// Start function
func (s *Server) Start(ctx context.Context) error {
	// Create a new Bitcoin peer
	addr := "54.169.45.196:8333"

	var err error

	s.peer, err = peer.NewOutboundPeer(s.config, addr)
	if err != nil {
		log.Fatalf("Failed to create peer: %v", err)
	}

	// Establish a connection to the peer
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to connect to peer: %v", err)
	}
	s.peer.AssociateConnection(conn)

	// Wait for connection
	time.Sleep(time.Second * 5)

	invMsg := wire.NewMsgGetHeaders()
	invMsg.AddBlockLocatorHash(lastHash) // First time this is Genesis block hash
	invMsg.HashStop = chainhash.Hash{}

	// Send the getheaders message
	fmt.Printf("Requesting headers starting from genesis\n")
	s.peer.QueueMessage(invMsg, nil)

	// Keep the program running to receive headers
	<-ctx.Done()

	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	return nil
}

// Function to verify the headers chain
func verifyHeadersChain(headers []*wire.BlockHeader, lastHash *chainhash.Hash) error {
	prevHash := lastHash

	for _, header := range headers {
		// fmt.Printf("Verifying header   %s\n", header.BlockHash())

		// Check if the header's previous block hash matches the previous header's hash
		if !header.PrevBlock.IsEqual(prevHash) {
			return fmt.Errorf("header's PrevBlock doesn't match previous header's hash")
		}

		// Serialize the header and double hash it to get the proof-of-work hash
		var buf bytes.Buffer
		err := header.Serialize(&buf)
		if err != nil {
			return fmt.Errorf("failed to serialize header: %v", err)
		}

		// Check if the proof-of-work hash meets the target difficulty
		if !checkProofOfWork(header) {
			return fmt.Errorf("header does not meet proof-of-work requirements")
		}

		// Move to the next header
		h := header.BlockHash()
		prevHash = &h
	}

	return nil
}

func checkProofOfWork(header *wire.BlockHeader) bool {
	// Serialize the header
	var buf bytes.Buffer
	err := header.Serialize(&buf)
	if err != nil {
		return false
	}

	bh, err := model.NewBlockHeaderFromBytes(buf.Bytes())
	if err != nil {
		return false
	}

	ok, _, err := bh.HasMetTargetDifficulty()
	if err != nil {
		return false
	}

	return ok
}
