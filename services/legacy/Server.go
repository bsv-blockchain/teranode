package legacy

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	"github.com/bitcoin-sv/ubsv/services/legacy/chaincfg"
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/lib/pq"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type Server struct {
	logger          ulogger.Logger
	stats           *gocore.Stat
	config          *peer.Config
	peer            *peer.Peer
	tb              *TeranodeBridge
	params          chaincfg.Params
	lastHash        *chainhash.Hash
	height          uint32
	wg              sync.WaitGroup
	blockchainStore blockchain.Store
	utxoStore       utxo.Store
	subtreeStore    blob.Store
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger, blockchainStore blockchain.Store, subtreeStore blob.Store, utxoStore utxo.Store) *Server {
	// initPrometheusMetrics()

	return &Server{
		logger:          logger,
		stats:           gocore.NewStat("legacy"),
		params:          chaincfg.MainNetParams,
		blockchainStore: blockchainStore,
		subtreeStore:    subtreeStore,
		utxoStore:       utxoStore,
	}
}

func (s *Server) Health(_ context.Context) (int, string, error) {
	return 0, "", nil
}

func (s *Server) Init(ctx context.Context) error {
	var err error

	// Create a new Teranode bridge
	if !gocore.Config().GetBool("legacy_direct", true) {
		s.tb, err = NewTeranodeBridge(ctx, s.logger)
		if err != nil {
			s.logger.Fatalf("Failed to create Teranode bridge: %v", err)
		}
	}

	// Create a new Bitcoin peer configuration
	s.config = &peer.Config{
		UserAgentName:    "headers-sync",
		UserAgentVersion: "0.0.1",
		ChainParams:      &s.params,
		Listeners: peer.MessageListeners{

			OnPing: func(p *peer.Peer, msg *wire.MsgPing) {
				s.logger.Infof("Received ping\n")
				pong := wire.NewMsgPong(msg.Nonce)
				s.peer.QueueMessage(pong, nil)
			},

			OnHeaders: func(p *peer.Peer, msg *wire.MsgHeaders) {
				s.logger.Infof("Received %d headers\n", len(msg.Headers))

				err := verifyHeadersChain(msg.Headers, s.lastHash)
				if err != nil {
					s.logger.Fatalf("Header chain verification failed: %v", err)
				}
				s.logger.Infof("Header chain is valid")

				s.wg.Add(len(msg.Headers))

				// Now get each block in turn and process it
				go func() {
					for _, header := range msg.Headers {
						blockHash := header.BlockHash()
						getDataMsg := wire.NewMsgGetData()
						getDataMsg.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &blockHash))

						s.peer.QueueMessage(getDataMsg, nil)
					}

					s.wg.Wait()

					h := msg.Headers[len(msg.Headers)-1].BlockHash()
					s.lastHash = &h

					invMsg := wire.NewMsgGetHeaders()
					invMsg.AddBlockLocatorHash(s.lastHash)
					invMsg.HashStop = chainhash.Hash{}

					// Send the getheaders message
					s.logger.Infof("Requesting headers starting from %s\n", s.lastHash)

					s.peer.QueueMessage(invMsg, nil)
				}()
			},

			OnBlock: func(p *peer.Peer, msg *wire.MsgBlock, buf []byte) {
				s.height++

				s.logger.Warnf("Received block with %d txns %s (Height: %d)\n", len(msg.Transactions), msg.BlockHash(), s.height)

				block := bsvutil.NewBlock(msg)
				block.SetHeight(int32(s.height))

				if gocore.Config().GetBool("legacy_direct", true) {
					if err := s.HandleBlockDirect(ctx, block); err != nil {
						s.logger.Fatalf("Failed to handle block: %v", err)
					}
				} else {
					if err := s.tb.HandleBlock(ctx, block); err != nil {
						s.logger.Fatalf("Failed to handle block: %v", err)
					}
				}

				s.wg.Done()
			},
		},
	}

	return nil
}

// Start function
func (s *Server) Start(ctx context.Context) error {
	var err error

	// Create a new Bitcoin peer
	addresses, _ := gocore.Config().GetMulti("legacy_connect_peers", "|", []string{"54.169.45.196:8333"})
	addr := addresses[0]

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

	bestBlockHeader, bestBlockMeta, err := s.blockchainStore.GetBestBlockHeader(ctx)
	if err != nil {
		log.Fatalf("Failed to get best block header: %v", err)
	}

	s.height = bestBlockMeta.Height
	s.lastHash = bestBlockHeader.Hash()

	if s.height > 0 {
		s.height--
		s.lastHash = bestBlockHeader.HashPrevBlock
	}

	invMsg := wire.NewMsgGetHeaders()
	invMsg.AddBlockLocatorHash(s.lastHash) // First time this is Genesis block hash
	invMsg.HashStop = chainhash.Hash{}

	// Send the getheaders message
	s.logger.Infof("Requesting headers starting from genesis\n")
	s.peer.QueueMessage(invMsg, nil)

	// Keep the program running to receive headers
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-time.After(time.Second * 10):
			// Send a ping message to the peer
			ping := wire.NewMsgPing(0)
			s.peer.QueueMessage(ping, nil)
		}
	}
}

func (s *Server) Stop(ctx context.Context) error {
	return nil
}

// Function to verify the headers chain
func verifyHeadersChain(headers []*wire.BlockHeader, lastHash *chainhash.Hash) error {
	prevHash := lastHash

	for _, header := range headers {
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

func (s *Server) HandleBlockDirect(ctx context.Context, block *bsvutil.Block) error {
	startTotal, stat, ctx := util.StartStatFromContext(ctx, "HandleBlockDirect")

	defer func() {
		stat.AddTime(startTotal)
	}()

	stat.NewStat("SubtreeStore").AddRanges(0, 1, 10, 100, 1000, 10000, 100000, 1000000)

	subtrees := make([]*chainhash.Hash, 0)

	subtree, err := util.NewIncompleteTreeByLeafCount(len(block.Transactions()))
	if err != nil {
		return fmt.Errorf("Failed to create subtree: %w", err)
	}

	// 3. Create a block message with (block hash, coinbase tx and slice if 1 subtree)
	var headerBytes bytes.Buffer
	if err := block.MsgBlock().Header.Serialize(&headerBytes); err != nil {
		return fmt.Errorf("Failed to serialize header: %w", err)
	}

	header, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
	if err != nil {
		return fmt.Errorf("Failed to create block header from bytes: %w", err)
	}

	var coinbase bytes.Buffer
	if err := block.Transactions()[0].MsgTx().Serialize(&coinbase); err != nil {
		return fmt.Errorf("Failed to serialize coinbase: %w", err)
	}

	coinbaseTx, err := bt.NewTxFromBytes(coinbase.Bytes())
	if err != nil {
		return fmt.Errorf("Failed to create bt.Tx for coinbase: %w", err)
	}

	blockSize := block.MsgBlock().SerializeSize()

	// We will first store this block with an empty slice of subtrees. If this block has more than just the 1 coinbase tx, we will
	// update the row to have a subtree after processing all the txs

	teranodeBlock, err := model.NewBlock(header, coinbaseTx, subtrees, uint64(len(block.Transactions())), uint64(blockSize), uint32(block.Height()))
	if err != nil {
		return fmt.Errorf("Failed to create model.NewBlock: %w", err)
	}

	start := gocore.CurrentTime()

	dbID, err := s.blockchainStore.StoreBlock(ctx, teranodeBlock, "LEGACY")
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) {
			if pqErr.Code == "23505" { // Duplicate constraint violation
				s.logger.Warnf("Block already exists in the database: %s", block.Hash().String())
			} else {
				return fmt.Errorf("Failed to store block: %w", err)
			}
		} else {
			return fmt.Errorf("Failed to store block: %w", err)
		}
	}

	start = stat.NewStat("StoreBlock").AddTime(start)

	// Add the placeholder to the subtree
	if err := subtree.AddNode(model.CoinbasePlaceholder, 0, 0); err != nil {
		return fmt.Errorf("Failed to add coinbase placeholder: %w", err)
	}

	for _, wireTx := range block.Transactions() {
		txHash := *wireTx.Hash()

		// Serialize the tx
		var txBytes bytes.Buffer
		if err := wireTx.MsgTx().Serialize(&txBytes); err != nil {
			return fmt.Errorf("Could not serialize msgTx: %w", err)
		}

		txSize := uint64(txBytes.Len())

		tx, err := bt.NewTxFromBytes(txBytes.Bytes())
		if err != nil {
			return fmt.Errorf("Failed to create bt.Tx: %w", err)
		}

		if !tx.IsCoinbase() {
			spends := make([]*utxo.Spend, len(tx.Inputs))

			if err := subtree.AddNode(txHash, 0, txSize); err != nil {
				return fmt.Errorf("Failed to add node (%s) to subtree: %w", txHash, err)
			}

			// Extend the tx with additional information
			previousOutputs := make([]*meta.PreviousOutput, len(tx.Inputs))

			for i, input := range tx.Inputs {
				previousOutputs[i] = &meta.PreviousOutput{
					PreviousTxID: *input.PreviousTxIDChainHash(),
					Vout:         input.PreviousTxOutIndex,
				}

				if input.PreviousTxSatoshis > 0 {
					hash, err := util.UTXOHashFromInput(input)
					if err != nil {
						return fmt.Errorf("error getting input utxo hash: %s", err.Error())
					}

					// v.logger.Debugf("spending utxo %s:%d -> %s", input.PreviousTxIDChainHash().String(), input.PreviousTxOutIndex, hash.String())
					spends[i] = &utxo.Spend{
						TxID:         input.PreviousTxIDChainHash(),
						Vout:         input.PreviousTxOutIndex,
						Hash:         hash,
						SpendingTxID: &txHash,
					}
				}
			}

			err := s.utxoStore.PreviousOutputsDecorate(context.Background(), previousOutputs)
			if err != nil {
				return fmt.Errorf("Failed to decorate previous outputs for tx %s: %w", txHash, err)
			}

			for i, po := range previousOutputs {
				if po.LockingScript == nil || len(po.LockingScript) == 0 {
					return fmt.Errorf("Previous output script is empty for %s:%d", po.PreviousTxID, po.Vout)
				}

				tx.Inputs[i].PreviousTxSatoshis = uint64(po.Satoshis)
				tx.Inputs[i].PreviousTxScript = bscript.NewFromBytes(po.LockingScript)
			}

			// Spend the inputs
			if err := s.utxoStore.Spend(ctx, spends); err != nil {
				return fmt.Errorf("Failed to spend utxos: %w", err)
			}
		}

		// Store the tx in the store
		if _, err := s.utxoStore.Create(ctx, tx, uint32(dbID)); err != nil {
			if !errors.Is(err, errors.ErrTxAlreadyExists) {
				return fmt.Errorf("Failed to store tx: %w", err)
			}
		}
	}

	if len(block.Transactions()) > 1 {
		// Add the subtree to the cache
		subtreeBytes, err := subtree.SerializeNodes()
		if err != nil {
			return fmt.Errorf("Failed to serialize subtree: %w", err)
		}

		s.subtreeStore.Set(ctx, subtree.RootHash()[:], subtreeBytes)

		stat.NewStat("SubtreeStore").AddTimeForRange(start, len(block.Transactions()))

		// subtrees = append(subtrees, subtree.RootHash())

		// Update the block with the correct subtree, if necessary
		// TODO s.blockchainStore.Se(ctx context.Context, blockHash *chainhash.Hash)
	}

	return nil
}
