package headers

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/gocore"
)

// BlockHeader represents the structure of the block response
type BlockHeader struct {
	Hash              string `json:"hash"`
	Height            int    `json:"height"`
	Chainwork         string `json:"chainwork"`
	PreviousBlockHash string `json:"previousblockhash"`
	Tx                []struct {
		Hex string `json:"hex"`
	} `json:"tx"`
}

func Start() {
	bitcoinURL := flag.String("bitcoinUrl", "http://user:password@localhost:8332", "Bitcoin RPC URL")
	startHeight := flag.Int("start", 0, "Starting block height")
	endHeight := flag.Int("end", 0, "Ending block height")

	flag.Parse()

	ctx := context.Background()
	logger := ulogger.NewGoCoreLogger("head")

	blockchainStoreURL, err, found := gocore.Config().GetURL("blockchain_store")
	if err != nil || !found {
		logger.Fatalf("Failed to get blockchain store URL")
	}

	blockchainStore, err := blockchain.NewStore(logger, blockchainStoreURL)
	if err != nil {
		logger.Fatalf("Failed to create blockchain client: %v", err)
	}

	_, meta, err := blockchainStore.GetBestBlockHeader(ctx)
	if err != nil {
		logger.Fatalf("Failed to get best block header: %v", err)
	}

	if meta.Height == 0 {
		// Empty blockchain
		if *startHeight != 0 {
			logger.Fatalf("Blockchain is empty. Start height should be 0")
		}

		*startHeight = 1
	} else if meta.Height+1 != uint32(*startHeight) {
		logger.Fatalf("Start height should be the next height in the blockchain.  Currently blockchain best height is %d", meta.Height)
	}

	rpcClient := NewRPCClient(*bitcoinURL)

	for height := *startHeight; ; height++ {
		if *endHeight != 0 && height > *endHeight {
			break
		}

		blockHeader, err := rpcClient.GetBlockByHeight(rpcClient, height)
		if err != nil {
			if err.Error() == "Block height out of range" {
				break
			}

			log.Fatalf("Failed to get block by height: %v", err)
		}

		// Get the 80 byte header directly from the node
		headerBytes, err := rpcClient.GetBlockHeader(rpcClient, blockHeader.Hash)
		if err != nil {
			log.Fatalf("Failed to get block header: %v", err)
		}

		header, err := model.NewBlockHeaderFromBytes(headerBytes)
		if err != nil {
			log.Fatalf("Failed to create block from bytes: %v", err)
		}

		coinbaseTx, err := bt.NewTxFromString(blockHeader.Tx[0].Hex)
		if err != nil {
			log.Fatalf("Failed to create coinbase tx: %v", err)
		}

		block := &model.Block{
			Header:           header,
			CoinbaseTx:       coinbaseTx,
			TransactionCount: meta.TxCount,
			SizeInBytes:      meta.SizeInBytes,
			Height:           meta.Height,
		}

		_, _, err = blockchainStore.StoreBlock(ctx, block, "headers")
		if err != nil {
			log.Fatalf("Failed to add block: %v", err)
		}

		fmt.Printf("Processed block %d\n", height)
	}
}
