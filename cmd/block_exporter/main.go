package main

// import (
// 	"context"
// 	"encoding/csv"
// 	"flag"
// 	"log"
// 	"os"
// 	"strings"
// 	"time"

// 	"github.com/bitcoin-sv/go-sdk/chainhash"
// 	"github.com/bitcoin-sv/teranode/services/blockchain"
// 	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
// 	"github.com/bitcoin-sv/teranode/ulogger"
// )

// func main() {
// 	// Define command-line flags
// 	startBlock := flag.String("start", "", "Block hash to start from")
// 	count := flag.Int("count", 100, "Maximum number of blocks to export")

// 	// Parse the flags
// 	flag.Parse()

// 	log.Printf("Exporting block DB with start: %v, count: %v", *startBlock, *count)

// 	// Convert start block from string to []*chainhash.Hash
// 	startBlockHash, err := chainhash.NewHashFromHex(*startBlock)
// 	if err != nil {
// 		log.Fatalf("Failed to parse start block: %v", err)
// 	}

// 	client, err := blockchain.NewClient(context.Background(), ulogger.New("blockchainClient"), "blockExporter")
// 	if err != nil {
// 		log.Printf("Failed to create blockchainClient: %v", err)
// 		return
// 	}

// 	// Create a request
// 	request := &blockchain_api.ExportBlockDBRequest{
// 		StartHash: startBlockHash.CloneBytes(),
// 		Count:     uint32(*count),
// 	}

// 	// Call the ExportBlockDB method
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
// 	defer cancel()

// 	response, err := client.ExportBlockDB(ctx, request)
// 	if err != nil {
// 		log.Printf("Failed to export block DB: %v\n", err)
// 		return
// 	}

// 	// Write the block headers to a CSV file
// 	writeToCSV(response.BlockBytes)
// }

// func writeToCSV(blockHeaders [][]byte) {
// 	file, err := os.Create("block_headers.csv")
// 	if err != nil {
// 		log.Fatalf("Failed to create CSV file: %v", err)
// 	}
// 	defer file.Close()

// 	writer := csv.NewWriter(file)
// 	defer writer.Flush()

// 	// Write headers
// 	if err := writer.Write([]string{"BlockHeader"}); err != nil {
// 		log.Printf("Failed to write CSV header: %v\n", err)
// 		return
// 	}

// 	// Write block headers
// 	for _, header := range blockHeaders {
// 		if err := writer.Write([]string{string(header)}); err != nil {
// 			log.Printf("Failed to write block header to CSV: %v\n", err)
// 			return
// 		}
// 	}
// }

// func parseHashes(hashes string) []*chainhash.Hash {
// 	if hashes == "" {
// 		return nil
// 	}
// 	hashList := strings.Split(hashes, ",")
// 	hh := make([]*chainhash.Hash, len(hashList))
// 	//byteList := make([][]byte, len(hashList))

// 	for i, hash := range hashList {
// 		h, err := chainhash.NewHashFromHex(hash)
// 		if err != nil {
// 			log.Fatalf("Failed to parse hash: %v", err)
// 		}
// 		hh[i] = h
// 	}

// 	return hh
// }
