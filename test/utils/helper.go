package helper

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/services/miner/cpuminer"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"

	block_model "github.com/bitcoin-sv/ubsv/model"
	ba "github.com/bitcoin-sv/ubsv/services/blockassembly"
	utxom "github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"
)

// Function to call the RPC endpoint with any method and parameters, returning the response and error
func CallRPC(url string, method string, params []interface{}) (string, error) {

	// Create the request payload
	requestBody, err := json.Marshal(map[string]interface{}{
		"method": method,
		"params": params,
	})
	if err != nil {
		return "", fmt.Errorf("failed to marshal request body: %v", err)
	}

	// Create the HTTP request
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}

	// Set the appropriate headers
	req.SetBasicAuth("bitcoin", "bitcoin")
	req.Header.Set("Content-Type", "application/json")

	// Perform the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to perform request: %v", err)
	}
	defer resp.Body.Close()

	// Check the status code
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("expected status code 200, got %v", resp.StatusCode)
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %v", err)
	}

	// Return the response as a string
	return string(body), nil
}

// Example usage of the function
//
//	func main() {
//		// Call the function with the "getblock" method and specific parameters
//		response, err := callRPC("http://localhost:19292", "getblock", []interface{}{"003e8c9abde82685fdacfd6594d9de14801c4964e1dbe79397afa6299360b521", 1})
//		if err != nil {
//			fmt.Printf("Error: %v\n", err)
//		} else {
//			fmt.Printf("Response: %s\n", response)
//		}
//	}
func GetBlockHeight(url string) (int, error) {
	resp, err := http.Get(url + "/api/v1/lastblocks?n=1")
	if err != nil {
		fmt.Printf("Error getting block height: %s\n", err)
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var blocks []struct {
		Height int `json:"height"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&blocks); err != nil {
		return 0, err
	}

	if len(blocks) == 0 {
		return 0, fmt.Errorf("no blocks found in response")
	}

	return blocks[0].Height, nil
}

func GetBlockStore(logger ulogger.Logger) blob.Store {
	blockStoreUrl, err, found := gocore.Config().GetURL("blockstore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("blockstore config not found")
	}

	blockStore, err := blob.NewStore(logger, blockStoreUrl)
	if err != nil {
		panic(err)
	}

	return blockStore
}

func ReadFile(ctx context.Context, ext string, logger ulogger.Logger, r io.Reader, queryTxId chainhash.Hash, dir string) (bool, error) {
	switch ext {
	case "utxodiff":
		utxodiff, err := utxom.NewUTXODiffFromReader(logger, r)
		if err != nil {
			return false, fmt.Errorf("error reading utxodiff: %w\n", err)
		}

		fmt.Printf("UTXODiff block hash: %v\n", utxodiff.BlockHash)

		fmt.Printf("UTXODiff removed %d UTXOs", utxodiff.Removed.Length())

		fmt.Printf("UTXODiff added %d UTXOs", utxodiff.Added.Length())

	case "utxoset":
		utxoSet, err := utxom.NewUTXOSetFromReader(logger, r)
		if err != nil {
			return false, fmt.Errorf("error reading utxoSet: %v\n", err)
		}

		fmt.Printf("UTXOSet block hash: %v\n", utxoSet.BlockHash)

		fmt.Printf("UTXOSet with %d UTXOs", utxoSet.Current.Length())

	case "subtree":
		bl := ReadSubtree(r, logger, true, queryTxId)
		// fmt.Printf("Number of transactions: %d\n", num)
		return bl, nil

	case "":
		blockHeaderBytes := make([]byte, 80)
		// read the first 80 bytes as the block header
		if _, err := io.ReadFull(r, blockHeaderBytes); err != nil {
			return false, errors.New(errors.ERR_BLOCK_INVALID, "error reading block header", err)
		}

		// read the transaction count
		txCount, err := wire.ReadVarInt(r, 0)
		if err != nil {
			return false, errors.New(errors.ERR_BLOCK_INVALID, "error reading transaction count", err)
		}

		fmt.Printf("\t%d transactions\n", txCount)

	case "block":
		block, err := block_model.NewBlockFromReader(r)
		if err != nil {
			return false, fmt.Errorf("error reading block: %v\n", err)
		}

		fmt.Printf("Block hash: %s\n", block.Hash())
		fmt.Printf("%s", block.Header.StringDump())
		fmt.Printf("Number of transactions: %d\n", block.TransactionCount)

		for _, subtree := range block.Subtrees {
			if true {
				filename := filepath.Join(dir, fmt.Sprintf("%s.subtree", subtree.String()))
				_, _, stReader, err := GetReader(ctx, filename, logger)
				if err != nil {
					return false, err
				}
				return ReadSubtree(stReader, logger, true, queryTxId), nil
			}
		}

	default:
		return false, fmt.Errorf("unknown file type")
	}

	return false, nil
}

func ReadSubtree(r io.Reader, logger ulogger.Logger, verbose bool, queryTxId chainhash.Hash) bool {
	var num uint32

	if err := binary.Read(r, binary.LittleEndian, &num); err != nil {
		fmt.Printf("error reading transaction count: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Number of transactions: %d\n", num)
	if verbose {
		for i := uint32(0); i < num; i++ {
			var tx bt.Tx
			_, err := tx.ReadFrom(r)
			if err != nil {
				fmt.Printf("error reading transaction: %v\n", err)
				os.Exit(1)
			}

			// if block_model.IsCoinbasePlaceHolderTx(&tx) {
			// 	fmt.Printf("%10d: Coinbase Placeholder\n", i)
			// } else {
			// 	fmt.Printf("%10d: %v", i, tx.TxIDChainHash())
			// }
			fmt.Printf("%10d: %v", i, tx.TxIDChainHash())
			if *tx.TxIDChainHash() == queryTxId {
				fmt.Printf(" (test txid) %v found\n", queryTxId)
				return true
			}

		}
	}
	return false
}

func GetReader(ctx context.Context, path string, logger ulogger.Logger) (string, string, io.Reader, error) {
	dir, file := filepath.Split(path)

	ext := filepath.Ext(file)
	fileWithoutExtension := strings.TrimSuffix(file, ext)

	if ext[0] == '.' {
		ext = ext[1:]
	}

	hash, err := chainhash.NewHashFromStr(fileWithoutExtension)

	if dir == "" && err == nil {
		store := GetBlockStore(logger)

		r, err := store.GetIoReader(ctx, hash[:], options.WithFileExtension(ext))
		if err != nil {
			return "", "", nil, fmt.Errorf("error getting reader from store: %w", err)
		}

		return dir, ext, r, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return "", "", nil, fmt.Errorf("error opening file: %v\n", err)
	}

	return dir, ext, f, nil
}

func MineBlock(ctx context.Context, baClient ba.Client, logger ulogger.Logger) ([]byte, error) {
	miningCandidate, err := baClient.GetMiningCandidate(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting mining candidate: %w", err)
	}

	solution, err := cpuminer.Mine(ctx, miningCandidate)
	if err != nil {
		return nil, fmt.Errorf("error mining block: %w", err)
	}

	blockHeader, err := cpuminer.BuildBlockHeader(miningCandidate, solution)
	if err != nil {
		return nil, fmt.Errorf("error building block header: %w", err)
	}

	blockHash := util.Sha256d(blockHeader)

	err = baClient.SubmitMiningSolution(ctx, solution)
	if err != nil {
		return nil, fmt.Errorf("error submitting mining solution: %w", err)
	}

	err = baClient.SubmitMiningSolution(ctx, solution)
	if err != nil {
		return nil, fmt.Errorf("error submitting mining solution: %w", err)
	}
	return blockHash, nil
}
