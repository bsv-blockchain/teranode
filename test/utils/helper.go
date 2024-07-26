package helper

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/services/miner/cpuminer"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"

	block_model "github.com/bitcoin-sv/ubsv/model"
	ba "github.com/bitcoin-sv/ubsv/services/blockassembly"
	utxom "github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"
	tf "github.com/bitcoin-sv/ubsv/test/test_framework"
)

type Transaction struct {
	Tx string `json:"tx"`
}

// Function to call the RPC endpoint with any method and parameters, returning the response and error
func CallRPC(url string, method string, params []interface{}) (string, error) {

	// Create the request payload
	requestBody, err := json.Marshal(map[string]interface{}{
		"method": method,
		"params": params,
	})
	if err != nil {
		return "", errors.New(errors.ERR_PROCESSING, "failed to marshal request body", err)
	}

	// Create the HTTP request
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return "", errors.New(errors.ERR_PROCESSING, "failed to create request", err)
	}

	// Set the appropriate headers
	req.SetBasicAuth("bitcoin", "bitcoin")
	req.Header.Set("Content-Type", "application/json")

	// Perform the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", errors.New(errors.ERR_PROCESSING, "failed to perform request", err)
	}
	defer resp.Body.Close()

	// Check the status code
	if resp.StatusCode != http.StatusOK {
		return "", errors.New(errors.ERR_PROCESSING, "expected status code 200, got %v", resp.StatusCode)
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", errors.New(errors.ERR_PROCESSING, "failed to read response body", err)
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
		return 0, errors.New(errors.ERR_PROCESSING, "unexpected status code: %d", resp.StatusCode)
	}

	var blocks []struct {
		Height int `json:"height"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&blocks); err != nil {
		return 0, err
	}

	if len(blocks) == 0 {
		return 0, errors.New(errors.ERR_PROCESSING, "no blocks found in response")
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

func ReadFile(ctx context.Context, ext string, logger ulogger.Logger, r io.Reader, queryTxId chainhash.Hash, dir *url.URL) (bool, error) {
	switch ext {
	case "utxodiff":
		utxodiff, err := utxom.NewUTXODiffFromReader(logger, r)
		if err != nil {
			return false, errors.New(errors.ERR_PROCESSING, "error reading utxodiff: %w\n", err)
		}

		fmt.Printf("UTXODiff block hash: %v\n", utxodiff.BlockHash)

		fmt.Printf("UTXODiff removed %d UTXOs", utxodiff.Removed.Length())

		fmt.Printf("UTXODiff added %d UTXOs", utxodiff.Added.Length())

	case "utxoset":
		utxoSet, err := utxom.NewUTXOSetFromReader(logger, r)
		if err != nil {
			return false, errors.New(errors.ERR_PROCESSING, "error reading utxoSet: %v\n", err)
		}

		fmt.Printf("UTXOSet block hash: %v\n", utxoSet.BlockHash)

		fmt.Printf("UTXOSet with %d UTXOs", utxoSet.Current.Length())

	case "subtree":
		bl := ReadSubtree(r, logger, true, queryTxId)
		return bl, nil

	case "":
		blockHeaderBytes := make([]byte, 80)
		// read the first 80 bytes as the block header
		if _, err := io.ReadFull(r, blockHeaderBytes); err != nil {
			return false, errors.New(errors.ERR_BLOCK_INVALID, "error reading block header", err)
		}

		txCount, err := wire.ReadVarInt(r, 0)
		if err != nil {
			return false, errors.New(errors.ERR_BLOCK_INVALID, "error reading transaction count", err)
		}

		fmt.Printf("\t%d transactions\n", txCount)

	case "block":
		block, err := block_model.NewBlockFromReader(r)
		if err != nil {
			return false, errors.New(errors.ERR_PROCESSING, "error reading block: %v\n", err)
		}

		fmt.Printf("Block hash: %s\n", block.Hash())
		fmt.Printf("%s", block.Header.StringDump())
		fmt.Printf("Number of transactions: %d\n", block.TransactionCount)

		for _, subtree := range block.Subtrees {
			if true {
				filename := fmt.Sprintf("%s.subtree", subtree.String())
				fmt.Printf("Reading subtree from %s\n", filename)
				_, _, stReader, err := GetReader(ctx, filename, dir, logger)
				if err != nil {
					return false, err
				}
				return ReadSubtree(stReader, logger, true, queryTxId), nil
			}
		}

	default:
		return false, errors.New(errors.ERR_PROCESSING, "unknown file type")
	}

	return false, nil
}

func ReadSubtree(r io.Reader, logger ulogger.Logger, verbose bool, queryTxId chainhash.Hash) bool {
	var num uint32

	if err := binary.Read(r, binary.LittleEndian, &num); err != nil {
		fmt.Printf("error reading transaction count: %v\n", err)
		os.Exit(1)
	}
	if verbose {
		for i := uint32(0); i < num; i++ {
			var tx bt.Tx
			_, err := tx.ReadFrom(r)
			if err != nil {
				fmt.Printf("error reading transaction: %v\n", err)
				os.Exit(1)
			}
			if *tx.TxIDChainHash() == queryTxId {
				fmt.Printf(" (test txid) %v found\n", queryTxId)
				return true
			}
		}
	}
	return false
}

func GetReader(ctx context.Context, file string, dir *url.URL, logger ulogger.Logger) (*url.URL, string, io.Reader, error) {
	ext := filepath.Ext(file)
	fileWithoutExtension := strings.TrimSuffix(file, ext)

	if ext[0] == '.' {
		ext = ext[1:]
	}

	hash, _ := chainhash.NewHashFromStr(fileWithoutExtension)

	store, err := blob.NewStore(logger, dir)
	if err != nil {
		return nil, "", nil, errors.New(errors.ERR_PROCESSING, "error creating block store: %w", err)
	}
	r, err := store.GetIoReader(ctx, hash[:], options.WithFileExtension(ext))
	if err != nil {
		return nil, "", nil, errors.New(errors.ERR_PROCESSING, "error getting reader from store: %w", err)
	}

	return dir, ext, r, nil
}

func GetMiningCandidate(ctx context.Context, baClient ba.Client, logger ulogger.Logger) (*block_model.MiningCandidate, error) {
	miningCandidate, err := baClient.GetMiningCandidate(ctx)
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "error getting mining candidate: %w", err)
	}
	return miningCandidate, nil
}

func MineBlock(ctx context.Context, baClient ba.Client, logger ulogger.Logger) ([]byte, error) {
	miningCandidate, err := baClient.GetMiningCandidate(ctx)
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "error getting mining candidate: %w", err)
	}

	solution, err := cpuminer.Mine(ctx, miningCandidate)
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "error mining block: %w", err)
	}

	blockHeader, err := cpuminer.BuildBlockHeader(miningCandidate, solution)
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "error building block header: %w", err)
	}

	blockHash := util.Sha256d(blockHeader)

	err = baClient.SubmitMiningSolution(ctx, solution)
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "error submitting mining solution: %w", err)
	}

	return blockHash, nil
}

func MineBlockWithCandidate(ctx context.Context, baClient ba.Client, miningCandidate *block_model.MiningCandidate, logger ulogger.Logger) ([]byte, error) {
	solution, err := cpuminer.Mine(ctx, miningCandidate)
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "error mining block: %w", err)
	}

	blockHeader, err := cpuminer.BuildBlockHeader(miningCandidate, solution)
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "error building block header: %w", err)
	}

	blockHash := util.Sha256d(blockHeader)

	err = baClient.SubmitMiningSolution(ctx, solution)
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "error submitting mining solution: %w", err)
	}

	err = baClient.SubmitMiningSolution(ctx, solution)
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "error submitting mining solution: %w", err)
	}
	return blockHash, nil
}

func CreateAndSendRawTx(ctx context.Context, node tf.BitcoinNode) (chainhash.Hash, error) {

nilHash := chainhash.Hash{}
privateKey, _ := bec.NewPrivateKey(bec.S256())

address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

coinbaseClient := node.CoinbaseClient

faucetTx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	if err != nil {
		return nilHash, errors.New(errors.ERR_PROCESSING, "Failed to request funds: %w", err)
	}
	_, err = node.DistributorClient.SendTransaction(ctx, faucetTx)
	if err != nil {
		return nilHash, errors.New(errors.ERR_PROCESSING, "Failed to send transaction: %w", err)
	}

	output := faucetTx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      faucetTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()
	err = newTx.FromUTXOs(utxo)
	if err != nil {
		return nilHash, errors.New(errors.ERR_PROCESSING, "error creating new transaction: %w", err)
	}

	err = newTx.AddP2PKHOutputFromAddress("1ApLMk225o7S9FvKwpNChB7CX8cknQT9Hy", 10000)
	if err != nil {
		return nilHash, errors.New(errors.ERR_PROCESSING, "Error adding output to transaction: %w", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nilHash, errors.New(errors.ERR_PROCESSING, "Error filling transaction inputs: %w", err)
	}

	_, err = node.DistributorClient.SendTransaction(ctx, newTx)
	if err != nil {
		return nilHash, errors.New(errors.ERR_PROCESSING, "Failed to send new transaction: %w", err)
	}

	return *newTx.TxIDChainHash(), nil
}

func CreateAndSendDoubleSpendTx(ctx context.Context, node []tf.BitcoinNode) (chainhash.Hash, error) {


	nilHash := chainhash.Hash{}

	privateKey, _ := bec.NewPrivateKey(bec.S256())

	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	coinbaseClient := node[0].CoinbaseClient

	faucetTx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	if err != nil {
		return nilHash, errors.New(errors.ERR_PROCESSING, "Failed to request funds: %w", err)
	}
	_, err = node[0].DistributorClient.SendTransaction(ctx, faucetTx)
	if err != nil {
		return nilHash, errors.New(errors.ERR_PROCESSING, "Failed to send transaction: %w", err)
	}

	output := faucetTx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      faucetTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()
	err = newTx.FromUTXOs(utxo)
	if err != nil {
		return nilHash, errors.New(errors.ERR_PROCESSING, "error creating new transaction: %w", err)
	}
	newTx.LockTime = 0

	newTxDouble := bt.NewTx()
	err = newTxDouble.FromUTXOs(utxo)
	if err != nil {
		return nilHash, errors.New(errors.ERR_PROCESSING, "error creating new transaction: %w", err)
	}
	newTxDouble.LockTime = 1

	err = newTx.AddP2PKHOutputFromAddress("1ApLMk225o7S9FvKwpNChB7CX8cknQT9Hy", 10000)
	if err != nil {
		return nilHash, errors.New(errors.ERR_PROCESSING, "Error adding output to transaction: %w", err)
	}
	err = newTxDouble.AddP2PKHOutputFromAddress("14qViLJfdGaP4EeHnDyJbEGQysnCpwk3gd", 10000)
	if err != nil {
		return nilHash, errors.New(errors.ERR_PROCESSING, "Error adding output to transaction: %w", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nilHash, errors.New(errors.ERR_PROCESSING, "Error filling transaction inputs: %w", err)
	}
	err = newTxDouble.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nilHash, errors.New(errors.ERR_PROCESSING, "Error filling transaction inputs: %w", err)
	}

	_, err = node[0].DistributorClient.SendTransaction(ctx, newTx)
	if err != nil {
		return nilHash, errors.New(errors.ERR_PROCESSING, "Failed to send new transaction: %w", err)
	}
	_, err = node[1].DistributorClient.SendTransaction(ctx, newTxDouble)
	if err != nil {
		return nilHash, errors.New(errors.ERR_PROCESSING, "Failed to send new transaction: %w", err)
	}

	return *newTx.TxIDChainHash(), nil
}

func CreateAndSendRawTxs(ctx context.Context, node tf.BitcoinNode, count int) ([]chainhash.Hash, error) {
	var txHashes []chainhash.Hash

	for i := 0; i < count; i++ {
		tx, err := CreateAndSendRawTx(ctx, node)
		if err != nil {
			return nil, errors.New(errors.ERR_PROCESSING, "error creating raw transaction : %w", err)
		}
		txHashes = append(txHashes, tx)
		time.Sleep(1 * time.Second) // Wait 10 seconds between transactions
	}

	return txHashes, nil
}

// faucetTx, err := bt.NewTxFromString(tx)
// if err != nil {
// 	fmt.Printf("error creating transaction from string", err)
// }

// payload := []byte(fmt.Sprintf(`{"address":"%s"}`, address.AddressString))
// req, err := http.NewRequest("POST", faucetURL, bytes.NewBuffer(payload))
// if err != nil {
// 	fmt.Printf("error creating request", err)
// }

// req.Header.Set("Content-Type", "application/json")
// client := &http.Client{}
// resp, err := client.Do(req)
// if err != nil {
// 	fmt.Printf("error sending request", err)
// }

// defer resp.Body.Close()

// var response Transaction
// err = json.NewDecoder(resp.Body).Decode(&response)
// if err != nil {
// 	fmt.Printf("error decoding response", err)
// }

// tx := response.Tx
