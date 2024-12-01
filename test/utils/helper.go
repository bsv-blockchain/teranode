package helper

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/go-sdk/script"
	"github.com/bitcoin-sv/ubsv/errors"
	block_model "github.com/bitcoin-sv/ubsv/model"
	ba "github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/services/miner/cpuminer"
	"github.com/bitcoin-sv/ubsv/services/rpc/bsvjson"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	tenv "github.com/bitcoin-sv/ubsv/test/testenv"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/go-utils"
)

type Transaction struct {
	Tx string `json:"tx"`
}

var allowedHosts = []string{
	"localhost:16090",
	"localhost:16091",
	"localhost:16092",
}

// Function to call the RPC endpoint with any method and parameters, returning the response and error
func CallRPC(url string, method string, params []interface{}) (string, error) {
	logger := ulogger.New("e2eTestRun", ulogger.WithLevel("INFO"))
	// Create the request payload
	requestBody, err := json.Marshal(map[string]interface{}{
		"method": method,
		"params": params,
	})
	logger.Infof("Request: %s", string(requestBody))

	if err != nil {
		return "", errors.NewProcessingError("failed to marshal request body", err)
	}

	// Create the HTTP request
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return "", errors.NewProcessingError("failed to create request", err)
	}

	// Set the appropriate headers
	req.SetBasicAuth("bitcoin", "bitcoin")
	req.Header.Set("Content-Type", "application/json")

	// Perform the request
	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return "", errors.NewProcessingError("failed to perform request", err)
	}

	defer resp.Body.Close()

	// Check the status code
	if resp.StatusCode != http.StatusOK {
		return "", errors.NewProcessingError("expected status code 200, got %v", resp.StatusCode)
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", errors.NewProcessingError("failed to read response body", err)
	}

	/*
		Example of a response:
		{
			"result": null,
			"error": {
				"code": -32601,
				"message": "Method not found"
		}
	*/
	// Check if the response body contains an error
	var jsonResponse struct {
		Error interface{} `json:"error"`
	}

	if err := json.Unmarshal(body, &jsonResponse); err != nil {
		return string(body), errors.NewProcessingError("failed to parse response JSON", err)
	}

	if jsonResponse.Error != nil {
		return string(body), errors.NewProcessingError("RPC returned error: %v", jsonResponse.Error)
	}

	// Return the response as a string
	return string(body), nil
}

func GetBlockHeight(url string) (uint32, error) {
	resp, err := http.Get(url + "/api/v1/lastblocks?n=1")
	if err != nil {
		fmt.Printf("Error getting block height: %s\n", err)
		return 0, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, errors.NewProcessingError("unexpected status code: %d", resp.StatusCode)
	}

	var blocks []struct {
		Height uint32 `json:"height"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&blocks); err != nil {
		return 0, err
	}

	if len(blocks) == 0 {
		return 0, errors.NewProcessingError("no blocks found in response")
	}

	return blocks[0].Height, nil
}

func ReadFile(ctx context.Context, ext string, logger ulogger.Logger, r io.Reader, queryTxId chainhash.Hash, dir *url.URL) (bool, error) { //nolint:stylecheck
	switch ext {
	case "subtree":
		bl := ReadSubtree(r, logger, true, queryTxId)
		return bl, nil

	case "":
		blockHeaderBytes := make([]byte, 80)
		// read the first 80 bytes as the block header
		if _, err := io.ReadFull(r, blockHeaderBytes); err != nil {
			return false, errors.NewBlockInvalidError("error reading block header", err)
		}

		txCount, err := wire.ReadVarInt(r, 0)
		if err != nil {
			return false, errors.NewBlockInvalidError("error reading transaction count", err)
		}

		fmt.Printf("\t%d transactions\n", txCount)

	case "block": // here
		block, err := block_model.NewBlockFromReader(r)
		if err != nil {
			return false, errors.NewProcessingError("error reading block: %v\n", err)
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
		return false, errors.NewProcessingError("unknown file type")
	}

	return false, nil
}

func ReadSubtree(r io.Reader, logger ulogger.Logger, verbose bool, queryTxId chainhash.Hash) bool { //nolint:stylecheck
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
		return nil, "", nil, errors.NewProcessingError("error creating block store: %w", err)
	}

	r, err := store.GetIoReader(ctx, hash[:], options.WithFileExtension(ext))
	if err != nil {
		return nil, "", nil, errors.NewProcessingError("error getting reader from store: %w", err)
	}

	return dir, ext, r, nil
}

func GetMiningCandidate(ctx context.Context, baClient ba.Client, logger ulogger.Logger) (*block_model.MiningCandidate, error) {
	miningCandidate, err := baClient.GetMiningCandidate(ctx)
	if err != nil {
		return nil, errors.NewProcessingError("error getting mining candidate: %w", err)
	}

	return miningCandidate, nil
}

func GetMiningCandidate_rpc(url string) (string, error) { //nolint:stylecheck
	method := "getminingcandidate"
	params := []interface{}{}

	return CallRPC(url, method, params)
}

func MineBlock(ctx context.Context, baClient ba.Client, logger ulogger.Logger) ([]byte, error) {
	miningCandidate, err := baClient.GetMiningCandidate(ctx)
	if err != nil {
		return nil, errors.NewProcessingError("error getting mining candidate: %w", err)
	}

	solution, err := cpuminer.Mine(ctx, miningCandidate)
	if err != nil {
		return nil, errors.NewProcessingError("error mining block: %w", err)
	}

	blockHeader, err := cpuminer.BuildBlockHeader(miningCandidate, solution)
	if err != nil {
		return nil, errors.NewProcessingError("error building block header: %w", err)
	}

	blockHash := util.Sha256d(blockHeader)

	err = baClient.SubmitMiningSolution(ctx, solution)
	if err != nil {
		return nil, errors.NewProcessingError("error submitting mining solution: %w", err)
	}

	return blockHash, nil
}

func MineBlockWithRPC(ctx context.Context, node tenv.TeranodeTestClient, logger ulogger.Logger) (string, error) {
	resp, err := CallRPC(node.RPCURL, "generate", []interface{}{1})
	if err != nil {
		return "", errors.NewProcessingError("error generating block: %w", err)
	}

	time.Sleep(5 * time.Second)

	return resp, nil
}

func MineBlockWithCandidate(ctx context.Context, baClient ba.Client, miningCandidate *block_model.MiningCandidate, logger ulogger.Logger) ([]byte, error) {
	solution, err := cpuminer.Mine(ctx, miningCandidate)
	if err != nil {
		return nil, errors.NewProcessingError("error mining block: %w", err)
	}

	blockHeader, err := cpuminer.BuildBlockHeader(miningCandidate, solution)
	if err != nil {
		return nil, errors.NewProcessingError("error building block header: %w", err)
	}

	blockHash := util.Sha256d(blockHeader)

	err = baClient.SubmitMiningSolution(ctx, solution)
	if err != nil {
		return nil, errors.NewProcessingError("error submitting mining solution: %w", err)
	}

	return blockHash, nil
}

func MineBlockWithCandidate_rpc(ctx context.Context, rpcUrl string, miningCandidate *block_model.MiningCandidate, logger ulogger.Logger) ([]byte, error) { //nolint:stylecheck
	solution, err := cpuminer.Mine(ctx, miningCandidate)
	if err != nil {
		return nil, errors.NewProcessingError("error mining block: %w", err)
	}

	blockHeader, err := cpuminer.BuildBlockHeader(miningCandidate, solution)
	if err != nil {
		return nil, errors.NewProcessingError("error building block header: %w", err)
	}

	blockHash := util.Sha256d(blockHeader)

	submitMiningSolutionCmd := bsvjson.MiningSolution{
		ID:       utils.ReverseAndHexEncodeSlice(solution.Id),
		Coinbase: hex.EncodeToString(solution.Coinbase),
		Time:     solution.Time,
		Nonce:    solution.Nonce,
		Version:  solution.Version,
	}

	solutionJSON, err := json.Marshal(submitMiningSolutionCmd)
	if err != nil {
		return nil, errors.NewProcessingError("error marshalling solution: %w", err)
	}

	method := "submitminingsolution"

	params := []interface{}{submitMiningSolutionCmd}

	logger.Infof("Submitting mining solution: %s", string(solutionJSON))

	resp, err := CallRPC(rpcUrl, method, params)
	if err != nil {
		return nil, errors.NewProcessingError("error submitting mining solution: %w", err)
	} else {
		fmt.Printf("Response: %s\n", resp)
	}

	return blockHash, nil
}

func CreateAndSendTx(ctx context.Context, node tenv.TeranodeTestClient) (chainhash.Hash, error) {
	logger := ulogger.New("e2eTestRun", ulogger.WithLevel("INFO"))

	nilHash := chainhash.Hash{}
	privateKey, _ := bec.NewPrivateKey(bec.S256())

	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	coinbaseClient := node.CoinbaseClient

	faucetTx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to request funds: %w", err)
	}

	_, err = node.DistributorClient.SendTransaction(ctx, faucetTx)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to send transaction: %w", err)
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
		return nilHash, errors.NewProcessingError("error creating new transaction: %w", err)
	}

	err = newTx.AddP2PKHOutputFromAddress("1ApLMk225o7S9FvKwpNChB7CX8cknQT9Hy", 10000)
	if err != nil {
		return nilHash, errors.NewProcessingError("Error adding output to transaction: %w", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nilHash, errors.NewProcessingError("Error filling transaction inputs: %w", err)
	}

	_, err = node.DistributorClient.SendTransaction(ctx, newTx)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to send new transaction: %w", err)
	}

	logger.Infof("Transaction sent: %s", newTx.TxID())

	return *newTx.TxIDChainHash(), nil
}

func CreateAndSendTxToSliceOfNodes(ctx context.Context, nodes []tenv.TeranodeTestClient) (chainhash.Hash, error) {
	nilHash := chainhash.Hash{}
	privateKey, _ := bec.NewPrivateKey(bec.S256())

	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	coinbaseClient := nodes[0].CoinbaseClient

	faucetTx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to request funds: %w", err)
	}

	for _, node := range nodes {
		_, err = node.DistributorClient.SendTransaction(ctx, faucetTx)
		if err != nil {
			return nilHash, errors.NewProcessingError("Failed to send transaction: %w", err)
		}
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
		return nilHash, errors.NewProcessingError("error creating new transaction: %w", err)
	}

	err = newTx.AddP2PKHOutputFromAddress("1ApLMk225o7S9FvKwpNChB7CX8cknQT9Hy", 10000)
	if err != nil {
		return nilHash, errors.NewProcessingError("Error adding output to transaction: %w", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nilHash, errors.NewProcessingError("Error filling transaction inputs: %w", err)
	}

	for _, node := range nodes {
		_, err = node.DistributorClient.SendTransaction(ctx, newTx)
		if err != nil {
			return nilHash, errors.NewProcessingError("Failed to send new transaction: %w", err)
		}
	}

	return *newTx.TxIDChainHash(), nil
}

func CreateAndSendDoubleSpendTx(ctx context.Context, nodes []tenv.TeranodeTestClient) (chainhash.Hash, error) {
	nilHash := chainhash.Hash{}

	privateKey, _ := bec.NewPrivateKey(bec.S256())

	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	coinbaseClient := nodes[0].CoinbaseClient

	faucetTx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to request funds: %w", err)
	}

	_, err = nodes[0].DistributorClient.SendTransaction(ctx, faucetTx)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to send transaction: %w", err)
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
		return nilHash, errors.NewProcessingError("error creating new transaction: %w", err)
	}

	newTx.LockTime = 0

	newTxDouble := bt.NewTx()

	err = newTxDouble.FromUTXOs(utxo)
	if err != nil {
		return nilHash, errors.NewProcessingError("error creating new transaction: %w", err)
	}

	newTxDouble.LockTime = 1

	err = newTx.AddP2PKHOutputFromAddress("1ApLMk225o7S9FvKwpNChB7CX8cknQT9Hy", 10000)
	if err != nil {
		return nilHash, errors.NewProcessingError("Error adding output to transaction: %w", err)
	}

	err = newTxDouble.AddP2PKHOutputFromAddress("14qViLJfdGaP4EeHnDyJbEGQysnCpwk3gd", 10000)
	if err != nil {
		return nilHash, errors.NewProcessingError("Error adding output to transaction: %w", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nilHash, errors.NewProcessingError("Error filling transaction inputs: %w", err)
	}

	err = newTxDouble.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nilHash, errors.NewProcessingError("Error filling transaction inputs: %w", err)
	}

	_, err = nodes[0].DistributorClient.SendTransaction(ctx, newTx)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to send new transaction: %w", err)
	}

	_, err = nodes[1].DistributorClient.SendTransaction(ctx, newTxDouble)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to send new transaction: %w", err)
	}

	return *newTx.TxIDChainHash(), nil
}

func CreateAndSendTxs(ctx context.Context, node tenv.TeranodeTestClient, count int) ([]chainhash.Hash, error) {
	var txHashes []chainhash.Hash

	for i := 0; i < count; i++ {
		tx, err := CreateAndSendTx(ctx, node)
		if err != nil {
			return nil, errors.NewProcessingError("error creating raw transaction : %w", err)
		}

		txHashes = append(txHashes, tx)

		time.Sleep(1 * time.Second) // Wait 10 seconds between transactions
	}

	return txHashes, nil
}

func CreateAndSendTxsToASliceOfNodes(ctx context.Context, nodes []tenv.TeranodeTestClient, count int) ([]chainhash.Hash, error) {
	var txHashes []chainhash.Hash

	for i := 0; i < count; i++ {
		tx, err := CreateAndSendTxToSliceOfNodes(ctx, nodes)
		if err != nil {
			return nil, errors.NewProcessingError("error creating raw transaction : %w", err)
		}

		txHashes = append(txHashes, tx)

		time.Sleep(1 * time.Second) // Wait 10 seconds between transactions
	}

	return txHashes, nil
}

func CreateAndSendTxsConcurrently(ctx context.Context, node tenv.TeranodeTestClient, count int) ([]chainhash.Hash, error) {
	var wg sync.WaitGroup

	var txHashes []chainhash.Hash

	var errorsList []error

	txErrors := make(chan error, count)
	txResults := make(chan chainhash.Hash, count)

	for i := 0; i < count; i++ {
		wg.Add(1)

		go func(index int) {
			defer wg.Done()

			tx, err := CreateAndSendTx(ctx, node)
			if err != nil {
				txErrors <- errors.NewProcessingError("error creating raw transaction: %w", err)
				return
			}
			txResults <- tx
		}(i)
		time.Sleep(100 * time.Millisecond)
	}

	for {
		select {
		case err := <-txErrors:
			if err != nil {
				fmt.Printf("Error received: %s\n", err)
				errorsList = append(errorsList, err)
			}
		case tx := <-txResults:
			txHashes = append(txHashes, tx)
		}

		if len(txHashes)+len(errorsList) >= count {
			break
		}
	}

	if len(errorsList) > 0 {
		return nil, errors.NewProcessingError("one or more errors occurred: %v", errorsList)
	}

	wg.Wait()
	close(txErrors)
	close(txResults)

	return txHashes, nil
}

func UseCoinbaseUtxo(ctx context.Context, node tenv.TeranodeTestClient, coinbaseTx *bt.Tx) (chainhash.Hash, error) {
	nilHash := chainhash.Hash{}
	privateKey, _ := bec.NewPrivateKey(bec.S256())

	output := coinbaseTx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()
	err := newTx.FromUTXOs(utxo)

	if err != nil {
		return nilHash, errors.NewProcessingError("error creating new transaction: %w", err)
	}

	err = newTx.AddP2PKHOutputFromAddress("1ApLMk225o7S9FvKwpNChB7CX8cknQT9Hy", 10000)
	if err != nil {
		return nilHash, errors.NewProcessingError("Error adding output to transaction: %w", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nilHash, errors.NewProcessingError("Error filling transaction inputs: %w", err)
	}

	_, err = node.DistributorClient.SendTransaction(ctx, newTx)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to send new transaction: %w", err)
	}

	return *newTx.TxIDChainHash(), nil
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

func UseCoinbaseUtxoV2(ctx context.Context, node tenv.TeranodeTestClient, coinbaseTx *bt.Tx) (chainhash.Hash, error) {
	nilHash := chainhash.Hash{}
	privateKey, _ := bec.NewPrivateKey(bec.S256())

	output := coinbaseTx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()
	err := newTx.FromUTXOs(utxo)

	if err != nil {
		return nilHash, errors.NewProcessingError("error creating new transaction: %w", err)
	}

	err = newTx.AddP2PKHOutputFromAddress("1ApLMk225o7S9FvKwpNChB7CX8cknQT9Hy", 10000)
	if err != nil {
		return nilHash, errors.NewProcessingError("Error adding output to transaction: %w", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nilHash, errors.NewProcessingError("Error filling transaction inputs: %w", err)
	}

	_, err = node.DistributorClient.SendTransaction(ctx, newTx)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to send new transaction: %w", err)
	}

	return *newTx.TxIDChainHash(), nil
}

func SendTXsWithDistributorV2(ctx context.Context, node tenv.TeranodeTestClient, logger ulogger.Logger, tSettings *settings.Settings, fees uint64) (bool, error) {
	var defaultSathosis uint64 = 10000

	logger.Infof("Sending transactions with distributor setting fees to %v", tSettings.Propagation.GRPCAddresses)

	// Send transactions
	txDistributor := node.DistributorClient

	coinbaseClient := node.CoinbaseClient

	coinbasePrivKey := tSettings.Coinbase.WalletPrivateKey
	if coinbasePrivKey == "" {
		return false, errors.NewProcessingError("Coinbase private key is not set")
	}

	coinbasePrivateKey, err := wif.DecodeWIF(coinbasePrivKey)
	if err != nil {
		return false, errors.NewProcessingError("Failed to decode Coinbase private key: %v", err)
	}

	coinbaseAddr, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)

	privateKey, err := bec.NewPrivateKey(bec.S256())
	if err != nil {
		return false, errors.NewProcessingError("Failed to generate private key: %v", err)
	}

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	if err != nil {
		return false, errors.NewProcessingError("Failed to create address: %v", err)
	}

	tx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	if err != nil {
		return false, errors.NewProcessingError("Failed to request funds: %v", err)
	}

	_, err = txDistributor.SendTransaction(ctx, tx)
	if err != nil {
		return false, errors.NewProcessingError("Failed to send transaction: %v", err)
	}

	fmt.Printf("Transaction sent: %s %v\n", tx.TxIDChainHash(), len(tx.Outputs))
	fmt.Printf("TxOut: %v\n", tx.Outputs[0].Satoshis)

	utxo := &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: tx.Outputs[0].LockingScript,
		Satoshis:      tx.Outputs[0].Satoshis,
	}

	newTx := bt.NewTx()
	err = newTx.FromUTXOs(utxo)

	if err != nil {
		return false, errors.NewProcessingError("Error adding UTXO to transaction: %s\n", err)
	}

	if fees != 0 {
		defaultSathosis -= defaultSathosis
	}

	err = newTx.AddP2PKHOutputFromAddress(coinbaseAddr.AddressString, defaultSathosis)
	if err != nil {
		return false, errors.NewProcessingError("Error adding output to transaction: %v", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return false, errors.NewProcessingError("Error filling transaction inputs: %v", err)
	}

	_, err = txDistributor.SendTransaction(ctx, newTx)
	if err != nil {
		return false, errors.NewProcessingError("Failed to send new transaction: %v", err)
	}

	fmt.Printf("Transaction sent: %s %s\n", newTx.TxIDChainHash(), newTx.TxID())
	time.Sleep(5 * time.Second)

	return true, nil
}

func GetBestBlockV2(ctx context.Context, node tenv.TeranodeTestClient) (*block_model.Block, error) {
	bbheader, _, errbb := node.BlockchainClient.GetBestBlockHeader(ctx)
	if errbb != nil {
		return nil, errors.NewProcessingError("Error getting best block header: %s\n", errbb)
	}

	block, errblock := node.BlockchainClient.GetBlock(ctx, bbheader.Hash())
	if errblock != nil {
		return nil, errors.NewProcessingError("Error getting block by height: %s\n", errblock)
	}

	return block, nil
}

func QueryPrometheusMetric(serverURL, metricName string) (float64, error) {
	parsedURL, err := url.Parse(serverURL)
	if err != nil {
		return 0, errors.New(errors.ERR_ERROR, "invalid server URL: %v", err)
	}

	if !isAllowedHost(parsedURL.Host) {
		return 0, errors.New(errors.ERR_ERROR, "host not allowed: %v", parsedURL.Host)
	}

	queryURL := fmt.Sprintf("%s/api/v1/query?query=%s", serverURL, metricName)

	req, err := http.NewRequest("GET", queryURL, nil)
	if err != nil {
		return 0, errors.New(errors.ERR_ERROR, "error creating HTTP request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, errors.New(errors.ERR_ERROR, "error sending HTTP request: %v", err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, errors.New(errors.ERR_ERROR, "error reading Prometheus response body: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, errors.New(errors.ERR_ERROR, "error unmarshaling Prometheus response: %v", err)
	}

	data, ok := result["data"].(map[string]interface{})
	if !ok {
		return 0, errors.New(errors.ERR_ERROR, "unexpected Prometheus response format: data field not found")
	}

	resultArray, ok := data["result"].([]interface{})
	if !ok || len(resultArray) == 0 {
		return 0, errors.New(errors.ERR_ERROR, "unexpected Prometheus response format: result field not found or empty")
	}

	metric, ok := resultArray[0].(map[string]interface{})
	if !ok {
		return 0, errors.New(errors.ERR_ERROR, "unexpected Prometheus response format: result array element is not a map")
	}

	value, ok := metric["value"].([]interface{})
	if !ok || len(value) < 2 {
		return 0, errors.New(errors.ERR_ERROR, "unexpected Prometheus response format: value field not found or invalid")
	}

	metricValueStr, ok := value[1].(string)
	if !ok {
		return 0, errors.New(errors.ERR_ERROR, "unexpected Prometheus response format: metric value is not a string")
	}

	metricValue, err := strconv.ParseFloat(metricValueStr, 64)
	if err != nil {
		return 0, errors.New(errors.ERR_ERROR, "error parsing Prometheus metric value: %v", err)
	}

	return metricValue, nil
}

func isAllowedHost(host string) bool {
	for _, allowedHost := range allowedHosts {
		if host == allowedHost {
			return true
		}
	}

	return false
}

func WaitForBlockHeight(url string, targetHeight uint32, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout*time.Second)
	defer cancel()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.NewError("timeout waiting for block height")
		case <-ticker.C:
			currentHeight, err := GetBlockHeight(url)

			if err != nil {
				return errors.NewError("error getting block height: %v", err)
			}

			if currentHeight >= targetHeight {
				return nil
			}
		}
	}
}

func CheckIfTxExistsInBlock(ctx context.Context, store blob.Store, storeURL *url.URL, block []byte, blockHeight uint32, tx chainhash.Hash, logger ulogger.Logger) (bool, error) {
	r, err := store.GetIoReader(ctx, block, options.WithFileExtension("block"))
	if err != nil {
		return false, errors.NewProcessingError("error getting block reader: %v", err)
	}

	if bl, err := ReadFile(ctx, "block", logger, r, tx, storeURL); err != nil {
		return false, errors.NewProcessingError("error reading block: %v", err)
	} else {
		logger.Infof("Block at height (%d): was tested for the test Tx\n", blockHeight)
		return bl, nil
	}
}

func Unzip(src, dest string) error {
	cmd := exec.Command("unzip", "-f", src, "-d", dest)
	err := cmd.Run()

	return err
}

func CopyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return err
	}

	err = dstFile.Sync()
	if err != nil {
		return err
	}

	return nil
}

func GetUtxoBalance(ctx context.Context, node tenv.TeranodeTestClient) uint64 {
	utxoBalance, _, _ := node.CoinbaseClient.GetBalance(ctx)
	return utxoBalance
}

func GeneratePrivateKeyAndAddress() (*bec.PrivateKey, *bscript.Address, error) {
	privateKey, err := bec.NewPrivateKey(bec.S256())
	if err != nil {
		return nil, nil, err
	}

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	if err != nil {
		return nil, nil, err
	}

	return privateKey, address, nil
}

func RequestFunds(ctx context.Context, node tenv.TeranodeTestClient, address string) (*bt.Tx, error) {
	faucetTx, err := node.CoinbaseClient.RequestFunds(ctx, address, true)
	if err != nil {
		return nil, err
	}

	return faucetTx, nil
}

func SendTransaction(ctx context.Context, node tenv.TeranodeTestClient, tx *bt.Tx) (bool, error) {
	_, err := node.DistributorClient.SendTransaction(ctx, tx)
	if err != nil {
		return false, err
	}

	return true, nil
}

func CreateUtxoFromTransaction(tx *bt.Tx, vout uint32) *bt.UTXO {
	output := tx.Outputs[vout]

	return &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          vout,
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}
}

func CreateTransaction(utxo *bt.UTXO, address string, satoshis uint64, privateKey *bec.PrivateKey) (*bt.Tx, error) {
	tx := bt.NewTx()

	err := tx.FromUTXOs(utxo)
	if err != nil {
		return nil, err
	}

	err = tx.AddP2PKHOutputFromAddress(address, satoshis)
	if err != nil {
		return nil, err
	}

	err = tx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func FreezeUtxos(ctx context.Context, testenv tenv.TeranodeTestEnv, tx *bt.Tx, logger ulogger.Logger) error {
	utxoHash, _ := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	spend := &utxo.Spend{
		TxID:     tx.TxIDChainHash(),
		Vout:     0,
		UTXOHash: utxoHash,
	}

	for _, node := range testenv.Nodes {
		err := node.UtxoStore.FreezeUTXOs(ctx, []*utxo.Spend{spend})
		if err != nil {
			logger.Errorf("Error freezing UTXOs on node %v: %v", err, node.Name)
		}
	}

	return nil
}

func ReassignUtxo(ctx context.Context, testenv tenv.TeranodeTestEnv, firstTx, reassignTx *bt.Tx, logger ulogger.Logger) error {
	publicKey, err := extractPublicKey(reassignTx.Inputs[0].UnlockingScript.Bytes())
	if err != nil {
		return err
	}

	newLockingScript, err := bscript.NewP2PKHFromPubKeyBytes(publicKey)
	if err != nil {
		return err
	}

	amendedOutputScript := &bt.Output{
		Satoshis:      firstTx.Outputs[0].Satoshis,
		LockingScript: newLockingScript,
	}

	oldUtxoHash, err := util.UTXOHashFromOutput(firstTx.TxIDChainHash(), firstTx.Outputs[0], 0)
	if err != nil {
		return err
	}

	newUtxoHash, err := util.UTXOHashFromOutput(firstTx.TxIDChainHash(), amendedOutputScript, 0)
	if err != nil {
		return err
	}

	newSpend := &utxo.Spend{
		TxID:     firstTx.TxIDChainHash(),
		Vout:     0,
		UTXOHash: newUtxoHash,
	}

	for _, node := range testenv.Nodes {
		err = node.UtxoStore.ReAssignUTXO(ctx, &utxo.Spend{
			TxID:     firstTx.TxIDChainHash(),
			Vout:     0,
			UTXOHash: oldUtxoHash,
		}, newSpend)

		if err != nil {
			return err
		}

		logger.Infof("UTXO reassigned on node %v", node.Name)
	}

	return nil
}

func extractPublicKey(scriptSig []byte) ([]byte, error) {
	// Parse the scriptSig into an array of elements (OpCodes or Data pushes)
	parsedScript := script.NewFromBytes(scriptSig)

	elements, err := parsedScript.ParseOps()
	if err != nil {
		return nil, err
	}

	// For P2PKH, the last element is the public key (the second item in scriptSig)
	if len(elements) < 2 {
		return nil, errors.NewProcessingError("invalid P2PKH scriptSig")
	}

	publicKey := elements[len(elements)-1].Data // Last element is the public key

	return publicKey, nil
}

func RemoveDataDirectory(dir string, useSudo bool) error {
	var cmd *exec.Cmd

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil
	}

	if !useSudo {
		cmd = exec.Command("rm", "-rf", dir)
	} else {
		cmd = exec.Command("sudo", "rm", "-rf", dir)
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		return err
	}

	return nil
}
