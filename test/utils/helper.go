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
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	block_model "github.com/bitcoin-sv/ubsv/model"
	ba "github.com/bitcoin-sv/ubsv/services/blockassembly"
	utxom "github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/services/miner/cpuminer"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	tf "github.com/bitcoin-sv/ubsv/test/test_framework"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/distributor"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
)

type Transaction struct {
	Tx string `json:"tx"`
}

var allowedHosts = []string{
	"localhost:19090",
	"localhost:29090",
	"localhost:39090",
}

// Function to call the RPC endpoint with any method and parameters, returning the response and error
func CallRPC(url string, method string, params []interface{}) (string, error) {

	// Create the request payload
	requestBody, err := json.Marshal(map[string]interface{}{
		"method": method,
		"params": params,
	})
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
		return 0, errors.NewProcessingError("unexpected status code: %d", resp.StatusCode)
	}

	var blocks []struct {
		Height int `json:"height"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&blocks); err != nil {
		return 0, err
	}

	if len(blocks) == 0 {
		return 0, errors.NewProcessingError("no blocks found in response")
	}

	return blocks[0].Height, nil
}

func GetBlockStore(logger ulogger.Logger) (blob.Store, error) {
	blockStoreUrl, err, found := gocore.Config().GetURL("blockstore")
	if err != nil {
		return nil, errors.NewConfigurationError("error getting blockstore config", err)
	}
	if !found {
		return nil, errors.NewConfigurationError("blockstore config not found")
	}

	blockStore, err := blob.NewStore(logger, blockStoreUrl)
	if err != nil {
		return nil, errors.NewServiceError("error creating block store", err)
	}

	return blockStore, nil
}

func ReadFile(ctx context.Context, ext string, logger ulogger.Logger, r io.Reader, queryTxId chainhash.Hash, dir *url.URL) (bool, error) {
	switch ext {
	case "utxodiff":
		utxodiff, err := utxom.NewUTXODiffFromReader(logger, r)
		if err != nil {
			return false, errors.NewProcessingError("error reading utxodiff: %w\n", err)
		}

		fmt.Printf("UTXODiff block hash: %v\n", utxodiff.BlockHash)

		fmt.Printf("UTXODiff removed %d UTXOs", utxodiff.Removed.Length())

		fmt.Printf("UTXODiff added %d UTXOs", utxodiff.Added.Length())

	case "utxoset":
		utxoSet, err := utxom.NewUTXOSetFromReader(logger, r)
		if err != nil {
			return false, errors.NewProcessingError("error reading utxoSet: %v\n", err)
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
			return false, errors.NewBlockInvalidError("error reading block header", err)
		}

		txCount, err := wire.ReadVarInt(r, 0)
		if err != nil {
			return false, errors.NewBlockInvalidError("error reading transaction count", err)
		}

		fmt.Printf("\t%d transactions\n", txCount)

	case "block":
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

func GetMiningCandidate_rpc(url string) (string, error) {
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

func MineBlockWithCandidate_rpc(ctx context.Context, rpcUrl string, miningCandidate *block_model.MiningCandidate, logger ulogger.Logger) ([]byte, error) {
	solution, err := cpuminer.Mine(ctx, miningCandidate)
	if err != nil {
		return nil, errors.NewProcessingError("error mining block: %w", err)
	}

	blockHeader, err := cpuminer.BuildBlockHeader(miningCandidate, solution)
	if err != nil {
		return nil, errors.NewProcessingError("error building block header: %w", err)
	}

	blockHash := util.Sha256d(blockHeader)

	solutionJSON, err := json.Marshal(solution)
	if err != nil {
		return nil, errors.NewProcessingError("error marshalling solution: %w", err)
	}
	method := "submitminingsolution"
	params := []interface{}{string(solutionJSON)}
	logger.Infof("Submitting mining solution: %s", string(solutionJSON))

	resp, err := CallRPC(rpcUrl, method, params)
	if err != nil {
		return nil, errors.NewProcessingError("error submitting mining solution: %w", err)
	} else {
		fmt.Printf("Response: %s\n", resp)
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

	return *newTx.TxIDChainHash(), nil
}

func CreateAndSendDoubleSpendTx(ctx context.Context, node []tf.BitcoinNode) (chainhash.Hash, error) {

	nilHash := chainhash.Hash{}

	privateKey, _ := bec.NewPrivateKey(bec.S256())

	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	coinbaseClient := node[0].CoinbaseClient

	faucetTx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to request funds: %w", err)
	}
	_, err = node[0].DistributorClient.SendTransaction(ctx, faucetTx)
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

	_, err = node[0].DistributorClient.SendTransaction(ctx, newTx)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to send new transaction: %w", err)
	}
	_, err = node[1].DistributorClient.SendTransaction(ctx, newTxDouble)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to send new transaction: %w", err)
	}

	return *newTx.TxIDChainHash(), nil
}

func CreateAndSendRawTxs(ctx context.Context, node tf.BitcoinNode, count int) ([]chainhash.Hash, error) {
	var txHashes []chainhash.Hash

	for i := 0; i < count; i++ {
		tx, err := CreateAndSendRawTx(ctx, node)
		if err != nil {
			return nil, errors.NewProcessingError("error creating raw transaction : %w", err)
		}
		txHashes = append(txHashes, tx)
		time.Sleep(1 * time.Second) // Wait 10 seconds between transactions
	}

	return txHashes, nil
}

func UseCoinbaseUtxo(ctx context.Context, node tf.BitcoinNode, coinbaseTx *bt.Tx) (chainhash.Hash, error) {
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

func SendTXsWithDistributor(ctx context.Context, node tf.BitcoinNode, logger ulogger.Logger, fees uint64) (bool, error) {
	var defaultSathosis uint64 = 10000

	// Send transactions
	txDistributor, _ := distributor.NewDistributor(ctx, logger,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)

	coinbaseClient := node.CoinbaseClient
	coinbasePrivKey, _ := gocore.Config().Get("coinbase_wallet_private_key")
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
	fmt.Printf("Transaction: %s %s\n", tx.TxIDChainHash(), tx.TxID())

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

	fmt.Println("Default Sathosis: ", defaultSathosis)

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

func GetBestBlock(ctx context.Context, node tf.BitcoinNode) (*block_model.Block, error) {
	_, bbhmeta, errbb := node.BlockchainClient.GetBestBlockHeader(ctx)
	if errbb != nil {
		return nil, errors.NewProcessingError("Error getting best block header: %s\n", errbb)
	}
	block, errblock := node.BlockchainClient.GetBlockByHeight(ctx, bbhmeta.Height)
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
	if err != nil {
		return 0, errors.New(errors.ERR_ERROR, "error querying Prometheus API: %v", err)
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

func WaitForBlockHeight(url string, targetHeight int, timeout time.Duration) error {
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
	var o []options.Options
	o = append(o, options.WithFileExtension("block"))
	r, err := store.GetIoReader(ctx, block, o...)

	if err != nil {
		return false, errors.NewProcessingError("error getting block reader: %v", err)
	}

	if err == nil {
		if bl, err := ReadFile(ctx, "block", logger, r, tx, storeURL); err != nil {
			return false, errors.NewProcessingError("error reading block: %v", err)
		} else {
			logger.Infof("Block at height (%d): was tested for the test Tx\n", blockHeight)
			return bl, nil
		}
	}

	return false, nil
}

func Unzip(src, dest string) error {
	cmd := exec.Command("unzip", src, "-d", dest)
	err := cmd.Run()

	time.Sleep(5 * time.Second)

	return err
}
