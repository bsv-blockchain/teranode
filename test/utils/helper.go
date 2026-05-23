package utils

import (
	"bufio"
	"bytes"
	"context"
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

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	blockmodel "github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	ba "github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockassembly/mining"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
)

// Transaction represents a simple transaction structure
type Transaction struct {
	Tx string `json:"tx"`
}

var allowedHosts = []string{
	"localhost:16090",
	"localhost:16091",
	"localhost:16092",
}

// CallRPC calls the RPC endpoint with the specified method and parameters, retrying up to 10 times if it encounters a specific error condition.
func CallRPC(url string, method string, params []interface{}) (body string, err error) {
	for i := 0; i < 10; i++ {
		body, err = callRPC(url, method, params)
		if err != nil && strings.Contains(err.Error(), "Loading ") {
			// special case for svnode - it takes a second to load wallets and addresses
			time.Sleep(1 * time.Second)
			continue
		}

		break
	}

	return body, err
}

// Function to call the RPC endpoint with any method and parameters, returning the response and error
func callRPC(url string, method string, params []interface{}) (string, error) {
	logger := ulogger.New("e2eTestRun", ulogger.WithLevel("INFO"))
	// Create the request payload
	requestBody, err := json.Marshal(map[string]interface{}{
		"method": method,
		"params": params,
	})
	logger.Infof("Request: url: %s method: %s params: %s", url, method, string(requestBody))

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

	defer func() {
		_ = resp.Body.Close()
	}()

	// // Check the status code
	// if resp.StatusCode != http.StatusOK {
	// 	return "", errors.NewProcessingError("expected status code 200, got %v", resp.StatusCode)
	// }

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
		Error *JSONError `json:"error"`
	}

	if err := json.Unmarshal(body, &jsonResponse); err != nil {
		return string(body), errors.NewProcessingError("failed to parse response JSON", err)
	}

	if jsonResponse.Error != nil {
		return string(body), errors.NewProcessingError("RPC returned error", jsonResponse.Error)
	}

	// Return the response as a string
	return string(body), nil
}

// GetBlockHeight retrieves the block height from the Teranode API.
func GetBlockHeight(url string) (uint32, error) {
	resp, err := http.Get(url + "/api/v1/lastblocks?n=1")
	if err != nil {
		fmt.Printf("Error getting block height: %s\n", err)
		return 0, err
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return 0, errors.NewProcessingError("unexpected status code: %d", resp.StatusCode)
	}

	var blocks []struct {
		Height uint32 `json:"height"`
	}

	if err = json.NewDecoder(resp.Body).Decode(&blocks); err != nil {
		return 0, err
	}

	if len(blocks) == 0 {
		return 0, errors.NewProcessingError("no blocks found in response")
	}

	return blocks[0].Height, nil
}

// isTxInBlock checks if a transaction with the specified txid exists in the block represented by the provided reader.
func isTxInBlock(ctx context.Context, l ulogger.Logger, storeSubtree blob.Store, blockReader io.Reader, queryTxId chainhash.Hash) (bool, error) {
	block, err := blockmodel.NewBlockFromReader(blockReader)
	if err != nil {
		return false, errors.NewProcessingError("error reading block", err)
	}

	l.Infof("Block hash: %s\n", block.Hash())
	l.Infof("%s", block.Header.StringDump())
	l.Infof("Number of transactions: %d\n", block.TransactionCount)

	for _, subtreeVal := range block.Subtrees {
		filename := fmt.Sprintf("%s.subtree", subtreeVal.String())
		l.Infof("Reading subtree from %s\n", filename)

		ext := filepath.Ext(filename)
		fileWithoutExtension := strings.TrimSuffix(filename, ext)

		if ext[0] == '.' {
			ext = ext[1:]
		}

		stHash, _ := chainhash.NewHashFromStr(fileWithoutExtension)
		var stReader io.Reader
		stReader, err = storeSubtree.GetIoReader(ctx, stHash[:], fileformat.FileType(ext))
		if err != nil {
			return false, errors.NewProcessingError("error getting subtree reader from store", err)
		}

		var ok bool
		ok, err = isTxInSubtree(l, stReader, queryTxId)
		if err != nil {
			return false, errors.NewProcessingError("error getting subtree reader from store", err)
		}

		// First found is the desired result
		if ok {
			return ok, nil
		}
	}

	return false, nil
}

// isTxInSubtree read the subtree binary and check if a tx with specified txid exist
func isTxInSubtree(_ ulogger.Logger, stReader io.Reader, queryTxId chainhash.Hash) (bool, error) {
	st := subtree.Subtree{}
	err := st.DeserializeFromReader(stReader)
	if err != nil {
		return false, errors.NewProcessingError("[writeTransactionsViaSubtreeStore] error deserializing subtree", err)
	}

	txHashes := make([]chainhash.Hash, len(st.Nodes))
	for i := 0; i < len(st.Nodes); i++ {
		txHashes[i] = st.Nodes[i].Hash
	}

	for _, txHash := range txHashes {
		if txHash == queryTxId {
			return true, nil
		}
	}

	return false, nil
}

// GetBlockSubtreeHashes retrieves the subtree hashes from a block given its hash.
func GetBlockSubtreeHashes(ctx context.Context, _ ulogger.Logger, blockHash []byte, storeBlock blob.Store) ([]*chainhash.Hash, error) {
	blockReader, err := storeBlock.GetIoReader(ctx, blockHash, fileformat.FileTypeBlock)
	if err != nil {
		return nil, errors.NewProcessingError("error getting block reader", err)
	}

	block, err := blockmodel.NewBlockFromReader(blockReader)
	if err != nil {
		return nil, errors.NewProcessingError("error reading block", err)
	}

	return block.Subtrees, nil
}

// GetSubtreeTxHashes retrieves the transaction hashes from a subtree given its hash and base URL.
func GetSubtreeTxHashes(ctx context.Context, logger ulogger.Logger, subtreeHash *chainhash.Hash, baseURL string, tSettings *settings.Settings) ([]chainhash.Hash, error) {
	if baseURL == "" {
		return nil, errors.NewInvalidArgumentError("[getSubtreeTxHashes][%s] baseUrl for subtree is empty", subtreeHash.String())
	}

	// do http request to baseUrl + subtreeHash.String()
	logger.Infof("[getSubtreeTxHashes][%s] getting subtree from %s", subtreeHash.String(), baseURL)
	urlVal := fmt.Sprintf("%s/api/v1/subtree/%s", baseURL, subtreeHash.String())

	body, err := util.DoHTTPRequestBodyReader(ctx, urlVal)
	if err != nil {
		return nil, errors.NewExternalError("[getSubtreeTxHashes][%s] failed to do http request", subtreeHash.String(), err)
	}
	defer func() {
		_ = body.Close()
	}()

	logger.Infof("[getSubtreeTxHashes][%s] processing subtree response into tx hashes", subtreeHash.String())

	txHashes := make([]chainhash.Hash, 0, tSettings.BlockAssembly.InitialMerkleItemsPerSubtree)
	buffer := make([]byte, chainhash.HashSize)
	bufferedReader := bufio.NewReaderSize(body, 1024*64)

	logger.Debugf("[getSubtreeTxHashes][%s] processing subtree response into tx hashes", subtreeHash.String())

	for {
		n, err := io.ReadFull(bufferedReader, buffer)
		if n > 0 {
			txHashes = append(txHashes, chainhash.Hash(buffer))
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			// Not recoverable, returning processing error
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, errors.NewProcessingError("[getSubtreeTxHashes][%s] unexpected EOF: partial hash read", subtreeHash.String())
			}

			return nil, errors.NewProcessingError("[getSubtreeTxHashes][%s] error reading stream", subtreeHash.String(), err)
		}
	}

	logger.Debugf("[getSubtreeTxHashes][%s] done with subtree response", subtreeHash.String())

	return txHashes, nil
}

// GetMiningCandidate retrieves the mining candidate from the block assembly client.
func GetMiningCandidate(ctx context.Context, baClient ba.Client, _ ulogger.Logger) (*blockmodel.MiningCandidate, error) {
	miningCandidate, err := baClient.GetMiningCandidate(ctx)
	if err != nil {
		return nil, errors.NewProcessingError("error getting mining candidate", err)
	}

	return miningCandidate, nil
}

// GetMiningCandidateRPC retrieves the mining candidate using an RPC call.
func GetMiningCandidateRPC(url string) (string, error) {
	method := "getminingcandidate"
	var params []interface{}

	return CallRPC(url, method, params)
}

// MineBlock mines a block using the provided mining candidate and returns the block hash.
func MineBlock(ctx context.Context, tSettings *settings.Settings, baClient ba.Client, _ ulogger.Logger) ([]byte, error) {
	miningCandidate, err := baClient.GetMiningCandidate(ctx)
	if err != nil {
		return nil, errors.NewProcessingError("error getting mining candidate", err)
	}

	solution, err := mining.Mine(ctx, tSettings, miningCandidate, nil)
	if err != nil {
		return nil, errors.NewProcessingError("error mining block", err)
	}

	blockHeader, err := mining.BuildBlockHeader(miningCandidate, solution)
	if err != nil {
		return nil, errors.NewProcessingError("error building block header", err)
	}

	blockHash := util.Sha256d(blockHeader)

	if err := baClient.SubmitMiningSolution(ctx, solution); err != nil {
		return nil, errors.NewProcessingError("error submitting mining solution", err)
	}

	return blockHash, nil
}

// MineBlockWithRPC mines a block using the Teranode RPC endpoint and returns the block hash.
func MineBlockWithRPC(_ context.Context, node TeranodeTestClient, _ ulogger.Logger) (string, error) {
	teranode1RPCEndpoint := node.RPCURL
	teranode1RPCEndpoint = "http://" + teranode1RPCEndpoint

	resp, err := CallRPC(teranode1RPCEndpoint, "generate", []interface{}{1})
	if err != nil {
		return "", errors.NewProcessingError("error generating block", err)
	}

	return resp, nil
}

// MineBlockWithCandidate mines a block using the provided mining candidate and returns the block hash.
func MineBlockWithCandidate(ctx context.Context, tSettings *settings.Settings, baClient ba.Client,
	miningCandidate *blockmodel.MiningCandidate, _ ulogger.Logger,
) ([]byte, error) {
	solution, err := mining.Mine(ctx, tSettings, miningCandidate, nil)
	if err != nil {
		return nil, errors.NewProcessingError("error mining block", err)
	}

	blockHeader, err := mining.BuildBlockHeader(miningCandidate, solution)
	if err != nil {
		return nil, errors.NewProcessingError("error building block header", err)
	}

	blockHash := util.Sha256d(blockHeader)

	if err := baClient.SubmitMiningSolution(ctx, solution); err != nil {
		return nil, errors.NewProcessingError("error submitting mining solution", err)
	}

	return blockHash, nil
}

// MineBlockWithCandidateRPC mines a block using the provided mining candidate and submits the solution via RPC, returning the block hash.
func MineBlockWithCandidateRPC(ctx context.Context, rpcUrl string, tSettings *settings.Settings, miningCandidate *blockmodel.MiningCandidate, logger ulogger.Logger) ([]byte, error) {
	solution, err := mining.Mine(ctx, tSettings, miningCandidate, nil)
	if err != nil {
		return nil, errors.NewProcessingError("error mining block", err)
	}

	blockHeader, err := mining.BuildBlockHeader(miningCandidate, solution)
	if err != nil {
		return nil, errors.NewProcessingError("error building block header", err)
	}

	blockHash := util.Sha256d(blockHeader)

	submitMiningSolutionCmd := bsvjson.MiningSolution{
		ID:       util.ReverseAndHexEncodeSlice(solution.Id),
		Coinbase: hex.EncodeToString(solution.Coinbase),
		Time:     solution.Time,
		Nonce:    solution.Nonce,
		Version:  solution.Version,
	}

	solutionJSON, err := json.Marshal(submitMiningSolutionCmd)
	if err != nil {
		return nil, errors.NewProcessingError("error marshalling solution", err)
	}

	method := "submitminingsolution"

	params := []interface{}{submitMiningSolutionCmd}

	logger.Infof("Submitting mining solution: %s", string(solutionJSON))

	resp, err := CallRPC(rpcUrl, method, params)
	if err != nil {
		return nil, errors.NewProcessingError("error submitting mining solution", err)
	} else {
		fmt.Printf("Response: %s\n", resp)
	}

	return blockHash, nil
}

// CreateAndSendTx creates a new transaction using the TeranodeTestClient, requests funds from the coinbase client, and sends the transaction to the distributor client.
func CreateAndSendTx(ctx context.Context, node TeranodeTestClient) (chainhash.Hash, error) {
	logger := ulogger.New("e2eTestRun", ulogger.WithLevel("INFO"))

	nilHash := chainhash.Hash{}
	privateKey, _ := bec.NewPrivateKey()

	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	coinbaseClient := node.CoinbaseClient

	timeout := time.After(5 * time.Second)

	var (
		faucetTx *bt.Tx
		err      error
	)

loop:
	for faucetTx == nil || err != nil {
		select {
		case <-timeout:
			break loop
		default:
			faucetTx, err = coinbaseClient.RequestFunds(ctx, address.AddressString, true)
			if err != nil {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to request funds", err)
	}

	err = node.PropagationClient.ProcessTransaction(ctx, faucetTx)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to send transaction", err)
	}

	output := faucetTx.Outputs[0]
	utxoVal := &bt.UTXO{
		TxIDHash:      faucetTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()

	err = newTx.FromUTXOs(utxoVal)
	if err != nil {
		return nilHash, errors.NewProcessingError("error creating new transaction", err)
	}

	err = newTx.AddP2PKHOutputFromAddress("1ApLMk225o7S9FvKwpNChB7CX8cknQT9Hy", 10000)
	if err != nil {
		return nilHash, errors.NewProcessingError("Error adding output to transaction", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nilHash, errors.NewProcessingError("Error filling transaction inputs", err)
	}

	err = node.PropagationClient.ProcessTransaction(ctx, newTx)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to send new transaction", err)
	}

	logger.Infof("Transaction sent: %s", newTx.TxID())

	return *newTx.TxIDChainHash(), nil
}

// CreateAndSendTxToSliceOfNodes creates a new transaction using the first node in the slice, requests funds from the coinbase client, and sends the transaction to all nodes in the slice.
func CreateAndSendTxToSliceOfNodes(ctx context.Context, nodes []TeranodeTestClient) (chainhash.Hash, error) {
	nilHash := chainhash.Hash{}
	privateKey, _ := bec.NewPrivateKey()

	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	coinbaseClient := nodes[0].CoinbaseClient

	faucetTx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to request funds", err)
	}

	for _, node := range nodes {
		err = node.PropagationClient.ProcessTransaction(ctx, faucetTx)
		if err != nil {
			return nilHash, errors.NewProcessingError("Failed to send transaction", err)
		}
	}

	output := faucetTx.Outputs[0]
	utxoVal := &bt.UTXO{
		TxIDHash:      faucetTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()
	err = newTx.FromUTXOs(utxoVal)
	if err != nil {
		return nilHash, errors.NewProcessingError("error creating new transaction", err)
	}

	err = newTx.AddP2PKHOutputFromAddress("1ApLMk225o7S9FvKwpNChB7CX8cknQT9Hy", 10000)
	if err != nil {
		return nilHash, errors.NewProcessingError("Error adding output to transaction", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nilHash, errors.NewProcessingError("Error filling transaction inputs", err)
	}

	for _, node := range nodes {
		err = node.PropagationClient.ProcessTransaction(ctx, newTx)
		if err != nil {
			return nilHash, errors.NewProcessingError("Failed to send new transaction", err)
		}
	}

	return *newTx.TxIDChainHash(), nil
}

// CreateAndSendDoubleSpendTx creates two transactions that double spend the same UTXO, with different lock times, and sends them to two different nodes.
func CreateAndSendDoubleSpendTx(ctx context.Context, nodes []TeranodeTestClient) (chainhash.Hash, error) {
	nilHash := chainhash.Hash{}

	privateKey, _ := bec.NewPrivateKey()

	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	coinbaseClient := nodes[0].CoinbaseClient

	faucetTx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to request funds", err)
	}

	err = nodes[0].PropagationClient.ProcessTransaction(ctx, faucetTx)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to send transaction", err)
	}

	output := faucetTx.Outputs[0]
	utxoVal := &bt.UTXO{
		TxIDHash:      faucetTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()

	err = newTx.FromUTXOs(utxoVal)
	if err != nil {
		return nilHash, errors.NewProcessingError("error creating new transaction", err)
	}

	newTx.LockTime = 0

	newTxDouble := bt.NewTx()

	err = newTxDouble.FromUTXOs(utxoVal)
	if err != nil {
		return nilHash, errors.NewProcessingError("error creating new transaction", err)
	}

	newTxDouble.LockTime = 1

	err = newTx.AddP2PKHOutputFromAddress("1ApLMk225o7S9FvKwpNChB7CX8cknQT9Hy", 10000)
	if err != nil {
		return nilHash, errors.NewProcessingError("Error adding output to transaction", err)
	}

	err = newTxDouble.AddP2PKHOutputFromAddress("14qViLJfdGaP4EeHnDyJbEGQysnCpwk3gd", 10000)
	if err != nil {
		return nilHash, errors.NewProcessingError("Error adding output to transaction", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nilHash, errors.NewProcessingError("Error filling transaction inputs", err)
	}

	err = newTxDouble.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nilHash, errors.NewProcessingError("Error filling transaction inputs", err)
	}

	err = nodes[0].PropagationClient.ProcessTransaction(ctx, newTx)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to send new transaction", err)
	}

	err = nodes[1].PropagationClient.ProcessTransaction(ctx, newTxDouble)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to send new transaction", err)
	}

	return *newTx.TxIDChainHash(), nil
}

// CreateAndSendTxs creates and sends a specified number of transactions using the provided TeranodeTestClient.
func CreateAndSendTxs(ctx context.Context, node TeranodeTestClient, count int) ([]chainhash.Hash, error) {
	var txHashes []chainhash.Hash

	for i := 0; i < count; i++ {
		tx, err := CreateAndSendTx(ctx, node)
		if err != nil {
			return nil, errors.NewProcessingError("error creating raw transaction ", err)
		}

		txHashes = append(txHashes, tx)
	}

	delay := node.Settings.BlockAssembly.DoubleSpendWindow
	if delay != 0 {
		time.Sleep(delay)
	}

	return txHashes, nil
}

// CreateAndSendTxsToASliceOfNodes creates and sends a specified number of transactions to a slice of TeranodeTestClient nodes.
func CreateAndSendTxsToASliceOfNodes(ctx context.Context, nodes []TeranodeTestClient, count int) ([]chainhash.Hash, error) {
	var txHashes []chainhash.Hash

	for i := 0; i < count; i++ {
		tx, err := CreateAndSendTxToSliceOfNodes(ctx, nodes)
		if err != nil {
			return nil, errors.NewProcessingError("error creating raw transaction ", err)
		}

		txHashes = append(txHashes, tx)
	}

	delay := nodes[0].Settings.BlockAssembly.DoubleSpendWindow
	if delay != 0 {
		time.Sleep(delay)
	}

	return txHashes, nil
}

// CreateAndSendTxsConcurrently creates and sends a specified number of transactions concurrently using the provided TeranodeTestClient.
func CreateAndSendTxsConcurrently(ctx context.Context, node TeranodeTestClient, count int) ([]chainhash.Hash, error) {
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
				txErrors <- errors.NewProcessingError("error creating raw transaction", err)
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

// UseCoinbaseUtxo uses a coinbase transaction's UTXO to create a new transaction, adds an output, fills inputs, and sends the transaction.
func UseCoinbaseUtxo(ctx context.Context, node TeranodeTestClient, coinbaseTx *bt.Tx) (chainhash.Hash, error) {
	nilHash := chainhash.Hash{}
	privateKey, _ := bec.NewPrivateKey()

	output := coinbaseTx.Outputs[0]
	utxoVal := &bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()

	err := newTx.FromUTXOs(utxoVal)
	if err != nil {
		return nilHash, errors.NewProcessingError("error creating new transaction", err)
	}

	err = newTx.AddP2PKHOutputFromAddress("1ApLMk225o7S9FvKwpNChB7CX8cknQT9Hy", 10000)
	if err != nil {
		return nilHash, errors.NewProcessingError("Error adding output to transaction", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nilHash, errors.NewProcessingError("Error filling transaction inputs", err)
	}

	err = node.PropagationClient.ProcessTransaction(ctx, newTx)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to send new transaction", err)
	}

	return *newTx.TxIDChainHash(), nil
}

// UseCoinbaseUtxoV2 uses a coinbase transaction's UTXO to create a new transaction, adds an output, fills inputs, and sends the transaction.
func UseCoinbaseUtxoV2(ctx context.Context, node TeranodeTestClient, coinbaseTx *bt.Tx) (chainhash.Hash, error) {
	nilHash := chainhash.Hash{}
	privateKey, _ := bec.NewPrivateKey()

	output := coinbaseTx.Outputs[0]
	utxoVal := &bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()
	err := newTx.FromUTXOs(utxoVal)
	if err != nil {
		return nilHash, errors.NewProcessingError("error creating new transaction", err)
	}

	err = newTx.AddP2PKHOutputFromAddress("1ApLMk225o7S9FvKwpNChB7CX8cknQT9Hy", 10000)
	if err != nil {
		return nilHash, errors.NewProcessingError("Error adding output to transaction", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		return nilHash, errors.NewProcessingError("Error filling transaction inputs", err)
	}

	err = node.PropagationClient.ProcessTransaction(ctx, newTx)
	if err != nil {
		return nilHash, errors.NewProcessingError("Failed to send new transaction", err)
	}

	return *newTx.TxIDChainHash(), nil
}

// GetBestBlockV2 retrieves the best block from the TeranodeTestClient's blockchain client.
func GetBestBlockV2(ctx context.Context, node TeranodeTestClient) (*blockmodel.Block, error) {
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

// QueryPrometheusMetric queries a Prometheus metric from the specified server URL and returns its value.
func QueryPrometheusMetric(serverURL, metricName string) (float64, error) {
	parsedURL, err := url.Parse(serverURL)
	if err != nil {
		return 0, errors.New(errors.ERR_ERROR, "invalid server URL", err)
	}

	if !isAllowedHost(parsedURL.Host) {
		return 0, errors.New(errors.ERR_ERROR, "host not allowed: %v", parsedURL.Host)
	}

	queryURL := fmt.Sprintf("%s/api/v1/query?query=%s", serverURL, metricName)

	req, err := http.NewRequest("GET", queryURL, nil)
	if err != nil {
		return 0, errors.New(errors.ERR_ERROR, "error creating HTTP request", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, errors.New(errors.ERR_ERROR, "error sending HTTP request", err)
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, errors.New(errors.ERR_ERROR, "error reading Prometheus response body", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, errors.New(errors.ERR_ERROR, "error unmarshaling Prometheus response", err)
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
		return 0, errors.New(errors.ERR_ERROR, "error parsing Prometheus metric value", err)
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

// WaitForBlockHeight waits for the blockchain to reach a specific block height.
func WaitForBlockHeight(url string, targetHeight uint32, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout*time.Second)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.NewError("timeout waiting for block height")
		case <-ticker.C:
			currentHeight, err := GetBlockHeight(url)
			if err != nil {
				return errors.NewError("error getting block height", err)
			}

			if currentHeight >= targetHeight {
				return nil
			}
		}
	}
}

// GenerateBlocks generates a specified number of blocks using the TeranodeTestClient's RPC endpoint.
func GenerateBlocks(_ context.Context, node TeranodeTestClient, numBlocks int, _ ulogger.Logger) (string, error) {
	teranode1RPCEndpoint := node.RPCURL
	teranode1RPCEndpoint = "http://" + teranode1RPCEndpoint
	// Generate blocks
	resp, err := CallRPC(teranode1RPCEndpoint, "generate", []interface{}{numBlocks})
	if err != nil {
		return "", errors.NewProcessingError("error generating blocks", err)
	}

	return resp, nil
}

// TestTxInBlock checks if a transaction is present in a block by reading the block from the store and checking for the transaction.
func TestTxInBlock(ctx context.Context, logger ulogger.Logger, storeBlock blob.Store, storeSubtree blob.Store, blockHash []byte, tx chainhash.Hash) (bool, error) {
	blockReader, err := storeBlock.GetIoReader(ctx, blockHash, fileformat.FileTypeBlock)
	if err != nil {
		return false, errors.NewProcessingError("error getting block reader", err)
	}

	if ok, err := isTxInBlock(ctx, logger, storeSubtree, blockReader, tx); err != nil {
		return false, errors.NewProcessingError("error reading block", err)
	} else {
		return ok, nil
	}
}

// TestTxInSubtree checks if a transaction is present in a subtree by reading the subtree from the store and checking for the transaction.
func TestTxInSubtree(ctx context.Context, logger ulogger.Logger, subtreeStore blob.Store, subtree []byte, tx chainhash.Hash) (bool, error) {
	subtreeReader, err := subtreeStore.GetIoReader(ctx, subtree, fileformat.FileTypeSubtree)
	if err != nil {
		subtreeReader, err = subtreeStore.GetIoReader(ctx, subtree, fileformat.FileTypeSubtreeToCheck)
		if err != nil {
			return false, errors.NewProcessingError("error getting subtree reader", err)
		}
	}

	return isTxInSubtree(logger, subtreeReader, tx)
}

// Unzip extracts the contents of a zip file to a specified destination directory.
func Unzip(src, dest string) error {
	cmd := exec.Command("unzip", "-f", src, "-d", dest)
	err := cmd.Run()

	return err
}

// TODO TO delete

// CopyFile copies a file from source to destination.
func CopyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		_ = srcFile.Close()
	}()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		_ = dstFile.Close()
	}()

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

// GetUtxoBalance retrieves the UTXO balance from the TeranodeTestClient's coinbase client.
func GetUtxoBalance(ctx context.Context, node TeranodeTestClient) uint64 {
	utxoBalance, _, _ := node.CoinbaseClient.GetBalance(ctx)
	return utxoBalance
}

// GeneratePrivateKeyAndAddress generates a new private key and address pair.
func GeneratePrivateKeyAndAddress() (*bec.PrivateKey, *bscript.Address, error) {
	privateKey, err := bec.NewPrivateKey()
	if err != nil {
		return nil, nil, err
	}

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	if err != nil {
		return nil, nil, err
	}

	return privateKey, address, nil
}

// RequestFunds requests funds from the TeranodeTestClient's coinbase client for a specified address and returns the transaction.
func RequestFunds(ctx context.Context, node TeranodeTestClient, address string) (*bt.Tx, error) {
	faucetTx, err := node.CoinbaseClient.RequestFunds(ctx, address, true)
	if err != nil {
		return nil, err
	}

	return faucetTx, nil
}

// SendTransaction sends a transaction using the TeranodeTestClient's distributor client and returns a boolean indicating success or failure.
func SendTransaction(ctx context.Context, node TeranodeTestClient, tx *bt.Tx) (bool, error) {
	if err := node.PropagationClient.ProcessTransaction(ctx, tx); err != nil {
		return false, err
	}

	return true, nil
}

// CreateUtxoFromTransaction creates a UTXO from a transaction and a specific output index (vout).
func CreateUtxoFromTransaction(tx *bt.Tx, vout uint32) *bt.UTXO {
	output := tx.Outputs[vout]

	return &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          vout,
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}
}

// CreateTransaction creates a new transaction from a UTXO, adds an output to a specified address with a given amount, fills the inputs using the provided private key, and returns the transaction.
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

// CreateTransactionObject creates a transaction object from a node's coinbase funds without sending it
func CreateTransactionObject(ctx context.Context, node TeranodeTestClient, address string, amount uint64, privateKey *bec.PrivateKey) (*bt.Tx, error) {
	coinbaseClient := node.CoinbaseClient

	faucetTx, err := coinbaseClient.RequestFunds(ctx, address, true)
	if err != nil {
		return nil, errors.NewProcessingError("Failed to request funds", err)
	}

	err = node.PropagationClient.ProcessTransaction(ctx, faucetTx)
	if err != nil {
		return nil, errors.NewProcessingError("Failed to send faucet transaction", err)
	}

	output := faucetTx.Outputs[0]
	u := &bt.UTXO{
		TxIDHash:      faucetTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	return CreateTransaction(u, address, amount, privateKey)
}

//nolint:govet // this needs to be refactored to pass
func FreezeUtxos(ctx context.Context, testenv TeranodeTestEnv, tx *bt.Tx, logger ulogger.Logger, tSettings *settings.Settings) error {
	utxoHash, _ := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	spend := &utxo.Spend{
		TxID:     tx.TxIDChainHash(),
		Vout:     0,
		UTXOHash: utxoHash,
	}

	for _, node := range testenv.Nodes {
		err := node.UtxoStore.FreezeUTXOs(ctx, []*utxo.Spend{spend}, tSettings)
		if err != nil {
			logger.Errorf("Error freezing UTXOs on node %v: %v", err, node.Name)
		}
	}

	return nil
}

//nolint:govet // this needs to be refactored to pass
func ReassignUtxo(ctx context.Context, testenv TeranodeTestEnv, firstTx, reassignTx *bt.Tx, logger ulogger.Logger, tSettings *settings.Settings) error {
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
		}, newSpend, tSettings)
		if err != nil {
			return err
		}

		logger.Infof("UTXO reassigned on node %v", node.Name)
	}

	return nil
}

// extractPublicKey extracts the public key from a P2PKH scriptSig.
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

// RemoveDataDirectory removes a specified data directory using either sudo or non-sudo command.
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

// WaitForHealthLiveness checks the health readiness endpoint of a service running on the specified port until it becomes healthy or the timeout is reached.
func WaitForHealthLiveness(port int, timeout time.Duration) error {
	healthReadinessEndpoint := fmt.Sprintf("http://localhost:%d/health/readiness", port)
	timeoutElapsed := time.After(timeout)

	var err error

	for {
		select {
		case <-timeoutElapsed:
			return errors.NewError("health check failed for port %d after timeout: %v", port, timeout, err)
		default:
			_, err = util.DoHTTPRequest(context.Background(), healthReadinessEndpoint, nil)
			if err != nil {
				time.Sleep(100 * time.Millisecond)

				continue
			}

			return nil
		}
	}
}

// SendEventRun sends a RUN event to the blockchain client and waits for it to be ready.
func SendEventRun(ctx context.Context, blockchainClient blockchain.ClientI, _ ulogger.Logger) error {
	var err error

	timeout := time.After(30 * time.Second)
	// wait for Blockchain GRPC to be ready and send FSM RUN event
	for {
		select {
		case <-timeout:
			return errors.NewError("Timeout waiting for Blockchain service", err)
		default:
			err = blockchainClient.Run(ctx, "test")
			if err != nil {
				time.Sleep(100 * time.Millisecond)

				continue
			}

			// status, _, err = blockchainClient.Health(ctx, readiness)
			// logger.Infof("Blockchain GRPC health status: %d", status)
			// if err != nil || status != http.StatusOK {
			// 	time.Sleep(100 * time.Millisecond)

			// 	continue
			// }

			// err = blockchainClient.Run(ctx, "test")
			// if err != nil {
			// 	time.Sleep(100 * time.Millisecond)

			// 	continue
			// }

			return err
		}
	}
}

// VerifyUTXOFileExists checks if a UTXO-related file exists in the block store
func VerifyUTXOFileExists(ctx context.Context, store blob.Store, blockHash chainhash.Hash, fileType fileformat.FileType) error {
	exists, err := store.Exists(ctx, blockHash[:], fileType)
	if err != nil {
		return errors.NewProcessingError("failed to check if %s exists: %v", fileType, err)
	}

	if !exists {
		return errors.NewProcessingError("%s file does not exist", fileType)
	}

	return nil
}

// WaitForBlockAccepted waits for a block with the specified hash to be accepted by the blockchain client.
func WaitForBlockAccepted(ctx context.Context, node TeranodeTestClient, expectedHash []byte, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for {
		bestBlockHeader, _, err := node.BlockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			return errors.NewProcessingError("failed to get best block header: %w", err)
		}

		if bytes.Equal(expectedHash, bestBlockHeader.Hash().CloneBytes()) {
			return nil
		}

		if time.Now().After(deadline) {
			return errors.NewProcessingError("timeout waiting for block acceptance")
		}
	}
}

// WaitForNodesToSync waits for all nodes in the provided slice to have the same best block header.
func WaitForNodesToSync(ctx context.Context, nodes []blockchain.ClientI, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for {
		if time.Now().After(deadline) {
			return errors.NewProcessingError("timeout waiting for nodes to sync")
		}

		allEqual := true

		var referenceHash *chainhash.Hash

		for i, node := range nodes {
			header, _, err := node.GetBestBlockHeader(ctx)
			if err != nil {
				return err
			}

			if i == 0 {
				referenceHash = header.Hash()
			} else if *referenceHash != *header.Hash() {
				allEqual = false
				break
			}
		}

		if allEqual {
			return nil
		}

		time.Sleep(500 * time.Millisecond)
	}
}

// WaitForNodeBlockHeight waits for a node to reach a specific block height within a timeout period.
func WaitForNodeBlockHeight(ctx context.Context, blockchainClient blockchain.ClientI, height uint32, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	bestHeight := uint32(0)

	for {
		if time.Now().After(deadline) {
			return errors.NewProcessingError("timeout waiting for block height %d, best height %d", height, bestHeight)
		}

		_, meta, err := blockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			time.Sleep(1 * time.Second)

			continue
		}

		bestHeight = meta.Height

		if bestHeight >= height {
			return nil
		}

		time.Sleep(1 * time.Second)
	}
}
