package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type bestBlock struct {
	Hash string `json:"hash"`
}

type blockData struct {
	Subtrees []string `json:"subtrees"`
}

type subtreeData struct {
	Nodes []subtreeNodes `json:"Nodes"`
}

type subtreeNodes struct {
	TxID string `json:"txid"`
	Fee  int    `json:"fee"`
	Size int    `json:"size"`
}

var (
	logger = ulogger.New("test")
)

func main() {
	baseUrl := "https://m1.scaling.ubsv.dev"

	resp, err := DoHTTPRequest(context.Background(), baseUrl+"/lastblocks?n=1", nil, nil, nil)
	if err != nil {
		panic(err)
	}

	//log.Printf("Block: %s", string(blockResp))

	var blockInfo []bestBlock
	err = json.Unmarshal(resp, &blockInfo)
	if err != nil {
		panic(err)
	}

	logger.Infof("Best block: %v", blockInfo[0])

	blockInfo[0].Hash = "00be0eb1a07bcbf9ee1a03a4d85232993ce7be950b778564777a23cd94564ad5"

	resp, err = DoHTTPRequest(context.Background(), fmt.Sprintf("%s/block/%s/json", baseUrl, blockInfo[0].Hash), nil, nil, nil)
	if err != nil {
		panic(err)
	}

	//log.Printf("Block: %s", string(blockResp))

	var block blockData
	err = json.Unmarshal(resp, &block)
	if err != nil {
		panic(err)
	}

	logger.Infof("Block data: %v", block)

	if len(block.Subtrees) == 0 {
		panic(errors.New(errors.ERR_SUBTREE_ERROR, "no subtrees"))
	}

	resp, err = DoHTTPRequest(context.Background(), fmt.Sprintf("%s/subtree/%s/json", baseUrl, block.Subtrees[0]), nil, nil, nil)
	if err != nil {
		panic(err)
	}

	//log.Printf("Block: %s", string(resp))

	var subtree subtreeData
	err = json.Unmarshal(resp, &subtree)
	if err != nil {
		panic(err)
	}

	logger.Infof("Subtree received with %d nodes", len(subtree.Nodes))

	for i := 1; i < len(subtree.Nodes); i += 500 {
		missingTxHashesBatch := subtree.Nodes[i:Min(i+500, len(subtree.Nodes))]
		//g.Go(func() error {
		missingTxsBatch, err := getMissingTransactionsBatch(context.Background(), missingTxHashesBatch, baseUrl)
		if err != nil {
			panic(errors.New(errors.ERR_TX_NOT_FOUND, "[getMissingTransactions] failed to get missing transactions batch", err))
		}

		logger.Infof("[getMissingTransactions] got %d txs from other miner", len(missingTxsBatch))

		//missingTxsMu.Lock()
		//for _, tx := range missingTxsBatch {
		//	missingTxsMap[*tx.TxIDChainHash()] = tx
		//}
		//missingTxsMu.Unlock()

		//	return nil
		//})
	}

}

// getMissingTransactionsBatch gets a batch of transactions from the network
// NOTE: it does not return the transactions in the same order as the txHashes
func getMissingTransactionsBatch(ctx context.Context, txHashes []subtreeNodes, baseUrl string) ([]*bt.Tx, error) {
	txIDBytes := make([]byte, 32*len(txHashes))
	for idx, txHash := range txHashes {
		hash, err := chainhash.NewHashFromStr(txHash.TxID)
		if err != nil {
			return nil, errors.New(errors.ERR_PROCESSING, "[getMissingTransactionsBatch] failed to parse tx hash", err)
		}
		copy(txIDBytes[idx*32:(idx+1)*32], hash[:])
	}
	logger.Infof("[getMissingTransactionsBatch] getting %d txs from other miner: %d", len(txHashes), len(txIDBytes)/32)

	// do http request to baseUrl + txHash.String()
	url := fmt.Sprintf("%s/txs", baseUrl)
	logger.Infof("[getMissingTransactionsBatch] getting %d txs from other miner %s", len(txHashes), url)

	body, err := DoHTTPRequestBodyReader(ctx, url, txIDBytes)
	if err != nil {
		return nil, errors.New(errors.ERR_SERVICE_ERROR, "[getMissingTransactionsBatch] failed to do http request", err)
	}
	defer body.Close()

	b, err := io.ReadAll(body)
	if err != nil {
		return nil, errors.New(errors.ERR_SERVICE_ERROR, "[getMissingTransactionsBatch] failed to read body", err)
	}
	//logger.Infof("[getMissingTransactionsBatch] got bytes: %s", string(b))
	buf := bytes.NewReader(b)
	logger.Infof("[getMissingTransactionsBatch] got %d bytes from other miner", len(b))

	// read the body into transactions using go-bt
	missingTxs := make([]*bt.Tx, 0, len(txHashes))
	var tx *bt.Tx
	for {
		tx, err = readTxFromReader(buf)
		if err != nil || tx == nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, errors.New(errors.ERR_PROCESSING, "[getMissingTransactionsBatch] failed to read transaction from body", err)
		}

		missingTxs = append(missingTxs, tx)
	}

	return missingTxs, nil
}

func readTxFromReader(body io.Reader) (tx *bt.Tx, err error) {
	defer func() {
		// there is a bug in go-bt, that does not check input and throws a runtime error in
		// github.com/libsv/go-bt/v2@v2.2.2/input.go:76 +0x16b
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(errors.ERR_PROCESSING, x)
			case error:
				err = x
			default:
				err = errors.New(errors.ERR_UNKNOWN, "unknown panic: %v", r)
			}
		}
	}()

	tx = &bt.Tx{}
	_, err = tx.ReadFrom(body)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func DoHTTPRequest(ctx context.Context, url string, requestBody ...[]byte) ([]byte, error) {
	httpClient := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.New(errors.ERR_SERVICE_ERROR, "failed to create http request", err)
	}

	// write request body
	if len(requestBody) > 0 {
		req.Body = io.NopCloser(bytes.NewReader(requestBody[0]))
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, errors.New(errors.ERR_SERVICE_ERROR, "failed to do http request", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(errors.ERR_SERVICE_ERROR, "http request [%s] returned status code [%d]", url, resp.StatusCode)
	}

	blockBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New(errors.ERR_SERVICE_ERROR, "failed to read http response body", err)
	}

	return blockBytes, nil
}

func DoHTTPRequestBodyReader(ctx context.Context, url string, requestBody ...[]byte) (io.ReadCloser, error) {
	httpClient := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.New(errors.ERR_SERVICE_ERROR, "failed to create http request", err)
	}

	// write request body
	if len(requestBody) > 0 {
		req.Body = io.NopCloser(bytes.NewReader(requestBody[0]))
		req.Method = http.MethodPost
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, errors.New(errors.ERR_SERVICE_ERROR, "failed to do http request", err)
	}

	if resp.StatusCode != http.StatusOK {
		if resp.Body != nil {
			b, _ := io.ReadAll(resp.Body)
			if b != nil {
				return nil, errors.New(errors.ERR_SERVICE_ERROR, "http request [%s] returned status code [%d] with body [%s]", url, resp.StatusCode, string(b), err)
			}
		}
		return nil, errors.New(errors.ERR_SERVICE_ERROR, "http request [%s] returned status code [%d]", url, resp.StatusCode)
	}

	return resp.Body, nil
}
