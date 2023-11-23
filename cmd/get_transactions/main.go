package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
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
	logger = gocore.Log("test")
)

func main() {
	baseUrl := "https://m1.scaling.ubsv.dev/"

	resp, err := util.DoHTTPRequest(context.Background(), baseUrl+"lastblocks?n=1", nil, nil, nil)
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

	resp, err = util.DoHTTPRequest(context.Background(), fmt.Sprintf("%sblock/%s/json", baseUrl, blockInfo[0].Hash), nil, nil, nil)
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
		panic(errors.New("no subtrees"))
	}

	resp, err = util.DoHTTPRequest(context.Background(), fmt.Sprintf("%ssubtree/%s/json", baseUrl, block.Subtrees[0]), nil, nil, nil)
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
			panic(errors.Join(fmt.Errorf("[getMissingTransactions] failed to get missing transactions batch"), err))
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
			return nil, errors.Join(fmt.Errorf("[getMissingTransactionsBatch] failed to parse tx hash"), err)
		}
		copy(txIDBytes[idx*32:(idx+1)*32], hash[:])
	}
	logger.Infof("[getMissingTransactionsBatch] getting %d txs from other miner: %d", len(txHashes), len(txIDBytes)/32)

	// do http request to baseUrl + txHash.String()
	url := fmt.Sprintf("%stxs", baseUrl)
	logger.Infof("[getMissingTransactionsBatch] getting %d txs from other miner %s", len(txHashes), url)

	body, err := util.DoHTTPRequestBodyReader(ctx, url, txIDBytes)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("[getMissingTransactionsBatch] failed to do http request"), err)
	}
	defer body.Close()

	//b, err := io.ReadAll(body)
	//if err != nil {
	//	return nil, errors.Join(fmt.Errorf("[getMissingTransactionsBatch] failed to read body"), err)
	//}
	//logger.Infof("[getMissingTransactionsBatch] got bytes: %s", string(b))
	//buf := bytes.NewReader(b)
	//logger.Infof("[getMissingTransactionsBatch] got %d bytes from other miner", len(b))

	// read the body into transactions using go-bt
	missingTxs := make([]*bt.Tx, 0, len(txHashes))
	var tx *bt.Tx
	for {
		tx, err = readTxFromReader(body)
		if err != nil || tx == nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, errors.Join(fmt.Errorf("[getMissingTransactionsBatch] failed to read transaction from body"), err)
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
				err = errors.New(x)
			case error:
				err = x
			default:
				err = fmt.Errorf("unknown panic: %v", r)
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
