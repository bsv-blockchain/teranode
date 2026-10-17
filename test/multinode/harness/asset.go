//go:build network_chaos

package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

// AssetGetJSON GETs path (relative to /api/v1, e.g. "/txmeta/<hash>/json")
// from node n's asset HTTP service. It returns the HTTP status code and, on a
// 200, decodes the body into out (if non-nil). A non-200 status is not an
// error so callers can assert on 404s.
func AssetGetJSON(ctx context.Context, node int, path string, out any) (int, error) {
	status, body, err := AssetGet(ctx, node, path)
	if err != nil {
		return 0, err
	}
	if status != http.StatusOK || out == nil {
		return status, nil
	}
	if err := json.Unmarshal(body, out); err != nil {
		return status, errors.NewProcessingError("asset %s: decode (body=%s)", path, truncate(body, 200), err)
	}
	return status, nil
}

// AssetGet GETs path (relative to /api/v1) from node n's asset HTTP service
// and returns the status code and raw body.
func AssetGet(ctx context.Context, node int, path string) (int, []byte, error) {
	url := fmt.Sprintf("http://127.0.0.1:%d/api/v1%s", DashboardPort(node), path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, err
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

// SpendingData mirrors the spendingData field of /utxos/:hash/json.
type SpendingData struct {
	TxID string `json:"txId"`
	Vin  int    `json:"vin"`
}

// UTXOItem mirrors one entry of /utxos/:hash/json.
type UTXOItem struct {
	Txid         string        `json:"txid"`
	Vout         uint32        `json:"vout"`
	Satoshis     uint64        `json:"satoshis"`
	Status       string        `json:"status"`
	SpendingData *SpendingData `json:"spendingData,omitempty"`
}

// UTXOs returns the per-output UTXO state of txid as seen by node n. The
// status is 404 if the node has no record of the transaction.
func UTXOs(ctx context.Context, node int, txid string) ([]UTXOItem, int, error) {
	var items []UTXOItem
	status, err := AssetGetJSON(ctx, node, "/utxos/"+txid+"/json", &items)
	return items, status, err
}

// TxMeta mirrors the subset of /txmeta/:hash/json the harness uses.
type TxMeta struct {
	BlockIDs    []uint32 `json:"blockIDs"`
	BlockHashes []string `json:"blockHashes"`
}

// GetTxMeta returns the tx meta of txid as seen by node n. The status is 404
// if the node has no record of the transaction.
func GetTxMeta(ctx context.Context, node int, txid string) (*TxMeta, int, error) {
	var meta TxMeta
	status, err := AssetGetJSON(ctx, node, "/txmeta/"+txid+"/json", &meta)
	return &meta, status, err
}

// Block mirrors the subset of /block/:hash/json (and /block/height/:h/json)
// the harness uses.
type Block struct {
	Height     uint32   `json:"height"`
	Subtrees   []string `json:"subtrees"`
	CoinbaseTx struct {
		TxID string `json:"txid"`
		Hex  string `json:"hex"`
	} `json:"coinbase_tx"`
}

// GetBlock returns block hash as seen by node n.
func GetBlock(ctx context.Context, node int, hash string) (*Block, error) {
	return getBlock(ctx, node, "/block/"+hash+"/json")
}

// GetBlockByHeight returns the block at height on node n's best chain.
func GetBlockByHeight(ctx context.Context, node int, height uint32) (*Block, error) {
	return getBlock(ctx, node, fmt.Sprintf("/block/height/%d/json", height))
}

func getBlock(ctx context.Context, node int, path string) (*Block, error) {
	var b Block
	status, err := AssetGetJSON(ctx, node, path, &b)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, errors.NewProcessingError("asset %s: status %d", path, status)
	}
	return &b, nil
}
