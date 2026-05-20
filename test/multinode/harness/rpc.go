//go:build network_chaos

package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// RPCClient talks JSON-RPC to a single teranode's host-exposed RPC port using
// the default bitcoin:bitcoin credentials baked into settings.conf.
//
// It deliberately speaks raw HTTP rather than importing the teranode RPC
// client package to keep the harness decoupled from in-process types and to
// match exactly what compose/generated/generate-blocks.sh sends.
type RPCClient struct {
	NodeIndex int
	BaseURL   string

	http *http.Client
	user string
	pass string
}

// ChainTip mirrors the JSON returned by getchaintips.
type ChainTip struct {
	Height    int64  `json:"height"`
	Hash      string `json:"hash"`
	Branchlen int64  `json:"branchlen"`
	Status    string `json:"status"`
}

// BlockchainInfo mirrors the subset of getblockchaininfo the harness uses.
type BlockchainInfo struct {
	Chain         string  `json:"chain"`
	Blocks        int64   `json:"blocks"`
	Headers       int64   `json:"headers"`
	BestBlockHash string  `json:"bestblockhash"`
	Difficulty    float64 `json:"difficulty"`
}

func newRPCClient(node int) *RPCClient {
	return &RPCClient{
		NodeIndex: node,
		BaseURL:   fmt.Sprintf("http://localhost:%d", RPCPort(node)),
		http:      &http.Client{Timeout: 10 * time.Second},
		user:      "bitcoin",
		pass:      "bitcoin",
	}
}

// call issues a single JSON-RPC request and decodes its "result" field into
// out. Errors from the RPC response surface as Go errors.
func (c *RPCClient) call(ctx context.Context, method string, params []any, out any) error {
	body, err := json.Marshal(map[string]any{
		"method": method,
		"params": params,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.user, c.pass)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var env struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("rpc %s: decode envelope (status=%d body=%s): %w", method, resp.StatusCode, truncate(raw, 200), err)
	}
	if env.Error != nil {
		return fmt.Errorf("rpc %s: error %d: %s", method, env.Error.Code, env.Error.Message)
	}
	if out == nil || len(env.Result) == 0 {
		return nil
	}
	if err := json.Unmarshal(env.Result, out); err != nil {
		return fmt.Errorf("rpc %s: decode result: %w", method, err)
	}
	return nil
}

// Ready returns nil once the node's RPC answers getinfo. Use it in a polling
// loop; a single call will fail fast if the service is not yet up.
func (c *RPCClient) Ready(ctx context.Context) error {
	return c.call(ctx, "getinfo", []any{}, nil)
}

// GetChainTips returns all chain tips known to the node.
func (c *RPCClient) GetChainTips(ctx context.Context) ([]ChainTip, error) {
	var tips []ChainTip
	if err := c.call(ctx, "getchaintips", []any{}, &tips); err != nil {
		return nil, err
	}
	return tips, nil
}

// GetBlockchainInfo returns the node's current best-chain summary.
func (c *RPCClient) GetBlockchainInfo(ctx context.Context) (*BlockchainInfo, error) {
	var info BlockchainInfo
	if err := c.call(ctx, "getblockchaininfo", []any{}, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// BestTip returns the active tip of the node's best-work chain.
//
// Important: teranode caches getchaintips for 5 minutes, so during tests it
// returns stale data minutes after the chain has moved on. We use
// getblockchaininfo (10-second cache) and synthesize a ChainTip from its
// height and besthash fields instead. Callers that genuinely need fork tips
// should use GetChainTips directly and accept the staleness.
func (c *RPCClient) BestTip(ctx context.Context) (ChainTip, error) {
	info, err := c.GetBlockchainInfo(ctx)
	if err != nil {
		return ChainTip{}, err
	}
	return ChainTip{
		Height: info.Blocks,
		Hash:   info.BestBlockHash,
		Status: "active",
	}, nil
}

// Generate mines count blocks on this node via the generate RPC.
func (c *RPCClient) Generate(ctx context.Context, count int) ([]string, error) {
	var hashes []string
	if err := c.call(ctx, "generate", []any{count}, &hashes); err != nil {
		return nil, err
	}
	return hashes, nil
}

// ConnectionCount returns the number of peers this node is currently
// connected to. It calls getpeerinfo (teranode doesn't implement
// getconnectioncount) and counts the entries.
func (c *RPCClient) ConnectionCount(ctx context.Context) (int, error) {
	var peers []map[string]any
	if err := c.call(ctx, "getpeerinfo", []any{}, &peers); err != nil {
		return 0, err
	}
	return len(peers), nil
}

func truncate(b []byte, n int) string {
	s := string(b)
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
