package svnode

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// DockerSVNode represents a running svnode instance in a Docker container
type DockerSVNode struct {
	container testcontainers.Container
	rpcURL    string
	p2pHost   string
	opts      Options
}

// Ensure DockerSVNode implements SVNodeI
var _ SVNodeI = (*DockerSVNode)(nil)

// NewDockerSVNode creates a new DockerSVNode instance
func NewDockerSVNode(opts Options) *DockerSVNode {
	if opts.DockerImage == "" {
		opts.DockerImage = DefaultDockerImage
	}
	if opts.RPCPort == 0 {
		opts.RPCPort = DefaultRPCPort
	}
	if opts.P2PPort == 0 {
		opts.P2PPort = DefaultP2PPort
	}

	return &DockerSVNode{
		opts:    opts,
		rpcURL:  fmt.Sprintf("http://localhost:%d", opts.RPCPort),
		p2pHost: fmt.Sprintf("localhost:%d", opts.P2PPort),
	}
}

// Start starts the svnode Docker container
func (d *DockerSVNode) Start(ctx context.Context) error {
	// Stop any existing container first
	_ = d.Stop(ctx)

	rpcPortStr := fmt.Sprintf("%d/tcp", d.opts.RPCPort)
	p2pPortStr := fmt.Sprintf("%d/tcp", d.opts.P2PPort)

	// Find the bitcoin.conf file path (relative to test directory)
	configPath, err := findConfigPath()
	if err != nil {
		return errors.NewProcessingError("failed to find bitcoin.conf", err)
	}

	// Build container request similar to docker-compose.e2etest.legacy.yml
	req := testcontainers.ContainerRequest{
		Image:        d.opts.DockerImage,
		ExposedPorts: []string{rpcPortStr, p2pPortStr},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.PortBindings = nat.PortMap{
				nat.Port(rpcPortStr): []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: fmt.Sprintf("%d", d.opts.RPCPort)}},
				nat.Port(p2pPortStr): []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: fmt.Sprintf("%d", d.opts.P2PPort)}},
			}
			hc.NetworkMode = "host"
			// Mount the config file (not read-only as entrypoint needs to chown it)
			hc.Binds = []string{
				fmt.Sprintf("%s:/data/bitcoin.conf", configPath),
			}
		},
		// Use entrypoint.sh like docker-compose does
		Cmd:        d.buildCmd(),
		WaitingFor: wait.ForLog("init message: Done loading").WithStartupTimeout(60 * time.Second),
	}

	// Set container name if specified
	if d.opts.ContainerName != "" {
		req.Name = d.opts.ContainerName
	}

	// Create and start container
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return errors.NewProcessingError("failed to start svnode container", err)
	}

	d.container = ctr

	// Wait for RPC to be ready
	if err := d.WaitForReady(ctx, 60*time.Second); err != nil {
		_ = d.Stop(ctx)
		return errors.NewProcessingError("svnode container started but RPC not ready", err)
	}

	return nil
}

// buildCmd returns the container command, including any -connect flags and additional args.
func (d *DockerSVNode) buildCmd() []string {
	cmd := []string{"/entrypoint.sh", "bitcoind"}

	// Override ports if non-default values are specified
	if d.opts.RPCPort != 0 && d.opts.RPCPort != DefaultRPCPort {
		cmd = append(cmd, fmt.Sprintf("-rpcport=%d", d.opts.RPCPort))
	}
	if d.opts.P2PPort != 0 && d.opts.P2PPort != DefaultP2PPort {
		cmd = append(cmd, fmt.Sprintf("-port=%d", d.opts.P2PPort))
	}

	for _, addr := range d.opts.ConnectTo {
		cmd = append(cmd, fmt.Sprintf("-connect=%s", addr))
	}
	cmd = append(cmd, d.opts.AdditionalArgs...)
	return cmd
}

// findConfigPath locates the bitcoin.conf file
func findConfigPath() (string, error) {
	// Try to find the config file by walking up from current directory
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	// Try common paths relative to where tests might run
	candidates := []string{
		filepath.Join(cwd, "test", "config", "svnode-1.conf"),
		filepath.Join(cwd, "..", "config", "svnode-1.conf"),
		filepath.Join(cwd, "..", "..", "config", "svnode-1.conf"),
		filepath.Join(cwd, "..", "..", "..", "config", "svnode-1.conf"),
		filepath.Join(cwd, "config", "svnode-1.conf"),
	}

	for _, path := range candidates {
		absPath, err := filepath.Abs(path)
		if err != nil {
			continue
		}
		if _, err := os.Stat(absPath); err == nil {
			return absPath, nil
		}
	}

	return "", errors.NewProcessingError("bitcoin.conf not found in any of: %v", candidates)
}

// Stop stops the svnode Docker container
func (d *DockerSVNode) Stop(ctx context.Context) error {
	if d.container == nil {
		return nil
	}

	// Skip cleanup if KeepRunning is set (for debugging)
	if d.opts.KeepRunning {
		// Note: To keep containers running after test process exits, also set
		// TESTCONTAINERS_RYUK_DISABLED=true when running the test
		return nil
	}

	err := d.container.Terminate(ctx)
	d.container = nil
	return err
}

// WaitForReady waits for the svnode to be ready to accept RPC commands
func (d *DockerSVNode) WaitForReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for {
		if time.Now().After(deadline) {
			return errors.NewProcessingError("timeout waiting for svnode to be ready")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			_, err := d.GetInfo()
			if err == nil {
				return nil
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// GetInfo calls the getinfo RPC method
func (d *DockerSVNode) GetInfo() (map[string]interface{}, error) {
	resp, err := helper.CallRPC(d.rpcURL, "getinfo", []interface{}{})
	if err != nil {
		return nil, err
	}

	var result struct {
		Result map[string]interface{} `json:"result"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return nil, errors.NewProcessingError("failed to parse getinfo response", err)
	}

	return result.Result, nil
}

// GetBlockCount returns the current block count
func (d *DockerSVNode) GetBlockCount() (int, error) {
	resp, err := helper.CallRPC(d.rpcURL, "getblockcount", []interface{}{})
	if err != nil {
		return 0, err
	}

	var result struct {
		Result int `json:"result"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return 0, errors.NewProcessingError("failed to parse getblockcount response", err)
	}

	return result.Result, nil
}

// Generate generates the specified number of blocks
func (d *DockerSVNode) Generate(numBlocks int) ([]string, error) {
	resp, err := helper.CallRPC(d.rpcURL, "generate", []interface{}{numBlocks})
	if err != nil {
		return nil, err
	}

	var result struct {
		Result []string `json:"result"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return nil, errors.NewProcessingError("failed to parse generate response", err)
	}

	return result.Result, nil
}

// SubmitBlock submits a raw block to the network
func (d *DockerSVNode) SubmitBlock(blockHex string) (string, error) {
	resp, err := helper.CallRPC(d.rpcURL, "submitblock", []interface{}{blockHex})
	if err != nil {
		return "", err
	}

	var result struct {
		Result interface{} `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return "", errors.NewProcessingError("failed to parse submitblock response", err)
	}

	if result.Error != nil {
		return "", errors.NewProcessingError("submitblock failed: %s", result.Error.Message)
	}

	// submitblock returns null on success, or an error string on failure
	if result.Result == nil {
		return "", nil // Success
	}

	return result.Result.(string), nil
}

// SetMockTime sets BSV regtest node's internal clock (regtest-only RPC).
// Pass 0 to reset to real time.
func (d *DockerSVNode) SetMockTime(timestamp int64) error {
	resp, err := helper.CallRPC(d.rpcURL, "setmocktime", []interface{}{timestamp})
	if err != nil {
		return err
	}

	var result struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return errors.NewProcessingError("failed to parse setmocktime response", err)
	}

	if result.Error != nil {
		return errors.NewProcessingError("setmocktime failed: %s", result.Error.Message)
	}

	return nil
}

// GetBlockHeader returns block header information
func (d *DockerSVNode) GetBlockHeader(blockHash string, verbose bool) (interface{}, error) {
	verboseInt := 0
	if verbose {
		verboseInt = 1
	}

	resp, err := helper.CallRPC(d.rpcURL, "getblockheader", []interface{}{blockHash, verboseInt})
	if err != nil {
		return nil, err
	}

	var result struct {
		Result interface{} `json:"result"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return nil, errors.NewProcessingError("failed to parse getblockheader response", err)
	}

	return result.Result, nil
}

// GetBlockchainInfo returns blockchain information
func (d *DockerSVNode) GetBlockchainInfo() (map[string]interface{}, error) {
	resp, err := helper.CallRPC(d.rpcURL, "getblockchaininfo", []interface{}{})
	if err != nil {
		return nil, err
	}

	var result struct {
		Result map[string]interface{} `json:"result"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return nil, errors.NewProcessingError("failed to parse getblockchaininfo response", err)
	}

	return result.Result, nil
}

// GetPeerInfo returns connected peer information
func (d *DockerSVNode) GetPeerInfo() ([]map[string]interface{}, error) {
	resp, err := helper.CallRPC(d.rpcURL, "getpeerinfo", []interface{}{})
	if err != nil {
		return nil, err
	}

	var result struct {
		Result []map[string]interface{} `json:"result"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return nil, errors.NewProcessingError("failed to parse getpeerinfo response", err)
	}

	return result.Result, nil
}

// WaitForBlockHeight waits for svnode to reach the specified block height
func (d *DockerSVNode) WaitForBlockHeight(ctx context.Context, height int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for {
		if time.Now().After(deadline) {
			currentHeight, _ := d.GetBlockCount()
			return errors.NewProcessingError("timeout waiting for block height %d, current height %d", height, currentHeight)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			currentHeight, err := d.GetBlockCount()
			if err == nil && currentHeight >= height {
				return nil
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// WaitForPeerCount waits for svnode to have at least the specified number of peers
func (d *DockerSVNode) WaitForPeerCount(ctx context.Context, minPeers int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for {
		if time.Now().After(deadline) {
			peers, _ := d.GetPeerInfo()
			return errors.NewProcessingError("timeout waiting for %d peers, current peers: %d", minPeers, len(peers))
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			peers, err := d.GetPeerInfo()
			if err == nil && len(peers) >= minPeers {
				return nil
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// RPCURL returns the RPC URL
func (d *DockerSVNode) RPCURL() string {
	return d.rpcURL
}

// P2PHost returns the P2P host address
func (d *DockerSVNode) P2PHost() string {
	return d.p2pHost
}

// IsRunning checks if the svnode container is currently running
func (d *DockerSVNode) IsRunning() bool {
	if d.container == nil {
		return false
	}
	_, err := d.GetInfo()
	return err == nil
}

// GetBestBlockHash returns the best block hash
func (d *DockerSVNode) GetBestBlockHash() (string, error) {
	resp, err := helper.CallRPC(d.rpcURL, "getbestblockhash", []interface{}{})
	if err != nil {
		return "", err
	}

	var result struct {
		Result string `json:"result"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return "", errors.NewProcessingError("failed to parse getbestblockhash response", err)
	}

	return result.Result, nil
}

// SendRawTransaction sends a raw transaction to the network
func (d *DockerSVNode) SendRawTransaction(txHex string) (string, error) {
	resp, err := helper.CallRPC(d.rpcURL, "sendrawtransaction", []interface{}{txHex})
	if err != nil {
		return "", err
	}

	var result struct {
		Result string `json:"result"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return "", errors.NewProcessingError("failed to parse sendrawtransaction response", err)
	}

	return result.Result, nil
}

// SendToAddress sends an amount to a given address
func (d *DockerSVNode) SendToAddress(address string, amount float64) (string, error) {
	resp, err := helper.CallRPC(d.rpcURL, "sendtoaddress", []interface{}{address, amount})
	if err != nil {
		return "", err
	}

	var result struct {
		Result string `json:"result"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return "", errors.NewProcessingError("failed to parse sendtoaddress response", err)
	}

	return result.Result, nil
}

// GetRawTransaction gets a raw transaction by txid
func (d *DockerSVNode) GetRawTransaction(txid string) (*bt.Tx, error) {
	// Get raw transaction as hex (verbosity 0)
	resp, err := helper.CallRPC(d.rpcURL, "getrawtransaction", []interface{}{txid, 0})
	if err != nil {
		return nil, err
	}

	var result struct {
		Result string `json:"result"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return nil, errors.NewProcessingError("failed to parse getrawtransaction response", err)
	}

	// Decode hex to transaction
	txBytes, err := hex.DecodeString(result.Result)
	if err != nil {
		return nil, errors.NewProcessingError("failed to decode transaction hex", err)
	}

	tx, err := bt.NewTxFromBytes(txBytes)
	if err != nil {
		return nil, errors.NewProcessingError("failed to parse transaction bytes", err)
	}

	return tx, nil
}

// GetRawTransactionVerbose gets verbose transaction info by txid (requires txindex=1)
// Returns the full transaction details including confirmations, blockhash, etc.
func (d *DockerSVNode) GetRawTransactionVerbose(txid string) (map[string]interface{}, error) {
	resp, err := helper.CallRPC(d.rpcURL, "getrawtransaction", []interface{}{txid, 1})
	if err != nil {
		return nil, err
	}

	var result struct {
		Result map[string]interface{} `json:"result"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return nil, errors.NewProcessingError("failed to parse getrawtransaction verbose response", err)
	}

	return result.Result, nil
}

// AddNode adds a node to connect to
func (d *DockerSVNode) AddNode(address string, command string) error {
	_, err := helper.CallRPC(d.rpcURL, "addnode", []interface{}{address, command})
	return err
}

// GetBlock returns block data for the given block hash
func (d *DockerSVNode) GetBlock(blockHash string, verbosity int) (map[string]interface{}, error) {
	resp, err := helper.CallRPC(d.rpcURL, "getblock", []interface{}{blockHash, verbosity})
	if err != nil {
		return nil, err
	}

	var result struct {
		Result map[string]interface{} `json:"result"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return nil, errors.NewProcessingError("failed to parse getblock response", err)
	}

	return result.Result, nil
}

// GetBlockHash returns the block hash at the given height
func (d *DockerSVNode) GetBlockHash(height int) (string, error) {
	resp, err := helper.CallRPC(d.rpcURL, "getblockhash", []interface{}{height})
	if err != nil {
		return "", err
	}

	var result struct {
		Result string `json:"result"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return "", errors.NewProcessingError("failed to parse getblockhash response", err)
	}

	return result.Result, nil
}

// GetNetworkInfo returns network information from svnode
func (d *DockerSVNode) GetNetworkInfo() (map[string]interface{}, error) {
	resp, err := helper.CallRPC(d.rpcURL, "getnetworkinfo", []interface{}{})
	if err != nil {
		return nil, err
	}

	var result struct {
		Result map[string]interface{} `json:"result"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return nil, errors.NewProcessingError("failed to parse getnetworkinfo response", err)
	}

	return result.Result, nil
}

// DisconnectNode disconnects from a specific node
func (d *DockerSVNode) DisconnectNode(address string) error {
	_, err := helper.CallRPC(d.rpcURL, "disconnectnode", []interface{}{address})
	return err
}

// GetConnectionCount returns the number of connections
func (d *DockerSVNode) GetConnectionCount() (int, error) {
	resp, err := helper.CallRPC(d.rpcURL, "getconnectioncount", []interface{}{})
	if err != nil {
		return 0, err
	}

	var result struct {
		Result int `json:"result"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return 0, errors.NewProcessingError("failed to parse getconnectioncount response", err)
	}

	return result.Result, nil
}

// VerifyChain verifies the blockchain database
func (d *DockerSVNode) VerifyChain(checkLevel, numBlocks int) (bool, error) {
	resp, err := helper.CallRPC(d.rpcURL, "verifychain", []interface{}{checkLevel, numBlocks})
	if err != nil {
		return false, err
	}

	var result struct {
		Result bool `json:"result"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return false, errors.NewProcessingError("failed to parse verifychain response", err)
	}

	return result.Result, nil
}

// IsSynced checks if the node believes it is synced
func (d *DockerSVNode) IsSynced() (bool, error) {
	info, err := d.GetBlockchainInfo()
	if err != nil {
		return false, err
	}

	headers, ok1 := info["headers"].(float64)
	blocks, ok2 := info["blocks"].(float64)

	if !ok1 || !ok2 {
		return false, errors.NewProcessingError("unable to parse headers/blocks from blockchain info")
	}

	return headers <= blocks, nil
}

// GetChainTips returns information about all known chain tips
func (d *DockerSVNode) GetChainTips() ([]map[string]interface{}, error) {
	resp, err := helper.CallRPC(d.rpcURL, "getchaintips", []interface{}{})
	if err != nil {
		return nil, err
	}

	var result struct {
		Result []map[string]interface{} `json:"result"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return nil, errors.NewProcessingError("failed to parse getchaintips response", err)
	}

	return result.Result, nil
}

// GetLogs returns the container's stdout/stderr logs
func (d *DockerSVNode) GetLogs(ctx context.Context) (string, error) {
	if d.container == nil {
		return "", nil
	}
	reader, err := d.container.Logs(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = reader.Close() }()
	buf := new(strings.Builder)
	_, _ = fmt.Fprintf(buf, "")
	b := make([]byte, 64*1024)
	for {
		n, readErr := reader.Read(b)
		if n > 0 {
			buf.Write(b[:n])
		}
		if readErr != nil {
			break
		}
	}
	return buf.String(), nil
}

// DebugString returns a debug representation of the svnode state
func (d *DockerSVNode) DebugString() string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("DockerSVNode RPC: %s, P2P: %s\n", d.rpcURL, d.p2pHost))

	if d.container != nil {
		sb.WriteString("  Container: running\n")
	} else {
		sb.WriteString("  Container: not running\n")
	}

	if blockCount, err := d.GetBlockCount(); err == nil {
		sb.WriteString(fmt.Sprintf("  Block count: %d\n", blockCount))
	}

	if peers, err := d.GetPeerInfo(); err == nil {
		sb.WriteString(fmt.Sprintf("  Peers: %d\n", len(peers)))
	}

	if hash, err := d.GetBestBlockHash(); err == nil {
		sb.WriteString(fmt.Sprintf("  Best block hash: %s\n", hash))
	}

	return sb.String()
}
