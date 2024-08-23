package headers

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"

	"github.com/bitcoin-sv/ubsv/errors"
)

// RPCResponse represents the structure of the RPC response
type RPCResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *RPCError       `json:"error"`
}

// RPCError represents the structure of the RPC error
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// RPCClient is a struct to handle the RPC connection
type RPCClient struct {
	url    string
	client *http.Client
}

// NewRPCClient initializes a new RPC client
func NewRPCClient(url string) *RPCClient {
	return &RPCClient{
		url:    url,
		client: &http.Client{},
	}
}

// Call makes the RPC call
func (rpc *RPCClient) Call(method string, params ...interface{}) (*RPCResponse, error) {
	// Prepare the request payload
	payload := map[string]interface{}{
		"jsonrpc": "1.0",
		"id":      "go-client",
		"method":  method,
		"params":  params,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	// Make the HTTP POST request
	resp, err := rpc.client.Post(rpc.url, "application/json", bytes.NewBuffer(payloadBytes))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Parse the RPC response
	var rpcResponse RPCResponse
	if err := json.Unmarshal(body, &rpcResponse); err != nil {
		return nil, err
	}

	return &rpcResponse, nil
}

func (rpc *RPCClient) GetBlockByHeight(rpcClient *RPCClient, height int) (*BlockHeader, error) {
	// Get block header by height
	response, err := rpc.Call("getblockbyheight", height, 3)
	if err != nil {
		return nil, err
	}

	// Check if there is an error in the response
	if response.Error != nil {
		return nil, errors.NewProcessingError("rpc error: %s", response.Error.Message)
	}

	// Parse the block header
	var blockHeader BlockHeader
	if err := json.Unmarshal(response.Result, &blockHeader); err != nil {
		return nil, errors.NewProcessingError("failed to unmarshal block header", err)
	}

	return &blockHeader, nil
}

func (rpc *RPCClient) GetBlockHeader(rpcClient *RPCClient, hash string) ([]byte, error) {
	// Get block header by height
	response, err := rpc.Call("getblockheader", hash, 0)
	if err != nil {
		return nil, err
	}

	// Check if there is an error in the response
	if response.Error != nil {
		return nil, errors.NewProcessingError("rpc error: %s", response.Error.Message)
	}

	// Parse the block header
	var blockHeader string
	if err := json.Unmarshal(response.Result, &blockHeader); err != nil {
		return nil, errors.NewProcessingError("Failed to unmarshal raw block header", err)
	}

	return hex.DecodeString(blockHeader)
}
