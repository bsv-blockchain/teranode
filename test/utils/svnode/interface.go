package svnode

import (
	"context"
	"time"
)

const (
	// DefaultRPCPort is the default RPC port for regtest svnode
	DefaultRPCPort = 18332
	// DefaultP2PPort is the default P2P port for regtest svnode
	DefaultP2PPort = 18333
	// DefaultDockerImage is the default Docker image for svnode
	DefaultDockerImage = "bitcoinsv/bitcoin-sv:1.2.0"
)

// SVNodeI defines the interface for interacting with a Bitcoin SV node
type SVNodeI interface {
	// Lifecycle
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	WaitForReady(ctx context.Context, timeout time.Duration) error
	IsRunning() bool

	// Blockchain queries
	GetInfo() (map[string]interface{}, error)
	GetBlockCount() (int, error)
	GetBestBlockHash() (string, error)
	GetBlockHash(height int) (string, error)
	GetBlock(blockHash string, verbosity int) (map[string]interface{}, error)
	GetBlockchainInfo() (map[string]interface{}, error)
	GetChainTips() ([]map[string]interface{}, error)
	IsSynced() (bool, error)
	VerifyChain(checkLevel, numBlocks int) (bool, error)

	// Block generation
	Generate(numBlocks int) ([]string, error)

	// Network
	GetPeerInfo() ([]map[string]interface{}, error)
	GetNetworkInfo() (map[string]interface{}, error)
	GetConnectionCount() (int, error)
	AddNode(address string, command string) error
	DisconnectNode(address string) error

	// Transactions
	SendRawTransaction(txHex string) (string, error)

	// Waiting helpers
	WaitForBlockHeight(ctx context.Context, height int, timeout time.Duration) error
	WaitForPeerCount(ctx context.Context, minPeers int, timeout time.Duration) error

	// Connection info
	RPCURL() string
	P2PHost() string

	// Debug
	DebugString() string

	// GetLogs returns the container's stdout/stderr logs
	GetLogs(ctx context.Context) (string, error)
}

// Options configures SVNode creation
type Options struct {
	// DockerImage is the Docker image to use (default: bitcoinsv/bitcoin-sv:1.2.0)
	DockerImage string
	// ContainerName is a custom container name (optional)
	ContainerName string
	// RPCPort is the RPC port to expose (default: 18332)
	RPCPort int
	// P2PPort is the P2P port to expose (default: 18333)
	P2PPort int
	// KeepRunning prevents container cleanup after Stop() for debugging
	KeepRunning bool
	// ConnectTo specifies addresses to connect to at startup via -connect flag.
	// Unlike addnode, -connect creates regular outbound connections that are
	// used for initial block download in Bitcoin SV.
	ConnectTo []string
}

// DefaultOptions returns sensible defaults for SVNode
func DefaultOptions() Options {
	return Options{
		DockerImage: DefaultDockerImage,
		RPCPort:     DefaultRPCPort,
		P2PPort:     DefaultP2PPort,
	}
}

// New creates a new SVNode instance (always uses Docker via testcontainers)
func New(opts Options) SVNodeI {
	return NewDockerSVNode(opts)
}
