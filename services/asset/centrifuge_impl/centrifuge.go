// Package centrifuge_impl provides real-time blockchain event broadcasting via WebSocket connections.
package centrifuge_impl

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/asset/asset_api"
	"github.com/bsv-blockchain/teranode/services/asset/httpimpl"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/centrifugal/centrifuge"
	"github.com/gorilla/websocket"
)

const (
	AccessControlAllowOrigin      = "Access-Control-Allow-Origin"
	AccessControlAllowHeaders     = "Access-Control-Allow-Headers"
	AccessControlAllowCredentials = "Access-Control-Allow-Credentials"
)

// Centrifuge manages real-time blockchain data broadcasting to WebSocket clients.
// It provides pub/sub for blockchain events (new blocks, node status updates) using
// the Centrifuge protocol. Caches current node status and broadcasts updates when
// blockchain state changes occur.
//
// Thread-safe: statusMutex protects cachedCurrentNodeStatus and currentNodePeerID.
type Centrifuge struct {
	logger                  ulogger.Logger
	settings                *settings.Settings
	repository              *repository.Repository
	baseURL                 string
	httpServer              *httpimpl.HTTP
	blockchainClient        blockchain.ClientI
	centrifugeNode          *centrifuge.Node
	cachedCurrentNodeStatus *notificationMsg // Cached current node status for new clients
	currentNodePeerID       string           // Track the current node's peer ID
	statusMutex             sync.RWMutex     // Protects cachedCurrentNodeStatus and currentNodePeerID
}

// notificationMsg represents a notification message relayed to WebSocket clients.
// Supports types: "block", "node_status", "sync_status".
// Uses omitempty to minimize payload size.
type notificationMsg struct {
	Timestamp         string  `json:"timestamp,omitempty"`
	Type              string  `json:"type"`
	Hash              string  `json:"hash,omitempty"`
	BaseURL           string  `json:"base_url,omitempty"`
	PeerID            string  `json:"peer_id,omitempty"`
	PreviousHash      string  `json:"previousblockhash,omitempty"`
	TxCount           uint64  `json:"tx_count,omitempty"`
	Height            uint32  `json:"height,omitempty"`
	SizeInBytes       uint64  `json:"size_in_bytes,omitempty"`
	Miner             string  `json:"miner,omitempty"`
	Version           string  `json:"version,omitempty"`
	CommitHash        string  `json:"commit_hash,omitempty"`
	BestBlockHash     string  `json:"best_block_hash,omitempty"`
	BestHeight        uint32  `json:"best_height,omitempty"`
	TxCountInAssembly int     `json:"tx_count_in_assembly"`
	FSMState          string  `json:"fsm_state,omitempty"`
	StartTime         int64   `json:"start_time,omitempty"`
	Uptime            float64 `json:"uptime,omitempty"`
	ClientName        string  `json:"client_name,omitempty"` // Added missing ClientName field
	MinerName         string  `json:"miner_name,omitempty"`
	ListenMode        string  `json:"listen_mode,omitempty"`
	ChainWork         string  `json:"chain_work,omitempty"`
	SyncPeerID        string  `json:"sync_peer_id,omitempty"`
	SyncPeerHeight    int32   `json:"sync_peer_height,omitempty"`
	SyncPeerBlockHash string  `json:"sync_peer_block_hash,omitempty"`
	SyncConnectedAt   int64   `json:"sync_connected_at,omitempty"`
}

// messageType represents the structure for incoming message type identification.
type messageType struct {
	Type string `json:"type"`
}

// New creates a new Centrifuge server instance with the provided dependencies.
// It initializes the server with the necessary components for handling blockchain
// data broadcasting and client connections.
//
// Parameters:
//   - logger: Logger instance for server operations
//   - repo: Repository for accessing blockchain data
//   - httpServer: HTTP server instance for handling WebSocket connections
//
// Returns:
//   - *Centrifuge: New Centrifuge server instance
//   - error: Any error encountered during initialization
func New(logger ulogger.Logger, tSettings *settings.Settings, repo *repository.Repository, httpServer *httpimpl.HTTP) (*Centrifuge, error) {
	assetHTTPAddress := tSettings.Asset.HTTPAddress
	if assetHTTPAddress == "" {
		return nil, errors.NewConfigurationError("asset_httpAddress not found in config")
	}

	if _, err := url.Parse(assetHTTPAddress); err != nil {
		return nil, errors.NewConfigurationError("asset_httpAddress is not a valid URL", err)
	}

	c := &Centrifuge{
		logger:     logger,
		settings:   tSettings,
		repository: repo,
		baseURL:    assetHTTPAddress,
		httpServer: httpServer,
	}

	// Only set blockchainClient if repo is not nil
	if repo != nil {
		c.blockchainClient = repo.BlockchainClient
	}

	return c, nil
}

// Init initializes the Centrifuge server and sets up event handlers for
// client connections and message processing. It configures subscription channels
// for various blockchain events.
//
// Parameters:
//   - ctx: Context for initialization
//
// Returns:
//   - error: Any error encountered during initialization
//
// The server initializes the following subscription channels:
//   - ping: For connection health checks
//   - block: For new block notifications
//   - subtree: For Merkle tree updates
//   - mining_on: For mining status updates
func (c *Centrifuge) Init(ctx context.Context) (err error) {
	c.logger.Infof("[AssetService] Centrifuge service initializing")

	c.centrifugeNode, err = centrifuge.New(centrifuge.Config{
		LogLevel: centrifuge.LogLevelDebug,
		LogHandler: func(e centrifuge.LogEntry) {
			c.logger.Infof("[Centrifuge] %s: %s", e.Message, e.Fields)
		},
	})
	if err != nil {
		return err
	}

	c.centrifugeNode.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Subscriptions: map[string]centrifuge.SubscribeOptions{
				"ping":        {},
				"block":       {},
				"subtree":     {},
				"mining_on":   {},
				"node_status": {},
			},
		}, nil
	})

	c.centrifugeNode.OnConnect(func(client *centrifuge.Client) {
		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			c.logger.Infof("user %s unsubscribed from %s", client.UserID(), e.Channel)
		})
		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			c.logger.Infof("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		})

		transport := client.Transport()
		c.logger.Infof("user %s connected via %s", client.UserID(), transport.Name())

		// Send cached current node status first (replicates p2p behavior)
		c.statusMutex.RLock()
		cachedStatus := c.cachedCurrentNodeStatus
		c.statusMutex.RUnlock()

		if cachedStatus != nil {
			// Convert cached status back to JSON message
			if statusData, err := json.Marshal(cachedStatus); err == nil {
				// Publish to node_status channel immediately for this new client
				_, err = c.centrifugeNode.Publish("node_status", statusData)
				if err != nil {
					c.logger.Errorf("[Centrifuge] Failed to publish cached node status: %v", err)
				} else {
					c.logger.Debugf("[Centrifuge] Sent cached current node status to new client %s (peer: %s)",
						client.UserID(), cachedStatus.PeerID)
				}
			} else {
				c.logger.Errorf("[Centrifuge] Failed to marshal cached node status: %v", err)
			}
		}
	})

	return c.centrifugeNode.Run()
}

// Start begins the Centrifuge server operation, setting up WebSocket handlers
// and starting the P2P listener. It handles client connections and message routing.
//
// Parameters:
//   - ctx: Context for server operation
//   - addr: Address to listen on for WebSocket connections
//
// Returns:
//   - error: Any error encountered during server operation
func (c *Centrifuge) Start(ctx context.Context, addr string) error {
	c.logger.Infof("[AssetService] Centrifuge service starting")

	err := c.startP2PListener(ctx)
	if err != nil {
		return err
	}

	websocketHandler := NewWebsocketHandler(c.centrifugeNode, WebsocketConfig{
		ReadBufferSize:     1024,
		UseWriteBufferPool: true,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	})
	_ = c.httpServer.AddHTTPHandler("/connection/websocket", c.authMiddleware(websocketHandler))
	_ = c.httpServer.AddHTTPHandler("/subscribe", handleSubscribe(c.centrifugeNode))
	_ = c.httpServer.AddHTTPHandler("/unsubscribe", handleUnsubscribe(c.centrifugeNode))
	_ = c.httpServer.AddHTTPHandler("/client/", http.FileServer(http.Dir("./client")))

	shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = shutdownCancel

	<-ctx.Done()

	c.logger.Infof("[AssetService] Centrifuge (impl) service shutting down")

	if err = c.centrifugeNode.Shutdown(shutdownContext); err != nil {
		c.logger.Errorf("[AssetService] Centrifuge (impl) node service shutdown error: %s", err)
	}

	return nil
}

// startP2PListener initializes and starts the P2P WebSocket client that connects
// to the blockchain network for receiving real-time updates.
//
// Parameters:
//   - ctx: Context for P2P operations
//
// Returns:
//   - error: Any error encountered during P2P listener startup
func (c *Centrifuge) startP2PListener(ctx context.Context) error {
	p2pServerAddress := c.settings.P2P.HTTPAddress
	if p2pServerAddress == "" {
		return errors.NewConfigurationError("p2p_httpAddress not found in config")
	}

	u := url.URL{Scheme: "ws", Host: p2pServerAddress, Path: "/p2p-ws"}
	c.logger.Infof("[Centrifuge] connecting to p2p server on %s", u.String())

	var client atomic.Pointer[websocket.Conn]

	var clientConnected atomic.Bool

	go c.connect(ctx, u, &client, &clientConnected)

	go c.readMessages(ctx, &client, &clientConnected)

	return nil
}

// connect manages the P2P WebSocket connection to the blockchain network.
// It handles connection establishment and reconnection attempts on failure.
//
// Parameters:
//   - ctx: Context for connection operations
//   - u: WebSocket server URL
//   - client: Atomic pointer to the WebSocket connection
//   - clientConnected: Atomic boolean indicating connection status
func (c *Centrifuge) connect(ctx context.Context, u url.URL, client *atomic.Pointer[websocket.Conn], clientConnected *atomic.Bool) {
	for {
		select {
		case <-ctx.Done():
			c.logger.Infof("[Centrifuge] p2p client shutting down")
			return
		default:
			if !clientConnected.Load() {
				c.logger.Infof("[Centrifuge] dialing p2p server at: %s", u.String())
				websocketClient, _, err := websocket.DefaultDialer.Dial(u.String(), nil)

				if err != nil {
					c.logger.Errorf("[Centrifuge] error dialing p2p server: %v", err)
					client.Store(nil)
				} else {
					c.logger.Infof("[Centrifuge] connected to p2p server on: %s", u.String())
					clientConnected.Store(true)
					client.Store(websocketClient)
				}
			}

			// retrying in 1 second
			time.Sleep(1 * time.Second)
		}
	}
}

// readMessages continuously reads messages from the P2P WebSocket connection
// and publishes them to appropriate Centrifuge channels based on message type.
//
// Parameters:
//   - ctx: Context for message reading operations
//   - client: Atomic pointer to the WebSocket connection
//   - clientConnected: Atomic boolean indicating connection status
func (c *Centrifuge) readMessages(ctx context.Context, client *atomic.Pointer[websocket.Conn], clientConnected *atomic.Bool) {
	for {
		select {
		case <-ctx.Done():
			c.logger.Infof("[Centrifuge] p2p client shutting down")
			return
		default:
			webSocketClient := client.Load()
			if webSocketClient != nil {
				_, message, err := webSocketClient.ReadMessage()

				if err != nil {
					c.logger.Debugf("[Centrifuge] error reading p2p server message: %v", err)
					time.Sleep(1 * time.Second)
					clientConnected.Store(false)

					// Reset cached status when p2p connection is lost
					c.statusMutex.Lock()
					if c.cachedCurrentNodeStatus != nil {
						c.logger.Infof("[Centrifuge] P2P connection lost - clearing cached current node status")
						c.cachedCurrentNodeStatus = nil
						c.currentNodePeerID = ""
					}
					c.statusMutex.Unlock()

					continue
				}

				// Unmarshal the message into a messageType struct
				var mType messageType

				err = json.Unmarshal(message, &mType)
				if err != nil {
					c.logger.Errorf("[Centrifuge] error unmarshalling message: %s", err)
					continue
				}

				// Handle node_status messages - cache first one and update if from same peer
				if mType.Type == "node_status" {
					var nodeStatus notificationMsg
					if err := json.Unmarshal(message, &nodeStatus); err == nil {
						c.statusMutex.Lock()

						// First node_status: cache it and remember the peer ID
						if c.cachedCurrentNodeStatus == nil {
							c.cachedCurrentNodeStatus = &nodeStatus
							c.currentNodePeerID = nodeStatus.PeerID
							c.logger.Infof("[Centrifuge] Asset service ready - cached current node status from peer: %s", nodeStatus.PeerID)
						} else if c.currentNodePeerID == nodeStatus.PeerID {
							// Update cache if this is from the same current node (keeps data fresh)
							c.cachedCurrentNodeStatus = &nodeStatus
							c.logger.Debugf("[Centrifuge] Updated cached node status for current node: %s (height: %d, uptime: %.0fs)",
								nodeStatus.PeerID, nodeStatus.BestHeight, nodeStatus.Uptime)
						}

						c.statusMutex.Unlock()
					}
				}

				// send the message on to the centrifuge node
				_, err = c.centrifugeNode.Publish(strings.ToLower(mType.Type), message)
				if err != nil {
					c.logger.Errorf("[Centrifuge] error publishing to %s channel: %s", mType.Type, err)
				}
			} else {
				c.logger.Debugf("[Centrifuge] p2p client not connected, waiting...")
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (c *Centrifuge) _(ctx context.Context, addr string) error {
	// Subscribe to the blockchain service
	blockchainSubscription, err := c.blockchainClient.Subscribe(ctx, blockchain.SubscriberAssetService)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				c.logger.Infof("[AssetService] Centrifuge service shutting down")
				return
			case notification := <-blockchainSubscription:
				if notification == nil {
					continue
				}

				var channel string

				var data []byte

				var block *model.Block

				var height uint32

				switch asset_api.Type(notification.Type) {
				case asset_api.Type_Block:
					channel = "block"

					hash, err := chainhash.NewHash(notification.Hash)
					if err != nil {
						c.logger.Errorf("[Blockchain] failed to parse hash: %v", err)
						continue
					}

					block, err = c.blockchainClient.GetBlock(ctx, hash)
					if err != nil {
						c.logger.Errorf("[Centrifuge] error getting block header: %s", err)
						continue
					}

					height, err = util.ExtractCoinbaseHeight(block.CoinbaseTx)
					if err != nil {
						c.logger.Errorf("[Centrifuge] error extracting coinbase height: %s", err)
					}

					miner, err := util.ExtractCoinbaseMiner(block.CoinbaseTx)
					if err != nil {
						c.logger.Errorf("[Centrifuge] error extracting coinbase miner: %s", err)
					}

					// marshal the block header to json
					data, err = json.Marshal(struct {
						Hash       string             `json:"hash"`
						Height     uint32             `json:"height"`
						Header     *model.BlockHeader `json:"header"`
						CoinbaseTx string             `json:"coinbaseTx"`
						Subtrees   []*chainhash.Hash  `json:"subtrees"`
						BaseURL    string             `json:"baseUrl"`
						Miner      string             `json:"miner"`
					}{
						Hash:       block.String(),
						Height:     height,
						Header:     block.Header,
						CoinbaseTx: block.CoinbaseTx.String(),
						Subtrees:   block.Subtrees,
						BaseURL:    c.baseURL,
						Miner:      miner,
					})
					if err != nil {
						c.logger.Errorf("[Centrifuge] error marshalling block: %s", err)
						continue
					}
				case asset_api.Type_Subtree:
					channel = "subtree"
					cHash := chainhash.Hash(notification.Hash)
					data = []byte(`{"hash": "` + cHash.String() + `","baseUrl": "` + c.baseURL + `"}`)
				}

				if channel != "" {
					_, err = c.centrifugeNode.Publish(channel, data)
					if err != nil {
						c.logger.Errorf("[Centrifuge] error publishing to block channel: %s", err)
					}
				}
			}
		}
	}()

	websocketHandler := NewWebsocketHandler(c.centrifugeNode, WebsocketConfig{
		ReadBufferSize:     1024,
		UseWriteBufferPool: true,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	})

	http.Handle("/connection/websocket", c.authMiddleware(websocketHandler))
	http.Handle("/subscribe", handleSubscribe(c.centrifugeNode))
	http.Handle("/unsubscribe", handleUnsubscribe(c.centrifugeNode))
	http.Handle("/client/", http.FileServer(http.Dir("./client")))

	srv := &http.Server{
		Addr:              addr,
		Handler:           nil,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()

		shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = shutdownCancel

		c.logger.Infof("[AssetService] Centrifuge (impl) service shutting down")

		if err = c.centrifugeNode.Shutdown(shutdownContext); err != nil {
			c.logger.Errorf("[AssetService] Centrifuge (impl) node service shutdown error: %s", err)
		}

		if err = srv.Shutdown(shutdownContext); err != nil {
			c.logger.Errorf("[AssetService] Centrifuge (impl) http service shutdown error: %s", err)
		}
	}()

	// this will block
	if err = srv.ListenAndServe(); err != nil {
		return err
	}

	return nil
}

// Stop gracefully shuts down the Centrifuge server.
//
// Parameters:
//   - ctx: Context for shutdown operation
//
// Returns:
//   - error: Any error encountered during shutdown
func (c *Centrifuge) Stop(ctx context.Context) error {
	c.logger.Infof("[AssetService] Centrifuge service shutting down")

	return nil
}

// authMiddleware provides authentication middleware for WebSocket connections.
// It sets up CORS headers and user credentials for connecting clients.
// It also checks if the asset service is ready (has cached current node status).
//
// Parameters:
//   - h: The HTTP handler to wrap with authentication
//
// Returns:
//   - http.Handler: Middleware-wrapped HTTP handler
func (c *Centrifuge) authMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if asset service is ready (has cached current node status)
		c.statusMutex.RLock()
		ready := c.cachedCurrentNodeStatus != nil
		c.statusMutex.RUnlock()

		if !ready {
			// Asset service not ready - return service unavailable
			c.logger.Debugf("[Centrifuge] WebSocket connection rejected - asset service not ready (no current node status cached)")
			http.Error(w, "Asset service not ready - current node status unknown", http.StatusServiceUnavailable)
			return
		}

		ctx := r.Context()
		newCtx := centrifuge.SetCredentials(ctx, &centrifuge.Credentials{
			UserID: "42",
		})
		r = r.WithContext(newCtx)

		header := w.Header()
		header.Set(AccessControlAllowOrigin, "*")
		header.Add(AccessControlAllowHeaders, "*")
		header.Set(AccessControlAllowCredentials, "true")

		h.ServeHTTP(w, r)
	})
}

// handleSubscribe creates an HTTP handler for client subscription requests.
// It manages subscriptions to various channels including ping, block, subtree,
// and mining status updates.
//
// Parameters:
//   - node: Centrifuge node instance
//
// Returns:
//   - http.HandlerFunc: Handler for subscription requests
func handleSubscribe(node *centrifuge.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		clientID := req.URL.Query().Get("client")
		if clientID == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err := node.Subscribe("42", "ping", centrifuge.WithSubscribeClient(clientID))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = node.Subscribe("42", "block", centrifuge.WithSubscribeClient(clientID))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = node.Subscribe("42", "subtree", centrifuge.WithSubscribeClient(clientID))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = node.Subscribe("42", "mining_on", centrifuge.WithSubscribeClient(clientID))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = node.Subscribe("42", "node_status", centrifuge.WithSubscribeClient(clientID))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		header := w.Header()
		header.Set(AccessControlAllowOrigin, "*")
		header.Add(AccessControlAllowHeaders, "*")
		header.Set(AccessControlAllowCredentials, "true")

		w.WriteHeader(http.StatusOK)
	}
}

// handleUnsubscribe creates an HTTP handler for client unsubscription requests.
// It manages the removal of subscriptions from various channels.
//
// Parameters:
//   - node: Centrifuge node instance
//
// Returns:
//   - http.HandlerFunc: Handler for unsubscription requests
func handleUnsubscribe(node *centrifuge.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		clientID := req.URL.Query().Get("client")
		if clientID == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err := node.Unsubscribe("42", "ping", centrifuge.WithUnsubscribeClient(clientID))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = node.Unsubscribe("42", "block", centrifuge.WithUnsubscribeClient(clientID))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = node.Unsubscribe("42", "subtree", centrifuge.WithUnsubscribeClient(clientID))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = node.Unsubscribe("42", "mining_on", centrifuge.WithUnsubscribeClient(clientID))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		header := w.Header()
		header.Set(AccessControlAllowOrigin, "*")
		header.Add(AccessControlAllowHeaders, "*")
		header.Set(AccessControlAllowCredentials, "true")

		w.WriteHeader(http.StatusOK)
	}
}
