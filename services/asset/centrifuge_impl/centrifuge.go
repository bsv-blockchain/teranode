package centrifuge_impl

import (
	"context"
	"encoding/json"
	"github.com/bitcoin-sv/ubsv/errors"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/services/asset/http_impl"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/gorilla/websocket"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/asset/asset_api"
	"github.com/bitcoin-sv/ubsv/services/asset/repository"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/centrifugal/centrifuge"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type Centrifuge struct {
	logger           ulogger.Logger
	repository       *repository.Repository
	baseURL          string
	httpServer       *http_impl.HTTP
	blockchainClient blockchain.ClientI
	centrifugeNode   *centrifuge.Node
}

type messageType struct {
	Type string `json:"type"`
}

func New(logger ulogger.Logger, repo *repository.Repository, httpServer *http_impl.HTTP) (*Centrifuge, error) {
	u, err, found := gocore.Config().GetURL("asset_httpAddress")
	if err != nil {
		return nil, errors.NewConfigurationError("asset_httpAddress is not a valid URL", err)
	}
	if !found {
		return nil, errors.NewConfigurationError("asset_httpAddress not found in config")
	}

	c := &Centrifuge{
		logger:     logger,
		repository: repo,
		baseURL:    u.String(),
		httpServer: httpServer,
	}

	return c, nil
}

func (c *Centrifuge) Init(ctx context.Context) (err error) {
	c.logger.Infof("[AssetService] Centrifuge service initializing")

	//c.blockchainClient, err = blockchain.NewClient(ctx, c.logger)
	//if err != nil {
	//	return err
	//}

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
				"ping":      {},
				"block":     {},
				"subtree":   {},
				"mining_on": {},
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
	})

	return c.centrifugeNode.Run()
}

func (c *Centrifuge) Start(ctx context.Context, addr string) error {
	c.logger.Infof("[AssetService] Centrifuge service starting")

	err := c.startP2PListener()
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
	_ = c.httpServer.AddHTTPHandler("/connection/websocket", authMiddleware(websocketHandler))
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

func (c *Centrifuge) startP2PListener() error {
	p2pServerAddress, _ := gocore.Config().Get("p2p_httpAddress", "localhost:9906")

	u := url.URL{Scheme: "ws", Host: p2pServerAddress, Path: "/p2p-ws"}
	c.logger.Infof("[Centrifuge] connecting to p2p server on %s", u.String())

	var client atomic.Pointer[websocket.Conn]
	var clientConnected atomic.Bool

	go func() {
		for {
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
	}()

	go func() {
		for {
			webSocketClient := client.Load()
			if webSocketClient != nil {
				_, message, err := webSocketClient.ReadMessage()
				if err != nil {
					c.logger.Debugf("[Centrifuge] error reading p2p server message: %v", err)
					time.Sleep(1 * time.Second)
					clientConnected.Store(false)
					continue
				}

				// Unmarshal the message into a messageType struct
				var mType messageType
				err = json.Unmarshal(message, &mType)
				if err != nil {
					c.logger.Errorf("[Centrifuge] error unmarshalling message: %s", err)
					continue
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
	}()

	return nil
}

func (c *Centrifuge) _(ctx context.Context, addr string) error {
	// Subscribe to the blockchain service
	blockchainSubscription, err := c.blockchainClient.Subscribe(ctx, "AssetService")
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
					block, err = c.blockchainClient.GetBlock(ctx, notification.Hash)
					if err != nil {
						c.logger.Errorf("[Centrifuge] error getting block header: %s", err)
						continue
					}

					height, err = util.ExtractCoinbaseHeight(block.CoinbaseTx)
					if err != nil {
						c.logger.Errorf("[Centrifuge] error extracting coinbase height: %s", err)
					}

					// marshal the block header to json
					data, err = json.Marshal(struct {
						Hash       string             `json:"hash"`
						Height     uint32             `json:"height"`
						Header     *model.BlockHeader `json:"header"`
						CoinbaseTx string             `json:"coinbaseTx"`
						Subtrees   []*chainhash.Hash  `json:"subtrees"`
						BaseURL    string             `json:"baseUrl"`
					}{
						Hash:       block.String(),
						Height:     height,
						Header:     block.Header,
						CoinbaseTx: block.CoinbaseTx.String(),
						Subtrees:   block.Subtrees,
						BaseURL:    c.baseURL,
					})
					if err != nil {
						c.logger.Errorf("[Centrifuge] error marshalling block: %s", err)
						continue
					}
				case asset_api.Type_Subtree:
					channel = "subtree"
					data = []byte(`{"hash": "` + notification.Hash.String() + `","baseUrl": "` + c.baseURL + `"}`)
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

	http.Handle("/connection/websocket", authMiddleware(websocketHandler))
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

func (c *Centrifuge) Stop(ctx context.Context) error {
	c.logger.Infof("[AssetService] Centrifuge GRPC (impl) service shutting down")

	return nil
}

func authMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		newCtx := centrifuge.SetCredentials(ctx, &centrifuge.Credentials{
			UserID: "42",
		})
		r = r.WithContext(newCtx)

		header := w.Header()
		header.Set("Access-Control-Allow-Origin", "*")
		header.Add("Access-Control-Allow-Headers", "*")
		header.Set("Access-Control-Allow-Credentials", "true")

		h.ServeHTTP(w, r)
	})
}

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

		header := w.Header()
		header.Set("Access-Control-Allow-Origin", "*")
		header.Add("Access-Control-Allow-Headers", "*")
		header.Set("Access-Control-Allow-Credentials", "true")

		w.WriteHeader(http.StatusOK)
	}
}

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
		header.Set("Access-Control-Allow-Origin", "*")
		header.Add("Access-Control-Allow-Headers", "*")
		header.Set("Access-Control-Allow-Credentials", "true")

		w.WriteHeader(http.StatusOK)
	}
}
