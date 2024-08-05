package bootstrap

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/services/bootstrap/bootstrap_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type void struct{}

type Peer struct {
	ConnectedAt      time.Time
	AssetGrpcAddress string
	AssetHttpAddress string
	Source           string
	Name             string
}

type Client struct {
	client           bootstrap_api.BootstrapAPIClient
	logger           ulogger.Logger
	AssetGrpcAddress string
	AssetHttpAddress string
	peers            map[Peer]void
	callbackFunc     func(peer Peer)
	source           string
	name             string
}

func NewClient(logger ulogger.Logger, source string, name string) *Client {
	return &Client{
		logger: logger.New("bootC"),
		peers:  make(map[Peer]void),
		source: source,
		name:   name,
	}
}

// WithLogger overrides with default logger for the client
func (c *Client) WithLogger(logger ulogger.Logger) *Client {
	c.logger = logger
	return c
}

// WithCallback sets a callback function that is called when a peer is added
func (c *Client) WithCallback(callbackFunc func(peer Peer)) *Client {
	c.callbackFunc = callbackFunc
	return c
}

// WithAssetGrpcAddress overrides the local GRPC address for the client
func (c *Client) WithAssetGrpcAddress(addr string) *Client {
	c.AssetGrpcAddress = addr
	c.logger.Debugf("Local address set to: %s", c.AssetGrpcAddress)
	return c
}

// WithAssetHttpAddress overrides the local HTTP address for the client
func (c *Client) WithAssetHttpAddress(addr string) *Client {
	c.AssetHttpAddress = addr
	c.logger.Debugf("Local address set to: %s", c.AssetHttpAddress)
	return c
}

func (c *Client) Start(ctx context.Context) error {
	bootstrap_grpcAddress, _ := gocore.Config().Get("bootstrap_grpcAddress")
	conn, err := util.GetGRPCClient(ctx, bootstrap_grpcAddress, &util.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		return err
	}

	c.client = bootstrap_api.NewBootstrapAPIClient(conn)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		c.logger.Infof("Asset GRPC address: %s", c.AssetGrpcAddress)

		var resp *bootstrap_api.Notification
		var stream bootstrap_api.BootstrapAPI_ConnectClient

		for {
			select {
			case <-ctx.Done():
				c.logger.Infof("Stopping BootstrapClient as ctx is done")
				return ctx.Err()

			default:
				c.logger.Infof("Connecting to bootstrap server at: %s", bootstrap_grpcAddress)
				c.logger.Debugf("AssetGRPC address: %s", c.AssetGrpcAddress)
				stream, err = c.client.Connect(ctx, &bootstrap_api.Info{
					AssetGRPCAddress: c.AssetGrpcAddress,
					AssetHTTPAddress: c.AssetHttpAddress,
					Source:           c.source,
					Name:             c.name,
				})
				if err != nil {
					c.logger.Errorf("Could not connect to bootstrap server: %v. Retry in 1 second", err)
					time.Sleep(1 * time.Second)
					continue
				}

				for {
					resp, err = stream.Recv()
					if err != nil {
						c.logger.Errorf("Could not receive from bootstrap server: %v. Retry in 1 second", err)
						time.Sleep(1 * time.Second)
						break
					}

					switch resp.Type {
					case bootstrap_api.Type_PING:
						// Ignore

					case bootstrap_api.Type_ADD:
						peer := Peer{
							ConnectedAt:      resp.Info.ConnectedAt.AsTime(),
							AssetGrpcAddress: resp.Info.AssetGRPCAddress,
							AssetHttpAddress: resp.Info.AssetHTTPAddress,
							Source:           resp.Info.Source,
							Name:             resp.Info.Name,
						}

						c.peers[peer] = void{}
						c.logger.Infof("Added peer %s / %s / %s (%s)", resp.Info.Name, resp.Info.AssetGRPCAddress, resp.Info.AssetHTTPAddress, resp.Info.Source)

						if c.callbackFunc != nil {
							c.callbackFunc(peer)
						}

					case bootstrap_api.Type_REMOVE:
						peer := Peer{
							ConnectedAt:      resp.Info.ConnectedAt.AsTime(),
							AssetGrpcAddress: resp.Info.AssetGRPCAddress,
							AssetHttpAddress: resp.Info.AssetHTTPAddress,
							Source:           resp.Info.Source,
							Name:             resp.Info.Name,
						}

						delete(c.peers, peer)
						c.logger.Infof("Removed peer %s / %s / %s (%s)", resp.Info.Name, resp.Info.AssetGRPCAddress, resp.Info.AssetHTTPAddress, resp.Info.Source)
					}
				}
			}
		}
	})

	return nil
}
