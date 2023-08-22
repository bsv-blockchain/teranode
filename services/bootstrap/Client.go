package bootstrap

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/services/bootstrap/bootstrap_api"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type void struct{}

type Peer struct {
	ConnectedAt           time.Time
	BlobServerGrpcAddress string
	BlobServerHttpAddress string
	Source                string
	Name                  string
}

type Client struct {
	client                bootstrap_api.BootstrapAPIClient
	logger                utils.Logger
	blobServerGrpcAddress string
	blobServerHttpAddress string
	peers                 map[Peer]void
	callbackFunc          func(peer Peer)
	source                string
	name                  string
}

func NewClient(source string, name string) *Client {
	return &Client{
		logger: gocore.Log("bootC"),
		peers:  make(map[Peer]void),
		source: source,
		name:   name,
	}
}

// WithLogger overrides with default logger for the client
func (c *Client) WithLogger(logger utils.Logger) *Client {
	c.logger = logger
	return c
}

// WithCallback sets a callback function that is called when a peer is added
func (c *Client) WithCallback(callbackFunc func(peer Peer)) *Client {
	c.callbackFunc = callbackFunc
	return c
}

// WithBlobServerGrpcAddress overrides the local GRPC address for the client
func (c *Client) WithBlobServerGrpcAddress(addr string) *Client {
	c.blobServerGrpcAddress = addr
	c.logger.Debugf("Local address set to: %s", c.blobServerGrpcAddress)
	return c
}

// WithBlobServerGrpcAddress overrides the local HTTP address for the client
func (c *Client) WithBlobServerHttpAddress(addr string) *Client {
	c.blobServerHttpAddress = addr
	c.logger.Debugf("Local address set to: %s", c.blobServerHttpAddress)
	return c
}

func (c *Client) Start(ctx context.Context) error {
	bootstrap_grpcAddress, _ := gocore.Config().Get("bootstrap_grpcAddress")
	conn, err := utils.GetGRPCClient(ctx, bootstrap_grpcAddress, &utils.ConnectionOptions{})
	if err != nil {
		return err
	}

	c.client = bootstrap_api.NewBootstrapAPIClient(conn)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		c.logger.Infof("BlobServer GRPC address: %s", c.blobServerGrpcAddress)

		var resp *bootstrap_api.Notification
		var stream bootstrap_api.BootstrapAPI_ConnectClient

		for {
			select {
			case <-ctx.Done():
				c.logger.Infof("Stopping BootstrapClient as ctx is done")
				return ctx.Err()

			default:
				c.logger.Infof("Connecting to bootstrap server at: %s", bootstrap_grpcAddress)
				c.logger.Debugf("BlobServerGRPC address: %s", c.blobServerGrpcAddress)
				stream, err = c.client.Connect(ctx, &bootstrap_api.Info{
					BlobServerGRPCAddress: c.blobServerGrpcAddress,
					BlobServerHTTPAddress: c.blobServerHttpAddress,
					Source:                c.source,
					Name:                  c.name,
				})
				if err != nil {
					time.Sleep(1 * time.Second)
					continue
				}

				for {
					resp, err = stream.Recv()
					if err != nil {
						time.Sleep(1 * time.Second)
						break
					}

					switch resp.Type {
					case bootstrap_api.Type_PING:
						// Ignore

					case bootstrap_api.Type_ADD:
						peer := Peer{
							ConnectedAt:           resp.Info.ConnectedAt.AsTime(),
							BlobServerGrpcAddress: resp.Info.BlobServerGRPCAddress,
							BlobServerHttpAddress: resp.Info.BlobServerHTTPAddress,
							Source:                resp.Info.Source,
							Name:                  resp.Info.Name,
						}

						c.peers[peer] = void{}
						c.logger.Infof("Added peer %s / %s / %s (%s)", resp.Info.Name, resp.Info.BlobServerGRPCAddress, resp.Info.BlobServerHTTPAddress, resp.Info.Source)

						if c.callbackFunc != nil {
							c.callbackFunc(peer)
						}

					case bootstrap_api.Type_REMOVE:
						peer := Peer{
							ConnectedAt:           resp.Info.ConnectedAt.AsTime(),
							BlobServerGrpcAddress: resp.Info.BlobServerGRPCAddress,
							BlobServerHttpAddress: resp.Info.BlobServerHTTPAddress,
							Source:                resp.Info.Source,
							Name:                  resp.Info.Name,
						}

						delete(c.peers, peer)
						c.logger.Infof("Removed peer %s / %s / %s (%s)", resp.Info.Name, resp.Info.BlobServerGRPCAddress, resp.Info.BlobServerHTTPAddress, resp.Info.Source)
					}
				}
			}
		}
	})

	return nil
}
