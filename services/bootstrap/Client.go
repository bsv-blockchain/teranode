package bootstrap

import (
	"context"
	"fmt"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/bootstrap/bootstrap_api"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type Peer struct {
	LocalAddress  string
	RemoteAddress string
}

type Client struct {
	client        bootstrap_api.BootstrapAPIClient
	logger        utils.Logger
	localAddress  string
	remoteAddress string
	peers         map[Peer]void
	callbackFunc  func(peer Peer)
}

func NewClient() *Client {
	return &Client{
		logger: gocore.Log("bootC"),
		peers:  make(map[Peer]void),
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

// WithLocalAddress overrides the local address for the client
func (c *Client) WithLocalAddress(localAddress string) *Client {
	c.localAddress = localAddress
	c.logger.Debugf("Local address set to: %s", c.localAddress)
	return c
}

// WithRemoteAddress overrides the remote address for the client
func (c *Client) WithRemoteAddress(remoteAddress string) *Client {
	c.remoteAddress = remoteAddress
	c.logger.Debugf("Remote address set to: %s", c.remoteAddress)
	return c
}

func (c *Client) Start(ctx context.Context) error {
	if c.localAddress == "" {
		hint, _ := gocore.Config().Get("ip_address_hint", "")
		localAddresses, err := utils.GetIPAddressesWithHint(hint)
		if err != nil {
			return fmt.Errorf("failed to get local ip address: %s", err)
		}

		c.localAddress = localAddresses[0]
		c.logger.Debugf("Local address automatically set to: %s", c.localAddress)
	}

	if c.remoteAddress == "" {
		remoteAddress, err := utils.GetPublicIPAddress()
		if err != nil {
			return fmt.Errorf("failed to get remote ip address: %s", err)
		}

		c.remoteAddress = remoteAddress
		c.logger.Debugf("Remote address automatically set to: %s", c.remoteAddress)
	}

	bootstrap_grpcAddress, _ := gocore.Config().Get("bootstrap_grpcAddress")
	conn, err := utils.GetGRPCClient(ctx, bootstrap_grpcAddress, &utils.ConnectionOptions{})
	if err != nil {
		return err
	}

	c.client = bootstrap_api.NewBootstrapAPIClient(conn)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		c.logger.Infof("Local / remote addresses: %s / %s", c.localAddress, c.remoteAddress)

	RETRY:
		for {
			select {
			case <-ctx.Done():
				c.logger.Infof("Stopping BootstrapClient as ctx is done")
				return ctx.Err()

			default:
				c.logger.Infof("Connecting to bootstrap server at: %s", bootstrap_grpcAddress)
				c.logger.Debugf("Local / remote addresses: %s / %s", c.localAddress, c.remoteAddress)
				stream, err := c.client.Connect(ctx, &bootstrap_api.Info{
					LocalAddress:  c.localAddress,
					RemoteAddress: c.remoteAddress,
				})
				if err != nil {
					time.Sleep(1 * time.Second)
					break RETRY
				}

				for {
					resp, err := stream.Recv()
					if err != nil {
						time.Sleep(1 * time.Second)
						break RETRY
					}

					switch resp.Type {
					case bootstrap_api.Type_PING:
						// Ignore

					case bootstrap_api.Type_ADD:
						peer := Peer{
							LocalAddress:  resp.Info.LocalAddress,
							RemoteAddress: resp.Info.RemoteAddress,
						}

						c.peers[peer] = void{}
						c.logger.Infof("Added peer %s / %s", resp.Info.LocalAddress, resp.Info.RemoteAddress)

						if c.callbackFunc != nil {
							c.callbackFunc(peer)
						}

					case bootstrap_api.Type_REMOVE:
						peer := Peer{
							LocalAddress:  resp.Info.LocalAddress,
							RemoteAddress: resp.Info.RemoteAddress,
						}

						delete(c.peers, peer)
						c.logger.Infof("Removed peer %s / %s", resp.Info.LocalAddress, resp.Info.RemoteAddress)
					}
				}
			}
		}

		return nil
	})

	return nil
}
