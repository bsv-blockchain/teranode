package asset

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/services/asset/asset_api"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
)

type Peer struct {
	source           string
	validationClient *blockvalidation.Client
	logger           ulogger.Logger
	address          string
	running          *atomic.Bool
	notificationCh   chan *asset_api.Notification
}

func NewPeer(ctx context.Context, logger ulogger.Logger, source string, addr string, notificationCh chan *asset_api.Notification) *Peer {
	running := atomic.Bool{}
	running.Store(true)

	return &Peer{
		logger:           logger.New("blobC"),
		address:          addr,
		source:           source,
		validationClient: blockvalidation.NewClient(ctx, logger),
		running:          &running,
		notificationCh:   notificationCh,
	}
}

func (c *Peer) Start(ctx context.Context) error {
	// define here to prevent malloc
	var stream asset_api.AssetAPI_SubscribeClient
	var resp *asset_api.Notification
	var hash *chainhash.Hash

	var conn *grpc.ClientConn
	var err error
	go func() {
		for c.running.Load() {
			c.logger.Infof("connecting to blob server at %s", c.address)
			conn, err = util.GetGRPCClient(ctx, c.address, &util.ConnectionOptions{
				OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
				Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
				MaxRetries:  3,
			})
			if err != nil {
				c.logger.Errorf("could not connect to blob server at %s: %v", c.address, err)
				time.Sleep(10 * time.Second)
				continue
			}

			c.logger.Infof("starting new subscription to blob server: %v", c.address)
			stream, err = asset_api.NewAssetAPIClient(conn).Subscribe(ctx, &asset_api.SubscribeRequest{
				Source: c.source,
			})
			if err != nil {
				c.logger.Errorf("could not subscribe to blob server at %s: %v", c.address, err)
				time.Sleep(10 * time.Second)
				continue
			}

			for c.running.Load() {
				resp, err = stream.Recv()
				if err != nil {
					if !strings.Contains(err.Error(), context.Canceled.Error()) && !strings.Contains(err.Error(), io.EOF.Error()) {
						c.logger.Errorf("[Asset] could not receive: %v", err)
					}
					_ = stream.CloseSend()
					time.Sleep(10 * time.Second)
					break
				}

				hash, err = chainhash.NewHash(resp.Hash)
				if err != nil {
					c.logger.Errorf("could not create hash from bytes: %v", err)
					continue
				}

				switch resp.Type {
				case asset_api.Type_PING:
					// do nothing except pass it to clients
				case asset_api.Type_Subtree:
					c.logger.Debugf("Received SUBTREE notification: %s", hash.String())

					if err = c.validationClient.SubtreeFound(ctx, hash, resp.BaseUrl); err != nil {
						c.logger.Errorf("could not validate subtree: %v", err)
						continue
					}

				case asset_api.Type_Block:
					c.logger.Debugf("Received BLOCK notification: %s", hash.String())

					if err = c.validationClient.BlockFound(ctx, hash, resp.BaseUrl); err != nil {
						c.logger.Errorf("could not validate block: %v", err)
						continue
					}
				}

				// If we reach here, the incoming message has validated successfully: pass it to the notification channel...
				c.notificationCh <- resp
			}
		}
	}()

	go func() {
		<-ctx.Done()
		c.logger.Infof("[Asset] context done, closing peer")
		c.running.Store(false)
		if conn != nil {
			err = conn.Close()
			if err != nil {
				c.logger.Errorf("[Asset] failed to close connection", err)
			}
		}
	}()

	return nil
}

func (c *Peer) Stop() error {
	c.running.Store(false)
	return nil
}
