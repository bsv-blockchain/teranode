package blobserver

import (
	"context"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blobserver/blobserver_api"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Client struct {
	client           blobserver_api.BlobServerAPIClient
	source           string
	validationClient *blockvalidation.Client
	logger           utils.Logger
	address          string
	running          bool
}

func NewClient(ctx context.Context, source string, addr string) *Client {
	return &Client{
		logger:           gocore.Log("blobC"),
		address:          addr,
		source:           source,
		validationClient: blockvalidation.NewClient(ctx),
		running:          true,
	}
}

func (c *Client) Start(ctx context.Context) error {
	conn, err := utils.GetGRPCClient(ctx, c.address, &utils.ConnectionOptions{})
	if err != nil {
		return err
	}

	c.client = blobserver_api.NewBlobServerAPIClient(conn)

	// define here to prevent malloc
	var stream blobserver_api.BlobServerAPI_SubscribeClient
	var resp *blobserver_api.Notification
	var hash *chainhash.Hash

	go func() {
		<-ctx.Done()
		c.logger.Infof("[BlobServer] context done, closing client")
		c.running = false
		err = conn.Close()
		if err != nil {
			c.logger.Errorf("[BlobServer] failed to close connection", err)
		}
	}()

	go func() {

		for c.running {
			c.logger.Infof("starting new subscription to blobserver: %v", c.address)
			stream, err = c.client.Subscribe(ctx, &blobserver_api.SubscribeRequest{
				Source: c.source,
			})
			if err != nil {
				c.logger.Errorf("could not subscribe to blobserver: %v", err)
				time.Sleep(10 * time.Second)
				continue
			}

			for c.running {
				resp, err = stream.Recv()
				if err != nil {
					if !strings.Contains(err.Error(), context.Canceled.Error()) {
						c.logger.Errorf("[BlobServer] could not receive: %v", err)
					}
					_ = stream.CloseSend()
					time.Sleep(10 * time.Second)
					break
				}

				hash, err = chainhash.NewHash(resp.Hash)
				if err != nil {
					c.logger.Errorf("could not create hash from bytes", "err", err)
					continue
				}

				switch resp.Type {
				case blobserver_api.Type_Subtree:
					c.logger.Debugf("Received SUBTREE notification: %s", hash.String())

					if err = c.validationClient.SubtreeFound(ctx, hash, resp.BaseUrl); err != nil {
						c.logger.Errorf("could not validate subtree", "err", err)
						continue
					}

				case blobserver_api.Type_Block:
					c.logger.Debugf("Received BLOCK notification: %s", hash.String())

					if err = c.validationClient.BlockFound(ctx, hash, resp.BaseUrl); err != nil {
						c.logger.Errorf("could not validate block", "err", err)
						continue
					}
				}
			}
		}
	}()

	return nil
}

func (c *Client) Stop() error {
	c.running = false
	return nil
}
