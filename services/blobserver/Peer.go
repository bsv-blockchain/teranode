package blobserver

import (
	"context"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blobserver/blobserver_api"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Peer struct {
	peer             blobserver_api.BlobServerAPIClient
	source           string
	validationClient *blockvalidation.Client
	logger           utils.Logger
	address          string
	running          bool
	notificationCh   chan *blobserver_api.Notification
}

func NewPeer(ctx context.Context, source string, addr string, notificationCh chan *blobserver_api.Notification) *Peer {
	return &Peer{
		logger:           gocore.Log("blobC"),
		address:          addr,
		source:           source,
		validationClient: blockvalidation.NewClient(ctx),
		running:          true,
		notificationCh:   notificationCh,
	}
}

func (c *Peer) Start(ctx context.Context) error {
	conn, err := util.GetGRPCClient(ctx, c.address, &util.ConnectionOptions{})
	if err != nil {
		return err
	}

	c.peer = blobserver_api.NewBlobServerAPIClient(conn)

	// define here to prevent malloc
	var stream blobserver_api.BlobServerAPI_SubscribeClient
	var resp *blobserver_api.Notification
	var hash *chainhash.Hash

	go func() {
		<-ctx.Done()
		c.logger.Infof("[BlobServer] context done, closing peer")
		c.running = false
		err = conn.Close()
		if err != nil {
			c.logger.Errorf("[BlobServer] failed to close connection", err)
		}
	}()

	go func() {

		for c.running {
			c.logger.Infof("starting new subscription to blobserver: %v", c.address)
			stream, err = c.peer.Subscribe(ctx, &blobserver_api.SubscribeRequest{
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
					c.logger.Errorf("could not create hash from bytes: %v", err)
					continue
				}

				switch resp.Type {
				case blobserver_api.Type_Subtree:
					c.logger.Debugf("Received SUBTREE notification: %s", hash.String())

					if err = c.validationClient.SubtreeFound(ctx, hash, resp.BaseUrl); err != nil {
						c.logger.Errorf("could not validate subtree: %v", err)
						continue
					}

				case blobserver_api.Type_Block:
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

	return nil
}

func (c *Peer) Stop() error {
	c.running = false
	return nil
}
