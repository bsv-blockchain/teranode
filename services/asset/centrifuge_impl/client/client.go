package client

import (
	"context"
	"fmt"

	"github.com/centrifugal/centrifuge-go"
	"github.com/ordishs/go-utils"
)

type Client struct {
	logger utils.Logger
	client *centrifuge.Client
}

func New(logger utils.Logger) *Client {
	return &Client{
		logger: logger,
	}
}

func (c *Client) Start(_ context.Context, addr string) error {
	c.logger.Infof("[CentrifugeClient] Starting client")

	c.client = centrifuge.NewJsonClient(fmt.Sprintf("ws://%s/connection/websocket", addr), centrifuge.Config{})

	c.client.OnConnecting(func(e centrifuge.ConnectingEvent) {
		c.logger.Infof("[CentrifugeClient] connecting - %d (%s)", e.Code, e.Reason)
	})

	c.client.OnConnected(func(e centrifuge.ConnectedEvent) {
		c.logger.Infof("[CentrifugeClient] connected with ID %s", e.ClientID)
	})

	c.client.OnDisconnected(func(e centrifuge.DisconnectedEvent) {
		c.logger.Infof("[CentrifugeClient] disconnected: %d (%s)", e.Code, e.Reason)
	})

	c.client.OnMessage(func(e centrifuge.MessageEvent) {
		c.logger.Infof("[CentrifugeClient] message: %s", string(e.Data))
	})

	c.client.OnPublication(func(e centrifuge.ServerPublicationEvent) {
		c.logger.Infof("[CentrifugeClient] publication: %s", string(e.Data))
	})

	c.client.OnError(func(e centrifuge.ErrorEvent) {
		c.logger.Errorf("[CentrifugeClient] error: %s", e.Error.Error())
	})

	subscriptions := []string{"blocks", "subtrees"}
	for _, channel := range subscriptions {
		c.logger.Infof("[CentrifugeClient] subscribing to %s", channel)
		sub, err := c.client.NewSubscription(channel, centrifuge.SubscriptionConfig{})
		if err != nil {
			return err
		}
		if err = sub.Subscribe(); err != nil {
			return err
		}
	}

	return c.client.Connect()
}

func (c *Client) Stop(_ context.Context) error {
	c.logger.Infof("Stopping client")

	return c.client.Disconnect()
}
