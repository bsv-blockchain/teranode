package http_impl

import (
	"encoding/json"

	"github.com/bitcoin-sv/ubsv/services/blobserver/blobserver_api"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/net/websocket"
)

type notificationMsg struct {
	Type    string `json:"type"`
	Hash    string `json:"hash"`
	BaseURL string `json:"base_url"`
}

func (h *HTTP) HandleWebSocket(notificationCh chan *blobserver_api.Notification) func(c echo.Context) error {

	clientChannels := make(map[chan []byte]struct{})
	newClientCh := make(chan chan []byte, 10)
	deadClientCh := make(chan chan []byte, 10)

	go func() {
		for {
			select {
			case ch := <-newClientCh:
				clientChannels[ch] = struct{}{}

			case clientCh := <-deadClientCh:
				delete(clientChannels, clientCh)

			case notification := <-notificationCh:
				hash, _ := chainhash.NewHash(notification.Hash)

				data, err := json.MarshalIndent(&notificationMsg{
					Type:    notification.Type.String(),
					Hash:    hash.String(),
					BaseURL: notification.BaseUrl,
				}, "", "  ")
				if err != nil {
					h.logger.Errorf("Error marshaling notification: %W", err)
					continue
				}

				for clientCh := range clientChannels {
					clientCh <- data
				}
			}
		}
	}()

	return func(c echo.Context) error {
		ch := make(chan []byte)

		websocket.Handler(func(ws *websocket.Conn) {
			defer ws.Close()

			newClientCh <- ch

			for data := range ch {
				// Write
				err := websocket.Message.Send(ws, data)
				if err != nil {
					deadClientCh <- ch
					h.logger.Errorf("Failed to Send WS message: %w", err)
					break
				}
			}
		}).ServeHTTP(c.Response(), c.Request())

		return nil
	}
}
