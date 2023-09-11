package http_impl

import (
	"encoding/json"
	"net/http"

	"github.com/bitcoin-sv/ubsv/services/blobserver/blobserver_api"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
)

type notificationMsg struct {
	Type    string `json:"type"`
	Hash    string `json:"hash"`
	BaseURL string `json:"base_url"`
}

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

func (h *HTTP) HandleWebSocket(notificationCh chan *blobserver_api.Notification) func(c echo.Context) error {
	clientChannels := make(map[chan []byte]struct{})
	newClientCh := make(chan chan []byte, 10)
	deadClientCh := make(chan chan []byte, 10)

	go func() {
		for {
			select {
			case newClient := <-newClientCh:
				clientChannels[newClient] = struct{}{}

			case deadClient := <-deadClientCh:
				delete(clientChannels, deadClient)

			case notification := <-notificationCh:
				if len(clientChannels) == 0 {
					continue
				}

				hash, _ := chainhash.NewHash(notification.Hash)

				data, err := json.MarshalIndent(&notificationMsg{
					Type:    notification.Type.String(),
					Hash:    hash.String(),
					BaseURL: notification.BaseUrl,
				}, "", "  ")
				if err != nil {
					h.logger.Errorf("Error marshaling notification: %w", err)
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

		ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
		if err != nil {
			return err
		}
		defer ws.Close()

		newClientCh <- ch

		for data := range ch {
			// Write
			err := ws.WriteMessage(websocket.TextMessage, data)
			if err != nil {
				deadClientCh <- ch
				h.logger.Errorf("Failed to Send WS message: %w", err)
				break
			}
		}

		return nil
	}
}
