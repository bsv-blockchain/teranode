package p2p

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/bitcoin-sv/ubsv/services/asset/asset_api"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
)

type notificationMsg struct {
	Timestamp    string `json:"timestamp,omitempty"`
	Type         string `json:"type"`
	Hash         string `json:"hash,omitempty"`
	BaseURL      string `json:"base_url,omitempty"`
	PeerId       string `json:"peer_id,omitempty"`
	PreviousHash string `json:"previousblockhash,omitempty"`
	TxCount      uint64 `json:"tx_count,omitempty"`
	Height       uint32 `json:"height,omitempty"`
	SizeInBytes  uint64 `json:"size_in_bytes,omitempty"`
	Miner        string `json:"miner,omitempty"`
}

const (
	isoFormat = "2006-01-02T15:04:05Z"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

func (s *Server) HandleWebSocket(notificationCh chan *notificationMsg, baseUrl string) func(c echo.Context) error {
	clientChannels := make(map[chan []byte]struct{})
	newClientCh := make(chan chan []byte, 1_000)
	deadClientCh := make(chan chan []byte, 1_000)

	pingTimer := time.NewTicker(10 * time.Second)

	go func() {
		for {
			select {
			case newClient := <-newClientCh:
				clientChannels[newClient] = struct{}{}

			case deadClient := <-deadClientCh:
				delete(clientChannels, deadClient)

			case <-pingTimer.C:
				if len(clientChannels) == 0 {
					continue
				}

				data, err := json.MarshalIndent(&notificationMsg{
					Timestamp: time.Now().UTC().Format(isoFormat),
					Type:      asset_api.Type_PING.String(),
					BaseURL:   baseUrl,
				}, "", "  ")
				if err != nil {
					s.logger.Errorf("Error marshaling notification: %v", err)
					continue
				}

				for clientCh := range clientChannels {
					select {
					case clientCh <- data:
						// Data sent successfully
					case <-time.After(time.Second): // Adjust timeout duration as needed
						s.logger.Errorf("Timeout sending data to client")
					}
				}

			case notification := <-notificationCh:
				if len(clientChannels) == 0 {
					continue
				}

				data, err := json.MarshalIndent(notification, "", "  ")
				if err != nil {
					s.logger.Errorf("Error marshaling notification: %v", err)
					continue
				}

				for clientCh := range clientChannels {
					select {
					case clientCh <- data:
						// Data sent successfully
					case <-time.After(time.Second): // Adjust timeout duration as needed
						s.logger.Errorf("Timeout sending data to client")
					}
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
				s.logger.Errorf("Failed to Send notification WS message: %v", err)
				break
			}
		}

		return nil
	}
}
