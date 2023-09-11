package bootstrap

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
)

type discoveryMsg struct {
	Type                  string    `json:"type"`
	ConnectedAt           time.Time `json:"connectedAt"`
	BlobServerGRPCAddress string    `json:"blobServerGRPCAddress"`
	BlobServerHTTPAddress string    `json:"blobServerHTTPAddress"`
	Source                string    `json:"source"`
	Ip                    string    `json:"ip"`
	Name                  string    `json:"name"`
}

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

func (s *Server) HandleWebSocket() func(c echo.Context) error {
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

			case msg := <-s.discoveryCh:
				if len(clientChannels) == 0 {
					continue
				}

				dm := &discoveryMsg{
					Type: msg.Type.String(),
				}

				if msg.Info != nil {
					if msg.Info.ConnectedAt != nil {
						dm.ConnectedAt = msg.Info.ConnectedAt.AsTime()
					}
					dm.BlobServerGRPCAddress = msg.Info.BlobServerGRPCAddress
					dm.BlobServerHTTPAddress = msg.Info.BlobServerHTTPAddress
					dm.Source = msg.Info.Source
					dm.Ip = msg.Info.Ip
					dm.Name = msg.Info.Name
				}

				data, err := json.MarshalIndent(dm, "", "  ")
				if err != nil {
					s.logger.Errorf("Error marshaling notification: %W", err)
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
				s.logger.Errorf("Failed to Send WS message: %w", err)
				break
			}
		}

		return nil
	}
}
