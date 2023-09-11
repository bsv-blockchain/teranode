package bootstrap

import (
	"encoding/json"
	"time"

	"github.com/labstack/echo/v4"
	"golang.org/x/net/websocket"
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

				data, err := json.MarshalIndent(&discoveryMsg{
					Type:                  msg.Type.String(),
					ConnectedAt:           msg.Info.ConnectedAt.AsTime(),
					BlobServerGRPCAddress: msg.Info.BlobServerGRPCAddress,
					BlobServerHTTPAddress: msg.Info.BlobServerHTTPAddress,
					Source:                msg.Info.Source,
					Ip:                    msg.Info.Ip,
					Name:                  msg.Info.Name,
				}, "", "  ")
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

		websocket.Handler(func(ws *websocket.Conn) {
			defer ws.Close()

			newClientCh <- ch

			for data := range ch {
				// Write
				err := websocket.Message.Send(ws, data)
				if err != nil {
					deadClientCh <- ch
					close(ch)
					s.logger.Errorf("Failed to Send WS message: %w", err)
					break
				}
			}
		}).ServeHTTP(c.Response(), c.Request())

		return nil
	}
}
