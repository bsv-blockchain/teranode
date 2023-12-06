package status

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

func (s *Server) HandleWebSocket(wsCh chan interface{}) func(c echo.Context) error {
	clientChannels := make(map[chan []byte]struct{})
	newClientCh := make(chan chan []byte, 10)
	deadClientCh := make(chan chan []byte, 10)

	pingTimer := time.NewTicker(20 * time.Second)

	go func() {
		for {
			select {
			case newClient := <-newClientCh:
				clientChannels[newClient] = struct{}{}

				values := make([]*model.AnnounceStatusRequest, 0, len(s.statusItems))
				for _, value := range s.statusItems {
					values = append(values, value)
				}

				data, err := json.MarshalIndent(values, "", "  ")
				if err != nil {
					s.logger.Errorf("Error marshaling status items: %w", err)
					continue
				}

				newClient <- data

			case deadClient := <-deadClientCh:
				delete(clientChannels, deadClient)

			case <-pingTimer.C:
				if len(clientChannels) == 0 {
					continue
				}

				now := time.Now()

				data, err := json.MarshalIndent(&model.AnnounceStatusRequest{
					Timestamp:   timestamppb.New(now),
					ClusterName: s.name,
					Type:        "PING",
					ExpiresAt:   timestamppb.New(now.Add(30 * time.Second)),
				}, "", "  ")
				if err != nil {
					s.logger.Errorf("Error marshaling notification: %w", err)
					continue
				}

				for clientCh := range clientChannels {
					clientCh <- data
				}

			case msg := <-wsCh:
				if len(clientChannels) == 0 {
					continue
				}

				data, err := json.MarshalIndent(msg, "", "  ")
				if err != nil {
					s.logger.Errorf("Error marshaling notification: %w", err)
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
				s.logger.Errorf("Failed to Send notification WS message: %v", err)
				break
			}
		}

		return nil
	}
}
