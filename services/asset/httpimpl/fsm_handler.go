package httpimpl

import (
	"net/http"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
)

// FSMHandler handles FSM state related API endpoints
type FSMHandler struct {
	blockchainClient blockchain.ClientI
	logger           ulogger.Logger
}

// NewFSMHandler creates a new FSM handler
func NewFSMHandler(blockchainClient blockchain.ClientI, logger ulogger.Logger) *FSMHandler {
	return &FSMHandler{
		blockchainClient: blockchainClient,
		logger:           logger,
	}
}

// getCurrentState retrieves the current FSM state and handles error logging
func (h *FSMHandler) getCurrentState(c echo.Context) (*blockchain.FSMStateType, error) {
	ctx := c.Request().Context()
	ctxLogger := h.logger.WithTraceContext(ctx)

	state, err := h.blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		ctxLogger.Errorf("Failed to get blockchain FSM state: %v", err)

		return nil, echo.NewHTTPError(http.StatusInternalServerError, "Failed to get FSM state: "+err.Error())
	}

	ctxLogger.Debugf("Current blockchain FSM state: %s (%d)", state.String(), state.Number())

	return state, nil
}

// respondWithState formats and returns the current state as a JSON response
func (h *FSMHandler) respondWithState(c echo.Context, state *blockchain.FSMStateType) error {
	return c.JSON(http.StatusOK, map[string]interface{}{
		"state":       state.String(),
		"state_value": int(state.Number()),
	})
}

// GetFSMState retrieves the current FSM state from the blockchain service
func (h *FSMHandler) GetFSMState(c echo.Context) error {
	h.logger.WithTraceContext(c.Request().Context()).Debugf("Getting current blockchain FSM state")

	state, err := h.getCurrentState(c)
	if err != nil {
		return err
	}

	return h.respondWithState(c, state)
}

// GetFSMEvents returns the available events for the blockchain FSM
func (h *FSMHandler) GetFSMEvents(c echo.Context) error {
	h.logger.WithTraceContext(c.Request().Context()).Debugf("Getting available blockchain FSM events")

	state, err := h.getCurrentState(c)
	if err != nil {
		return err
	}

	var events []string
	// these transitions are stable and documented in stateMachine.md.
	switch *state {
	case blockchain_api.FSMStateType_IDLE:
		events = []string{
			blockchain_api.FSMEventType_RUN.String(),
			blockchain_api.FSMEventType_LEGACYSYNC.String(),
		}
	case blockchain_api.FSMStateType_RUNNING:
		events = []string{
			blockchain_api.FSMEventType_STOP.String(),
			blockchain_api.FSMEventType_CATCHUPBLOCKS.String(),
		}
	case blockchain_api.FSMStateType_LEGACYSYNCING:
		events = []string{
			blockchain_api.FSMEventType_RUN.String(),
			blockchain_api.FSMEventType_STOP.String(),
		}
	case blockchain_api.FSMStateType_CATCHINGBLOCKS:
		events = []string{
			blockchain_api.FSMEventType_RUN.String(),
		}
	default:
		events = []string{}
	}

	h.logger.WithTraceContext(c.Request().Context()).Debugf("Available blockchain FSM events: %v", events)

	// Return the events as JSON
	return c.JSON(http.StatusOK, map[string]interface{}{
		"events": events,
	})
}

func extractInvalidArgumentMessage(err error) string {
	if err == nil {
		return ""
	}

	msg := err.Error()

	marker := ""
	markerIdx := -1
	for _, m := range []string{"INVALID_ARGUMENT", "InvalidArgument"} {
		if idx := strings.LastIndex(msg, m); idx >= 0 && idx > markerIdx {
			marker = m
			markerIdx = idx
		}
	}
	if markerIdx >= 0 {
		remaining := msg[markerIdx+len(marker):]
		if colon := strings.Index(remaining, ":"); colon >= 0 {
			return strings.TrimSpace(remaining[colon+1:])
		}
	}

	if idx := strings.LastIndex(msg, "desc ="); idx >= 0 {
		return strings.TrimSpace(msg[idx+len("desc ="):])
	}

	return msg
}

// GetFSMStates returns all possible FSM states
func (h *FSMHandler) GetFSMStates(c echo.Context) error {
	ctxLogger := h.logger.WithTraceContext(c.Request().Context())
	ctxLogger.Debugf("Getting all possible blockchain FSM states")

	// Create a map of all possible states
	states := make([]map[string]interface{}, 0)
	for name, value := range blockchain_api.FSMStateType_value {
		states = append(states, map[string]interface{}{
			"name":  name,
			"value": int(value),
		})
	}

	ctxLogger.Debugf("All possible blockchain FSM states: %v", states)

	return c.JSON(http.StatusOK, states)
}

// validateAndGetEventType validates the event request and returns the corresponding event type
func (h *FSMHandler) validateAndGetEventType(c echo.Context) (blockchain_api.FSMEventType, error) {
	// Parse the event from the request body
	var request struct {
		Event string `json:"event"`
	}

	if err := c.Bind(&request); err != nil {
		return 0, echo.NewHTTPError(http.StatusBadRequest, "Invalid request body: "+err.Error())
	}

	if request.Event == "" {
		return 0, echo.NewHTTPError(http.StatusBadRequest, "Event is required")
	}

	// Convert the event string to an FSMEventType
	eventType, ok := blockchain_api.FSMEventType_value[request.Event]
	if !ok {
		h.logger.WithTraceContext(c.Request().Context()).Errorf("Invalid FSM event type: %s", request.Event)
		return 0, echo.NewHTTPError(http.StatusBadRequest, "Invalid event type: "+request.Event)
	}

	return blockchain_api.FSMEventType(eventType), nil
}

// SendFSMEvent sends a custom event to the blockchain FSM
func (h *FSMHandler) SendFSMEvent(c echo.Context) error {
	eventType, err := h.validateAndGetEventType(c)
	if err != nil {
		return err
	}

	ctx := c.Request().Context()
	ctxLogger := h.logger.WithTraceContext(ctx)

	ctxLogger.Infof("Sending custom FSM event: %s (%d)", eventType.String(), eventType)

	// Send the event to the blockchain service
	if err := h.blockchainClient.SendFSMEvent(ctx, eventType); err != nil {
		ctxLogger.Errorf("Failed to send custom FSM event %s: %v", eventType.String(), err)
		if strings.Contains(err.Error(), "INVALID_ARGUMENT") || strings.Contains(err.Error(), "InvalidArgument") {
			return sendError(c, http.StatusBadRequest, int32(errors.ERR_INVALID_ARGUMENT), errors.NewInvalidArgumentError(extractInvalidArgumentMessage(err)))
		}
		return sendError(c, http.StatusInternalServerError, int32(errors.ERR_SERVICE_ERROR), errors.NewServiceError("failed to send FSM event", err))
	}

	state, err := h.getCurrentState(c)
	if err != nil {
		return err
	}

	return h.respondWithState(c, state)
}
