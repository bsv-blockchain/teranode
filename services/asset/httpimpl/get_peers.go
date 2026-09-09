package httpimpl

import (
	"context"
	"net/http"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/labstack/echo/v4"
)

// LegacyPeerResponse carries the fields only a Bitcoin wire-protocol (legacy)
// peer supplies. It is absent for a libp2p peer.
//
// swagger:model LegacyPeerResponse
type LegacyPeerResponse struct {
	Inbound         bool   `json:"inbound"`
	ProtocolVersion uint32 `json:"protocol_version"`
	ServiceFlags    uint64 `json:"service_flags"`
	PingMicros      int64  `json:"ping_micros"`
	TimeOffsetSecs  int64  `json:"time_offset_secs"`
	StartingHeight  int32  `json:"starting_height"`
	IsSyncPeer      bool   `json:"is_sync_peer"`
	TimeConnected   int64  `json:"time_connected"`
}

// PeerInfoResponse represents the JSON response for a single peer.
//
// swagger:model PeerInfoResponse
type PeerInfoResponse struct {
	ID              string `json:"id"`
	Transport       string `json:"transport"`
	ClientName      string `json:"client_name"`
	Height          uint32 `json:"height"`
	BlockHash       string `json:"block_hash"`
	DataHubURL      string `json:"data_hub_url"`
	NetworkAddress  string `json:"network_address,omitempty"`
	BanScore        int    `json:"ban_score"`
	IsBanned        bool   `json:"is_banned"`
	IsConnected     bool   `json:"is_connected"`
	ConnectedAt     int64  `json:"connected_at"`
	BytesSent       uint64 `json:"bytes_sent"`
	BytesReceived   uint64 `json:"bytes_received"`
	LastBlockTime   int64  `json:"last_block_time"`
	LastMessageTime int64  `json:"last_message_time"`

	// Catchup metrics
	CatchupAttempts        int64   `json:"catchup_attempts"`
	CatchupSuccesses       int64   `json:"catchup_successes"`
	CatchupFailures        int64   `json:"catchup_failures"`
	CatchupLastAttempt     int64   `json:"catchup_last_attempt"`
	CatchupLastSuccess     int64   `json:"catchup_last_success"`
	CatchupLastFailure     int64   `json:"catchup_last_failure"`
	CatchupReputationScore float64 `json:"catchup_reputation_score"`
	CatchupMaliciousCount  int64   `json:"catchup_malicious_count"`
	CatchupAvgResponseTime int64   `json:"catchup_avg_response_ms"`
	LastCatchupError       string  `json:"last_catchup_error"`
	LastCatchupErrorTime   int64   `json:"last_catchup_error_time"`

	// Legacy holds wire-protocol-only fields, absent for a libp2p peer.
	Legacy *LegacyPeerResponse `json:"legacy,omitempty"`
}

// PeersResponse represents the JSON response containing all peers.
//
// swagger:model PeersResponse
type PeersResponse struct {
	Peers []PeerInfoResponse `json:"peers"`
	Count int                `json:"count"`
}

// transportLabel maps a registry transport type to the string the dashboard
// switches on.
func transportLabel(transport blockchain_api.TransportType) string {
	if transport == blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL {
		return "legacy"
	}

	return "libp2p"
}

// timeToUnix converts a timestamp to Unix seconds, mapping an unset time to 0.
// The guard matters: time.Time{}.Unix() is -62135596800, which is truthy in
// JavaScript, so the dashboard would render year 1 where it means to render
// "Never". The p2p read path this endpoint replaced applied the same guard.
func timeToUnix(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}

	return t.Unix()
}

// peerInfoToResponse converts one registry peer to its JSON form.
func peerInfoToResponse(peer *blockchain.PeerInfo) PeerInfoResponse {
	blockHashStr := ""
	if peer.BlockHash != nil {
		blockHashStr = peer.BlockHash.String()
	}

	response := PeerInfoResponse{
		ID:              peer.ID,
		Transport:       transportLabel(peer.TransportType),
		ClientName:      peer.ClientName,
		Height:          peer.Height,
		BlockHash:       blockHashStr,
		DataHubURL:      peer.DataHubURL,
		NetworkAddress:  peer.NetworkAddress,
		BanScore:        int(peer.BanScore),
		IsBanned:        peer.IsBanned,
		IsConnected:     peer.IsConnected,
		ConnectedAt:     timeToUnix(peer.ConnectedAt),
		BytesSent:       peer.BytesSent,
		BytesReceived:   peer.BytesReceived,
		LastBlockTime:   timeToUnix(peer.LastBlockTime),
		LastMessageTime: timeToUnix(peer.LastMessageTime),

		// Catchup-specific counters. The timestamps remain the generic
		// interaction ones; catchup-scoped timestamps are not tracked.
		CatchupAttempts:        peer.CatchupAttempts,
		CatchupSuccesses:       peer.CatchupSuccesses,
		CatchupFailures:        peer.CatchupFailures,
		CatchupLastAttempt:     timeToUnix(peer.LastInteractionAttempt),
		CatchupLastSuccess:     timeToUnix(peer.LastInteractionSuccess),
		CatchupLastFailure:     timeToUnix(peer.LastInteractionFailure),
		CatchupReputationScore: peer.ReputationScore,
		CatchupMaliciousCount:  peer.MaliciousCount,
		CatchupAvgResponseTime: peer.AvgResponseTimeMs,
		LastCatchupError:       peer.LastCatchupError,
		LastCatchupErrorTime:   timeToUnix(peer.LastCatchupErrorTime),
	}

	if peer.Legacy != nil {
		response.Legacy = &LegacyPeerResponse{
			Inbound:         peer.Legacy.Inbound,
			ProtocolVersion: peer.Legacy.ProtocolVersion,
			ServiceFlags:    peer.Legacy.ServiceFlags,
			PingMicros:      peer.Legacy.PingMicros,
			TimeOffsetSecs:  peer.Legacy.TimeOffsetSecs,
			StartingHeight:  peer.Legacy.StartingHeight,
			IsSyncPeer:      peer.Legacy.IsSyncPeer,
			TimeConnected:   timeToUnix(peer.Legacy.TimeConnected),
		}
	}

	return response
}

// GetPeers returns every peer in the centralized registry, of either transport.
// It reads the registry directly rather than through the p2p service: the
// registry keys legacy peers by a "legacy:host:port" string, which a libp2p
// peer.ID round trip would corrupt, and the p2p hop added no logic.
func (h *HTTP) GetPeers(c echo.Context) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), 5*time.Second)
	defer cancel()

	registry := h.repository.GetPeerRegistryClient()
	if registry == nil {
		h.logger.Errorf("[GetPeers] peer registry client not available")

		return c.JSON(http.StatusServiceUnavailable, PeersResponse{
			Peers: []PeerInfoResponse{},
			Count: 0,
		})
	}

	peers, err := registry.ListPeers(ctx, nil, 0, 0, false, false)
	if err != nil {
		h.logger.Errorf("[GetPeers] Failed to list peers: %v", err)

		return c.JSON(http.StatusInternalServerError, PeersResponse{
			Peers: []PeerInfoResponse{},
			Count: 0,
		})
	}

	peerResponses := make([]PeerInfoResponse, 0, len(peers))

	for _, peer := range peers {
		if peer == nil {
			continue
		}

		peerResponses = append(peerResponses, peerInfoToResponse(peer))
	}

	return c.JSON(http.StatusOK, PeersResponse{
		Peers: peerResponses,
		Count: len(peerResponses),
	})
}
