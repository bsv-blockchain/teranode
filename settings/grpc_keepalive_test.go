package settings

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A client keepalive interval equal to the server's minimum ping interval is
// a boundary race: a ping that lands a few milliseconds early is "too many
// pings" and the server answers GOAWAY ENHANCE_YOUR_CALM. The defaults must
// keep the server floor strictly below the client cadence.
func TestGRPCKeepaliveDefaults_ServerMinPingBelowClientInterval(t *testing.T) {
	s := NewSettings()

	require.Less(t, s.GRPC.ServerMinPingTime, s.GRPC.KeepaliveTime,
		"grpc_server_min_ping_time_seconds must be strictly below grpc_keepalive_time_seconds")
	require.Positive(t, s.GRPC.ServerMinPingTime)
}
