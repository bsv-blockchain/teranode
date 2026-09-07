package svp2p

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// newMultistreamServer is newIntegrationServer with the node's logger under
// the test's control, so the two halves of the stream setup can be read back
// from what the production code already logs.
func newMultistreamServer(t *testing.T, role string, connectTo []string, logger ulogger.Logger) *Server {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Context = tSettings.Context + "-" + role
	tSettings.Legacy.ListenAddresses = []string{"127.0.0.1:0"}
	tSettings.Legacy.GRPCListenAddress = svp2ptest.FreePort(t)
	tSettings.Legacy.WorkingDir = t.TempDir()
	tSettings.Legacy.ConnectPeers = connectTo
	tSettings.GRPCAdminAPIKey = "test-admin-key"

	require.True(t, tSettings.Legacy.AllowBlockPriority, "legacy_allowBlockPriority must default on")

	store, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	return New(logger, tSettings, blockchainClient)
}

// holdsOneDataStream reports whether the registry holds exactly one
// association and that association carries both GENERAL and DATA1.
func holdsOneDataStream(associations map[string][]wire.StreamType) bool {
	if len(associations) != 1 {
		return false
	}

	for _, streams := range associations {
		general, data1 := false, false

		for _, t := range streams {
			switch t {
			case wire.StreamTypeGeneral:
				general = true
			case wire.StreamTypeData1:
				data1 = true
			}
		}

		return general && data1 && len(streams) == 2
	}

	return false
}

// Two whole svp2p stacks negotiate BlockPriority and open DATA1 between them:
// the dialling side runs association.cpp:111-129 OpenRequiredStreams and the
// accepting side runs net.cpp:3188-3240 MoveStream, and the connection the
// second dial made ends up as a stream of the same association on both.
func TestIntegrationTwoServersOpenADataStream(t *testing.T) {
	ctx := context.Background()

	logA := &svp2ptest.RecordingLogger{}

	serverA := newMultistreamServer(t, "msa", nil, logA)
	cancelA := startServer(t, serverA)

	defer cancelA()
	defer func() { require.NoError(t, serverA.Stop(ctx)) }()

	listenAddrs := serverA.manager.ListenAddrs()
	require.Len(t, listenAddrs, 1)

	logB := &svp2ptest.RecordingLogger{}

	serverB := newMultistreamServer(t, "msb", []string{listenAddrs[0]}, logB)
	cancelB := startServer(t, serverB)

	defer cancelB()
	defer func() { require.NoError(t, serverB.Stop(ctx)) }()

	require.Eventually(t, func() bool { return len(logB.Matching("opened to")) > 0 },
		20*time.Second, 100*time.Millisecond, "the dialling node never opened DATA1")

	require.Eventually(t, func() bool { return len(logA.Matching("moved from")) > 0 },
		20*time.Second, 100*time.Millisecond, "the accepting node never moved the stream in")

	opened := logB.Matching("opened to")
	require.Len(t, opened, 1, "exactly one DATA1 stream per association")
	require.Contains(t, opened[0], "under policy BlockPriority")

	moved := logA.Matching("moved from")
	require.Len(t, moved, 1)

	// The registry, not the log: a reworded log line must not be able to make
	// this test pass or fail on its own.
	require.Eventually(t, func() bool { return holdsOneDataStream(serverB.manager.AssociationStreams()) },
		10*time.Second, 100*time.Millisecond, "the dialling node holds no DATA1 stream")

	require.Eventually(t, func() bool { return holdsOneDataStream(serverA.manager.AssociationStreams()) },
		10*time.Second, 100*time.Millisecond, "the accepting node holds no DATA1 stream")

	// Neither side may count the DATA1 connection as a second peer.
	require.Never(t, func() bool {
		return serverA.manager.ConnectedCount() != 1 || serverB.manager.ConnectedCount() != 1
	}, 3*time.Second, 200*time.Millisecond)
}
