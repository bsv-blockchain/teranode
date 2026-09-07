package protocol

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// TestConfigureSync_WiresTxIngestorToPeers is the plumbing proof for the
// TxIngestor seam through the real manager stack: ConfigureSync stores it
// (independently of Ingestor/SyncEnabled — see ConfigureSync's own comment),
// runPeer reads it back out, and a genuinely dialed connection's inbound tx
// reaches it. Block sync is deliberately left unconfigured (no Ingestor) to
// prove TxIngestor does not depend on it.
func TestConfigureSync_WiresTxIngestorToPeers(t *testing.T) {
	m := newTestManager(t, nil)

	idx, err := NewHeaderIndex(syncGenesis())
	require.NoError(t, err)

	ing := newBlockingTxIngestor(TxIngestOutcome{Accepted: true})
	close(ing.release)

	require.NoError(t, m.ConfigureSync(SyncConfig{
		Index:      idx,
		TxIngestor: ing,
	}))
	require.False(t, m.SyncEnabled(), "test setup: no Ingestor means block sync stays off")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, m.Start(ctx, []string{"127.0.0.1:0"}))
	t.Cleanup(func() { _ = m.Stop() })

	far := dialScripted(t, m.ListenAddrs()[0])
	far.completeOutboundHandshake(t)

	far.write(t, wire.NewMsgTx(1))

	require.Eventually(t, func() bool {
		return ing.callCount() > 0
	}, 2*time.Second, 10*time.Millisecond, "the dialed peer's inbound tx never reached the configured TxIngestor")
}
