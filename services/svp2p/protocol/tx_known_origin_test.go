package protocol

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// barrierPingPong sends a ping and waits for its matching pong, so a test
// can be sure everything the peer already wrote to the wire (a verack, a
// tx) has been fully processed on the manager's side before proceeding —
// the same technique relayTestFixture.connect and manager_test.go's
// connectTx already use for exactly this reason.
func barrierPingPong(t *testing.T, sp *scriptedPeer) {
	t.Helper()

	nonce := uint64(time.Now().UnixNano()) //nolint:gosec // test-only nonce
	sp.write(t, wire.NewMsgPing(nonce))

	pong, ok := sp.readUntil(t, wire.CmdPong).(*wire.MsgPong)
	require.True(t, ok)
	require.Equal(t, nonce, pong.Nonce)
}

// TestPeerManager_DoesNotAnnounceTxBackToItsSender is Important 2's TDD
// test: legacy's OnTx marks the delivering peer's known-inventory set
// BEFORE queueing the tx (services/legacy/peer_server.go:906-908,
// AddKnownInventory ahead of QueueTx), precisely so the relay never
// re-announces a tx back to the peer that just sent it. This is the tx-side
// counterpart of PeerManager.Inv's unconditional knownBlocks.mark for block
// invs (manager.go:800-806).
//
// Paired with a positive-arrival barrier (F4): a second, unrelated
// connection ("other") must still receive the inv for the same hash,
// proving RelayTxs is alive and this hash is genuinely relayable — the
// absence of an inv on the sender's connection is the suppression working,
// not merely nothing having run.
func TestPeerManager_DoesNotAnnounceTxBackToItsSender(t *testing.T) {
	m := newTestManager(t, nil)

	idx, err := NewHeaderIndex(syncGenesis())
	require.NoError(t, err)

	ing := newBlockingTxIngestor(TxIngestOutcome{Accepted: true})
	close(ing.release)

	require.NoError(t, m.ConfigureSync(SyncConfig{
		Index:      idx,
		TxIngestor: ing,
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, m.Start(ctx, []string{"127.0.0.1:0"}))
	t.Cleanup(func() { _ = m.Stop() })

	sender := dialScripted(t, m.ListenAddrs()[0])
	sender.completeOutboundHandshake(t)
	barrierPingPong(t, sender)

	other := dialScripted(t, m.ListenAddrs()[0])
	other.completeOutboundHandshake(t)
	barrierPingPong(t, other)

	tx := wire.NewMsgTx(1)
	hash := tx.TxHash()
	sender.write(t, tx)

	// By the time the ingestor has been called, dispatchTx's mark (which
	// runs synchronously, ahead of queueTx, on the same call) has already
	// happened — see dispatchTx's own doc comment on ordering.
	require.Eventually(t, func() bool {
		return ing.callCount() > 0
	}, 2*time.Second, 10*time.Millisecond, "the tx never reached the ingestor")

	m.RelayTxs([]TxHashAndFee{{TxHash: hash, Fee: 1000, Size: 250}})

	// The barrier: "other" must receive the inv.
	invMsg := other.readUntil(t, wire.CmdInv)
	inv, ok := invMsg.(*wire.MsgInv)
	require.True(t, ok)
	require.Len(t, inv.InvList, 1)
	require.Equal(t, hash, inv.InvList[0].Hash)

	// The sender must never be handed this as an inv: it delivered the tx
	// itself. Drain whatever else arrives (pings) under a short deadline and
	// confirm no inv for this hash — or any inv at all — shows up.
	require.NoError(t, sender.nc.SetReadDeadline(time.Now().Add(300*time.Millisecond)))

	for {
		_, msg, _, err := wire.ReadMessageWithEncodingN(sender.nc, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
		if err != nil {
			break // deadline reached: nothing else arrived, as expected.
		}

		require.NotEqual(t, wire.CmdInv, msg.Command(), "the sending peer must never be announced its own tx")
	}
}
