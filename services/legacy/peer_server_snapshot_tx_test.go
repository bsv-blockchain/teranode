package legacy

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// A real, fully-formed transaction. Its Bytes() round-trip through
// bsvutil.NewTxFromBytes, so it reaches the wire rather than failing to
// deserialize on the way — which is what makes it usable both as the
// must-be-served case and as the wrong-key case.
const completeTxHex = "010000000000000000ef01c0f6beed3f280acac9e3268b3a4b6cecac6160f84f750fdd2f8eac06284d960a000000006a47304402206b2782cc5b4a1d68d34f36df0241964bbc23eca0d2d8d698407429541993b063022016954b628894df8f6295097403148c3d7ae84097b538ab3c46cba2727f6deafd4121030ca32438b798eda7d8a818f108340a85bf77fefe24850979ac5dd7e15000ee1affffffff80746802000000001976a914f13bf914962276da063784e9e8b7ecbd59b20bf888ac0a002d3101000000001976a914954dede73fba730977b8630e3f7c93024b33795f88ac404b4c00000000001976a914e429e73ad33123c1a7248f660a162f0098fb819988ac80841e00000000001976a914df7974fdbb7890e0a608f923ef59112c475c078688ac80841e00000000001976a91422f9476db77bcad3998a9d4f96dbcaa2c9ef507288aca0860100000000001976a9143729fa58808bf6db6bf69e15adc96e0f20c26e6a88ac50c30000000000001976a91417accfc5f92836427c14299c51abbdbaedb791ce88ac204e0000000000001976a91462a4e3fab0ef92f1c130681aa657f8c858b59def88ac10270000000000001976a9149928c96c401b326f93043ce1434680ac502f487b88aca00a0000000000001976a9146ed6d5942deab79b654c1b31b86c3e62a7b5e61c88ac1528ab00000000001976a914239bae4bd2abf49a0a493b962cc0c027936b1b4788ac00000000"

// newTestServerForPushTx builds the minimum server/serverPeer pair that
// pushTxMsg touches, with a mocked UTXO store so a test can hand back the exact
// degenerate *bt.Tx the aerospike store produces for a transaction written from
// a UTXO-set snapshot. The sqlitememory store cannot produce that shape.
//
// serverPeer's embedded *peer.Peer is left nil: a transaction that passes the
// gate panics at QueueMessageWithEncoding, and pushTxMsg's own deferred recover
// swallows that and returns nil. So a nil error means "the gate passed it" and a
// non-nil error means "the gate rejected it", which is exactly the boundary
// under test.
func newTestServerForPushTx(t *testing.T) (*serverPeer, *utxo.MockUtxostore) {
	t.Helper()

	mockUtxoStore := &utxo.MockUtxostore{}

	s := &server{
		ctx:       context.Background(),
		logger:    ulogger.NewErrorTestLogger(t),
		utxoStore: mockUtxoStore,
	}

	return &serverPeer{server: s}, mockUtxoStore
}

// snapshotOutput returns a minimal spendable output.
func snapshotOutput(satoshis uint64) *bt.Output {
	return &bt.Output{Satoshis: satoshis, LockingScript: bscript.NewFromBytes([]byte{0x51})}
}

// TestPushTxMsg_RejectsSnapshotReconstruction covers the getdata path an
// unauthenticated peer reaches for any InvTypeTx it names. On a snapshot-seeded
// node the UTXO store hands back an outputs-only reconstruction, and turning that
// into bytes either panics inside go-bt (a nil output hole) or serializes cleanly
// into a short, 0-input transaction with the wrong txid — which is then queued to
// the peer as if it were the transaction that was asked for. Either way the peer
// must get an error, not silence and not fabricated bytes.
func TestPushTxMsg_RejectsSnapshotReconstruction(t *testing.T) {
	completeTx, err := bt.NewTxFromString(completeTxHex)
	require.NoError(t, err)

	tests := []struct {
		name string
		tx   *bt.Tx
	}{
		{
			// The .outputs-blob reconstruction with a spent output below the
			// live max index. Tx.Bytes() panics on this shape; the deferred
			// recover then swallows it and the peer gets silence.
			name: "nil output hole",
			tx: &bt.Tx{
				Outputs: []*bt.Output{nil, snapshotOutput(1000)},
			},
		},
		{
			// Every output still live: serializes cleanly, so nothing downstream
			// notices that the bytes do not hash to the requested txid.
			name: "fully live reconstruction, no nil hole",
			tx: &bt.Tx{
				Outputs: []*bt.Output{snapshotOutput(1000)},
			},
		},
		{
			// getTxFromBins allocates a non-nil empty input slice, which is
			// enough to make bt.Tx.IsExtended() report true.
			name: "hollow inline reconstruction, non-nil empty Inputs",
			tx: &bt.Tx{
				Inputs:  []*bt.Input{},
				Outputs: []*bt.Output{snapshotOutput(1000)},
			},
		},
		{
			// A complete, wire-valid transaction that is simply not the one the
			// peer named. Only a key-vs-value check catches this.
			name: "complete tx filed under the wrong key",
			tx:   completeTx,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp, mockUtxoStore := newTestServerForPushTx(t)

			requested, err := chainhash.NewHashFromStr("3333333333333333333333333333333333333333333333333333333333333333")
			require.NoError(t, err)

			mockUtxoStore.On("Get", mock.Anything, requested, mock.Anything).
				Return(&meta.Data{Tx: tt.tx}, nil)

			doneChan := make(chan struct{}, 1)

			pushErr := sp.server.pushTxMsg(sp, requested, doneChan, nil, wire.BaseEncoding)

			require.Error(t, pushErr, "a tx that is not the requested one must not be pushed to the peer")
			require.Len(t, doneChan, 1, "the caller must still be released on rejection")
		})
	}
}

// TestPushTxMsg_PushesAMatchingTx guards against over-rejection: a complete
// transaction whose txid matches what the peer asked for must still be served.
// A nil error here means the gate passed it — see newTestServerForPushTx.
func TestPushTxMsg_PushesAMatchingTx(t *testing.T) {
	sp, mockUtxoStore := newTestServerForPushTx(t)

	tx, err := bt.NewTxFromString(completeTxHex)
	require.NoError(t, err)

	requested := tx.TxIDChainHash()

	mockUtxoStore.On("Get", mock.Anything, requested, mock.Anything).
		Return(&meta.Data{Tx: tx}, nil)

	doneChan := make(chan struct{}, 1)

	pushErr := sp.server.pushTxMsg(sp, requested, doneChan, nil, wire.BaseEncoding)

	require.NoError(t, pushErr, "a matching tx must reach the queue, not be rejected by the gate")
	mockUtxoStore.AssertExpectations(t)
}
