package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/rejectedtx"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// countingPublishServer is capturePublishServer with a publish counter, for
// tests that care how many messages reached the network rather than which.
// publishErr, when non-nil, is returned by the mock for the first failFirstN
// publishes so the retry path can be exercised.
func countingPublishServer(t *testing.T, publishErr error, failFirstN int) (*Server, *int) {
	t.Helper()

	count := 0
	mockP2P := &MockServerP2PClient{peerID: mustNewPeerID(t)}

	if publishErr != nil && failFirstN > 0 {
		mockP2P.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(publishErr).Times(failFirstN)
	}

	mockP2P.On("Publish", mock.Anything, mock.Anything, mock.Anything).Run(func(_ mock.Arguments) {
		count++
	}).Return(nil)

	s := &Server{
		logger:              &ulogger.TestLogger{},
		settings:            &settings.Settings{P2P: settings.P2PSettings{ListenMode: settings.ListenModeFull}},
		P2PClient:           mockP2P,
		rejectedTxTopicName: "test-rejected",
	}

	return s, &count
}

func internalRejection(t *testing.T, txID string) *kafka.KafkaMessage {
	t.Helper()

	value, err := proto.Marshal(&kafkamessage.KafkaRejectedTxTopicMessage{
		TxHash: txID,
		PeerId: "",
		Reason: "TX_INVALID",
	})
	require.NoError(t, err)

	return &kafka.KafkaMessage{Value: value}
}

func distinctTxID(i int) string {
	return fmt.Sprintf("%064x", i+1)
}

// fakeClock drives a gate on a synthetic timeline so no assertion depends on
// how fast the test host runs.
type fakeClock struct {
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) read() time.Time { return c.now }

func (c *fakeClock) advance(d time.Duration) { c.now = c.now.Add(d) }

// gateWithClock returns a zero-value gate on a fake clock.
func gateWithClock() (*rejectedTxEgressGate, *fakeClock) {
	clock := newFakeClock()

	return &rejectedTxEgressGate{clock: clock.read}, clock
}

// The remote exploit path: an attacker gets thousands of distinct junk
// transactions rejected per second. The validator forwards each one; the p2p
// egress must cap what reaches the mesh at the configured burst, not one
// publish per rejection. The gate runs on a frozen clock so the cap is exact.
func TestRejectedTxHandler_DistinctTxIDFloodIsRateLimited(t *testing.T) {
	s, published := countingPublishServer(t, nil, 0)
	s.rejectedTxEgress.setLimits(1, 5, 0, 0)
	s.rejectedTxEgress.clock = newFakeClock().read

	handler := s.rejectedTxHandler(context.Background())

	const flood = 500
	for i := 0; i < flood; i++ {
		require.NoError(t, handler(internalRejection(t, distinctTxID(i))))
	}

	require.Equal(t, 5, *published, "a flood of %d rejections must be capped at the burst", flood)
}

// A repeated txid is re-broadcast once per publish window, and a repeat never
// consumes a rate-limit token that a fresh txid could have used.
func TestRejectedTxHandler_RepeatedTxIDPublishedOnce(t *testing.T) {
	s, published := countingPublishServer(t, nil, 0)
	s.rejectedTxEgress.setLimits(1, 2, 0, 0)

	handler := s.rejectedTxHandler(context.Background())

	for i := 0; i < 10; i++ {
		require.NoError(t, handler(internalRejection(t, testBlockHashHex)))
	}
	require.Equal(t, 1, *published, "a repeated txid must be re-broadcast once")

	require.NoError(t, handler(internalRejection(t, distinctTxID(1))))
	require.Equal(t, 2, *published, "repeats must not have spent the second burst token")
}

// A re-broadcast the network refused must not leave the txid marked as
// announced, and must not keep the rate token: the next rejection of the same
// txid retries, and with a burst of one it can only do so if the failed
// attempt's token came back.
func TestRejectedTxHandler_PublishFailureAllowsRetry(t *testing.T) {
	s, published := countingPublishServer(t, errors.NewServiceError("mesh unavailable"), 1)
	s.rejectedTxEgress.setLimits(1, 1, 0, 0)

	handler := s.rejectedTxHandler(context.Background())

	require.NoError(t, handler(internalRejection(t, testBlockHashHex)))
	require.Equal(t, 0, *published, "first publish fails")

	require.NoError(t, handler(internalRejection(t, testBlockHashHex)))
	require.Equal(t, 1, *published, "the retry must not be suppressed as a duplicate or rate limited")

	require.NoError(t, handler(internalRejection(t, testBlockHashHex)))
	require.Equal(t, 1, *published, "once published the txid is deduplicated")
}

// A message the publish gate drops (silent listen mode reached via the
// chokepoint rather than the handler pre-check) must also return the grant.
func TestRejectedTxHandler_GateDropReturnsDedupGrant(t *testing.T) {
	s, published := countingPublishServer(t, nil, 0)

	// Silent mode at the handler pre-check returns before the gate, so drive
	// the chokepoint directly with a message the gate will refuse: unknown topic.
	selfID := s.P2PClient.GetID()
	grant, _ := s.rejectedTxEgress.allow(testBlockHashHex, selfID)
	require.NotNil(t, grant)

	sent, err := s.publishToNetwork(context.Background(), "not-a-known-topic", []byte("{}"))
	require.NoError(t, err)
	require.False(t, sent)
	require.Equal(t, 0, *published)

	grant.publishFailed()

	grant, _ = s.rejectedTxEgress.allow(testBlockHashHex, selfID)
	require.NotNil(t, grant, "a returned grant must allow the txid to retry")
}

// There is no unbounded mode: a Server constructed without applyPeerMapLimits
// (as every direct-construction test does) still caps the egress at the
// package defaults.
func TestRejectedTxEgressGate_ZeroValueUsesDefaults(t *testing.T) {
	g, clock := gateWithClock()

	allowed := 0
	for i := 0; i < defaultRejectedTxPublishBurst*3; i++ {
		if grant, _ := g.allow(distinctTxID(i), "self"); grant != nil {
			allowed++
		}
	}

	require.Equal(t, defaultRejectedTxPublishBurst, allowed, "an unconfigured gate must apply the default burst")

	// The bucket refills at the default rate.
	clock.advance(time.Second)
	grant, why := g.allow(distinctTxID(10_000), "self")
	require.NotNil(t, grant, "tokens must refill over time: %s", why)
}

// Suppression reasons are distinguishable so operators can tell a duplicate
// storm from a distinct-txid flood.
func TestRejectedTxEgressGate_Reasons(t *testing.T) {
	g, clock := gateWithClock()
	g.setLimits(1, 1, 0, 0)

	grant, why := g.allow("a", "self")
	require.NotNil(t, grant)
	require.Empty(t, why)

	grant, why = g.allow("a", "self")
	require.Nil(t, grant)
	require.Equal(t, rejectedTxSuppressedDuplicate, why)

	grant, why = g.allow("b", "self")
	require.Nil(t, grant)
	require.Equal(t, rejectedTxSuppressedRateLimited, why)

	// A rate-limited txid was not marked announced: once a token is back it
	// goes out.
	clock.advance(2 * time.Second)
	grant, why = g.allow("b", "self")
	require.NotNil(t, grant, why)
}

// A refused reservation must not debit the bucket: refusing txids while the
// bucket is empty does not push the refill further out.
func TestRejectedTxEgressGate_RefusalDoesNotDebitBucket(t *testing.T) {
	g, clock := gateWithClock()
	g.setLimits(1, 1, 0, 0)

	grant, _ := g.allow("a", "self")
	require.NotNil(t, grant)

	for i := 0; i < 100; i++ {
		grant, why := g.allow(distinctTxID(i), "self")
		require.Nil(t, grant)
		require.Equal(t, rejectedTxSuppressedRateLimited, why)
	}

	clock.advance(time.Second)
	grant, why := g.allow("b", "self")
	require.NotNil(t, grant, "one refill later exactly one token must be back: %s", why)
}

// Both charges come back when the publish did not happen: the dedup slot and
// the rate token. With a burst of one, the second txid can only be granted if
// the first grant's token was refunded.
func TestRejectedTxEgressGate_PublishFailedReturnsToken(t *testing.T) {
	g, _ := gateWithClock()
	g.setLimits(1, 1, 0, 0)

	grant, _ := g.allow("a", "self")
	require.NotNil(t, grant)

	refused, why := g.allow("b", "self")
	require.Nil(t, refused)
	require.Equal(t, rejectedTxSuppressedRateLimited, why)

	grant.publishFailed()

	granted, why := g.allow("b", "self")
	require.NotNil(t, granted, "the failed publish's token must be back: %s", why)
}

// publishFailed reads the gate's clock, not the wall clock: on a synthetic
// timeline a refund must not rewind the bucket or pin it to the present.
func TestRejectedTxEgressGate_PublishFailedUsesGateClock(t *testing.T) {
	g, clock := gateWithClock()
	g.setLimits(1, 1, 0, 0)

	grant, _ := g.allow("a", "self")
	require.NotNil(t, grant)
	grant.publishFailed()

	require.Equal(t, clock.now, g.bucket.last, "the refund must be stamped with the gate clock")

	granted, _ := g.allow("b", "self")
	require.NotNil(t, granted)

	clock.advance(time.Second)
	granted, why := g.allow("c", "self")
	require.NotNil(t, granted, "the synthetic timeline must still refill: %s", why)
}

// Pins the dedup period the gate documents: with this node as the single
// announcer a repeated txid is refused inside the publish window and again on
// the first rollover (last window's grantee is withheld the retry grant), and
// granted only after the second rollover, about every two windows.
func TestRejectedTxEgressGate_RepeatPeriodIsTwoPublishWindows(t *testing.T) {
	g, clock := gateWithClock()
	g.setLimits(100, 100, 0, 10*time.Minute)

	grant, _ := g.allow("a", "self")
	require.NotNil(t, grant, "first rejection is re-broadcast")

	clock.advance(seenHashPublishWindow / 2)
	grant, why := g.allow("a", "self")
	require.Nil(t, grant, "repeat inside the window is a duplicate")
	require.Equal(t, rejectedTxSuppressedDuplicate, why)

	clock.advance(seenHashPublishWindow / 2)
	grant, why = g.allow("a", "self")
	require.Nil(t, grant, "first rollover withholds the retry grant from last window's grantee")
	require.Equal(t, rejectedTxSuppressedDuplicate, why)

	clock.advance(seenHashPublishWindow)
	grant, _ = g.allow("a", "self")
	require.NotNil(t, grant, "second rollover re-broadcasts the repeat")

	clock.advance(seenHashPublishWindow)
	grant, _ = g.allow("a", "self")
	require.Nil(t, grant, "and the cycle repeats")
}

// A seen-hash TTL below the publish window must not switch the dedup off: the
// cache clamps it to the window, so a txid rejected every few seconds is still
// suppressed rather than re-published on every repeat.
func TestRejectedTxEgressGate_SubWindowTTLStillDeduplicates(t *testing.T) {
	g, clock := gateWithClock()
	g.setLimits(100, 100, 0, 5*time.Second)

	grant, _ := g.allow("a", "self")
	require.NotNil(t, grant)

	granted := 0
	for i := 0; i < 20; i++ {
		clock.advance(6 * time.Second)

		if grant, _ := g.allow("a", "self"); grant != nil {
			granted++
		}
	}

	require.LessOrEqual(t, granted, 8, "with a clamped TTL of one window a repeat every 6s is granted at most once per window, not 20 times")
	require.Positive(t, granted, "the clamp must not suppress forever either")
}

// clear drops the dedup state and keeps the bucket, so a Stop/Start cycle
// cannot be used to widen the burst.
func TestRejectedTxEgressGate_ClearKeepsBucket(t *testing.T) {
	g, _ := gateWithClock()
	g.setLimits(1, 2, 0, 0)

	grant, _ := g.allow("a", "self")
	require.NotNil(t, grant)
	require.Equal(t, 1, g.size())

	g.clear()
	require.Equal(t, 0, g.size(), "dedup state is dropped")

	grant, _ = g.allow("a", "self")
	require.NotNil(t, grant, "after clear the txid is fresh again")

	grant, why := g.allow("b", "self")
	require.Nil(t, grant, "the bucket must not have been refilled by clear: %s", why)
	require.Equal(t, rejectedTxSuppressedRateLimited, why)
}

// The reason the validator sends is held to util/rejectedtx's grammar; every
// shape it can produce, including the detail-carrying one with its colon,
// space and hyphens, must pass this node's own ingress sanitizer unchanged and
// sit well under the display bound, so truncation is never the normal case.
func TestRejectedTxMessage_ReasonShapesPassIngressBounds(t *testing.T) {
	reasons := make([]string, 0, 2+len(rejectedtx.Details()))
	reasons = append(reasons, "TX_INVALID", "TX_INVALID/PROCESSING/UTXO_FROZEN/UTXO_NON_FINAL")

	for _, detail := range rejectedtx.Details() {
		require.Equal(t, detail, sanitizePeerDisplayString(detail, maxGossipReasonLen), "detail %q must survive the sanitizer", detail)

		reasons = append(reasons, "TX_INVALID/TX_POLICY: "+detail)
	}

	for _, reason := range reasons {
		msg := RejectedTxMessage{
			TxID:   testBlockHashHex,
			Reason: reason,
			PeerID: mustNewPeerID(t).String(),
		}

		msg.sanitizeFields()
		require.Equal(t, reason, msg.Reason, "reason %q must not be altered by the sanitizer", reason)
		require.NoError(t, msg.validateFields())
		require.True(t, rejectedtx.Valid(msg.Reason), "reason %q must still fit the grammar after sanitizing", reason)
	}
}

// The reason grammar is enforced at this chokepoint, not only at the producer:
// a pre-upgrade validator's err.Error() on the in-cluster topic leaves the
// node as the fallback code, while a well-formed reason passes through intact.
func TestRejectedTxHandler_NormalizesReasonAtChokepoint(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"pre-upgrade validator error text", "TX_INVALID (30): GoBDK fail to ValidateTransaction -> TX_INVALID (30): script failed: OP_RETURN <attacker blob>", "TX_INVALID"},
		{"unlisted detail", "TX_INVALID: something the producer made up", "TX_INVALID"},
		{"well-formed code list", "TX_INVALID/TX_POLICY", "TX_INVALID/TX_POLICY"},
		{"well-formed detail", "TX_INVALID: bad-txns-in-belowout", "TX_INVALID: bad-txns-in-belowout"},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, published := capturePublishServer(t)

			value, err := proto.Marshal(&kafkamessage.KafkaRejectedTxTopicMessage{TxHash: distinctTxID(i), Reason: tc.in})
			require.NoError(t, err)

			require.NoError(t, s.rejectedTxHandler(context.Background())(&kafka.KafkaMessage{Value: value}))

			var msg RejectedTxMessage
			require.NoError(t, json.Unmarshal(published["test-rejected"], &msg))
			require.Equal(t, tc.want, msg.Reason)
		})
	}
}

// External rejections (a peer ID on the Kafka message) return before the gate,
// so what other nodes announce can never drain this node's own egress budget.
func TestRejectedTxHandler_ExternalRejectionsDoNotTouchTheGate(t *testing.T) {
	s, published := countingPublishServer(t, nil, 0)
	s.rejectedTxEgress.setLimits(1, 1, 0, 0)
	s.rejectedTxEgress.clock = newFakeClock().read

	handler := s.rejectedTxHandler(context.Background())

	for i := 0; i < 50; i++ {
		value, err := proto.Marshal(&kafkamessage.KafkaRejectedTxTopicMessage{
			TxHash: distinctTxID(i),
			PeerId: "12D3KooWSomeOtherPeer",
			Reason: "TX_INVALID",
		})
		require.NoError(t, err)
		require.NoError(t, handler(&kafka.KafkaMessage{Value: value}))
	}

	require.Equal(t, 0, *published, "external rejections are never re-broadcast")
	require.Equal(t, 0, s.rejectedTxEgress.size(), "external rejections must not enter the dedup")

	require.NoError(t, handler(internalRejection(t, distinctTxID(1_000))))
	require.Equal(t, 1, *published, "the single burst token must still be available to our own rejection")
}

// Guards the settings-to-gate path end to end, the same way
// TestApplyPeerMapLimits_ConfiguresSeenHashCaches guards the seen-hash keys:
// the two p2p_rejected_tx_publish_* keys do something only because
// applyPeerMapLimits hands them to the gate.
func TestApplyPeerMapLimits_ConfiguresRejectedTxEgress(t *testing.T) {
	s := &Server{logger: ulogger.TestLogger{}}
	s.applyPeerMapLimits(&settings.Settings{P2P: settings.P2PSettings{
		RejectedTxPublishRate:  1,
		RejectedTxPublishBurst: 2,
		SeenHashMaxSize:        5,
		SeenHashTTL:            time.Minute,
	}})

	clock := newFakeClock()
	s.rejectedTxEgress.clock = clock.read

	granted := 0
	for i := 0; i < 8; i++ {
		if grant, _ := s.rejectedTxEgress.allow(distinctTxID(i), "self"); grant != nil {
			granted++
		}
	}
	require.Equal(t, 2, granted, "the configured burst must be in force")
	require.Equal(t, 5, s.rejectedTxEgress.size(), "the configured seen-hash size cap must be in force on the dedup")

	clock.advance(time.Second)
	grant, why := s.rejectedTxEgress.allow(distinctTxID(100), "self")
	require.NotNil(t, grant, "the configured rate must be in force: one token per second: %s", why)

	clock.advance(time.Minute)
	grant, why = s.rejectedTxEgress.allow(distinctTxID(0), "self")
	require.NotNil(t, grant, "the configured TTL must be in force: the entry expired after one minute: %s", why)
}

// The dedup cache is swept with the sibling seen-hash caches, so a
// distinct-txid flood does not leave it pinned at its size cap for the process
// lifetime.
func TestCleanupPeerMaps_SweepsRejectedTxDedup(t *testing.T) {
	s := &Server{logger: ulogger.TestLogger{}}
	s.rejectedTxEgress.setLimits(100, 100, 0, time.Minute)

	// Entries stamped in the past, so the sweep's wall-clock now is past their TTL.
	s.rejectedTxEgress.clock = func() time.Time { return time.Now().Add(-2 * time.Minute) }

	for i := 0; i < 10; i++ {
		grant, _ := s.rejectedTxEgress.allow(distinctTxID(i), "self")
		require.NotNil(t, grant)
	}
	require.Equal(t, 10, s.rejectedTxEgress.size())

	s.cleanupPeerMaps()

	require.Equal(t, 0, s.rejectedTxEgress.size(), "expired dedup entries must be released by the sweep")
}

// A refund never lifts the bucket above burst and time never runs backwards:
// a full bucket refunded stays at burst, and an earlier now neither refills
// nor rewinds.
func TestTokenBucket_RefundCapsAtBurstAndTimeIsMonotonic(t *testing.T) {
	b := newTokenBucket(1, 2)
	now := time.Now()

	b.refund(now)
	require.Equal(t, float64(2), b.tokens, "refund into a full bucket is lost")

	require.True(t, b.take(now))
	require.True(t, b.take(now))
	require.False(t, b.take(now), "bucket empty")

	require.False(t, b.take(now.Add(-time.Hour)), "an earlier now must not mint tokens")
	require.True(t, b.take(now.Add(time.Second)), "one second refills one token")
	require.False(t, b.take(now.Add(time.Second)))
}
