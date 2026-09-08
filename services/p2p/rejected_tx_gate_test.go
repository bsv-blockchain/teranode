package p2p

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
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

// The remote exploit path: an attacker gets thousands of distinct junk
// transactions rejected per second. The validator forwards each one; the p2p
// egress must cap what reaches the mesh at the configured burst plus refill,
// not one publish per rejection.
func TestRejectedTxHandler_DistinctTxIDFloodIsRateLimited(t *testing.T) {
	s, published := countingPublishServer(t, nil, 0)
	s.rejectedTxEgress.setLimits(1, 5, 0, 0)

	handler := s.rejectedTxHandler(context.Background())

	const flood = 500
	for i := 0; i < flood; i++ {
		require.NoError(t, handler(internalRejection(t, distinctTxID(i))))
	}

	// The loop runs in well under a second; allow for at most one refill.
	require.GreaterOrEqual(t, *published, 5, "the burst must still be re-broadcast")
	require.LessOrEqual(t, *published, 6, "a flood of %d rejections must be capped at burst plus refill", flood)
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
	grant, _ := s.rejectedTxEgress.allow(testBlockHashHex, selfID, time.Now())
	require.NotNil(t, grant)

	sent, err := s.publishToNetwork(context.Background(), "not-a-known-topic", []byte("{}"))
	require.NoError(t, err)
	require.False(t, sent)
	require.Equal(t, 0, *published)

	grant.publishFailed()

	grant, _ = s.rejectedTxEgress.allow(testBlockHashHex, selfID, time.Now())
	require.NotNil(t, grant, "a returned grant must allow the txid to retry")
}

// There is no unbounded mode: a Server constructed without applyPeerMapLimits
// (as every direct-construction test does) still caps the egress at the
// package defaults.
func TestRejectedTxEgressGate_ZeroValueUsesDefaults(t *testing.T) {
	var g rejectedTxEgressGate

	now := time.Now()
	allowed := 0
	for i := 0; i < defaultRejectedTxPublishBurst*3; i++ {
		if grant, _ := g.allow(distinctTxID(i), "self", now); grant != nil {
			allowed++
		}
	}

	require.Equal(t, defaultRejectedTxPublishBurst, allowed, "an unconfigured gate must apply the default burst")

	// The bucket refills at the default rate.
	grant, why := g.allow(distinctTxID(10_000), "self", now.Add(time.Second))
	require.NotNil(t, grant, "tokens must refill over time: %s", why)
}

// Suppression reasons are distinguishable so operators can tell a duplicate
// storm from a distinct-txid flood.
func TestRejectedTxEgressGate_Reasons(t *testing.T) {
	var g rejectedTxEgressGate
	g.setLimits(1, 1, 0, 0)

	now := time.Now()

	grant, why := g.allow("a", "self", now)
	require.NotNil(t, grant)
	require.Empty(t, why)

	grant, why = g.allow("a", "self", now)
	require.Nil(t, grant)
	require.Equal(t, rejectedTxSuppressedDuplicate, why)

	grant, why = g.allow("b", "self", now)
	require.Nil(t, grant)
	require.Equal(t, rejectedTxSuppressedRateLimited, why)

	// A rate-limited txid was not marked announced: once a token is back it
	// goes out.
	grant, why = g.allow("b", "self", now.Add(2*time.Second))
	require.NotNil(t, grant, why)
}

// A refused reservation must not debit the bucket: refusing txids while the
// bucket is empty does not push the refill further out.
func TestRejectedTxEgressGate_RefusalDoesNotDebitBucket(t *testing.T) {
	var g rejectedTxEgressGate
	g.setLimits(1, 1, 0, 0)

	now := time.Now()

	grant, _ := g.allow("a", "self", now)
	require.NotNil(t, grant)

	for i := 0; i < 100; i++ {
		grant, why := g.allow(distinctTxID(i), "self", now)
		require.Nil(t, grant)
		require.Equal(t, rejectedTxSuppressedRateLimited, why)
	}

	grant, why := g.allow("b", "self", now.Add(time.Second))
	require.NotNil(t, grant, "one refill later exactly one token must be back: %s", why)
}

// Both charges come back when the publish did not happen: the dedup slot and
// the rate token. With a burst of one, the second txid can only be granted if
// the first grant's token was refunded.
func TestRejectedTxEgressGate_PublishFailedReturnsToken(t *testing.T) {
	var g rejectedTxEgressGate
	g.setLimits(1, 1, 0, 0)

	now := time.Now()

	grant, _ := g.allow("a", "self", now)
	require.NotNil(t, grant)

	refused, why := g.allow("b", "self", now)
	require.Nil(t, refused)
	require.Equal(t, rejectedTxSuppressedRateLimited, why)

	grant.publishFailed()

	granted, why := g.allow("b", "self", now)
	require.NotNil(t, granted, "the failed publish's token must be back: %s", why)
}

// Pins the dedup period the gate documents: with this node as the single
// announcer a repeated txid is refused inside the publish window and again on
// the first rollover (last window's grantee is withheld the retry grant), and
// granted only after the second rollover, about every two windows.
func TestRejectedTxEgressGate_RepeatPeriodIsTwoPublishWindows(t *testing.T) {
	var g rejectedTxEgressGate
	g.setLimits(100, 100, 0, 10*time.Minute)

	start := time.Now()

	grant, _ := g.allow("a", "self", start)
	require.NotNil(t, grant, "first rejection is re-broadcast")

	grant, why := g.allow("a", "self", start.Add(seenHashPublishWindow/2))
	require.Nil(t, grant, "repeat inside the window is a duplicate")
	require.Equal(t, rejectedTxSuppressedDuplicate, why)

	grant, why = g.allow("a", "self", start.Add(seenHashPublishWindow))
	require.Nil(t, grant, "first rollover withholds the retry grant from last window's grantee")
	require.Equal(t, rejectedTxSuppressedDuplicate, why)

	grant, _ = g.allow("a", "self", start.Add(2*seenHashPublishWindow))
	require.NotNil(t, grant, "second rollover re-broadcasts the repeat")

	grant, _ = g.allow("a", "self", start.Add(3*seenHashPublishWindow))
	require.Nil(t, grant, "and the cycle repeats")
}

// Non-positive limits select the defaults rather than disabling the gate.
func TestRejectedTxEgressGate_NonPositiveLimitsSelectDefaults(t *testing.T) {
	var g rejectedTxEgressGate
	g.setLimits(0, -1, 0, 0)

	require.Equal(t, float64(defaultRejectedTxPublishRate), g.bucket.perSecond)
	require.Equal(t, float64(defaultRejectedTxPublishBurst), g.bucket.burst)
}

// The reason the validator now sends is a bounded code list; make sure it is
// clean under our own ingress validation and well under the display bound so
// the truncation path is no longer the normal case.
func TestRejectedTxMessage_CodeReasonPassesIngressBounds(t *testing.T) {
	msg := RejectedTxMessage{
		TxID:   testBlockHashHex,
		Reason: strings.Repeat("TX_INVALID/", 3) + "PROCESSING",
		PeerID: mustNewPeerID(t).String(),
	}

	before := msg.Reason
	msg.sanitizeFields()
	require.Equal(t, before, msg.Reason, "a code list must not need truncation")
	require.NoError(t, msg.validateFields())
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
