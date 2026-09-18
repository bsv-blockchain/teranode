package blockchain

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// newTestBlockchainForNotifications builds a minimal Blockchain sufficient to
// exercise SendNotification's validation and channel-send logic directly,
// without a running gRPC server or store.
func newTestBlockchainForNotifications(t *testing.T, bufferSize int) *Blockchain {
	t.Helper()

	initPrometheusMetrics()

	return &Blockchain{
		logger:        ulogger.TestLogger{},
		notifications: make(chan *blockchain_api.Notification, bufferSize),
	}
}

func TestSendNotification_Validation(t *testing.T) {
	ctx := context.Background()
	validHash := make([]byte, chainhash.HashSize)

	t.Run("nil request is rejected", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, nil)
		require.Error(t, err)
	})

	t.Run("unrecognized type is rejected", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type: model.NotificationType(999),
			Hash: validHash,
		})
		require.Error(t, err)
	})

	t.Run("PING with a hash is rejected", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type: model.NotificationType_PING,
			Hash: validHash,
		})
		require.Error(t, err)
	})

	t.Run("non-PING with an empty hash is rejected", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type: model.NotificationType_Block,
		})
		require.Error(t, err)
	})

	t.Run("non-PING with a short hash is rejected", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type: model.NotificationType_Block,
			Hash: []byte{1, 2, 3},
		})
		require.Error(t, err)
	})

	t.Run("valid PING notification is accepted", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type: model.NotificationType_PING,
		})
		require.NoError(t, err)
	})

	t.Run("valid Block notification is accepted", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type: model.NotificationType_Block,
			Hash: validHash,
		})
		require.NoError(t, err)
	})
}

func TestSendNotification_BackpressureAndCancellation(t *testing.T) {
	b := newTestBlockchainForNotifications(t, 1)
	notification := &blockchain_api.Notification{Type: model.NotificationType_Block, Hash: make([]byte, chainhash.HashSize)}
	_, err := b.SendNotification(context.Background(), notification)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = b.SendNotification(ctx, notification)
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	require.Len(t, b.notifications, 1)

	done := make(chan error, 1)
	go func() { _, err := b.SendNotification(context.Background(), notification); done <- err }()
	<-b.notifications
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("notification did not resume after the queue drained")
	}
	require.Same(t, notification, <-b.notifications)
}

func TestSendNotification_PINGMayDrop(t *testing.T) {
	b := newTestBlockchainForNotifications(t, 1)
	ping := &blockchain_api.Notification{Type: model.NotificationType_PING}
	for i := 0; i < 2; i++ {
		_, err := b.SendNotification(context.Background(), ping)
		require.NoError(t, err)
	}
	require.Len(t, b.notifications, 1)
}

func TestReportPeerFailure_LongReasonStillNotifies(t *testing.T) {
	b := newTestBlockchainForNotifications(t, 1)
	_, err := b.ReportPeerFailure(context.Background(), &blockchain_api.ReportPeerFailureRequest{
		PeerId: "peer", FailureType: "catchup", Hash: make([]byte, chainhash.HashSize), Reason: strings.Repeat("界", 200),
	})
	require.NoError(t, err)
	notification := <-b.notifications
	require.Equal(t, "catchup", notification.Metadata.Metadata["failure_type"])
	reason := notification.Metadata.Metadata["reason"]
	require.LessOrEqual(t, len(reason), maxNotificationMetadataFieldLen)
	require.True(t, utf8.ValidString(reason))
}

func TestSendNotification_PayloadBounds(t *testing.T) {
	ctx := context.Background()
	validHash := make([]byte, chainhash.HashSize)

	t.Run("oversized base_URL is rejected", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type:     model.NotificationType_Block,
			Hash:     validHash,
			Base_URL: strings.Repeat("a", maxNotificationBaseURLLen+1),
		})
		require.Error(t, err)
	})

	t.Run("too many metadata entries are rejected", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		md := make(map[string]string, maxNotificationMetadataEntries+1)
		for i := 0; i <= maxNotificationMetadataEntries; i++ {
			md[strconv.Itoa(i)] = "v"
		}

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type:     model.NotificationType_Block,
			Hash:     validHash,
			Metadata: &blockchain_api.NotificationMetadata{Metadata: md},
		})
		require.Error(t, err)
	})

	t.Run("oversized metadata value is rejected", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type: model.NotificationType_Block,
			Hash: validHash,
			Metadata: &blockchain_api.NotificationMetadata{
				Metadata: map[string]string{"k": strings.Repeat("v", maxNotificationMetadataFieldLen+1)},
			},
		})
		require.Error(t, err)
	})

	t.Run("bounded base_URL and metadata are accepted", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type:     model.NotificationType_Block,
			Hash:     validHash,
			Base_URL: "http://example.com",
			Metadata: &blockchain_api.NotificationMetadata{Metadata: map[string]string{"k": "v"}},
		})
		require.NoError(t, err)
	})
}

// TestSanitizeSubscriberSource pins that a caller-supplied Subscribe source
// cannot forge log lines with embedded newlines, cannot grow without bound, and
// cannot become an unbounded Prometheus label value.
func TestSanitizeSubscriberSource(t *testing.T) {
	require.Equal(t, "unknown", sanitizeSubscriberSource(""))
	require.Equal(t, "unknown", sanitizeSubscriberSource("\n\r\t"))
	require.Equal(t, "p2pServer", sanitizeSubscriberSource("p2pServer"))
	require.Equal(t, "evilINFO fake log line", sanitizeSubscriberSource("evil\nINFO fake log line"))
	require.Len(t, sanitizeSubscriberSource(strings.Repeat("x", 1024)), 64)

	// A multi-byte rune sitting exactly on the 64-byte truncation boundary
	// must not be split in half: the result must stay valid UTF-8, or
	// protobuf refuses to marshal it into GetSubscribersResponse.sources.
	multiByte := strings.Repeat("a", 63) + "é" // 'é' is 2 bytes, so byte 64 lands mid-rune
	sanitized := sanitizeSubscriberSource(multiByte)
	require.True(t, utf8.ValidString(sanitized), "sanitized source must be valid UTF-8, got %q", sanitized)
	_, err := proto.Marshal(&blockchain_api.GetSubscribersResponse{Sources: []string{sanitized}})
	require.NoError(t, err, "sanitized source must marshal into the public GetSubscribers response")

	require.Equal(t, SubscriberP2P, metricSourceLabel(SubscriberP2P))
	require.Equal(t, "other", metricSourceLabel(strings.Repeat("x", 64)))
	require.Equal(t, "other", metricSourceLabel("unknown"))
}

func TestGRPCPanicRecoveryKeepsProcessAlive(t *testing.T) {
	interceptor := util.CreatePanicRecoveryUnaryInterceptor(ulogger.TestLogger{}, "blockchain")

	_, err := interceptor(context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: "/blockchain_api.BlockchainAPI/GetBlockIsMined"},
		func(ctx context.Context, req any) (any, error) {
			var short []byte
			_ = chainhash.Hash(short) //nolint:staticcheck // deliberately panics

			return nil, nil
		})

	require.Equal(t, codes.Internal, status.Code(err))
}

// TestHashValidationRejectsShortHashes covers every handler that converts a
// caller-supplied slice into a chainhash.Hash. Before validation these panicked
// on any slice shorter than 32 bytes, killing the process - and three of them
// (GetBlockIsMined, GetSuitableBlock, GetHashOfAncestorBlock) are public, so no
// API key was needed to do it.
func TestHashValidationRejectsShortHashes(t *testing.T) {
	ctx := context.Background()

	b := &Blockchain{logger: ulogger.TestLogger{}, stats: gocore.NewStat("test")}

	for _, short := range [][]byte{nil, {}, {1, 2, 3}, make([]byte, chainhash.HashSize-1)} {
		require.NotPanics(t, func() {
			_, err := b.GetBlockIsMined(ctx, &blockchain_api.GetBlockIsMinedRequest{BlockHash: short})
			require.Error(t, err)

			_, err = b.SetBlockMinedSet(ctx, &blockchain_api.SetBlockMinedSetRequest{BlockHash: short})
			require.Error(t, err)

			_, err = b.ClearBlockMinedSet(ctx, &blockchain_api.ClearBlockMinedSetRequest{BlockHash: short})
			require.Error(t, err)

			_, err = b.SetBlockPersistedAt(ctx, &blockchain_api.SetBlockPersistedAtRequest{BlockHash: short})
			require.Error(t, err)

			_, err = b.SetBlockSubtreesSet(ctx, &blockchain_api.SetBlockSubtreesSetRequest{BlockHash: short})
			require.Error(t, err)

			_, err = b.GetSuitableBlock(ctx, &blockchain_api.GetSuitableBlockRequest{Hash: short})
			require.Error(t, err)

			_, err = b.GetHashOfAncestorBlock(ctx, &blockchain_api.GetHashOfAncestorBlockRequest{Hash: short, Depth: 1})
			require.Error(t, err)
		}, "short hash of length %d must be rejected, not panic", len(short))
	}
}

// TestHeightRangeBoundsRejectUnboundedRequests covers the unauthenticated
// range-query DoS surface: a reversed range used to underflow uint32
// subtraction into a ~4 billion element preallocation.
func TestHeightRangeBoundsRejectUnboundedRequests(t *testing.T) {
	ctx := context.Background()

	b := &Blockchain{logger: ulogger.TestLogger{}, stats: gocore.NewStat("test")}

	_, err := b.GetBlocksByHeight(ctx, &blockchain_api.GetBlocksByHeightRequest{StartHeight: 2, EndHeight: 0})
	require.Error(t, err, "reversed range must be rejected before it reaches the store")

	_, err = b.GetBlocksByHeight(ctx, &blockchain_api.GetBlocksByHeightRequest{StartHeight: 0, EndHeight: 4294967294})
	require.Error(t, err, "an oversized range must be rejected")

	// The reversed header range keeps its long-standing empty-result contract
	// (callers compute these bounds), but must never reach the store's capacity
	// calculation, where it used to underflow into a ~4 billion preallocation.
	resp, err := b.GetBlockHeadersByHeight(ctx, &blockchain_api.GetBlockHeadersByHeightRequest{StartHeight: 2, EndHeight: 0})
	require.NoError(t, err)
	require.Empty(t, resp.BlockHeaders)
	// The store-side preallocation bound for the same range is covered by
	// TestPreallocBounds in stores/blockchain/sql.

	_, err = b.GetMedianTimePastByHeights(ctx, &blockchain_api.GetMedianTimePastByHeightsRequest{
		Heights: make([]uint32, defaultMaxMedianTimePastHeights+1),
	})
	require.Error(t, err, "an oversized heights list must be rejected")

	_, err = b.GetBlockHeaders(ctx, &blockchain_api.GetBlockHeadersRequest{
		StartHash:       make([]byte, chainhash.HashSize),
		NumberOfHeaders: defaultMaxBlockHeadersPerRequest + 1,
	})
	require.Error(t, err, "an oversized numberOfHeaders must be rejected before it reaches the store LIMIT")

	_, err = b.GetBlockHeadersFromOldestRequest(ctx, &blockchain_api.GetBlockHeadersFromOldestRequest{
		ChainTipHash:    make([]byte, chainhash.HashSize),
		TargetHash:      make([]byte, chainhash.HashSize),
		NumberOfHeaders: defaultMaxBlockHeadersPerRequest + 1,
	})
	require.Error(t, err, "an oversized numberOfHeaders must be rejected before GetBlockHeadersFromOldest reaches the store")

	_, err = b.LocateBlockHeaders(ctx, &blockchain_api.LocateBlockHeadersRequest{
		HashStop:  make([]byte, chainhash.HashSize),
		MaxHashes: defaultMaxBlockHeadersPerRequest + 1,
	})
	require.Error(t, err, "an oversized maxHashes must be rejected before LocateBlockHeaders reaches the store")
}

// TestGetMedianTimePastByHeightsRejectsOversizedSpan covers the span the count
// cap does not: len(req.Heights) can be tiny while minHeight/maxHeight still
// drive a whole-chain header read and dense cache write.
func TestGetMedianTimePastByHeightsRejectsOversizedSpan(t *testing.T) {
	ctx := context.Background()

	b := &Blockchain{logger: ulogger.TestLogger{}, stats: gocore.NewStat("test")}

	_, err := b.GetMedianTimePastByHeights(ctx, &blockchain_api.GetMedianTimePastByHeightsRequest{
		Heights: []uint32{0, 920000},
	})
	require.Error(t, err, "a two-element request spanning the whole chain must be rejected, not just a request with many heights")
}
