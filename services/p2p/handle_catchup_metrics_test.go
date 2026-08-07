package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// freshTestServer wires a P2P Server backed by a real in-memory
// CentralizedPeerRegistry via NewLocalPeerRegistryClient — same code path the
// gRPC-mode daemon uses, just bypassing the wire round-trip.
func freshTestServer(t *testing.T) (*Server, *blockchain.CentralizedPeerRegistry, peer.ID) {
	t.Helper()

	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	client := blockchain.NewLocalPeerRegistryClient(reg)

	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
	require.NoError(t, err)
	pid, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)

	s := &Server{
		peerRegistry: client,
		logger:       ulogger.TestLogger{},
	}
	return s, reg, pid
}

type failingValidatedProgressRegistry struct {
	blockchain.PeerRegistryClientI
	err error
}

func (f *failingValidatedProgressRegistry) RecordValidatedPeerProgress(_ context.Context, _ string, _ uint32, _ *chainhash.Hash, _ []byte) error {
	return f.err
}

func TestRecordCatchupAttempt_RegistersSyncAttempt(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	resp, err := s.RecordCatchupAttempt(context.Background(), &p2p_api.RecordCatchupAttemptRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	got, _ := reg.Get(pid.String())
	require.Equal(t, int32(1), got.SyncAttemptCount)
	require.False(t, got.LastSyncAttempt.IsZero())
	require.Equal(t, int64(1), got.CatchupAttempts)
}

func TestRecordCatchupAttempt_InvalidPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)

	_, err := s.RecordCatchupAttempt(context.Background(), &p2p_api.RecordCatchupAttemptRequest{PeerId: "not-a-peer-id"})
	require.Error(t, err)
}

func TestRecordCatchupSuccess_UpdatesInteractionMetrics(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	resp, err := s.RecordCatchupSuccess(context.Background(), &p2p_api.RecordCatchupSuccessRequest{
		PeerId:     pid.String(),
		DurationMs: 250,
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	got, _ := reg.Get(pid.String())
	require.Equal(t, int64(1), got.InteractionSuccesses)
	require.Equal(t, int64(250), got.AvgResponseTimeMs)
	require.Equal(t, int64(1), got.CatchupSuccesses)
}

func TestRecordCatchupFailure_UpdatesInteractionMetrics(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	resp, err := s.RecordCatchupFailure(context.Background(), &p2p_api.RecordCatchupFailureRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	got, _ := reg.Get(pid.String())
	require.Equal(t, int64(1), got.InteractionFailures)
	require.Equal(t, int64(1), got.CatchupFailures)
}

func TestRecordCatchupFailure_BlockIncomplete_DowngradesFullPeer(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	s.settings = &settings.Settings{
		P2P: settings.P2PSettings{
			FullStoragePenaltyDuration: 30 * time.Minute,
		},
	}
	reg.Register(&blockchain.PeerInfo{ID: pid.String(), Storage: "full"})

	blockHash := chainhash.HashH([]byte("incomplete-block"))
	resp, err := s.RecordCatchupFailure(context.Background(), &p2p_api.RecordCatchupFailureRequest{
		PeerId:      pid.String(),
		FailureKind: catchupFailureKindBlockIncomplete,
		BlockHash:   blockHash.String(),
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	got, ok := reg.Get(pid.String())
	require.True(t, ok)
	require.Equal(t, int64(1), got.InteractionFailures)
	require.Equal(t, int64(1), got.FullStorageContradictions)
	require.NotEqual(t, "full", got.Storage)
	require.True(t, got.FullStoragePenaltyUntil.After(time.Now()))
	require.Contains(t, got.LastCatchupError, blockHash.String())
}

// P2-c: validated header work is credited before delivery, so a header-only
// non-deliverer of any storage class must have its ranking withheld on a
// block-incomplete failure — not just peers that claimed full storage.
func TestRecordCatchupFailure_BlockIncomplete_PenalizesNonFullPeer(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	s.settings = &settings.Settings{
		P2P: settings.P2PSettings{
			FullStoragePenaltyDuration: 30 * time.Minute,
		},
	}
	reg.Register(&blockchain.PeerInfo{ID: pid.String(), Storage: "pruned"})

	blockHash := chainhash.HashH([]byte("incomplete-block-pruned"))
	resp, err := s.RecordCatchupFailure(context.Background(), &p2p_api.RecordCatchupFailureRequest{
		PeerId:      pid.String(),
		FailureKind: catchupFailureKindBlockIncomplete,
		BlockHash:   blockHash.String(),
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	got, ok := reg.Get(pid.String())
	require.True(t, ok)
	require.True(t, got.FullStoragePenaltyUntil.After(time.Now()), "penalty window opens for any storage class")
	require.Zero(t, got.FullStorageContradictions, "non-full peer is not counted as a full-storage contradiction")
	require.Equal(t, "pruned", got.Storage, "non-full storage is left unchanged")
	require.Contains(t, got.LastCatchupError, blockHash.String())
}

// N2: header work is credited before a block body is delivered, so a
// block-incomplete failure must decay the stale validated credit — otherwise it
// re-confers top-tier ranking once the penalty window expires. Verified on the
// real RecordCatchupFailure path for both full and non-full peers.
func TestRecordCatchupFailure_BlockIncomplete_DecaysValidatedWork(t *testing.T) {
	for _, storage := range []string{"full", "pruned"} {
		t.Run(storage, func(t *testing.T) {
			s, reg, pid := freshTestServer(t)
			s.settings = &settings.Settings{
				P2P: settings.P2PSettings{
					FullStoragePenaltyDuration: 30 * time.Minute,
				},
			}
			reg.Register(&blockchain.PeerInfo{ID: pid.String(), Storage: storage})

			validatedHash := chainhash.HashH([]byte("validated-" + storage))
			require.NoError(t, reg.RecordValidatedPeerProgress(pid.String(), 100, &validatedHash, []byte{0x05}))

			seeded, ok := reg.Get(pid.String())
			require.True(t, ok)
			require.Equal(t, []byte{0x05}, seeded.ValidatedChainWork, "precondition: validated work is credited")

			blockHash := chainhash.HashH([]byte("incomplete-" + storage))
			resp, err := s.RecordCatchupFailure(context.Background(), &p2p_api.RecordCatchupFailureRequest{
				PeerId:      pid.String(),
				FailureKind: catchupFailureKindBlockIncomplete,
				BlockHash:   blockHash.String(),
			})
			require.NoError(t, err)
			require.True(t, resp.Ok)

			got, ok := reg.Get(pid.String())
			require.True(t, ok)
			require.True(t, got.FullStoragePenaltyUntil.After(time.Now()))
			require.Empty(t, got.ValidatedChainWork, "stale validated work must be decayed on block-incomplete failure")
			require.Nil(t, got.ValidatedBlockHash)
			require.Equal(t, uint32(0), got.ValidatedHeight)
		})
	}
}

func TestRecordCatchupFailure_BlockIncomplete_DoesNotBanPeer(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String(), Storage: "full"})

	resp, err := s.RecordCatchupFailure(context.Background(), &p2p_api.RecordCatchupFailureRequest{
		PeerId:      pid.String(),
		FailureKind: catchupFailureKindBlockIncomplete,
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	got, ok := reg.Get(pid.String())
	require.True(t, ok)
	require.False(t, got.IsBanned)
	require.Zero(t, got.BanScore)
	require.Zero(t, got.MaliciousCount)

	banned, err := s.peerRegistry.IsPeerBanned(context.Background(), pid.String())
	require.NoError(t, err)
	require.False(t, banned)
}

func TestRecordCatchupFailure_UnknownFailureKind_Generic(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String(), Storage: "full"})

	resp, err := s.RecordCatchupFailure(context.Background(), &p2p_api.RecordCatchupFailureRequest{
		PeerId:      pid.String(),
		FailureKind: "not_a_known_failure",
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	got, ok := reg.Get(pid.String())
	require.True(t, ok)
	require.Equal(t, int64(1), got.InteractionFailures)
	require.Equal(t, "full", got.Storage)
	require.Zero(t, got.FullStorageContradictions)
	require.True(t, got.FullStoragePenaltyUntil.IsZero())
	require.Empty(t, got.LastCatchupError)
}

func TestRecordCatchupMalicious_PinsReputationLow(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	_, err := s.RecordCatchupMalicious(context.Background(), &p2p_api.RecordCatchupMaliciousRequest{PeerId: pid.String()})
	require.NoError(t, err)

	got, _ := reg.Get(pid.String())
	require.Equal(t, int64(1), got.MaliciousCount)
	require.Equal(t, 5.0, got.ReputationScore)
}

func TestUpdateCatchupError_StoresMessageAndTime(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	_, err := s.UpdateCatchupError(context.Background(), &p2p_api.UpdateCatchupErrorRequest{
		PeerId:   pid.String(),
		ErrorMsg: "block 0xdead missing",
	})
	require.NoError(t, err)

	got, _ := reg.Get(pid.String())
	require.Equal(t, "block 0xdead missing", got.LastCatchupError)
	require.False(t, got.LastCatchupErrorTime.IsZero())
}

func TestGetPeersForCatchup_FiltersAndSorts(t *testing.T) {
	s, reg, _ := freshTestServer(t)

	// Three peers: full + http url, pruned + http url, banned + http url.
	reg.Register(&blockchain.PeerInfo{ID: "full", DataHubURL: "http://full", Storage: "full", Height: 100})
	reg.Register(&blockchain.PeerInfo{ID: "pruned", DataHubURL: "http://pruned", Storage: "pruned", Height: 100})
	reg.Register(&blockchain.PeerInfo{ID: "no-url", Storage: "full", Height: 100})
	reg.Register(&blockchain.PeerInfo{ID: "banned", DataHubURL: "http://banned", Storage: "full", Height: 100})
	reg.AddBanScore("banned", "spam", 0)
	reg.AddBanScore("banned", "spam", 0)

	resp, err := s.GetPeersForCatchup(context.Background(), &p2p_api.GetPeersForCatchupRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Peers, 2, "no-url and banned must be excluded")

	ids := []string{resp.Peers[0].Id, resp.Peers[1].Id}
	require.ElementsMatch(t, []string{"full", "pruned"}, ids)
	require.Equal(t, "full", resp.Peers[0].Id, "full must sort ahead of pruned")
}

func TestGetPeersForCatchup_UsesCatchupSpecificCounters(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String(), DataHubURL: "http://peer", Height: 100})

	ctx := context.Background()

	// Three catchup attempts: one succeeds, one fails, one produces no outcome.
	for i := 0; i < 3; i++ {
		_, err := s.RecordCatchupAttempt(ctx, &p2p_api.RecordCatchupAttemptRequest{PeerId: pid.String()})
		require.NoError(t, err)
	}
	_, err := s.RecordCatchupSuccess(ctx, &p2p_api.RecordCatchupSuccessRequest{PeerId: pid.String(), DurationMs: 100})
	require.NoError(t, err)
	_, err = s.RecordCatchupFailure(ctx, &p2p_api.RecordCatchupFailureRequest{PeerId: pid.String()})
	require.NoError(t, err)

	// Non-catchup interactions must not bleed into the catchup counters.
	_, err = s.ReportValidBlock(ctx, &p2p_api.ReportValidBlockRequest{PeerId: pid.String(), BlockHash: "abc"})
	require.NoError(t, err)
	_, err = s.ReportValidSubtree(ctx, &p2p_api.ReportValidSubtreeRequest{PeerId: pid.String(), SubtreeHash: "def"})
	require.NoError(t, err)

	resp, err := s.GetPeersForCatchup(ctx, &p2p_api.GetPeersForCatchupRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Peers, 1)

	p := resp.Peers[0]
	require.Equal(t, int64(3), p.CatchupAttempts, "attempts without an outcome must still be counted")
	require.Equal(t, int64(1), p.CatchupSuccesses)
	require.Equal(t, int64(1), p.CatchupFailures)
}

func TestReportValidBlockHeaders_CreditsReputationNotCatchupCounters(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	resp, err := s.ReportValidBlockHeaders(context.Background(), &p2p_api.ReportValidBlockHeadersRequest{
		PeerId:     pid.String(),
		DurationMs: 120,
	})
	require.NoError(t, err)
	require.True(t, resp.Success)

	got, _ := reg.Get(pid.String())
	require.Equal(t, int64(1), got.InteractionSuccesses)
	require.Equal(t, int64(120), got.AvgResponseTimeMs)
	require.Equal(t, int64(0), got.CatchupSuccesses, "headers batch must not count as a completed catchup")
	require.Equal(t, int64(0), got.CatchupAttempts)
}

// TestReportValidBlockHeaders_KeepsFailingPeerHealthy guards the catchup
// recovery dynamics: a peer that serves headers fine but keeps failing at the
// block-fetch stage (e.g. it rate-limits subtree requests) must not collapse to
// an unhealthy reputation, or catchup would exclude the only peer that has the
// blocks and never recover.
func TestReportValidBlockHeaders_KeepsFailingPeerHealthy(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	ctx := context.Background()

	// Three catchup cycles: headers served ok, then the catchup fails.
	for i := 0; i < 3; i++ {
		_, err := s.RecordCatchupAttempt(ctx, &p2p_api.RecordCatchupAttemptRequest{PeerId: pid.String()})
		require.NoError(t, err)
		_, err = s.ReportValidBlockHeaders(ctx, &p2p_api.ReportValidBlockHeadersRequest{PeerId: pid.String(), DurationMs: 100})
		require.NoError(t, err)
		_, err = s.RecordCatchupFailure(ctx, &p2p_api.RecordCatchupFailureRequest{PeerId: pid.String()})
		require.NoError(t, err)
	}

	unhealthy, err := s.IsPeerUnhealthy(ctx, &p2p_api.IsPeerUnhealthyRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.False(t, unhealthy.IsUnhealthy, "peer serving headers must stay healthy despite catchup failures (reputation: %.2f, reason: %s)", unhealthy.ReputationScore, unhealthy.Reason)
}

func TestReportValidBlockHeaders_InvalidPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)
	_, err := s.ReportValidBlockHeaders(context.Background(), &p2p_api.ReportValidBlockHeadersRequest{PeerId: "not-a-peer"})
	require.Error(t, err)

	_, err = s.ReportValidBlockHeaders(context.Background(), &p2p_api.ReportValidBlockHeadersRequest{})
	require.Error(t, err)
}

func TestReportValidSubtree_HappyPath(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	resp, err := s.ReportValidSubtree(context.Background(), &p2p_api.ReportValidSubtreeRequest{
		PeerId:      pid.String(),
		SubtreeHash: "abc",
	})
	require.NoError(t, err)
	require.True(t, resp.Success)

	got, _ := reg.Get(pid.String())
	require.Equal(t, int64(1), got.SubtreesReceived)
}

func TestReportValidSubtree_RejectsEmpty(t *testing.T) {
	s, _, _ := freshTestServer(t)

	_, err := s.ReportValidSubtree(context.Background(), &p2p_api.ReportValidSubtreeRequest{})
	require.Error(t, err)

	_, err = s.ReportValidSubtree(context.Background(), &p2p_api.ReportValidSubtreeRequest{
		PeerId: "x",
	})
	require.Error(t, err)
}

func TestReportValidBlock_HappyPath(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	resp, err := s.ReportValidBlock(context.Background(), &p2p_api.ReportValidBlockRequest{
		PeerId:    pid.String(),
		BlockHash: "abc",
	})
	require.NoError(t, err)
	require.True(t, resp.Success)

	got, _ := reg.Get(pid.String())
	require.Equal(t, int64(1), got.BlocksReceived)
}

func TestIsPeerMalicious_BannedPeerIsMalicious(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.AddBanScore(pid.String(), "spam", 0)
	reg.AddBanScore(pid.String(), "spam", 0)

	resp, err := s.IsPeerMalicious(context.Background(), &p2p_api.IsPeerMaliciousRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.True(t, resp.IsMalicious)
}

func TestIsPeerMalicious_CleanPeer(t *testing.T) {
	s, _, pid := freshTestServer(t)

	resp, err := s.IsPeerMalicious(context.Background(), &p2p_api.IsPeerMaliciousRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.False(t, resp.IsMalicious)
}

func TestIsPeerMalicious_EmptyID(t *testing.T) {
	s, _, _ := freshTestServer(t)

	resp, err := s.IsPeerMalicious(context.Background(), &p2p_api.IsPeerMaliciousRequest{PeerId: ""})
	require.NoError(t, err)
	require.False(t, resp.IsMalicious)
}

func TestIsPeerUnhealthy_LowReputation(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})
	// Drive reputation below 40 by recording a malicious event.
	reg.UpdateMetrics(pid.String(), 0, 0, 0, false, false, true, 0)

	resp, err := s.IsPeerUnhealthy(context.Background(), &p2p_api.IsPeerUnhealthyRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.True(t, resp.IsUnhealthy)
}

func TestIsPeerUnhealthy_UnknownPeer(t *testing.T) {
	s, _, pid := freshTestServer(t)

	resp, err := s.IsPeerUnhealthy(context.Background(), &p2p_api.IsPeerUnhealthyRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.True(t, resp.IsUnhealthy)
}

func TestRecordCatchupSuccess_InvalidPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)
	_, err := s.RecordCatchupSuccess(context.Background(), &p2p_api.RecordCatchupSuccessRequest{PeerId: "not-a-peer"})
	require.Error(t, err)
}

func TestRecordCatchupFailure_InvalidPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)
	_, err := s.RecordCatchupFailure(context.Background(), &p2p_api.RecordCatchupFailureRequest{PeerId: "not-a-peer"})
	require.Error(t, err)
}

func TestRecordCatchupMalicious_InvalidPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)
	_, err := s.RecordCatchupMalicious(context.Background(), &p2p_api.RecordCatchupMaliciousRequest{PeerId: "not-a-peer"})
	require.Error(t, err)
}

func TestUpdateCatchupError_InvalidPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)
	_, err := s.UpdateCatchupError(context.Background(), &p2p_api.UpdateCatchupErrorRequest{PeerId: "not-a-peer"})
	require.Error(t, err)
}

func TestReportValidSubtree_InvalidPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)
	_, err := s.ReportValidSubtree(context.Background(), &p2p_api.ReportValidSubtreeRequest{
		PeerId: "not-a-peer", SubtreeHash: "abc",
	})
	require.Error(t, err)
}

func TestReportValidBlock_InvalidPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)
	_, err := s.ReportValidBlock(context.Background(), &p2p_api.ReportValidBlockRequest{
		PeerId: "not-a-peer", BlockHash: "abc",
	})
	require.Error(t, err)
}

func TestReportValidBlock_RejectsEmpty(t *testing.T) {
	s, _, _ := freshTestServer(t)

	_, err := s.ReportValidBlock(context.Background(), &p2p_api.ReportValidBlockRequest{})
	require.Error(t, err)

	_, err = s.ReportValidBlock(context.Background(), &p2p_api.ReportValidBlockRequest{PeerId: "x"})
	require.Error(t, err)
}

func TestReportValidatedChainProgress_UpdatesRegistry(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String(), Height: 500})
	blockHash := chainhash.HashH([]byte("validated-progress"))
	chainWork := []byte{0x01, 0x02, 0x03}

	resp, err := s.ReportValidatedChainProgress(context.Background(), &p2p_api.ReportValidatedChainProgressRequest{
		PeerId:    pid.String(),
		Height:    300,
		BlockHash: blockHash.String(),
		ChainWork: chainWork,
	})
	require.NoError(t, err)
	require.True(t, resp.Success)

	got, ok := reg.Get(pid.String())
	require.True(t, ok)
	require.Equal(t, uint32(500), got.Height)
	require.Equal(t, uint32(300), got.ValidatedHeight)
	require.NotNil(t, got.ValidatedBlockHash)
	require.Equal(t, blockHash.String(), got.ValidatedBlockHash.String())
	require.Equal(t, chainWork, got.ValidatedChainWork)
	require.False(t, got.LastValidatedAt.IsZero())
}

func TestReportValidatedChainProgress_RegistryRPCErrorReturnsSuccess(t *testing.T) {
	_, _, pid := freshTestServer(t)
	blockHash := chainhash.HashH([]byte("validated-progress-rpc-error"))
	s := &Server{
		peerRegistry: &failingValidatedProgressRegistry{err: status.Error(codes.Unavailable, "registry unavailable")},
		logger:       ulogger.TestLogger{},
	}

	resp, err := s.ReportValidatedChainProgress(context.Background(), &p2p_api.ReportValidatedChainProgressRequest{
		PeerId:    pid.String(),
		Height:    300,
		BlockHash: blockHash.String(),
		ChainWork: []byte{0x01},
	})
	require.NoError(t, err)
	require.True(t, resp.Success)
}

func TestReportValidatedChainProgress_RegistryUnimplementedReturnsSuccess(t *testing.T) {
	_, _, pid := freshTestServer(t)
	blockHash := chainhash.HashH([]byte("validated-progress-unimplemented"))
	s := &Server{
		peerRegistry: &failingValidatedProgressRegistry{err: status.Error(codes.Unimplemented, "method not implemented")},
		logger:       ulogger.TestLogger{},
	}

	resp, err := s.ReportValidatedChainProgress(context.Background(), &p2p_api.ReportValidatedChainProgressRequest{
		PeerId:    pid.String(),
		Height:    300,
		BlockHash: blockHash.String(),
		ChainWork: []byte{0x01},
	})
	require.NoError(t, err)
	require.True(t, resp.Success)
}

func TestReportValidatedChainProgress_RejectsInvalidInputs(t *testing.T) {
	s, _, pid := freshTestServer(t)
	blockHash := chainhash.HashH([]byte("validated-progress-invalid"))

	tests := []struct {
		name string
		req  *p2p_api.ReportValidatedChainProgressRequest
	}{
		{
			name: "empty peer",
			req: &p2p_api.ReportValidatedChainProgressRequest{
				Height:    1,
				BlockHash: blockHash.String(),
				ChainWork: []byte{0x01},
			},
		},
		{
			name: "invalid peer",
			req: &p2p_api.ReportValidatedChainProgressRequest{
				PeerId:    "not-a-peer",
				Height:    1,
				BlockHash: blockHash.String(),
				ChainWork: []byte{0x01},
			},
		},
		{
			name: "zero height",
			req: &p2p_api.ReportValidatedChainProgressRequest{
				PeerId:    pid.String(),
				BlockHash: blockHash.String(),
				ChainWork: []byte{0x01},
			},
		},
		{
			name: "empty block hash",
			req: &p2p_api.ReportValidatedChainProgressRequest{
				PeerId:    pid.String(),
				Height:    1,
				ChainWork: []byte{0x01},
			},
		},
		{
			name: "invalid block hash",
			req: &p2p_api.ReportValidatedChainProgressRequest{
				PeerId:    pid.String(),
				Height:    1,
				BlockHash: "not-a-hash",
				ChainWork: []byte{0x01},
			},
		},
		{
			name: "empty chainwork",
			req: &p2p_api.ReportValidatedChainProgressRequest{
				PeerId:    pid.String(),
				Height:    1,
				BlockHash: blockHash.String(),
			},
		},
		{
			name: "oversized chainwork",
			req: &p2p_api.ReportValidatedChainProgressRequest{
				PeerId:    pid.String(),
				Height:    1,
				BlockHash: blockHash.String(),
				ChainWork: make([]byte, maxReportedChainWorkBytes+1),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := s.ReportValidatedChainProgress(context.Background(), tt.req)
			require.Error(t, err)
			require.False(t, resp.Success)
		})
	}
}

func TestIsPeerUnhealthy_InvalidPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)
	resp, err := s.IsPeerUnhealthy(context.Background(), &p2p_api.IsPeerUnhealthyRequest{PeerId: "not-a-peer"})
	require.NoError(t, err)
	require.True(t, resp.IsUnhealthy)
}

func TestIsPeerUnhealthy_LowSuccessRate(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})
	// Give 12 interactions: 4 success, 8 failure. Handler uses
	// `successes < total/2` (integer div), so 4 < 12/2 = 6 → unhealthy.
	for i := 0; i < 4; i++ {
		reg.UpdateMetrics(pid.String(), 0, 0, 0, true, false, false, 100)
	}
	for i := 0; i < 8; i++ {
		reg.UpdateMetrics(pid.String(), 0, 0, 0, false, true, false, 0)
	}

	resp, err := s.IsPeerUnhealthy(context.Background(), &p2p_api.IsPeerUnhealthyRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.True(t, resp.IsUnhealthy)
	require.Contains(t, resp.Reason, "low")
}

func TestIsPeerUnhealthy_EmptyID(t *testing.T) {
	s, _, _ := freshTestServer(t)
	resp, err := s.IsPeerUnhealthy(context.Background(), &p2p_api.IsPeerUnhealthyRequest{PeerId: ""})
	require.NoError(t, err)
	require.True(t, resp.IsUnhealthy)
	require.Contains(t, resp.Reason, "empty")
}

func TestIsPeerUnhealthy_HealthyPeer(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})
	// Push reputation above the unhealthy threshold via successful interactions.
	for i := 0; i < 5; i++ {
		reg.UpdateMetrics(pid.String(), 0, 0, 0, true, false, false, 100)
	}

	resp, err := s.IsPeerUnhealthy(context.Background(), &p2p_api.IsPeerUnhealthyRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.False(t, resp.IsUnhealthy)
}
