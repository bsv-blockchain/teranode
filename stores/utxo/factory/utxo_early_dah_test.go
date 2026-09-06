package factory

// Tests for earlyDAHRatchet — the climber that publishes the highest CONFIRMED
// hardcoded checkpoint height to a store's early-DAH boundary as the header
// chain grows. Fail-safe: probe errors and missing headers leave the boundary
// at the last confirmed checkpoint and retry later; a hash mismatch at a
// checkpoint height stops the ratchet permanently WITHOUT advancing (the
// already-published boundary, proven by its own checkpoint, remains).

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// earlyDAHSetterStub records every SetEarlyDAHBoundary call so tests can
// assert the ratchet's publish sequence, not just its final value.
type earlyDAHSetterStub struct {
	heights []uint32
}

func (s *earlyDAHSetterStub) SetEarlyDAHBoundary(h uint32) {
	s.heights = append(s.heights, h)
}

func (s *earlyDAHSetterStub) last() uint32 {
	if len(s.heights) == 0 {
		return 0
	}

	return s.heights[len(s.heights)-1]
}

func TestEarlyDAHRatchet(t *testing.T) {
	nBits := model.NBit{0xff, 0xff, 0x00, 0x1d}
	zeroHash := &chainhash.Hash{}

	newHeader := func(nonce uint32) *model.BlockHeader {
		return &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  zeroHash,
			HashMerkleRoot: zeroHash,
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           nBits,
			Nonce:          nonce,
		}
	}

	// Three checkpoints at 100 < 200 < 300, deliberately supplied unsorted to
	// prove the ratchet sorts.
	h100, h200, h300 := newHeader(100), newHeader(200), newHeader(300)
	checkpoints := []chaincfg.Checkpoint{
		{Height: 300, Hash: h300.Hash()},
		{Height: 100, Hash: h100.Hash()},
		{Height: 200, Hash: h200.Hash()},
	}

	settingsWith := func(cps []chaincfg.Checkpoint, enabled bool) *settings.Settings {
		s := &settings.Settings{ChainCfgParams: &chaincfg.Params{Checkpoints: cps}}
		s.UtxoStore.EarlyDAHBelowCheckpoint = enabled

		return s
	}

	headersAt := func(mockBC *blockchain.Mock, height uint32, hdr *model.BlockHeader) {
		if hdr == nil {
			mockBC.On("GetBlockHeadersFromHeight", mock.Anything, height, uint32(1)).
				Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil)
			return
		}

		mockBC.On("GetBlockHeadersFromHeight", mock.Anything, height, uint32(1)).
			Return([]*model.BlockHeader{hdr}, []*model.BlockHeaderMeta{{}}, nil)
	}

	t.Run("climbs every already-covered checkpoint in one probe, done at top", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		headersAt(mockBC, 100, h100)
		headersAt(mockBC, 200, h200)
		headersAt(mockBC, 300, h300)

		store := &earlyDAHSetterStub{}
		r := newEarlyDAHRatchet(settingsWith(checkpoints, true), store)
		r.probe(context.Background(), ulogger.TestLogger{}, mockBC)

		require.Equal(t, []uint32{100, 200, 300}, store.heights, "publishes ascending, sorted despite unsorted input")
		require.True(t, r.done)
	})

	t.Run("partial header coverage: boundary stops at last confirmed, resumes later", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		headersAt(mockBC, 100, h100)
		headersAt(mockBC, 200, h200)
		headersAt(mockBC, 300, nil) // header chain not there yet

		store := &earlyDAHSetterStub{}
		r := newEarlyDAHRatchet(settingsWith(checkpoints, true), store)
		r.probe(context.Background(), ulogger.TestLogger{}, mockBC)

		require.Equal(t, uint32(200), store.last())
		require.False(t, r.done, "must keep probing for 300 on later notifications")

		// Headers advance past 300 — the next probe finishes the climb.
		mockBC2 := &blockchain.Mock{}
		headersAt(mockBC2, 300, h300)
		r.probe(context.Background(), ulogger.TestLogger{}, mockBC2)

		require.Equal(t, uint32(300), store.last())
		require.True(t, r.done)
	})

	t.Run("lookup error: boundary keeps last confirmed, retries later", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		headersAt(mockBC, 100, h100)
		mockBC.On("GetBlockHeadersFromHeight", mock.Anything, uint32(200), uint32(1)).
			Return([]*model.BlockHeader(nil), []*model.BlockHeaderMeta(nil), errors.NewServiceError("boom"))

		store := &earlyDAHSetterStub{}
		r := newEarlyDAHRatchet(settingsWith(checkpoints, true), store)
		r.probe(context.Background(), ulogger.TestLogger{}, mockBC)

		require.Equal(t, uint32(100), store.last())
		require.False(t, r.done)
	})

	t.Run("hash mismatch: stops permanently at last confirmed boundary", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		headersAt(mockBC, 100, h100)
		headersAt(mockBC, 200, h300) // wrong header at checkpoint 200

		store := &earlyDAHSetterStub{}
		r := newEarlyDAHRatchet(settingsWith(checkpoints, true), store)
		r.probe(context.Background(), ulogger.TestLogger{}, mockBC)

		require.Equal(t, uint32(100), store.last(), "boundary must not advance past the contradiction")
		require.True(t, r.done, "mismatch stops the ratchet permanently")

		// A later probe must be a no-op: no further RPCs, no further publishes.
		published := len(store.heights)
		mockBC2 := &blockchain.Mock{}
		r.probe(context.Background(), ulogger.TestLogger{}, mockBC2)
		require.Len(t, store.heights, published)
		mockBC2.AssertNotCalled(t, "GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("feature flag off: done immediately, no queries, no publishes", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		store := &earlyDAHSetterStub{}
		r := newEarlyDAHRatchet(settingsWith(checkpoints, false), store)

		require.True(t, r.done)
		r.probe(context.Background(), ulogger.TestLogger{}, mockBC)
		require.Empty(t, store.heights)
		mockBC.AssertNotCalled(t, "GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("no checkpoints configured: done immediately", func(t *testing.T) {
		store := &earlyDAHSetterStub{}
		r := newEarlyDAHRatchet(settingsWith(nil, true), store)
		require.True(t, r.done)
	})

	t.Run("store does not implement the setter interface: done immediately", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		r := newEarlyDAHRatchet(settingsWith(checkpoints, true), struct{}{})

		require.True(t, r.done)
		r.probe(context.Background(), ulogger.TestLogger{}, mockBC)
		mockBC.AssertNotCalled(t, "GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything)
	})
}
