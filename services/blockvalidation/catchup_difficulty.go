// This file contains difficulty (DAA) validation for headers fetched during catchup.
package blockvalidation

import (
	"context"
	"encoding/binary"
	"math/big"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/work"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util"
)

// validateCatchupHeaderDifficulty verifies that the headers fetched during catchup carry
// the difficulty (nBits) required by the Difficulty Adjustment Algorithm (DAA), and flags
// the peer as malicious if they don't.
//
// Header-fetch validation (validateBatchHeaders) only checks each header's proof-of-work
// against the target encoded in its own nBits, so a peer could otherwise feed us a
// self-consistent low-difficulty chain. This step recomputes the DAA-required nBits from
// the fetched header chain itself and rejects mismatches before we spend resources
// fetching and fully validating the corresponding blocks.
func (u *Server) validateCatchupHeaderDifficulty(ctx context.Context, catchupCtx *CatchupContext) error {
	u.logger.Debugf("[catchup][%s] Step 9.5: Validating header chain difficulty (DAA)", catchupCtx.blockUpTo.Hash().String())

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := validateHeaderChainDifficulty(u.settings, catchupCtx.commonAncestorMeta, catchupCtx.blockHeaders); err != nil {
		if errors.IsMaliciousResponseError(err) {
			if prometheusCatchupErrors != nil && catchupCtx.peerID != "" {
				prometheusCatchupErrors.WithLabelValues(catchupCtx.peerID, "invalid_difficulty").Inc()
			}

			u.reportCatchupMalicious(ctx, catchupCtx.peerID, "invalid header difficulty during catchup")
			u.logger.Errorf("[catchup][%s] SECURITY: Peer %s sent headers with invalid difficulty: %v", catchupCtx.blockUpTo.Hash().String(), catchupCtx.baseURL, err)
		}

		return err
	}

	return nil
}

// validateHeaderChainDifficulty checks the DAA-required nBits for every header whose full
// difficulty-adjustment window lies within the in-memory header chain (anchored at the
// common ancestor).
//
// The headers are contiguous and ascending, with headers[0] a child of the common ancestor
// (anchor). Cumulative chainwork is reconstructed forward from the anchor's stored chainwork,
// mirroring stores/blockchain calculateAndPrepareChainWork; the expected target is then
// computed with the exact same arithmetic as the store-backed path via blockchain.ComputeTarget
// and the same median-of-three "suitable block" selection as GetSuitableBlock.
//
// Headers closer to the anchor than the window depth (whose window would reach blocks below
// the anchor, i.e. already-stored blocks on our own chain) are skipped here; the downstream
// full-block DAA check (BlockValidation.ValidateBlock via GetNextWorkRequired) covers those
// using the store. This step exists to reject deep, self-consistent low-difficulty chains
// early — exactly the region an attacker fully controls.
//
// Parameters:
//   - tSettings: chain settings (DAA rules, target spacing, pow limit)
//   - anchor: metadata of the common ancestor (supplies Height and cumulative ChainWork)
//   - headers: fetched headers after the common ancestor, ascending and contiguous
//
// Returns a NetworkPeerMaliciousError if any checked header's nBits differ from the DAA
// requirement, or nil if all checked headers are correct (or none can be checked yet).
func validateHeaderChainDifficulty(tSettings *settings.Settings, anchor *model.BlockHeaderMeta, headers []*model.BlockHeader) error {
	// Regtest and similar: difficulty is never adjusted, so any nBits is acceptable.
	if tSettings.ChainCfgParams.NoDifficultyAdjustment {
		return nil
	}

	if anchor == nil || len(headers) == 0 {
		return nil
	}

	window := blockchain.DifficultyAdjustmentWindow

	// Cumulative chainwork per header, reconstructed from the anchor's chainwork.
	// ChainWork is stored big-endian (see stores/blockchain/sql calculateAndPrepareChainWork),
	// so SetBytes reconstructs the value directly.
	cumWork := make([]*big.Int, len(headers))
	acc := new(big.Int).SetBytes(anchor.ChainWork)

	for i, h := range headers {
		bits := binary.LittleEndian.Uint32(h.Bits.CloneBytes())
		acc = new(big.Int).Add(acc, work.CalcBlockWork(bits))
		cumWork[i] = acc
	}

	targetSpacing := int64(tSettings.ChainCfgParams.TargetTimePerBlock.Seconds())

	// suitable builds a SuitableBlock view of the header at the given in-memory index.
	suitable := func(idx int) *model.SuitableBlock {
		h := headers[idx]

		return &model.SuitableBlock{
			NBits:     h.Bits.CloneBytes(),
			Time:      h.Timestamp,
			ChainWork: cumWork[idx].Bytes(),
			Height:    anchor.Height + 1 + uint32(idx),
		}
	}

	// median3 returns the median-by-time of the three headers ending at idx, mirroring
	// stores/blockchain/sql getMedianBlock. Order candidates oldest-first (depth DESC in
	// the store) so the unstable sort's tie-break matches the store path exactly.
	median3 := func(idx int) *model.SuitableBlock {
		s := []*model.SuitableBlock{suitable(idx - 2), suitable(idx - 1), suitable(idx)}
		util.SortForDifficultyAdjustment(s)

		return s[1]
	}

	powLimitBits := powLimitNBit(tSettings)

	for i := 1; i < len(headers); i++ {
		// The DAA target for header i is derived from its parent (index i-1): the last
		// suitable block ends at the parent, and the first suitable block ends at the
		// parent's ancestor `window` blocks back. That ancestor's own median lookback of
		// two more blocks must also be in memory, so the first fully-checkable header is
		// the one whose (parentIdx - window) >= 2.
		parentIdx := i - 1
		firstIdx := parentIdx - window

		if firstIdx < 2 {
			continue
		}

		parentHeight := anchor.Height + 1 + uint32(parentIdx)

		var expected *model.NBit

		switch {
		case tSettings.ChainCfgParams.ReduceMinDifficulty &&
			int64(headers[i].Timestamp) > int64(headers[parentIdx].Timestamp)+2*targetSpacing:
			// Testnet minimum-difficulty rule: a block more than 2*spacing after its
			// parent may be mined at the pow limit.
			expected = powLimitBits

		case parentHeight < uint32(window)+4:
			// Not enough history for a difficulty adjustment yet.
			expected = powLimitBits

		default:
			lastSuitable := median3(parentIdx)
			firstSuitable := median3(firstIdx)

			nBits, err := blockchain.ComputeTarget(tSettings, firstSuitable, lastSuitable)
			if err != nil {
				return errors.NewProcessingError("[catchup:difficulty] failed to compute expected nBits at height %d", parentHeight+1, err)
			}

			expected = nBits
		}

		if headers[i].Bits != *expected {
			return errors.NewNetworkPeerMaliciousError(
				"[catchup:difficulty] header at height %d has incorrect difficulty bits: got %s, expected %s",
				parentHeight+1, headers[i].Bits.String(), expected.String(),
			)
		}
	}

	return nil
}

// powLimitNBit returns the minimum-difficulty (pow limit) nBits for the chain, matching
// how blockchain.NewDifficulty derives its powLimitnBits.
func powLimitNBit(tSettings *settings.Settings) *model.NBit {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, tSettings.ChainCfgParams.PowLimitBits)

	nb, _ := model.NewNBitFromSlice(b)

	return nb
}
