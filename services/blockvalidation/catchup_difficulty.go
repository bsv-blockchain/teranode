// This file contains difficulty (DAA) validation for headers fetched during catchup.
package blockvalidation

import (
	"context"
	"encoding/binary"
	"math/big"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/work"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// validateCatchupHeaderDifficulty verifies that the headers fetched during catchup carry
// the difficulty (nBits) required by the Difficulty Adjustment Algorithm (DAA), and flags
// the peer as malicious if they don't.
//
// Header-fetch validation (validateBatchHeaders) only checks each header's proof-of-work
// against the target encoded in its own nBits, so a peer could otherwise feed us a
// self-consistent low-difficulty chain. This step computes the required nBits for every
// fetched header and rejects mismatches before we spend resources fetching the
// corresponding blocks, including the subtree prewarm that fetchAndValidateBlocks starts.
func (u *Server) validateCatchupHeaderDifficulty(ctx context.Context, catchupCtx *CatchupContext) error {
	u.logger.Debugf("[catchup][%s] Step 9.5: Validating header chain difficulty (DAA)", catchupCtx.blockUpTo.Hash().String())

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// The chain is anchored on the common ancestor, so this must run on the untrimmed header list,
	// before fetchAndValidateBlocks drops the stored front.
	if err := validateHeaderChainDifficulty(ctx, u.logger, u.settings, u.blockchainClient, catchupCtx.commonAncestorHash, catchupCtx.blockHeaders); err != nil {
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

// validateHeaderChainDifficulty checks the required nBits of every header in a fetched run.
//
// The headers are contiguous and ascending, with headers[0] a child of the common ancestor
// (anchorHash), which trunk holds. The expected nBits come from
// blockchain.Difficulty.CalcNextWorkRequiredFrom, the calculator full-block validation uses,
// reading ancestry through blockchain.NewBranchSource: the fetched headers answer for
// themselves and trunk answers for the anchor and everything below it. So a header whose
// adjustment window reaches below the anchor is judged against the stored blocks, and
// pre-DAA, testnet minimum-difficulty, STN and no-adjustment chains follow the same rules
// as full-block validation. Cumulative chain work for the fetched headers is reconstructed
// forward from the anchor's stored chain work, mirroring stores/blockchain
// calculateAndPrepareChainWork.
//
// Parameters:
//   - ctx: context for the trunk reads
//   - logger: logger for the difficulty calculator
//   - tSettings: chain settings (DAA rules, target spacing, pow limit)
//   - trunk: source for the committed chain, the blockchain client in production
//   - anchorHash: hash of the common ancestor
//   - headers: fetched headers after the common ancestor, ascending and contiguous
//
// Returns a NetworkPeerMaliciousError if any header's nBits differ from the requirement, a
// ServiceError if the requirement cannot be computed, or nil if every header is correct.
// Failing closed matters: anything this lets through goes on to have its blocks fetched.
func validateHeaderChainDifficulty(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, trunk blockchain.HeaderSource,
	anchorHash *chainhash.Hash, headers []*model.BlockHeader) error {
	if len(headers) == 0 {
		return nil
	}

	if anchorHash == nil {
		return errors.NewProcessingError("[catchup:difficulty] no common ancestor to anchor the header chain on")
	}

	anchor, anchorMeta, err := trunk.GetBlockHeader(ctx, anchorHash)
	if err != nil || anchor == nil || anchorMeta == nil {
		return errors.NewServiceError("[catchup:difficulty] failed to get common ancestor %s", anchorHash.String(), err)
	}

	branch := make(map[chainhash.Hash]*blockchain.BranchHeader, len(headers))
	acc := new(big.Int).SetBytes(anchorMeta.ChainWork)
	prevHash := anchorHash

	for i, h := range headers {
		// Step 8 already verified this; the source walks HashPrevBlock, so a gap would make it
		// read the wrong ancestry rather than fail.
		if !h.HashPrevBlock.IsEqual(prevHash) {
			return errors.NewProcessingError("[catchup:difficulty] header %d (%s) does not link to %s", i, h.Hash().String(), prevHash.String())
		}

		acc.Add(acc, work.CalcBlockWork(binary.LittleEndian.Uint32(h.Bits.CloneBytes())))

		branch[*h.Hash()] = &blockchain.BranchHeader{
			Header:    h,
			Height:    anchorMeta.Height + 1 + uint32(i), //nolint:gosec // bounded by the fetched run
			ChainWork: acc.FillBytes(make([]byte, chainhash.HashSize)),
		}
		prevHash = h.Hash()
	}

	src := blockchain.NewBranchSource(trunk, func(hash chainhash.Hash) (*blockchain.BranchHeader, bool) {
		bh, ok := branch[hash]
		return bh, ok
	})

	// A fresh calculator per run: its last-hash cache must never outlive the branch it read.
	difficulty, err := blockchain.NewDifficulty(nil, logger, tSettings)
	if err != nil {
		return errors.NewProcessingError("[catchup:difficulty] failed to create difficulty calculator", err)
	}

	parent, parentHeight := anchor, anchorMeta.Height

	for _, h := range headers {
		expected, err := difficulty.CalcNextWorkRequiredFrom(ctx, src, parent, parentHeight, int64(h.Timestamp))
		if err != nil || expected == nil {
			return errors.NewServiceError("[catchup:difficulty] failed to compute expected nBits at height %d", parentHeight+1, err)
		}

		if h.Bits != *expected {
			return errors.NewNetworkPeerMaliciousError(
				"[catchup:difficulty] header %s at height %d has incorrect difficulty bits: got %s, expected %s",
				h.Hash().String(), parentHeight+1, h.Bits.String(), expected.String(),
			)
		}

		parent, parentHeight = h, parentHeight+1
	}

	return nil
}
