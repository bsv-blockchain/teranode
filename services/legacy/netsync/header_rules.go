package netsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// headerRejection says why a header was refused, in SV Node's reject-reason
// words, and decides what the sender's connection costs.
type headerRejection uint8

const (
	// headerAccepted is the zero value: nothing was refused.
	headerAccepted headerRejection = iota

	// rejectCheckpointMismatch is CheckIndexAgainstCheckpoint's "checkpoint
	// mismatch": a header at a checkpoint height that is not the pinned hash
	// (validation.cpp:5757-5761, DoS 100).
	rejectCheckpointMismatch

	// rejectForkBeforeCheckpoint is CheckIndexAgainstCheckpoint's
	// "bad-fork-prior-to-checkpoint": a header below the highest checkpoint this
	// node holds that is not on that checkpoint's chain
	// (validation.cpp:5763-5772, DoS 100).
	rejectForkBeforeCheckpoint

	// rejectBadDiffBits is ContextualCheckBlockHeader's "bad-diffbits": the
	// header's nBits is not what the difficulty rules demand at its height
	// (validation.cpp:5789-5793, DoS 100).
	rejectBadDiffBits

	// rejectTimeTooOld is "time-too-old": the timestamp is not after the
	// parent's median time past (validation.cpp:5796-5799). SV Node marks the
	// header invalid but scores no DoS.
	rejectTimeTooOld

	// rejectTimeTooNew is "time-too-new": the timestamp is more than
	// MAX_FUTURE_BLOCK_TIME (block_index.h:32, two hours) past this node's clock
	// (validation.cpp:5802-5805). No DoS: it can be our clock that is wrong.
	rejectTimeTooNew

	// rejectBadVersion is "bad-version": a version below 2, 3 or 4 at or above
	// the BIP34, BIP66 or BIP65 height (validation.cpp:5810-5817). No DoS.
	rejectBadVersion

	// rejectUnjudgeable is not an SV Node reason. This node could not read the
	// ancestry a rule needed (a store lookup failed), so it can say nothing
	// about the header and nothing about the peer.
	rejectUnjudgeable
)

// disconnects reports whether SV Node scores this reason DoS 100, which here
// costs the sender its connection. The rest refuse the header and keep the
// peer, as SV Node's Invalid-without-DoS does.
func (r headerRejection) disconnects() bool {
	return r == rejectBadDiffBits || r == rejectCheckpointMismatch || r == rejectForkBeforeCheckpoint
}

func (r headerRejection) String() string {
	switch r {
	case headerAccepted:
		return "accepted"
	case rejectCheckpointMismatch:
		return "checkpoint mismatch"
	case rejectForkBeforeCheckpoint:
		return "bad-fork-prior-to-checkpoint"
	case rejectBadDiffBits:
		return "bad-diffbits"
	case rejectTimeTooOld:
		return "time-too-old"
	case rejectTimeTooNew:
		return "time-too-new"
	case rejectBadVersion:
		return "bad-version"
	case rejectUnjudgeable:
		return "unjudgeable"
	default:
		return fmt.Sprintf("headerRejection(%d)", uint8(r))
	}
}

// maxFutureBlockTime is SV Node's MAX_FUTURE_BLOCK_TIME (block_index.h:32).
const maxFutureBlockTime = 2 * time.Hour

// medianTimeSpan is SV Node's CBlockIndex::nMedianTimeSpan (block_index.h:722).
const medianTimeSpan = 11

// headerRules is SV Node's ContextualCheckBlockHeader (validation.cpp:5778-5820)
// for a header the cache is about to hold: expected difficulty, median time
// past, future time and version.
//
// The difficulty half is not a second calculator. It is
// blockchain.Difficulty.CalcNextWorkRequiredFrom, the same code full block
// validation runs, reading the ancestry from a source that answers for the
// committed chain from the store and for the cached headers above it from the
// cache.
type headerRules struct {
	params     *chaincfg.Params
	difficulty *blockchain.Difficulty

	// trunk answers for every block this node has committed. It is the
	// blockchain client in production and a client over a sqlitememory store in
	// tests.
	trunk blockchain.HeaderSource

	// now is the clock time-too-new is judged against. SV Node uses
	// GetAdjustedTime, the local clock moved by the median peer offset; teranode
	// keeps no peer time offsets, so this is the local clock.
	now func() time.Time
}

// newHeaderRules builds the rules for one chain. The calculator is given a copy
// of tSettings whose ChainCfgParams is params, because the rules must judge the
// chain the sync manager is on and Difficulty reads the chain from its settings.
// Its store is nil: every call passes its own source.
func newHeaderRules(logger ulogger.Logger, tSettings *settings.Settings, params *chaincfg.Params, trunk blockchain.HeaderSource) (*headerRules, error) {
	if tSettings == nil || params == nil || trunk == nil {
		return nil, nil
	}

	chainSettings := *tSettings
	chainSettings.ChainCfgParams = params

	difficulty, err := blockchain.NewDifficulty(nil, logger, &chainSettings)
	if err != nil {
		return nil, err
	}

	return &headerRules{
		params:     params,
		difficulty: difficulty,
		trunk:      trunk,
		now:        time.Now,
	}, nil
}

// check runs ContextualCheckBlockHeader for header, whose parent is parent at
// parentHeight, reading ancestors through src. The order is SV Node's: bits,
// then time-too-old, then time-too-new, then version.
func (r *headerRules) check(ctx context.Context, src blockchain.HeaderSource, parent *model.BlockHeader, parentHeight int32, header *wire.BlockHeader) (headerRejection, string) {
	height := parentHeight + 1
	blockTime := header.Timestamp.Unix()

	expected, err := r.difficulty.CalcNextWorkRequiredFrom(ctx, src, parent, uint32(parentHeight), blockTime) //nolint:gosec // a chain height, never negative here
	if err != nil || expected == nil {
		return rejectUnjudgeable, fmt.Sprintf("expected difficulty at height %d: %v", height, err)
	}

	if want := binary.LittleEndian.Uint32(expected.CloneBytes()); header.Bits != want {
		return rejectBadDiffBits, fmt.Sprintf("height %d carries bits %08x, the rules demand %08x", height, header.Bits, want)
	}

	mtp, err := medianTimePast(ctx, src, parent.Hash())
	if err != nil {
		return rejectUnjudgeable, fmt.Sprintf("median time past of height %d: %v", parentHeight, err)
	}

	if blockTime <= mtp {
		return rejectTimeTooOld, fmt.Sprintf("height %d time %d is not after median time past %d", height, blockTime, mtp)
	}

	if limit := r.now().Add(maxFutureBlockTime).Unix(); blockTime > limit {
		return rejectTimeTooNew, fmt.Sprintf("height %d time %d is more than two hours ahead of %d", height, blockTime, limit-int64(maxFutureBlockTime.Seconds()))
	}

	if (header.Version < 2 && height >= r.params.BIP0034Height) ||
		(header.Version < 3 && height >= r.params.BIP0066Height) ||
		(header.Version < 4 && height >= r.params.BIP0065Height) {
		return rejectBadVersion, fmt.Sprintf("bad-version(0x%08x) at height %d", uint32(header.Version), height) //nolint:gosec // printed as SV Node prints it
	}

	return headerAccepted, ""
}

// medianTimePast is CBlockIndex::GetMedianTimePast (block_index.h:724-738): the
// median of hash's timestamp and up to ten before it, fewer near genesis, taking
// the element at index n/2 of the sorted times as nth_element does.
func medianTimePast(ctx context.Context, src blockchain.HeaderSource, hash *chainhash.Hash) (int64, error) {
	headers, _, err := src.GetBlockHeaders(ctx, hash, medianTimeSpan)
	if err != nil {
		return 0, err
	}

	if len(headers) == 0 || !headers[0].Hash().IsEqual(hash) {
		return 0, errors.NewProcessingError("no header for %s", hash)
	}

	times := make([]int64, 0, len(headers))

	expected := *hash

	for _, h := range headers {
		if h == nil || !h.Hash().IsEqual(&expected) {
			return 0, errors.NewProcessingError("discontinuous ancestry below %s", hash)
		}

		times = append(times, int64(h.Timestamp))
		expected = *h.HashPrevBlock
	}

	slices.Sort(times)

	return times[len(times)/2], nil
}

// modelHeader is header in the model's form, which is what the difficulty
// calculator and the store speak. Hash() of the result is header.BlockHash():
// both serialise the same 80 bytes.
func modelHeader(header *wire.BlockHeader) *model.BlockHeader {
	prev := header.PrevBlock
	merkle := header.MerkleRoot

	var bits model.NBit

	binary.LittleEndian.PutUint32(bits[:], header.Bits)

	return &model.BlockHeader{
		Version:        uint32(header.Version), //nolint:gosec // the same 32 bits either way
		HashPrevBlock:  &prev,
		HashMerkleRoot: &merkle,
		Timestamp:      uint32(header.Timestamp.Unix()), //nolint:gosec // a header timestamp is 32 bits on the wire
		Bits:           bits,
		Nonce:          header.Nonce,
	}
}
