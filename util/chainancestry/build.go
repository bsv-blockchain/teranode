package chainancestry

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// Row is one header as the builder needs it: enough to prove linkage and to keep the id.
type Row struct {
	Hash     chainhash.Hash
	PrevHash chainhash.Hash
	Height   uint32
	ID       uint32
	MinedSet bool
}

// Fetcher walks parent links back from a hash. Headers returns up to n rows, newest first,
// the first being the block at from. A short answer is not an error at this seam; the builder
// is what proves the length.
type Fetcher interface {
	Headers(ctx context.Context, from chainhash.Hash, n uint32) ([]Row, error)
}

// RejectedError is a walk the builder could not prove. Check names the check that failed,
// with one of the five fixed values below, so a counter can be labelled by it.
type RejectedError struct {
	Check  string
	Detail string
}

// The five checks, each fatal to the whole ancestry. Their names are the counter labels.
const (
	CheckLength    = "length"     // fewer rows than asked for, and the walk did not reach height 0
	CheckAnchor    = "anchor"     // the first row is not the anchor asked for, or not at its height
	CheckDescent   = "descent"    // heights do not fall by exactly one at every step
	CheckLinkage   = "linkage"    // a row's previous hash does not name the next row
	CheckChunkJoin = "chunk_join" // a chunk's top does not continue the previous chunk's bottom
)

func (e *RejectedError) Error() string {
	return fmt.Sprintf("chainancestry: ancestry rejected by the %s check: %s", e.Check, e.Detail)
}

// DefaultChunkHeights is how many heights one fetch asks for. A span longer than this takes
// several fetches joined by the chunk-join check. It is a compile-time constant of the builder.
const DefaultChunkHeights = 10_000

// Build walks parent links from anchor, whose height the caller has read, down to lo, and
// proves the walk with five checks. Any failure rejects the whole ancestry: none is a warning.
//
// The anchor's hash and height are read once by the caller, uncached, and everything is proven
// against that hash, so a chain switch after the read shows up as a linkage or anchor failure
// rather than as a wrong answer. chunk is the number of heights per fetch; zero means
// DefaultChunkHeights.
func Build(ctx context.Context, f Fetcher, anchor chainhash.Hash, anchorHeight, lo uint32, chunk uint32) (*Ancestry, error) {
	if lo > anchorHeight {
		return nil, errors.NewProcessingError("[chainancestry] lo %d is above the anchor height %d", lo, anchorHeight)
	}

	if chunk == 0 {
		chunk = DefaultChunkHeights
	}

	span := anchorHeight - lo + 1

	// Filled from each row's own height, never from its position, so a duplicate row at one
	// height cannot shift every entry below it by one and still pass a length check.
	ids := make([]uint32, span)
	seen := make([]bool, span)
	notMined := make([]uint32, 0)

	from := anchor
	wantHeight := anchorHeight
	remaining := span
	var prevBottomPrev chainhash.Hash
	first := true

	for remaining > 0 {
		n := chunk
		if n > remaining {
			n = remaining
		}

		rows, err := f.Headers(ctx, from, n)
		if err != nil {
			return nil, err
		}

		// Check 1, length. A short list with a nil error is how a stream that failed part way
		// arrives, because the header fetches do not check the stream's own error. The one
		// legitimate short answer ends at height 0.
		if uint32(len(rows)) != n && (len(rows) == 0 || rows[len(rows)-1].Height != 0) { //nolint:gosec // len fits
			return nil, &RejectedError{CheckLength, fmt.Sprintf("asked for %d rows from %s, got %d", n, from, len(rows))}
		}

		if len(rows) == 0 {
			return nil, &RejectedError{CheckLength, fmt.Sprintf("no rows from %s", from)}
		}

		// Check 2, anchor: the first row of the first chunk is the anchor at its height.
		if first {
			if rows[0].Hash != anchor || rows[0].Height != anchorHeight {
				return nil, &RejectedError{CheckAnchor, fmt.Sprintf("first row is %s at %d, anchor is %s at %d", rows[0].Hash, rows[0].Height, anchor, anchorHeight)}
			}

			first = false
		} else {
			// Check 5, chunk join: this chunk's top continues the previous chunk's bottom.
			if rows[0].Hash != prevBottomPrev {
				return nil, &RejectedError{CheckChunkJoin, fmt.Sprintf("chunk top %s does not continue the previous chunk's parent %s", rows[0].Hash, prevBottomPrev)}
			}

			if rows[0].Height != wantHeight {
				return nil, &RejectedError{CheckDescent, fmt.Sprintf("chunk top at %d, wanted %d", rows[0].Height, wantHeight)}
			}
		}

		for i := range rows {
			r := rows[i]

			// Check 3, descent: heights fall by exactly one.
			if r.Height != wantHeight {
				return nil, &RejectedError{CheckDescent, fmt.Sprintf("row %s at %d, wanted %d", r.Hash, r.Height, wantHeight)}
			}

			// Check 4, linkage: each row's previous hash names the next row.
			if i+1 < len(rows) && r.PrevHash != rows[i+1].Hash {
				return nil, &RejectedError{CheckLinkage, fmt.Sprintf("row %s at %d names parent %s, next row is %s", r.Hash, r.Height, r.PrevHash, rows[i+1].Hash)}
			}

			if r.Height >= lo {
				idx := r.Height - lo
				if seen[idx] {
					return nil, &RejectedError{CheckDescent, fmt.Sprintf("height %d seen twice", r.Height)}
				}

				seen[idx] = true
				ids[idx] = r.ID

				if !r.MinedSet {
					notMined = append(notMined, r.Height)
				}
			}

			if wantHeight == 0 {
				break
			}

			wantHeight--
		}

		last := rows[len(rows)-1]
		if last.Height == 0 {
			// The walk reached genesis. Everything from lo up is filled, or the chain is shorter
			// than the span asked for, which the seen check below catches.
			break
		}

		prevBottomPrev = last.PrevHash
		from = last.PrevHash
		remaining -= uint32(len(rows)) //nolint:gosec // len fits
	}

	for i := range seen {
		if !seen[i] {
			return nil, &RejectedError{CheckLength, fmt.Sprintf("height %d never arrived", lo+uint32(i))} //nolint:gosec // bounded by span
		}
	}

	return New(lo, anchor, ids, notMined)
}
