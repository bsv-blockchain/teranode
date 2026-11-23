package subtreeprocessor

import (
	"sync"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
)

// SubtreePins counts the readers of mmap-backed subtrees that closeRetiredSubtrees must not
// unmap under them. Heap subtrees need no close, so they are never pinned and the registry
// stays empty with mmap off. The zero value is ready to use.
type SubtreePins struct {
	mu sync.Mutex
	n  map[*subtreepkg.Subtree]int
}

// Pinned lists the subtrees with at least one pin.
func (p *SubtreePins) Pinned() []*subtreepkg.Subtree {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]*subtreepkg.Subtree, 0, len(p.n))
	for st := range p.n {
		out = append(out, st)
	}

	return out
}

// NewHold returns an empty hold on p.
func (p *SubtreePins) NewHold() *SubtreeHold {
	return &SubtreeHold{pins: p}
}

// SubtreeHold is the set of pins one reader took, released together.
type SubtreeHold struct {
	pins *SubtreePins
	held []*subtreepkg.Subtree
}

// Pin keeps the mmap-backed subtrees among sts mapped until Release. The caller must know
// they are not closed yet: see PinCurrent for the mining data and the job store for jobs.
func (h *SubtreeHold) Pin(sts ...*subtreepkg.Subtree) {
	h.pins.mu.Lock()
	defer h.pins.mu.Unlock()

	if h.pins.n == nil {
		h.pins.n = make(map[*subtreepkg.Subtree]int)
	}

	for _, st := range sts {
		if st != nil && st.IsMmapBacked() {
			h.pins.n[st]++
			h.held = append(h.held, st)
		}
	}
}

// Release drops every pin of the hold. It is safe to call more than once.
func (h *SubtreeHold) Release() { h.releaseFrom(0) }

// PinCurrent loads the precomputed mining data and pins its subtrees, retrying until the
// data it pinned is still the current data. Data that is still current was held by the
// processor at every close until the pin existed, and it is replaced only on the processor
// goroutine, so a close that runs after the replacement sees the pin.
func (h *SubtreeHold) PinCurrent(load func() *PrecomputedMiningData) *PrecomputedMiningData {
	for {
		data := load()
		if data == nil || len(data.Subtrees) == 0 {
			return data
		}

		mark := len(h.held)

		h.Pin(data.Subtrees...)

		if load() == data {
			return data
		}

		// replaced while pinning: whatever it pinned may already have been closed
		h.releaseFrom(mark)
	}
}

func (h *SubtreeHold) releaseFrom(mark int) {
	h.pins.mu.Lock()
	defer h.pins.mu.Unlock()

	for _, st := range h.held[mark:] {
		if h.pins.n[st]--; h.pins.n[st] <= 0 {
			delete(h.pins.n, st)
		}
	}

	h.held = h.held[:mark]
}
