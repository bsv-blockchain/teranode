package subtreevalidation

import (
	"context"
	"net/url"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// skippedSubtreeMaxAge is how old a kept subtree may be and still be replayed. A catchup can be
// followed by an operator STOP that lasts days; by then the peers may have pruned the subtree, and
// a replay that cannot fetch it reports the peer.
const skippedSubtreeMaxAge = 30 * time.Minute

// skippedSubtree is a peer subtree announced while the node was catching up.
type skippedSubtree struct {
	hash       chainhash.Hash
	baseURL    *url.URL
	peerID     string
	recordedAt time.Time
}

// skippedSubtrees keeps the peer subtrees announced while the node was catching up, so they are
// validated once it is RUNNING again instead of lost. Dropping them left the next block with every
// subtree announced during a catchup unvalidated (286 subtrees and 300M txmeta reads from the UTXO
// store on scale-3, 2026-10-26). Entries keep announcement order, also when a replay keeps one
// again; when full, the oldest announcement goes, since the newest are the ones the next block
// needs. The zero value is ready to use.
type skippedSubtrees struct {
	mu      sync.Mutex
	entries map[chainhash.Hash]skippedSubtree
	order   []chainhash.Hash // announcement order, oldest first

	// replaying is set while a replay runs, so only one runs at a time.
	replaying atomic.Bool

	// watching is set while a goroutine waits for RUNNING to start the replay.
	watching atomic.Bool
}

// record keeps entry at its place in announcement order (recordedAt, now when unset). When max are
// already kept the oldest announcement goes, which is entry itself when it is older than all of
// them. A subtree already kept is left as it is. It reports whether entry is kept and whether an
// entry was dropped for the cap. With max 0 or less nothing is kept.
func (s *skippedSubtrees) record(entry skippedSubtree, max int) (added, evicted bool) {
	if max <= 0 {
		return false, false
	}

	if entry.recordedAt.IsZero() {
		entry.recordedAt = time.Now()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.entries[entry.hash]; ok {
		return false, false
	}

	if s.entries == nil {
		s.entries = make(map[chainhash.Hash]skippedSubtree)
	}

	// New announcements go at the back; an entry kept again by a replay goes back among them.
	i := sort.Search(len(s.order), func(i int) bool {
		return s.entries[s.order[i]].recordedAt.After(entry.recordedAt)
	})

	for len(s.order) >= max {
		if i == 0 {
			return false, true
		}

		delete(s.entries, s.order[0])
		s.order = s.order[1:]
		i--
		evicted = true
	}

	s.entries[entry.hash] = entry
	s.order = slices.Insert(s.order, i, entry.hash)

	return true, evicted
}

// take removes every kept subtree and returns those younger than skippedSubtreeMaxAge, oldest
// first.
func (s *skippedSubtrees) take() []skippedSubtree {
	s.mu.Lock()
	defer s.mu.Unlock()

	taken := make([]skippedSubtree, 0, len(s.order))
	for _, hash := range s.order {
		if entry := s.entries[hash]; time.Since(entry.recordedAt) <= skippedSubtreeMaxAge {
			taken = append(taken, entry)
		} else {
			prometheusSkippedSubtrees.WithLabelValues("expired").Inc()
		}
	}

	s.entries = nil
	s.order = nil

	return taken
}

func (s *skippedSubtrees) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.order)
}

// recordSkippedSubtreeMessage keeps a peer subtree announced while the node is catching up. A
// malformed message is ignored without the malformed-message metric, which stays a RUNNING-only
// signal. A blocks-only node never validates peer subtrees, so it keeps none.
func (u *Server) recordSkippedSubtreeMessage(ctx context.Context, state blockchain.FSMStateType, value []byte) {
	if u.settings.SubtreeValidation.BlocksOnly {
		return
	}

	var kafkaMsg kafkamessage.KafkaSubtreeTopicMessage
	if err := proto.Unmarshal(value, &kafkaMsg); err != nil {
		return
	}

	hash, err := chainhash.NewHashFromStr(kafkaMsg.Hash)
	if err != nil {
		return
	}

	baseURL, err := url.Parse(kafkaMsg.URL)
	if err != nil {
		return
	}

	if u.recordSkippedSubtree(ctx, state, skippedSubtree{hash: *hash, baseURL: baseURL, peerID: kafkaMsg.PeerId, recordedAt: time.Now()}) {
		prometheusSkippedSubtrees.WithLabelValues("recorded").Inc()
	}
}

// recordSkippedSubtree keeps a peer subtree that was not validated because the node is catching
// up, and reports whether it is kept. Only CATCHINGBLOCKS: IDLE (an operator STOP) can last days,
// by which time the peers may have pruned the subtree, and a replay that cannot fetch it reports
// the peer. A kept subtree starts a watcher, unless one is waiting, that replays once the node is
// RUNNING.
func (u *Server) recordSkippedSubtree(ctx context.Context, state blockchain.FSMStateType, entry skippedSubtree) bool {
	if state != blockchain.FSMStateCATCHINGBLOCKS {
		return false
	}

	added, evicted := u.skippedSubtrees.record(entry, u.settings.SubtreeValidation.SkippedSubtreeBackfillMax)
	if added {
		u.watchForRunning(ctx)
	}

	if evicted {
		prometheusSkippedSubtrees.WithLabelValues("evicted").Inc()
	}

	return added
}

// watchForRunning starts, unless one is already waiting, a goroutine that replays the kept
// subtrees as soon as the node is RUNNING. Catchup sends RUN only after its last block, and the
// blockchain client does not pass FSM notifications to subscribers, so without it the replay would
// wait for the next peer subtree message or block; on a quiet network that is the next block,
// validated with the kept subtrees still unvalidated.
func (u *Server) watchForRunning(ctx context.Context) {
	if !u.skippedSubtrees.watching.CompareAndSwap(false, true) {
		return
	}

	go func() {
		err := u.blockchainClient.WaitForFSMtoTransitionToGivenState(ctx, blockchain.FSMStateRUNNING)

		// Cleared before the replay starts: a replay that is running already starts a new watcher
		// when it ends, for the subtrees kept meanwhile, and that one must not find this one.
		u.skippedSubtrees.watching.Store(false)

		if err != nil {
			return
		}

		u.replaySkippedSubtrees(ctx)

		if u.watcherReplayCalledFn != nil {
			u.watcherReplayCalledFn()
		}
	}()
}

// replayLimit is how many kept subtrees are validated at once: half the peer-subtree admission
// slots (at least one), so live peer subtrees keep the rest.
func replayLimit(peerSubtreeConcurrency int) int {
	if peerSubtreeConcurrency <= 0 {
		peerSubtreeConcurrency = runtime.NumCPU()
	}

	return max(1, peerSubtreeConcurrency/2)
}

// replaySkippedSubtreesIfRunning starts a replay when subtrees are kept and the node is RUNNING.
// The block notification calls it, so a block that arrives before any peer subtree message still
// sets the replay going.
func (u *Server) replaySkippedSubtreesIfRunning(ctx context.Context) {
	if u.skippedSubtrees.len() == 0 {
		return
	}

	state, err := u.blockchainClient.GetFSMCurrentState(ctx)
	if err != nil || state == nil || *state != blockchain.FSMStateRUNNING {
		return
	}

	u.replaySkippedSubtrees(ctx)
}

// replaySkippedSubtrees validates the kept subtrees in the background, in announcement order,
// with the admission limit, the RUNNING check and the quorum lock (which skips a subtree validated
// meanwhile, for example by a block) of a live peer subtree. A subtree that meets a catching-up
// node again is kept again, with its announcement time. Subtrees kept while it runs are replayed
// when it ends, once the node is RUNNING. It returns at once, and does nothing while a replay is
// running or nothing is kept.
func (u *Server) replaySkippedSubtrees(ctx context.Context) {
	if u.skippedSubtrees.len() == 0 || !u.skippedSubtrees.replaying.CompareAndSwap(false, true) {
		return
	}

	entries := u.skippedSubtrees.take()

	go func() {
		u.logger.Infof("[replaySkippedSubtrees] validating %d peer subtrees announced while the node was catching up", len(entries))

		// A plain group on ctx, not errgroup.WithContext: a subtree kept again during the replay
		// starts a watcher with this ctx, which must outlive the replay.
		var g errgroup.Group

		g.SetLimit(replayLimit(u.settings.SubtreeValidation.PeerSubtreeConcurrency))

		for _, entry := range entries {
			if ctx.Err() != nil {
				break
			}

			g.Go(func() error {
				outcome, err := u.replaySubtree(ctx, entry)

				switch {
				case err == nil:
					if outcome != "" {
						prometheusSkippedSubtrees.WithLabelValues(outcome).Inc()
					}
				case ctx.Err() != nil:
					// shutting down
				default:
					// Like a live peer subtree, a failed one is not retried: block validation
					// validates it if a block needs it.
					prometheusSkippedSubtrees.WithLabelValues("replay_failed").Inc()
					u.logger.Warnf("[replaySkippedSubtrees] subtree %s from %s: %v", entry.hash.String(), entry.peerID, err)
				}

				return nil
			})
		}

		_ = g.Wait()

		// Cleared before the check, so a subtree kept after it starts its own watcher. One kept
		// earlier started a watcher that found this replay running and gave up.
		u.skippedSubtrees.replaying.Store(false)

		if ctx.Err() == nil && u.skippedSubtrees.len() > 0 {
			u.watchForRunning(ctx)
		}
	}()
}

// replaySubtree validates one kept subtree once it has an admission slot, and returns the metric
// outcome: replayed (validated, or already stored), expired, requeued (kept again because the node
// is catching up again), dropped (the node is IDLE), or none (evicted for a newer announcement, or
// another validation holds its lock).
func (u *Server) replaySubtree(ctx context.Context, entry skippedSubtree) (string, error) {
	releaseAdmission, err := u.admitPeerSubtree(ctx)
	if err != nil {
		return "", err
	}
	defer releaseAdmission()

	// Checked again after the wait for a slot: the peer may have pruned an older subtree, and a
	// fetch that fails reports the peer.
	if time.Since(entry.recordedAt) > skippedSubtreeMaxAge {
		return "expired", nil
	}

	state, err := u.blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		return "", errors.NewProcessingError("[replaySkippedSubtrees] failed to get FSM current state", err)
	}

	if *state != blockchain.FSMStateRUNNING {
		// IDLE (an operator STOP, or the blockchain client's cached IDLE after its notification
		// stream broke) keeps nothing; block validation validates the subtree if a block needs it.
		if *state != blockchain.FSMStateCATCHINGBLOCKS {
			return "dropped", nil
		}

		if u.recordSkippedSubtree(ctx, *state, entry) {
			return "requeued", nil
		}

		return "", nil
	}

	if u.replaySubtreeFn != nil {
		return "replayed", u.replaySubtreeFn(ctx, &entry.hash, entry.baseURL, entry.peerID)
	}

	result, err := u.validateAdmittedPeerSubtree(ctx, &entry.hash, entry.baseURL, entry.peerID)
	if err != nil || result == peerSubtreeLocked {
		return "", err
	}

	return "replayed", nil
}
