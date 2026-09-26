package netsync

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// THE FRONTIER RACE, SV Node's rule. A block comes from one peer at a time. When the peer sending
// the block the chain needs is struggling, delivering under raceStallRate after raceSlowFetchAfter,
// one other peer is asked for it and the struggling peer is disconnected, as SV Node drops a
// staller. Disconnecting stops its half-converted copy, so the one extra copy converts instead of
// being drained as a duplicate. A block is raced at most once: on 2026-09-24 a race that re-armed
// whenever a copy finished asked three peers for the same 2 GB block and threw all three copies
// away while a 13 MB/s peer, which was not struggling at all, finished the first.
//
// SV Node: DEFAULT_BLOCK_DOWNLOAD_SLOW_FETCH_TIMEOUT is 30 s, DEFAULT_MIN_BLOCK_STALLING_RATE is
// 100 KB/s.
//
// The race judges only a block whose bytes have started. A peer that has not started sending a
// block is not struggling with it: it may be sending blocks queued ahead of it, or reading it from
// disk before its first byte. That case is handled by letting another peer be asked after
// blockRequestRetryInterval, whose comment explains what an SV Node peer is doing while it is
// quiet. Do not extend the race to it.

const (
	// raceCheckInterval is how often the race is considered. It runs on its own ticker because
	// while the chain waits on a block no commit arrives to trigger anything else.
	raceCheckInterval = 5 * time.Second
	// raceSlowFetchAfter is how long a block must have been arriving before its peer is judged.
	raceSlowFetchAfter = 30 * time.Second
	// raceStallRate is the delivery rate, in bytes a second, below which a peer is struggling.
	raceStallRate = 100_000
	// peerRateWeight is the weight of a peer's newest completed block in its rolling rate.
	peerRateWeight = 0.5
	// raceExpiry is how long a block's race mark lasts. It is never cleared sooner, so a block is
	// raced at most once in that time.
	raceExpiry = 10 * time.Minute
)

// blockStream is one block body arriving from the wire.
type blockStream struct {
	hash   chainhash.Hash
	height int32
	// owner is the one peer the ledger says owes this block, or nil when it is not exactly one.
	owner *peerpkg.Peer
	total int64
	read  atomic.Int64
	// lastRead is when bytes last arrived for this block, in unix nanoseconds.
	lastRead atomic.Int64
	// received is the node-wide count of block bytes received, or nil.
	received *atomic.Int64
	start    time.Time
	// requestedAt is when the ledger first recorded a request for this block, zero if none.
	requestedAt time.Time
	// admitWait and path are set by admitPipelineSink under the registry's lock: how long this
	// block waited for an admission slot, and which path its bytes took.
	admitWait time.Duration
	path      string
}

func (s *blockStream) rate(now time.Time) float64 {
	elapsed := now.Sub(s.start).Seconds()
	if elapsed <= 0 {
		return 0
	}

	return float64(s.read.Load()) / elapsed
}

// countingReader counts the bytes the sink reads from a block stream.
type countingReader struct {
	r io.Reader
	s *blockStream
}

func (c countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.s.read.Add(int64(n))
		c.s.lastRead.Store(time.Now().UnixNano())

		if c.s.received != nil {
			c.s.received.Add(int64(n))
		}
	}

	return n, err
}

// raceCandidate is why a block was picked, for the log line.
type raceCandidate struct {
	eta, need time.Duration
	rate      float64
}

// streamRegistry holds the blocks arriving now and each peer's rate on completed blocks.
type streamRegistry struct {
	mu     sync.Mutex
	active map[*blockStream]struct{}
	rates  map[*peerpkg.Peer]float64
	raced  map[chainhash.Hash]time.Time
	// lastBlock is when each peer last finished delivering a block.
	lastBlock map[*peerpkg.Peer]time.Time
}

func newStreamRegistry() *streamRegistry {
	return &streamRegistry{
		lastBlock: make(map[*peerpkg.Peer]time.Time),
		active:    make(map[*blockStream]struct{}),
		rates:     make(map[*peerpkg.Peer]float64),
		raced:     make(map[chainhash.Hash]time.Time),
	}
}

func (r *streamRegistry) start(hash chainhash.Hash, height int32, owner *peerpkg.Peer, total int64, now time.Time) *blockStream {
	s := &blockStream{hash: hash, height: height, owner: owner, total: total, start: now}

	r.mu.Lock()
	r.active[s] = struct{}{}
	r.mu.Unlock()

	return s
}

// finish removes a stream. A complete one records its owner's rate. The block's race mark is left
// alone, so it is never raced twice.
func (r *streamRegistry) finish(s *blockStream, now time.Time, complete bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.active, s)

	if !complete {
		return
	}

	if s.owner == nil {
		return
	}

	r.lastBlock[s.owner] = now

	if bps := s.rate(now); bps > 0 {
		if prev, ok := r.rates[s.owner]; ok {
			bps = peerRateWeight*bps + (1-peerRateWeight)*prev
		}

		r.rates[s.owner] = bps
	}
}

// pending is how many bytes are still to come on the blocks p is sending now, and how many
// blocks that is.
func (r *streamRegistry) pending(p *peerpkg.Peer) (int64, int) {
	if r == nil || p == nil {
		return 0, 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var (
		bytes int64
		n     int
	)

	for s := range r.active {
		if s.owner != p {
			continue
		}

		n++
		bytes += max(0, s.total-s.read.Load())
	}

	return bytes, n
}

// arriving reports whether bytes of block h are arriving now, from any peer.
func (r *streamRegistry) arriving(h chainhash.Hash) bool {
	if r == nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for s := range r.active {
		if s.hash == h {
			return true
		}
	}

	return false
}

// arrivingBytes is the declared size of every block arriving now, and how many there are.
func (r *streamRegistry) arrivingBytes() (int64, int) {
	if r == nil {
		return 0, 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var total int64
	for s := range r.active {
		total += s.total
	}

	return total, len(r.active)
}

// medianRate is the median of the peers' measured rates on completed blocks, or zero with none.
func (r *streamRegistry) medianRate() float64 {
	if r == nil {
		return 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	rates := make([]float64, 0, len(r.rates))
	for _, bps := range r.rates {
		rates = append(rates, bps)
	}

	if len(rates) == 0 {
		return 0
	}

	sort.Float64s(rates)

	return rates[len(rates)/2]
}

func (r *streamRegistry) peerRate(p *peerpkg.Peer) float64 {
	if r == nil {
		return 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.rates[p]
}

func (r *streamRegistry) markRaced(h chainhash.Hash, now time.Time) {
	r.mu.Lock()
	r.raced[h] = now
	r.mu.Unlock()
}

// forgetPeer drops a departed peer's rate and activity.
func (r *streamRegistry) forgetPeer(p *peerpkg.Peer) {
	r.mu.Lock()
	delete(r.rates, p)
	delete(r.lastBlock, p)
	r.mu.Unlock()
}

// lastBlockBytes is when block bytes last came from p: the latest byte of a block it is sending
// now, or the moment it finished its last block. Other traffic on the connection, pings and
// announcements, does not count: a peer that has dropped our request still sends those.
func (r *streamRegistry) lastBlockBytes(p *peerpkg.Peer) time.Time {
	if r == nil || p == nil {
		return time.Time{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	latest := r.lastBlock[p]

	for s := range r.active {
		if s.owner != p {
			continue
		}

		if at := s.lastRead.Load(); at > 0 {
			if t := time.Unix(0, at); t.After(latest) {
				latest = t
			}
		}
	}

	return latest
}

// expireRacesLocked drops race marks past their expiry.
func (r *streamRegistry) expireRacesLocked(now time.Time) {
	for h, at := range r.raced {
		if now.Sub(at) > raceExpiry {
			delete(r.raced, h)
		}
	}
}

// pickRace returns the lowest block above the tip whose one peer is struggling, if the chain will
// reach it before it arrives. tip is the committed height and commitRate the blocks a second
// joining the chain; with no rate measured the chain is taken to need the block now.
func (r *streamRegistry) pickRace(now time.Time, tip int32, commitRate float64) (*blockStream, raceCandidate, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.expireRacesLocked(now)

	var (
		best     *blockStream
		bestCand raceCandidate
	)

	for s := range r.active {
		if s.height <= tip || s.owner == nil || now.Sub(s.start) < raceSlowFetchAfter {
			continue
		}

		if _, raced := r.raced[s.hash]; raced {
			continue
		}

		rate := s.rate(now)
		if rate >= raceStallRate {
			continue
		}

		var need time.Duration
		if commitRate > 0 {
			need = time.Duration(float64(s.height-tip) / commitRate * float64(time.Second))
		}

		eta := time.Duration(1<<63 - 1)
		if remaining := s.total - s.read.Load(); rate > 0 {
			eta = time.Duration(float64(remaining) / rate * float64(time.Second))
		}

		if eta <= need {
			continue
		}

		if best == nil || s.height < best.height {
			best = s
			bestCand = raceCandidate{eta: eta, need: need, rate: rate}
		}
	}

	return best, bestCand, best != nil
}

// chooseRacer picks who to ask: never an owner, the fastest measured peer first, then the peer
// with the fewest blocks already queued, because a request waits behind what a peer already owes.
func (r *streamRegistry) chooseRacer(candidates, owners []*peerpkg.Peer, queued func(*peerpkg.Peer) int) *peerpkg.Peer {
	r.mu.Lock()
	defer r.mu.Unlock()

	isOwner := make(map[*peerpkg.Peer]bool, len(owners))
	for _, o := range owners {
		isOwner[o] = true
	}

	var best *peerpkg.Peer

	better := func(a, b *peerpkg.Peer) bool {
		ra, rb := r.rates[a], r.rates[b]
		if ra != rb {
			return ra > rb
		}

		return queued(a) < queued(b)
	}

	for _, p := range candidates {
		if p == nil || isOwner[p] {
			continue
		}

		if best == nil || better(p, best) {
			best = p
		}
	}

	return best
}

// trackBlockStreams wraps the installed block sink so every block body arriving is measured.
func (sm *SyncManager) trackBlockStreams(inner func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error)) func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error) {
	return func(hash chainhash.Hash, header *wire.BlockHeader, r io.Reader, n int64) (bool, error) {
		height, _ := sm.headerCache.HeightOf(hash)

		var owner *peerpkg.Peer
		if owners := sm.blockDownloads.OwnersOf(hash); len(owners) == 1 {
			owner = owners[0]
		}

		s := sm.streams.start(hash, height, owner, n, time.Now())
		s.received = &sm.waste.received
		if at, ok := sm.blockDownloads.RequestedAt(hash); ok {
			s.requestedAt = at
		}

		converted, err := inner(hash, header, countingReader{r: r, s: s}, n)

		now := time.Now()

		// Complete when the sink succeeded. The reader starts after the 80-byte header, so the
		// bytes counted never reach n, and a test against n counted no stream as complete.
		complete := err == nil
		sm.streams.finish(s, now, complete)

		sm.streams.mu.Lock()
		drained := s.path == admitRawDuplicate
		sm.streams.mu.Unlock()

		switch {
		case !complete:
			sm.waste.streamsFailed.Add(1)
			sm.waste.bytesWasted.Add(s.read.Load())
		case drained:
			sm.waste.bytesWasted.Add(s.read.Load())
		}

		// The size ladder and the queue estimate read the average block size. Only the path
		// that decodes a whole block used to feed it, and with the park on that path never
		// runs, so every block has to feed it here.
		if complete && sm.blockSizeTracker != nil {
			sm.blockSizeTracker.addBlockSize(n)
		}

		// The committed tip is read only for a download worth reporting: it is a blockchain
		// call, and this runs on the peer's read loop for every block.
		sm.streams.mu.Lock()
		_, worth := s.report(now, s.height)
		sm.streams.mu.Unlock()

		if worth {
			tip, _, _ := sm.committedTip()

			sm.streams.mu.Lock()
			line, _ := s.report(now, tip)
			sm.streams.mu.Unlock()

			sm.logger.Infof("[blockDownload][%s] %s", hash, line)
		}

		return converted, err
	}
}

// runFrontierRace considers the race every raceCheckInterval until the manager quits.
func (sm *SyncManager) runFrontierRace() {
	ticker := time.NewTicker(raceCheckInterval)
	defer ticker.Stop()

	ticks := 0

	for {
		select {
		case <-sm.quit:
			return
		case <-ticker.C:
			sm.maybeRaceSlowBlock(time.Now())

			if ticks++; ticks%queueReportEvery == 0 {
				sm.logDownloadQueues()
			}
		}
	}
}

// maybeRaceSlowBlock asks a second peer for the block the chain is about to wait on, if one is
// arriving from a slow peer. The committed tip is read here, outside any lock.
func (sm *SyncManager) maybeRaceSlowBlock(now time.Time) {
	if sm.streams == nil {
		return
	}

	tip, _, ok := sm.committedTip()
	if !ok {
		return
	}

	s, c, ok := sm.streams.pickRace(now, tip, sm.commitRate.rate())
	if !ok {
		return
	}

	eligible := sm.eligibleBlockPeers()
	candidates := make([]*peerpkg.Peer, 0, len(eligible))

	for _, bp := range eligible {
		if bp.state != nil && bp.state.BestKnownHeight() > 0 && bp.state.BestKnownHeight() < s.height {
			continue
		}

		candidates = append(candidates, bp.peer)
	}

	racer := sm.streams.chooseRacer(candidates, sm.blockDownloads.OwnersOf(s.hash), sm.blockDownloads.CountForPeer)
	if racer == nil {
		sm.logger.Debugf("[frontierRace][%s] block %d is arriving at %.0f KB/s but there is no other peer to ask", s.hash, s.height, c.rate/1e3)

		return
	}

	if !sm.askRacer(racer, s.hash, now) {
		return
	}

	// Dropped as SV Node drops a staller. Its copy stops converting, which frees the block for
	// the one extra copy; left connected, the extra copy would arrive as a duplicate and be
	// drained unwritten.
	s.owner.DisconnectWithInfo(fmt.Sprintf("stalling on block %d at %.0f KB/s", s.height, c.rate/1e3))

	sm.logger.Infof("[frontierRace][%s] asked %s for block %d and dropped %s, which was sending it at %.0f KB/s after %s",
		s.hash, racer, s.height, s.owner, c.rate/1e3, now.Sub(s.start).Round(time.Second))
}

// askRacer records racer as a second owner of h and sends it the getdata. Recording first means
// whichever copy lands second is still admitted rather than costing a peer its connection.
func (sm *SyncManager) askRacer(racer *peerpkg.Peer, h chainhash.Hash, now time.Time) bool {
	if !sm.blockDownloads.Add(racer, h) {
		return false
	}

	getData := wire.NewMsgGetDataSizeHint(1)
	if err := getData.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &h)); err != nil {
		sm.blockDownloads.RemoveOwner(racer, h)

		return false
	}

	sm.streams.markRaced(h, now)
	racer.QueueMessage(getData, nil)

	if prometheusLegacyNetsyncFrontierRaces != nil {
		prometheusLegacyNetsyncFrontierRaces.Inc()
	}

	return true
}

// queueReportEvery is how many race checks pass between download queue reports: every 30 s.
const queueReportEvery = 6

// logDownloadQueues reports, for each eligible peer, what it owes and what it is sending, and in
// one summary line how many peers are idle or below streamingPeerDepth requests, and the bytes
// really held ahead of the chain against the disk backstop. The download is judged on that line:
// no peer below two requests unless the backstop is reached.
func (sm *SyncManager) logDownloadQueues() {
	if sm.streams == nil || sm.blockSizeTracker == nil || sm.blockDownloads == nil {
		return
	}

	largest := sm.blockSizeTracker.largestRecentSize()
	eligible := sm.eligibleBlockPeers()
	idle, short := 0, 0
	depth := sm.streamingPeerDepth()
	var fastest float64
	for _, bp := range eligible {
		fastest = max(fastest, sm.streams.peerRate(bp.peer))
	}

	for _, bp := range eligible {
		owed := sm.blockDownloads.CountForPeer(bp.peer)
		remaining, sending := sm.streams.pending(bp.peer)

		peerDepth := sm.peerQueueDepth(bp.peer, depth, fastest)

		if owed == 0 && sending == 0 {
			idle++
		}

		if owed < peerDepth {
			short++
		}

		sm.logger.Infof("[downloadQueue] %s owes %d of %d, sending %d with %.0f MB left, rate %.1f MB/s",
			bp.peer, owed, peerDepth, sending, float64(remaining)/1e6, sm.streams.peerRate(bp.peer)/1e6)
	}

	sm.logger.Infof("[downloadQueue] %d eligible peers, %d idle; %d below their speed-scaled depth of up to %d requests; %.1f GB held ahead of the chain, parked and arriving, against a %.1f GB backstop; %d blocks owed; receiving %.1f MB/s",
		len(eligible), idle, short, depth, float64(sm.bytesAhead(largest))/1e9, float64(parkBackstopBytes)/1e9, sm.blockDownloads.Len(), sm.waste.rateSinceLast(time.Now())/1e6)

	w := &sm.waste
	sm.logger.Infof("[downloadWaste] since start: received %.1f GB; duplicate copies drained %d, converted %d; streams cut part way %d; %.1f GB wasted; peers dropped owing blocks %d (%d blocks); blocks re-asked after a quiet peer %d",
		float64(w.received.Load())/1e9, w.dupDrained.Load(), w.dupConverted.Load(), w.streamsFailed.Load(),
		float64(w.bytesWasted.Load())/1e9, w.droppedOwing.Load(), w.blocksOwedAtDrop.Load(), w.reAskedQuiet.Load())
}
