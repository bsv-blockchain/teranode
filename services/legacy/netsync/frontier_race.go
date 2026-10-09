package netsync

import (
	"fmt"
	"io"
	"math"
	"slices"
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
// staller. Disconnecting stops its half-converted copy, so the extra copy converts instead of
// being drained as a duplicate. A block is raced only when every copy of it is struggling, at most
// once in raceSlowFetchAfter, and another peer is asked only while the block has fewer than
// maxBlockCopies live copies: on 2026-09-24 a race that re-armed whenever a copy finished asked
// three peers for the same 2 GB block and threw all three copies away while a 13 MB/s peer, which
// was not struggling at all, finished the first. A live copy is an owner not let off the block or
// one sending it now (liveCopies), so a forgiven owner that sends nothing still owes the block but
// is not counted, and more than maxBlockCopies peers can owe it. At the cap the struggling peers
// are still disconnected and only the extra request is skipped, unless an owner asked again after
// it was let off is still connected, still owes the block and sends nothing
// (reaskedOwnersNotSending). Below the cap the struggling peers are kept when the peer just asked
// is itself an owner asked again. Both keep an honest slow peer connected while the only other
// peer for the block is one that went quiet on it. In any raceExpiry a block gets at most
// maxBlockCopies-1 extra copies and one round of disconnects, and nobody is disconnected while
// this node itself is backpressured.
//
// SV Node: DEFAULT_BLOCK_DOWNLOAD_SLOW_FETCH_TIMEOUT is 30 s, DEFAULT_MIN_BLOCK_STALLING_RATE is
// 100 KB/s.
//
// The race judges only a block whose bytes have started, and only in headers-first mode:
// trackBlockStreams takes a stream's height from the header cache, which is empty above the last
// checkpoint, so pickRace's height test skips every tip stream. That makes the race narrower than SV
// Node's parallel fetch, which after DEFAULT_BLOCK_DOWNLOAD_SLOW_FETCH_TIMEOUT (30 s) asks another
// peer for the first in-flight block of a peer whose block-stream bandwidth is below the stalling
// rate, bytes started or not (FindNextBlocksToDownload, net_processing.cpp:462-507, capped at
// DEFAULT_MAX_BLOCK_PARALLEL_FETCH, 3). A peer that has not started sending a block is not
// struggling with it here: it may be sending blocks queued ahead of it, or reading it from disk
// before its first byte. Above the last checkpoint that case is handled by the download pass,
// assignWantedBlocks, which after blockRequestRetryInterval lets a quiet owner off and asks another
// peer, one head-of-queue block per quiet owner (appendOutstandingAtTip). The 60-second quiet
// rule is the looser cousin of SV Node's: any quiet owner rather than a bandwidth test, 60 s rather
// than 30, one re-ask per owner per retry window rather than three parallel fetches.
// blockRequestRetryInterval's comment explains what an SV Node peer is doing while it is quiet. Do
// not extend the race to it. Below the last checkpoint a block that has not started, because it
// waits behind other blocks at a busy peer or its owner is quiet, is the watcher's (watcher.go),
// which runs on this ticker, keeps the owner, and shares the race's mark.

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
	// raceExpiry is how long a block's race history is kept: when each extra copy was asked for
	// and when the race last dropped its peers. Another copy is asked for only after
	// raceSlowFetchAfter, only when every copy is late, and at most maxBlockCopies-1 times in
	// raceExpiry; the race drops a block's peers at most once in raceExpiry (maybeRaceSlowBlock).
	raceExpiry = 10 * time.Minute
	// maxBlockCopies is the most live copies of one block at once: the first request and at most
	// two extra copies, from the race or the watcher. A live copy is an owner not let off
	// the block, or one sending it now (liveCopies); a forgiven owner sending nothing is not a
	// copy. SV Node fetches the first in-flight block from up to DEFAULT_MAX_BLOCK_PARALLEL_FETCH
	// (3) peers (net/net.h:167). It asks for another copy only when every connected peer the
	// block is in flight from is stalling, and counts those stallers against the cap
	// (stallerCount < maxParallelFetch, net/net_processing.cpp:464-496). Its DetectStalling then
	// disconnects a staller (net_processing.cpp:5446-5466), which takes it out of the count. A
	// forgiven owner here is never disconnected for its silence, so it is not counted.
	maxBlockCopies = 3
	// minRateSample is the least delivery time one rate sample covers. A block that took less is
	// pooled with the peer's next blocks until together they took this long. A transfer that short
	// is mostly round trip and buffering: on 2026-10-07 one 1 MB copy read in 5 ms gave a peer a
	// rate of 190 MB/s, and one such sample in the rolling rate made that peer most of the measured
	// bandwidth. Pooling rather than dropping keeps small blocks measured: at early heights every
	// block takes far less than a second, and a peer never measured holds one block.
	minRateSample = time.Second
	// rateDecayAfter is how long a peer that owes blocks may send no block bytes before its rate
	// starts to fall: a round trip and SV Node reading the next block from disk fit in it.
	rateDecayAfter = 10 * time.Second
	// rateDecayHalfLife is how long a silent peer's rate takes to halve after rateDecayAfter. SV
	// Node averages each peer's block-stream bandwidth over its last 60 s in 5 s spots
	// (net/stream.cpp:312-346, net/stream.h:173, net/net.cpp:2617), so a peer that stops sending is at half its rate
	// 30 s later. The rate here never reaches zero, which would read as unmeasured.
	rateDecayHalfLife = raceSlowFetchAfter
	// liveRateWindow is the window of a copy's live rate (judgedRateLocked). A copy is judged on
	// the lower of its average and its rate over this window once it is this old, as SV Node
	// judges a peer on its block-stream bandwidth over a recent window (net/stream.cpp:312-346).
	liveRateWindow = raceSlowFetchAfter
)

// blockStream is one block body arriving from the wire.
type blockStream struct {
	hash   chainhash.Hash
	height int32
	// owner is the peer sending this copy, when the download ledger says that peer owes the
	// block. It is nil for a copy from a peer that does not owe the block: such a copy is drained
	// unwritten (admitPipelineSink), and gives no peer a rate, an activity time or a size sample,
	// and does not count as the block arriving. Every field is set before the stream is published
	// (add), because the sync manager reads them from other goroutines.
	owner *peerpkg.Peer
	// sender is the peer sending this copy, resolved to its association primary, owed or not, or
	// nil when the reader names no peer. Its bytes keep that peer's connection busy whether or
	// not they are kept, so they count in what it is sending (pending).
	sender *peerpkg.Peer
	total  int64
	read   atomic.Int64
	// lastRead is when bytes last arrived for this block, or when its admission slot was granted
	// if later (admit), in unix nanoseconds.
	lastRead atomic.Int64
	// received is the node-wide count of block bytes received, or nil.
	received *atomic.Int64
	// start is when the copy's clock starts: its first byte, moved to when its admission slot
	// was granted if it waited for one (admit). Written under the registry's lock after add.
	start time.Time
	// awaiting is true while the copy waits for an admission slot (admitPipelineSink). This node
	// reads none of its bytes then, so the copy is not judged and its owner is not quiet.
	awaiting atomic.Bool
	// waited is how long the copy waited for admission, the amount start was moved forward.
	// Written under the registry's lock.
	waited time.Duration
	// samples is the bytes read at moments in the last liveRateWindow and the newest moment
	// before it, oldest first (sampleStreams). Written under the registry's lock.
	samples []readSample
	// requestedAt is when the ledger first recorded a request for this block, zero if none.
	requestedAt time.Time
	// from is when owner could start sending this block: the later of when it was asked for it and
	// when it finished the block before, and never after the first byte. A completed block is timed
	// from it, so a peer that takes long to start a block is not measured as fast.
	from time.Time
	// admitWait and path are set by admitPipelineSink under the registry's lock: how long this
	// block waited for an admission slot, and which path its bytes took.
	admitWait time.Duration
	path      string
}

// readSample is the bytes a copy had read at a moment.
type readSample struct {
	at   time.Time
	read int64
}

// complete reports whether this node has read the copy's full declared length. total is the
// block's wire payload with its header, and the reader starts after the wire.MaxBlockHeaderPayload
// (80) header bytes, so the full length is total less the header. A complete copy is not judged:
// its stream stays active while it waits to take over from a slower copy (raceDuplicateCopy) or
// while it converts, and with no more bytes to read its live rate falls to zero.
func (s *blockStream) complete() bool {
	return s.total > wire.MaxBlockHeaderPayload && s.read.Load() >= s.total-wire.MaxBlockHeaderPayload
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
	eta, need, age time.Duration
	rate           float64
}

// streamRegistry holds the blocks arriving now and each peer's rate on completed blocks.
type streamRegistry struct {
	mu     sync.Mutex
	active map[*blockStream]struct{}
	rates  map[*peerpkg.Peer]float64
	// raced is each block's race history over the last raceExpiry: when extra copies were asked
	// for, by the race or the watcher, and when the race last dropped its copies.
	raced map[chainhash.Hash]*raceHistory
	// lastBlock is when each peer last finished delivering a block.
	lastBlock map[*peerpkg.Peer]time.Time
	// pooled is each peer's completed blocks not yet in its rate, until they cover minRateSample.
	pooled map[*peerpkg.Peer]rateSample
	// decay is the factor a peer's rate is cut to while it owes blocks and sends none
	// (decayQuiet). It stays until the peer's next rate sample.
	decay map[*peerpkg.Peer]float64
	// remembered is the last rate of each peer address this process has not measured since it
	// started, from the rates file (peerRatesFile) or from a peer that disconnected.
	remembered map[string]float64
}

// raceHistory is what the race and the watcher did for one block in the last raceExpiry. It is
// kept apart from the ledger: a dropped peer leaves the ledger (ClearPeer), so the live owners
// cannot tell how many copies a block has already cost.
type raceHistory struct {
	// asked is when each extra copy was asked for, oldest first.
	asked []time.Time
	// dropped is when the race last disconnected the peers sending this block, or zero.
	dropped time.Time
	// reasked is each owner asked for this block again after it was forgiven, and when.
	reasked map[*peerpkg.Peer]time.Time
}

// newest is when the newest extra copy was asked for, or zero.
func (h *raceHistory) newest() time.Time {
	if h == nil || len(h.asked) == 0 {
		return time.Time{}
	}

	return h.asked[len(h.asked)-1]
}

// rateSample is block bytes delivered and the time they took.
type rateSample struct {
	bytes int64
	took  time.Duration
}

func newStreamRegistry() *streamRegistry {
	return &streamRegistry{
		lastBlock:  make(map[*peerpkg.Peer]time.Time),
		pooled:     make(map[*peerpkg.Peer]rateSample),
		decay:      make(map[*peerpkg.Peer]float64),
		active:     make(map[*blockStream]struct{}),
		rates:      make(map[*peerpkg.Peer]float64),
		raced:      make(map[chainhash.Hash]*raceHistory),
		remembered: make(map[string]float64),
	}
}

func (r *streamRegistry) start(hash chainhash.Hash, height int32, owner *peerpkg.Peer, total int64, now time.Time) *blockStream {
	s := &blockStream{hash: hash, height: height, owner: owner, sender: owner, total: total, start: now}
	r.add(s)

	return s
}

// add publishes s. Readers take r.mu and the sink goroutine that built s does not, so a field
// that is not atomic must not change after this unless it is written under r.mu, as path and
// admitWait are.
func (r *streamRegistry) add(s *blockStream) {
	r.mu.Lock()
	s.samples = []readSample{{at: s.start, read: s.read.Load()}}
	r.active[s] = struct{}{}
	r.mu.Unlock()
}

// sampleStreams records the bytes each copy has read at now, for its live rate. The race's ticker
// calls it every raceCheckInterval.
func (r *streamRegistry) sampleStreams(now time.Time) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for s := range r.active {
		s.samples = append(s.samples, readSample{at: now, read: s.read.Load()})

		// Keep the newest sample at or before the window's start and each one after it.
		cut := 0

		for i, smp := range s.samples {
			if now.Sub(smp.at) >= liveRateWindow {
				cut = i
			}
		}

		s.samples = s.samples[cut:]
	}
}

// judgedRateLocked is the rate s is judged on: its average, or once it is liveRateWindow old, the
// lower of that and its rate since the newest sample at least liveRateWindow before now. A burst
// then a stall stayed above raceStallRate on the average alone for hours. Called with r.mu held.
func (r *streamRegistry) judgedRateLocked(s *blockStream, now time.Time) float64 {
	rate := s.rate(now)
	if now.Sub(s.start) < liveRateWindow {
		return rate
	}

	var (
		ref   readSample
		found bool
	)

	for _, smp := range s.samples {
		if now.Sub(smp.at) >= liveRateWindow {
			ref, found = smp, true
		}
	}

	if !found {
		return rate
	}

	if live := float64(s.read.Load()-ref.read) / now.Sub(ref.at).Seconds(); live < rate {
		return live
	}

	return rate
}

// awaitAdmission marks s as waiting for an admission slot.
func (r *streamRegistry) awaitAdmission(s *blockStream) {
	if r == nil || s == nil {
		return
	}

	s.awaiting.Store(true)
}

// admit ends s's wait for admission at now. A copy that waited has its clock, and its owner's
// silence, start again at now: the wait was this node's, not the peer's. A copy that did not wait
// is left as it is.
func (r *streamRegistry) admit(s *blockStream, now time.Time) {
	if r == nil || s == nil || !s.awaiting.Load() {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if now.After(s.start) {
		s.waited += now.Sub(s.start)
		s.start = now
	}

	s.samples = []readSample{{at: s.start, read: s.read.Load()}}

	if at := s.lastRead.Load(); at < now.UnixNano() {
		s.lastRead.Store(now.UnixNano())
	}

	s.awaiting.Store(false)
}

// awaitAdmissionOf and admitOf are awaitAdmission and admit for the stream behind a reader that
// trackBlockStreams handed down. Any other reader is left alone.
func (r *streamRegistry) awaitAdmissionOf(reader io.Reader) {
	if c, ok := reader.(countingReader); ok {
		r.awaitAdmission(c.s)
	}
}

func (r *streamRegistry) admitOf(reader io.Reader, now time.Time) {
	if c, ok := reader.(countingReader); ok {
		r.admit(c.s, now)
	}
}

// awaitingAdmission reports whether the copy of h from p waits for an admission slot.
func (r *streamRegistry) awaitingAdmission(h chainhash.Hash, p *peerpkg.Peer) bool {
	if r == nil || p == nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for s := range r.active {
		if s.hash == h && s.owner == p && s.awaiting.Load() {
			return true
		}
	}

	return false
}

// couldStart is when p could start sending a block it was asked for at requested and whose first
// byte came at firstByte: the later of the request and the end of p's block before, and never
// after the first byte. A peer sends its queue one block at a time, so a block starts when the
// one ahead of it ends.
func (r *streamRegistry) couldStart(p *peerpkg.Peer, requested, firstByte time.Time) time.Time {
	from := requested

	r.mu.Lock()
	if prev := r.lastBlock[p]; prev.After(from) {
		from = prev
	}
	r.mu.Unlock()

	if from.IsZero() || from.After(firstByte) {
		return firstByte
	}

	return from
}

// finish removes a stream. A complete one adds its block to its owner's next rate sample, timed
// from when the owner could start it (blockStream.from). The block's race mark is left alone, so
// it is never raced twice.
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

	// Timed from when the owner could start, less this node's own admission wait.
	firstByte := s.start.Add(-s.waited)

	from := s.from
	if from.IsZero() || from.After(firstByte) {
		from = firstByte
	}

	took := now.Sub(from) - s.waited
	if took <= 0 {
		return
	}

	pool := r.pooled[s.owner]
	pool.bytes += s.read.Load()
	pool.took += took

	if pool.took < minRateSample {
		r.pooled[s.owner] = pool

		return
	}

	delete(r.pooled, s.owner)

	bps := float64(pool.bytes) / pool.took.Seconds()
	if bps <= 0 {
		return
	}

	// The previous rate counts as decayed: a peer that went quiet and then sends again is
	// judged on how it has delivered lately, not on the rate it had before it stopped.
	if _, ok := r.rates[s.owner]; ok {
		bps = peerRateWeight*bps + (1-peerRateWeight)*r.rateLocked(s.owner)
	}

	r.rates[s.owner] = bps
	delete(r.decay, s.owner)
}

// decayQuiet cuts the rate of each peer that owes blocks and has sent no block bytes for more than
// rateDecayAfter, halving it every rateDecayHalfLife after that. owedSince is, for each peer that
// owes blocks, when it was asked for the block at the head of its queue; the silence runs from
// the later of that and its last block bytes. A rate measured on blocks a peer no longer sends
// otherwise stayed as it was: a peer that sent one fast block and then stopped kept the top rate,
// kept most of the measured bandwidth, and kept every other peer on standby.
func (r *streamRegistry) decayQuiet(now time.Time, owedSince map[*peerpkg.Peer]time.Time) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for p, since := range owedSince {
		if r.rates[p] <= 0 || r.awaitingFromLocked(p) {
			continue
		}

		if last := r.lastLiveBytesLocked(p, now); last.After(since) {
			since = last
		}

		quiet := now.Sub(since) - rateDecayAfter
		if quiet <= 0 {
			continue
		}

		f := math.Pow(0.5, quiet.Seconds()/rateDecayHalfLife.Seconds())
		if cur, ok := r.decay[p]; !ok || f < cur {
			r.decay[p] = f
		}
	}
}

// awaitingFromLocked reports whether a copy from owner p waits for an admission slot: p is then
// not quiet, this node is not reading. Called with r.mu held.
func (r *streamRegistry) awaitingFromLocked(p *peerpkg.Peer) bool {
	for s := range r.active {
		if s.owner == p && s.awaiting.Load() {
			return true
		}
	}

	return false
}

// rateLocked is p's rate with any decay applied. Called with r.mu held.
func (r *streamRegistry) rateLocked(p *peerpkg.Peer) float64 {
	bps := r.rates[p]
	if f, ok := r.decay[p]; ok {
		bps *= f
	}

	return bps
}

// pending is how many bytes are still to come on every copy p is sending now, owed or not: they
// all hold up what p sends next. n counts only the blocks p owes, the ones in its ledger queue.
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
		if s.sender != p {
			continue
		}

		if s.owner == p {
			n++
		}

		bytes += max(0, s.total-s.read.Load())
	}

	return bytes, n
}

// arriving reports whether bytes of block h are arriving now from a peer that owes it.
func (r *streamRegistry) arriving(h chainhash.Hash) bool {
	if r == nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for s := range r.active {
		if s.hash == h && s.owner != nil {
			return true
		}
	}

	return false
}

// arrivingFrom reports the progress of block h's bytes from peer p, which owes it: read so far,
// its declared size, and when they began. ok is false when none are arriving from p.
func (r *streamRegistry) arrivingFrom(h chainhash.Hash, p *peerpkg.Peer) (read, total int64, start time.Time, ok bool) {
	if r == nil || p == nil {
		return 0, 0, time.Time{}, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for s := range r.active {
		if s.hash == h && s.owner == p {
			return s.read.Load(), s.total, s.start, true
		}
	}

	return 0, 0, time.Time{}, false
}

// arrivingBytes is the declared size of every block arriving now from a peer that owes it, and how
// many there are. A copy from a peer that does not owe the block is drained, not held, so its
// declared size is not counted against the disk.
func (r *streamRegistry) arrivingBytes() (int64, int) {
	if r == nil {
		return 0, 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var (
		total int64
		n     int
	)

	for s := range r.active {
		if s.owner == nil {
			continue
		}

		total += s.total
		n++
	}

	return total, n
}

// medianRate is the median of the peers' measured rates on completed blocks, or zero with none.
func (r *streamRegistry) medianRate() float64 {
	if r == nil {
		return 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	rates := make([]float64, 0, len(r.rates))
	for p := range r.rates {
		rates = append(rates, r.rateLocked(p))
	}

	if len(rates) == 0 {
		return 0
	}

	sort.Float64s(rates)

	return rates[len(rates)/2]
}

// peerRate is p's download rate: the rate of the last liveRateWindow of a copy p sends, when one
// has samples over that window; else its rate on completed blocks; else the rate remembered for its
// address; else zero, which means unmeasured. A 4 GB block at a slow peer takes half an hour, and
// a rate from completed blocks alone stays at the peer's earlier speed for all that time.
func (r *streamRegistry) peerRate(p *peerpkg.Peer) float64 {
	if r == nil || p == nil {
		return 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if live, ok := r.liveRateLocked(p); ok {
		return live
	}

	if bps := r.rateLocked(p); bps > 0 {
		return bps
	}

	// A remembered rate becomes the peer's rate the first time it is read, so it decays while the
	// peer owes blocks and sends none (decayQuiet), as a measured rate does. A rate read only from
	// the file did not, and a silent remembered peer kept its rate until the getdata deadline.
	if bps, ok := r.remembered[p.Addr()]; ok && bps > 0 {
		r.rates[p] = bps
		delete(r.remembered, p.Addr())

		return r.rateLocked(p)
	}

	return 0
}

// liveRateLocked is the highest rate over the sampled window of a copy p sends, when a copy has
// samples over the full liveRateWindow. A copy that is complete, or waits for admission, reads no
// bytes and is not counted. A copy that sends nothing gives 1 byte a second, not zero, which
// reads as unmeasured. Called with r.mu held.
func (r *streamRegistry) liveRateLocked(p *peerpkg.Peer) (float64, bool) {
	var (
		best  float64
		found bool
	)

	for s := range r.active {
		if s.sender != p || s.awaiting.Load() || s.complete() || len(s.samples) < 2 {
			continue
		}

		first, last := s.samples[0], s.samples[len(s.samples)-1]

		span := last.at.Sub(first.at)
		if span < liveRateWindow {
			continue
		}

		if rate := max(1, float64(last.read-first.read)/span.Seconds()); !found || rate > best {
			best, found = rate, true
		}
	}

	return best, found
}

// remember loads rates by peer address, for peers this process has not measured yet.
func (r *streamRegistry) remember(rates map[string]float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for addr, bps := range rates {
		if bps > 0 {
			r.remembered[addr] = bps
		}
	}
}

// rememberedRates is each measured peer's rate by address, plus the remembered rates of peers not
// connected now, for the rates file.
func (r *streamRegistry) rememberedRates() map[string]float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make(map[string]float64, len(r.remembered)+len(r.rates))
	for addr, bps := range r.remembered {
		out[addr] = bps
	}

	for p := range r.rates {
		if bps := r.rateLocked(p); bps > 0 {
			out[p.Addr()] = bps
		}
	}

	return out
}

// wasRaced reports whether h had an extra copy asked for, by the race or by the watcher,
// which share the mark, less than raceSlowFetchAfter ago. The newest copy then has not had SV
// Node's slow-fetch time, and no further copy is asked for.
func (r *streamRegistry) wasRaced(h chainhash.Hash, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.expireRacesLocked(now)

	at := r.raced[h].newest()

	return !at.IsZero() && now.Sub(at) < raceSlowFetchAfter
}

// markRaced records that an extra copy of h was asked for at now.
func (r *streamRegistry) markRaced(h chainhash.Hash, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.historyLocked(h).asked = append(r.historyLocked(h).asked, now)
}

// markReasked records that owner p, forgiven for h, was asked for h again at now.
func (r *streamRegistry) markReasked(h chainhash.Hash, p *peerpkg.Peer, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	hist := r.historyLocked(h)
	if hist.reasked == nil {
		hist.reasked = make(map[*peerpkg.Peer]time.Time)
	}

	hist.reasked[p] = now
}

// reaskedNotSending is each owner asked for h again after it was forgiven that has sent no copy
// of h since. A peer has started to send when a stream of h from it is active.
func (r *streamRegistry) reaskedNotSending(h chainhash.Hash) []*peerpkg.Peer {
	r.mu.Lock()
	defer r.mu.Unlock()

	hist := r.raced[h]
	if hist == nil || len(hist.reasked) == 0 {
		return nil
	}

	sending := make(map[*peerpkg.Peer]bool)

	for s := range r.active {
		if s.hash == h && s.owner != nil {
			sending[s.owner] = true
		}
	}

	var silent []*peerpkg.Peer

	for p := range hist.reasked {
		if !sending[p] {
			silent = append(silent, p)
		}
	}

	return silent
}

// markDropped records that the race disconnected the peers sending h at now.
func (r *streamRegistry) markDropped(h chainhash.Hash, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.historyLocked(h).dropped = now
}

// raceCost is how many extra copies of h were asked for in the last raceExpiry, and whether the
// race disconnected the peers sending it in that time.
func (r *streamRegistry) raceCost(h chainhash.Hash, now time.Time) (asked int, dropped bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.expireRacesLocked(now)

	hist := r.raced[h]
	if hist == nil {
		return 0, false
	}

	return len(hist.asked), !hist.dropped.IsZero()
}

// historyLocked is h's race history, made on first use. Called with r.mu held.
func (r *streamRegistry) historyLocked(h chainhash.Hash) *raceHistory {
	hist := r.raced[h]
	if hist == nil {
		hist = &raceHistory{}
		r.raced[h] = hist
	}

	return hist
}

// forgetPeer drops a departed peer's rate and activity, and keeps its rate by address for the
// rates file.
func (r *streamRegistry) forgetPeer(p *peerpkg.Peer) {
	r.mu.Lock()
	// An inbound peer connects from a different port each time, so its address never comes
	// back, and its entry stayed in the rates file for good.
	if bps := r.rateLocked(p); bps > 0 && !p.Inbound() {
		r.remembered[p.Addr()] = bps
	}

	delete(r.rates, p)
	delete(r.lastBlock, p)
	delete(r.pooled, p)
	delete(r.decay, p)
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

	return r.lastBlockBytesLocked(p)
}

// lastBlockBytesLocked is lastBlockBytes with r.mu held.
func (r *streamRegistry) lastBlockBytesLocked(p *peerpkg.Peer) time.Time {
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

// lastLiveBytesLocked is lastBlockBytesLocked for the rate decay: a copy whose judged rate is
// under raceStallRate once it is liveRateWindow old does not count as activity. A trickle of one
// byte every 10 s kept its owner's rate from the decay. Called with r.mu held.
func (r *streamRegistry) lastLiveBytesLocked(p *peerpkg.Peer, now time.Time) time.Time {
	latest := r.lastBlock[p]

	for s := range r.active {
		if s.owner != p {
			continue
		}

		if !s.complete() && now.Sub(s.start) >= liveRateWindow && r.judgedRateLocked(s, now) < raceStallRate {
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
	for h, hist := range r.raced {
		kept := hist.asked[:0]

		for _, at := range hist.asked {
			if now.Sub(at) <= raceExpiry {
				kept = append(kept, at)
			}
		}

		hist.asked = kept

		if !hist.dropped.IsZero() && now.Sub(hist.dropped) > raceExpiry {
			hist.dropped = time.Time{}
		}

		for p, at := range hist.reasked {
			if now.Sub(at) > raceExpiry {
				delete(hist.reasked, p)
			}
		}

		if len(hist.asked) == 0 && hist.dropped.IsZero() && len(hist.reasked) == 0 {
			delete(r.raced, h)
		}
	}
}

// pickRace returns the lowest block above the tip whose every copy is struggling, if the chain
// will reach it before it arrives, and the peers sending those copies. Each copy from a peer that
// owes the block is judged, as SV Node judges each peer a block is in flight from: one copy
// younger than raceSlowFetchAfter or at raceStallRate or more keeps the block out of the race.
// A block whose newest extra copy was asked for less than raceSlowFetchAfter ago is left alone
// too. tip is the committed height and commitRate the blocks a second joining the chain; with no
// rate measured the chain is taken to need the block now.
func (r *streamRegistry) pickRace(now time.Time, tip int32, commitRate float64) (*blockStream, raceCandidate, []*peerpkg.Peer, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.expireRacesLocked(now)

	var (
		best     *blockStream
		bestCand raceCandidate
	)

	healthy := make(map[chainhash.Hash]bool)

	for s := range r.active {
		// A copy waiting for admission is not judged, and keeps its block out of the race: its
		// bytes are not read because this node is busy. A complete copy is not judged either, and
		// keeps its block out of the race: the full body is here. A racer that took over waits
		// with its full body in its side file, and its live rate of zero made it a staller.
		if s.owner != nil && (s.awaiting.Load() || s.complete() || now.Sub(s.start) < raceSlowFetchAfter || r.judgedRateLocked(s, now) >= raceStallRate) {
			healthy[s.hash] = true
		}
	}

	for s := range r.active {
		if s.height <= tip || s.owner == nil || healthy[s.hash] {
			continue
		}

		if at := r.raced[s.hash].newest(); !at.IsZero() && now.Sub(at) < raceSlowFetchAfter {
			continue
		}

		rate := r.judgedRateLocked(s, now)
		if rate >= raceStallRate {
			continue
		}

		// Both through estimate: a copy trickling under 1 B/s with gigabytes to come is past the
		// int64 limit as nanoseconds, and on amd64 the conversion wrapped negative, so eta <= need
		// held and the race skipped the staller.
		var need time.Duration
		if commitRate > 0 {
			need = estimate(float64(s.height-tip), commitRate)
		}

		eta := estimate(float64(s.total-s.read.Load()), rate)

		if eta <= need {
			continue
		}

		if best == nil || s.height < best.height {
			best = s
			bestCand = raceCandidate{eta: eta, need: need, rate: rate, age: now.Sub(s.start)}
		}
	}

	if best == nil {
		return nil, raceCandidate{}, nil, false
	}

	var stalling []*peerpkg.Peer

	for s := range r.active {
		if s.hash == best.hash && s.owner != nil {
			stalling = append(stalling, s.owner)
		}
	}

	return best, bestCand, stalling, true
}

// chooseRacer picks who to ask: the fastest measured peer first, then the peer with the fewest
// blocks already queued, because a request waits behind what a peer already owes. A peer that
// does not owe the block is asked first. Only when there is none is a forgiven owner sending
// nothing asked again: it was let off this block for its silence, but it is not a live copy
// (liveCopies), and with only owners connected the race found nobody to ask. An owner asked again
// drops no stalling copy (maybeRaceSlowBlock). An owner in live is never asked.
func (r *streamRegistry) chooseRacer(candidates, owners, live []*peerpkg.Peer, queued func(*peerpkg.Peer) int) *peerpkg.Peer {
	r.mu.Lock()
	defer r.mu.Unlock()

	better := func(a, b *peerpkg.Peer) bool {
		ra, rb := r.rateLocked(a), r.rateLocked(b)
		if ra != rb {
			return ra > rb
		}

		return queued(a) < queued(b)
	}

	pick := func(skip []*peerpkg.Peer) *peerpkg.Peer {
		var best *peerpkg.Peer

		for _, p := range candidates {
			if p == nil || slices.Contains(skip, p) {
				continue
			}

			if best == nil || better(p, best) {
				best = p
			}
		}

		return best
	}

	if best := pick(owners); best != nil {
		return best
	}

	return pick(live)
}

// trackBlockStreams wraps the installed block sink so every block body arriving is measured.
func (sm *SyncManager) trackBlockStreams(inner func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error)) func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error) {
	return func(hash chainhash.Hash, header *wire.BlockHeader, r io.Reader, n int64) (bool, error) {
		height, _ := sm.headerCache.HeightOf(hash)

		// The stream is the sender's, not the ledger owner's: a peer that does not owe the block
		// could otherwise set an owner's rate, keep a stalled owner looking busy, and set the
		// largest recent block size. owingSender names the sender only when it owes the block.
		now := time.Now()
		owner := sm.owingSender(r, hash)

		sender := deliveringPeer(r)
		if sender != nil && sm.peerStates != nil {
			_, sender, _ = sm.peerStateResolvingPrimary(sender)
		}

		s := &blockStream{hash: hash, height: height, owner: owner, sender: sender, total: n, start: now, received: &sm.waste.received}
		if at, ok := sm.blockDownloads.RequestedAt(hash); ok {
			s.requestedAt = at
		}

		if owner != nil {
			asked, _ := sm.blockDownloads.RequestedOf(owner, hash)
			s.from = sm.streams.couldStart(owner, asked, now)
		}

		sm.streams.add(s)

		converted, err := inner(hash, header, countingReader{r: r, s: s}, n)

		now = time.Now()

		// Complete when the sink succeeded. The reader starts after the 80-byte header, so the
		// bytes counted never reach n, and a test against n counted no stream as complete.
		complete := err == nil
		sm.streams.finish(s, now, complete)

		sm.streams.mu.Lock()
		drained := s.path == admitRawDuplicate || s.path == admitLocalFault || s.path == admitNotOwed
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
		// runs, so every block has to feed it here. Only a converted copy from a peer that
		// owes the block: any peer can declare any size for a block it was not asked for, and a
		// drained copy of a block another copy converted would count that block twice.
		if complete && converted && s.owner != nil && sm.blockSizeTracker != nil {
			sm.blockSizeTracker.addBlockSize(n)
		}

		// The committed tip is read only for a download worth reporting: it is a blockchain
		// call, and this runs on the peer's read loop for every block.
		sm.streams.mu.Lock()
		_, worth := s.report(now, s.height)
		sm.streams.mu.Unlock()

		if worth {
			// The lead is only printed for a block whose height is known, so the
			// tip is only read for one.
			var tip int32
			if s.height > 0 {
				tip, _, _ = sm.committedTip()
			}

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
			sm.streams.sampleStreams(time.Now())
			sm.decayQuietRates(time.Now())
			sm.maybeRaceSlowBlock(time.Now())
			sm.watchOwedBlocks(time.Now())

			if ticks++; ticks%queueReportEvery == 0 {
				sm.logDownloadQueues()
			}

			if ticks%peerRatesSaveEvery == 0 {
				sm.savePeerRates()
			}
		}
	}
}

// decayQuietRates cuts the rate of each peer that owes blocks and has gone quiet
// (streamRegistry.decayQuiet). It runs on the race's ticker, ahead of the rules that read rates.
func (sm *SyncManager) decayQuietRates(now time.Time) {
	if sm.streams == nil || sm.blockDownloads == nil {
		return
	}

	owedSince := make(map[*peerpkg.Peer]time.Time)

	for p, queue := range sm.blockDownloads.Queues() {
		if len(queue) > 0 {
			owedSince[p] = queue[0].at
		}
	}

	sm.streams.decayQuiet(now, owedSince)
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

	s, c, stalling, ok := sm.streams.pickRace(now, tip, sm.commitRate.rate())
	if !ok {
		return
	}

	// The race's cost to one block is bounded by its history over raceExpiry, not by who owes the
	// block now: a dropped peer leaves the ledger (ClearPeer), so with this node's own sink slow,
	// every copy under 100 KB/s, a cap on live owners never bound and an honest peer was dropped
	// about every 30 to 35 s. A block gets at most maxBlockCopies-1 extra copies and one round of
	// disconnects in raceExpiry.
	//
	// Nobody is dropped while this node is backpressured: a read loop waits for an admission slot
	// (localReadBackpressured, the signal the peer stall detector reads), so the slow bytes are
	// this node's. Only that signal is measured today. A slow disk with admission slots still
	// free does not raise it; the history bound above is what limits the cost then.
	asked, droppedRecently := sm.streams.raceCost(s.hash, now)
	mayDrop := !droppedRecently && !sm.localReadBackpressured()

	// At the cap no extra copy is asked for, but each stalling copy is still dropped, as SV Node's
	// DetectStalling drops a staller whatever its parallel fetch count (net_processing.cpp:5446-5466).
	// Before, the cap returned first: two forgiven owners and one copy at 50 KB/s left the block
	// to the peer layer's deadline, an hour or more.
	owners := sm.blockDownloads.OwnersOf(s.hash)
	live := sm.liveCopies(s.hash, owners)
	if copies := len(live); copies >= maxBlockCopies || asked >= maxBlockCopies-1 {
		if !mayDrop {
			sm.logger.Debugf("[frontierRace][%s] block %d is arriving at %.0f KB/s; %d live copies and %d extra copies asked in %s, and its peers were dropped in that time or this node is backpressured, so nothing was done",
				s.hash, s.height, c.rate/1e3, copies, asked, raceExpiry)

			return
		}

		if waiting := sm.reaskedOwnersNotSending(s.hash); len(waiting) > 0 {
			sm.logger.Infof("[frontierRace][%s] block %d is arriving at %.0f KB/s; %d live copies and %d extra copies asked in %s, and %v, asked again, have sent nothing, so %v kept",
				s.hash, s.height, c.rate/1e3, copies, asked, raceExpiry, waiting, stalling)

			return
		}

		sm.dropStallingCopies(s, stalling, now)
		sm.logger.Infof("[frontierRace][%s] dropped %v, which were sending block %d at under %.0f KB/s; %d live copies and %d extra copies asked in %s, so no other peer was asked",
			s.hash, stalling, s.height, float64(raceStallRate)/1e3, copies, asked, raceExpiry)

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

	// With nobody to ask, nobody is dropped, as in SV Node: SendGetDataBlocks marks a staller only
	// when another peer has no block in flight to fetch it (net_processing.cpp:5532-5541), and
	// otherwise only the block download timeout disconnects (net_processing.cpp:5481-5500).
	racer := sm.streams.chooseRacer(candidates, owners, live, sm.blockDownloads.CountForPeer)
	if racer == nil {
		sm.logger.Debugf("[frontierRace][%s] block %d is arriving at %.0f KB/s but there is no other peer to ask", s.hash, s.height, c.rate/1e3)

		return
	}

	if !sm.askRacer(racer, s.hash, now) {
		return
	}

	// An owner asked again is not a peer with no block in flight, so it marks no staller (SV Node,
	// net_processing.cpp:5532). The copies that stall are the only peers sending the block, and the
	// owner asked again was let off it for its silence. Dropping them for it let a peer that takes
	// blocks and goes quiet get an honest slow peer dropped each raceExpiry, and a block that needs
	// more than that at its rate never arrived. A later round may drop once this owner sends.
	if slices.Contains(owners, racer) {
		sm.logger.Infof("[frontierRace][%s] asked %s again for block %d, which is arriving at under %.0f KB/s; %v kept, as %s was forgiven for its silence and sends nothing yet",
			s.hash, racer, s.height, float64(raceStallRate)/1e3, stalling, racer)

		return
	}

	if !mayDrop {
		sm.logger.Infof("[frontierRace][%s] asked %s for block %d, which is arriving at under %.0f KB/s; %v kept, as this block's peers were dropped in the last %s or this node is backpressured",
			s.hash, racer, s.height, float64(raceStallRate)/1e3, stalling, raceExpiry)

		return
	}

	// Each peer sending a copy is struggling, and each is dropped as SV Node drops a staller. The
	// copy converting stops, which frees the block for the extra copy; left connected, the extra
	// copy would arrive as a duplicate and be drained unwritten.
	sm.dropStallingCopies(s, stalling, now)

	sm.logger.Infof("[frontierRace][%s] asked %s for block %d and dropped %v, which were sending it at under %.0f KB/s; one at %.0f KB/s after %s",
		s.hash, racer, s.height, stalling, float64(raceStallRate)/1e3, c.rate/1e3, c.age.Round(time.Second))
}

// dropStallingCopies disconnects each peer sending a struggling copy of s's block, and records the
// round in the block's race history.
func (sm *SyncManager) dropStallingCopies(s *blockStream, stalling []*peerpkg.Peer, now time.Time) {
	sm.streams.markDropped(s.hash, now)

	for _, p := range stalling {
		p.DisconnectWithInfo(fmt.Sprintf("stalling on block %d at under %.0f KB/s", s.height, float64(raceStallRate)/1e3))
	}
}

// liveCopies is the owners of h with a live copy: an owner not let off the block, or one whose
// copy is arriving now. A forgiven owner sending nothing will not deliver (ownerArrival reads it as
// far off). Counting it held the race and the watcher off a block whose only copy was
// stalling, and leaving it out of the peers to ask let the race find nobody to ask.
func (sm *SyncManager) liveCopies(h chainhash.Hash, owners []*peerpkg.Peer) []*peerpkg.Peer {
	active, _ := sm.blockDownloads.ActiveOwners(h)

	live := make([]*peerpkg.Peer, 0, len(owners))

	for _, o := range owners {
		if slices.Contains(active, o) {
			live = append(live, o)

			continue
		}

		if _, _, _, arriving := sm.streams.arrivingFrom(h, o); arriving {
			live = append(live, o)
		}
	}

	return live
}

// reaskedOwnersNotSending is each peer asked for h again after it was forgiven that is still
// connected, still owes h, and sends no copy of h. While one exists, the race drops no copy of h
// at the cap: it was asked for h in place of those copies, and it has not proved it will send.
func (sm *SyncManager) reaskedOwnersNotSending(h chainhash.Hash) []*peerpkg.Peer {
	var waiting []*peerpkg.Peer

	for _, p := range sm.streams.reaskedNotSending(h) {
		if p.Connected() && sm.blockDownloads.HasOwner(p, h) {
			waiting = append(waiting, p)
		}
	}

	return waiting
}

// askRacer records racer as a second owner of h and sends it the getdata. Recording first means
// whichever copy lands second is still admitted rather than costing a peer its connection. A
// racer that already owes h is a forgiven owner asked again, and is recorded as such.
func (sm *SyncManager) askRacer(racer *peerpkg.Peer, h chainhash.Hash, now time.Time) bool {
	reasked := sm.blockDownloads.HasOwner(racer, h)

	if !sm.blockDownloads.Add(racer, h) {
		return false
	}

	getData := wire.NewMsgGetDataSizeHint(1)
	if err := getData.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &h)); err != nil {
		sm.blockDownloads.RemoveOwner(racer, h)

		return false
	}

	sm.streams.markRaced(h, now)

	if reasked {
		sm.streams.markReasked(h, racer, now)
	}

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
//
// The queue lines describe the headers-first scheduler, so they are printed only in
// headers-first mode. Above the last checkpoint blocks arrive on the inv path, which never
// uses the scheduler, and every peer reads as idle there whatever it is doing. The
// download-waste line counts every delivery in every mode and is always printed.
//
// The same tick publishes the download gauges and counters, ahead of the guard below,
// because publishDownloadMetrics copes with each of the fields it checks being nil.
func (sm *SyncManager) logDownloadQueues() {
	sm.publishDownloadMetrics()

	if sm.streams == nil || sm.blockSizeTracker == nil || sm.blockDownloads == nil {
		return
	}

	if sm.headersFirstMode.Load() {
		sm.logSchedulerQueues()
	}

	w := &sm.waste
	sm.logger.Infof("[downloadWaste] since start: received %.1f GB; duplicate copies drained %d, converted %d; copies drained for this node's own store faults %d; streams cut part way %d; %.1f GB wasted; peers dropped owing blocks %d (%d blocks); blocks re-asked after a quiet peer %d, rescued %d",
		float64(w.received.Load())/1e9, w.dupDrained.Load(), w.dupConverted.Load(), w.localFaultDrained.Load(), w.streamsFailed.Load(),
		float64(w.bytesWasted.Load())/1e9, w.droppedOwing.Load(), w.blocksOwedAtDrop.Load(), w.reAskedQuiet.Load(), w.rescued.Load())
}

// publishDownloadMetrics sets the download gauges, blocks owed, heights the header cache names
// and bytes held ahead of the chain, and adds the waste counters' increase since the last call.
// The report tick is its only production caller, so the gauges lag by up to 30 seconds.
func (sm *SyncManager) publishDownloadMetrics() {
	if prometheusLegacyNetsyncBlocksOwed == nil {
		return
	}

	prometheusLegacyNetsyncBlocksOwed.Set(float64(sm.blockDownloads.Len()))
	prometheusLegacyNetsyncHeaderCacheHeights.Set(float64(sm.headerCache.Len()))

	var largest int64
	if sm.blockSizeTracker != nil {
		largest = sm.blockSizeTracker.largestRecentSize()
	}

	prometheusLegacyNetsyncBytesAhead.Set(float64(sm.bytesAhead(largest)))

	sm.waste.publish()
}

// logSchedulerQueues is logDownloadQueues' per-peer lines and summary, for the headers-first
// scheduler.
func (sm *SyncManager) logSchedulerQueues() {
	largest := sm.blockSizeTracker.largestRecentSize()
	typical := sm.blockSizeTracker.getAverageSize()
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
		rate := sm.streams.peerRate(bp.peer)

		peerDepth := sm.peerQueueDepth(bp.peer, depth, fastest)

		if owed == 0 && sending == 0 {
			idle++
		}

		if owed < peerDepth {
			short++
		}

		// When a new block of the largest recent size would land at this peer: what the deadline
		// rule compares with each block's deadline.
		landing := "unknown, no rate"
		if rate > 0 {
			bytes := float64(remaining + int64(max(0, owed-sending))*typical + largest)
			landing = time.Duration(bytes / rate * float64(time.Second)).Round(time.Second).String()
		}

		sm.logger.Infof("[downloadQueue] %s owes %d of %d, sending %d with %.0f MB left, rate %.1f MB/s, a %.0f MB block lands in %s",
			bp.peer, owed, peerDepth, sending, float64(remaining)/1e6, rate/1e6, float64(largest)/1e6, landing)
	}

	sm.logger.Infof("[downloadQueue] %d eligible peers, %d idle; %d below their depth of up to %d requests; pace %.2f blocks/s; %.1f GB held ahead of the chain, parked and arriving, against a %.1f GB backstop; %d blocks owed; receiving %.1f MB/s",
		len(eligible), idle, short, depth, sm.commitRate.pace(), float64(sm.bytesAhead(largest))/1e9, float64(parkBackstopBytes)/1e9, sm.blockDownloads.Len(), sm.waste.rateSinceLast(time.Now())/1e6)
}
