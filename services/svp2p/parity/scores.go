package parity

import (
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/services/svp2p"
)

// legacyScoreLine is services/legacy/peer_server.go addBanScore's warning:
// "Misbehaving peer 127.0.0.1:1234 (outbound, ...): reason -- ban score increased to 20".
var legacyScoreLine = regexp.MustCompile(`Misbehaving peer (\S+) .*ban score (?:increased to|is) (\d+)`)

// svp2pThresholdLine is the disconnect svp2p logs when a peer crosses the ban
// threshold: "[svp2p] peer 127.0.0.1:1234 done: ... misbehavior threshold
// reached (score 100)". A peer dropped this way is gone from PeerScores before
// a sampler can see it, so the line is the record.
var svp2pThresholdLine = regexp.MustCompile(`peer (\S+) done: .*misbehavior threshold reached \(score (\d+)\)`)

// svp2pRejectedBlockLine is the other disconnect that carries a score without
// naming it: manager.go BlockDone sets delta = scoreInvalidBlock (100, the
// DoS(100) of validation.cpp) in the same statement that raises "block %s was
// rejected", so the line stands for that score.
var svp2pRejectedBlockLine = regexp.MustCompile(`peer (\S+) done: .*svp2p: block \S+ was rejected`)

// svp2pInvalidCompactLine is the third disconnect that carries a score without
// naming it. Both compact-block READ_STATUS_INVALID branches
// (compactdispatch.go CompactBlock and BlockTxn) score the peer
// scoreInvalidBlock and then return the reconstruction error; handleBlockTxn
// returns that error in preference to the threshold error, so the peer loop's
// "done" line names the malformed message and not the score. Every such error
// wraps ErrCompactBlockInvalid, whose text is what this matches.
var svp2pInvalidCompactLine = regexp.MustCompile(`peer (\S+) done: .*svp2p: invalid compact block`)

const svp2pInvalidBlockScore = 100

// Scores is the node's current misbehaviour total per peer address. svp2p
// exposes it; legacy only logs it, so its figure is the highest total logged.
func (n *nodeUnderTest) Scores() map[string]int {
	switch n.Impl {
	case Svp2p:
		out := make(map[string]int)

		if srv, ok := n.svc.(*svp2p.Server); ok {
			for addr, v := range srv.PeerScores() {
				out[addr] = v
			}
		}

		for _, line := range n.Logger.Lines() {
			if m := svp2pThresholdLine.FindStringSubmatch(line); m != nil {
				if v, err := strconv.Atoi(m[2]); err == nil && v > out[m[1]] {
					out[m[1]] = v
				}
			}

			if m := svp2pRejectedBlockLine.FindStringSubmatch(line); m != nil && out[m[1]] < svp2pInvalidBlockScore {
				out[m[1]] = svp2pInvalidBlockScore
			}

			if m := svp2pInvalidCompactLine.FindStringSubmatch(line); m != nil && out[m[1]] < svp2pInvalidBlockScore {
				out[m[1]] = svp2pInvalidBlockScore
			}
		}

		return out
	case Legacy:
		out := make(map[string]int)

		for _, line := range n.Logger.Lines() {
			if m := legacyScoreLine.FindStringSubmatch(line); m != nil {
				if v, err := strconv.Atoi(m[2]); err == nil && v > out[m[1]] {
					out[m[1]] = v
				}
			}
		}

		return out
	}

	return nil
}

// scoreSampler keeps the highest score seen per peer while a scenario runs, so a
// peer that is disconnected for reaching the ban threshold still reports the
// total that dropped it.
type scoreSampler struct {
	mu   sync.Mutex
	max  map[string]int
	stop chan struct{}
	done chan struct{}
}

func (n *nodeUnderTest) sampleScores() *scoreSampler {
	s := &scoreSampler{max: make(map[string]int), stop: make(chan struct{}), done: make(chan struct{})}

	go func() {
		defer close(s.done)

		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()

		for {
			s.record(n.Scores())

			select {
			case <-s.stop:
				s.record(n.Scores())
				return
			case <-tick.C:
			}
		}
	}()

	return s
}

func (s *scoreSampler) record(cur map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for addr, v := range cur {
		if v > s.max[addr] {
			s.max[addr] = v
		}
	}
}

// Result stops sampling and returns the maxima keyed by peer address.
func (s *scoreSampler) Result() map[string]int {
	close(s.stop)
	<-s.done

	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[string]int, len(s.max))
	for k, v := range s.max {
		out[k] = v
	}

	return out
}
