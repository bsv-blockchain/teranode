package blockvalidation

import "sync"

// blockFoundReply carries the one answer that a BlockFound caller waiting with WaitToComplete
// gets: nil once its block is stored, or the error that ended the attempt. The item that holds it
// travels from blockFoundCh through the priority queue to the worker that settles the block, and
// whichever step settles the block answers it. That may be an early return before the queue, or
// the worker after the last retry.
//
// It answers once. A later answer is dropped, so a path that answers and then panics, or two steps
// that both think they settled the block, can never block a worker or hand the caller a second
// value. The channel has room for the one answer, so answering never blocks, even after the caller
// has given up on its context.
//
// A nil *blockFoundReply is valid and does nothing: the Kafka path queues items with no reply.
type blockFoundReply struct {
	ch chan error

	mu       sync.Mutex
	answered bool
	result   error

	// joined are the replies of other callers that waited on the same hash. Their announcements
	// were merged into the item that holds this reply, so they get this reply's answer.
	joined []*blockFoundReply
}

func newBlockFoundReply() *blockFoundReply {
	return &blockFoundReply{ch: make(chan error, 1)}
}

// answer sends err to the caller, and to every joined caller, unless an answer was already sent.
func (r *blockFoundReply) answer(err error) {
	if r == nil {
		return
	}

	r.mu.Lock()

	if r.answered {
		r.mu.Unlock()
		return
	}

	r.answered = true
	r.result = err
	joined := r.joined
	r.joined = nil

	r.mu.Unlock()

	r.ch <- err

	for _, other := range joined {
		other.answer(err)
	}
}

// join makes other get this reply's answer. If this reply has already been answered, other gets
// the same answer at once.
func (r *blockFoundReply) join(other *blockFoundReply) {
	if r == nil || other == nil || other == r {
		return
	}

	r.mu.Lock()

	if r.answered {
		result := r.result
		r.mu.Unlock()
		other.answer(result)

		return
	}

	r.joined = append(r.joined, other)

	r.mu.Unlock()
}

// isAnswered reports whether an answer has been sent. A nil reply counts as answered: there is no
// caller to answer.
func (r *blockFoundReply) isAnswered() bool {
	if r == nil {
		return true
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.answered
}

// mergeReply returns the reply that answers both a and b: a, with b joined to it, or b when a is
// nil.
func mergeReply(a, b *blockFoundReply) *blockFoundReply {
	if a == nil {
		return b
	}

	a.join(b)

	return a
}
