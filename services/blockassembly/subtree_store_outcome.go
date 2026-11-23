package blockassembly

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
)

// Production values; tests shorten them. Package level so that no field on BlockAssembly or
// signature has to carry them.
var (
	subtreeRetryBackoffUnit = time.Second // unit of BackoffAndSleep, (2*retries+1) units
	subtreeRetryLimit       = 10          // retries after which a retry entry is exhausted
	metaBuildBackoff        = 100 * time.Millisecond
)

// metaBuildRetries is how many times a failed meta build is tried again.
const metaBuildRetries = 3

// subtreeStoreOutcome collects the final outcome of one subtree request: the first-try goroutines
// and every retry entry of the request share it, each settles its part, and the typed result is
// posted once, when both parts are settled. Nil-safe, so retry entries made without a request work
// unchanged.
type subtreeStoreOutcome struct {
	mu      sync.Mutex
	result  chan<- subtreeprocessor.StoreResult
	res     subtreeprocessor.StoreResult
	pending int
	failed  bool

	// skip is the request's SkipNotification, honoured by a late announcement.
	skip bool

	// wants is the request's WantsAnnounce: a fork reorg switches a silent request to announcing.
	wants func() bool

	// late is the notification sender's input for the announcement of a subtree a retry stored.
	// Nil when no sender runs, in which case the announcement is sent directly.
	late chan<- chainhash.Hash

	// settled is the request's Settled: the store queue's charge for it ends with its store.
	settled func()

	// set is the stored set the settled request reports to, with the request's root, when
	// blockassembly_storedPrefixTemplate is on; nil otherwise.
	set      *subtreeprocessor.StoredSet
	root     chainhash.Hash
	started  time.Time
	retrying bool // a write was handed to the retry processor
}

func newSubtreeStoreOutcome(request subtreeprocessor.NewSubtreeRequest, metaApplicable bool, late chan<- chainhash.Hash) *subtreeStoreOutcome {
	o := &subtreeStoreOutcome{result: request.Result, skip: request.SkipNotification, late: late, pending: 1, settled: request.SettledFunc(), wants: request.WantsAnnounceFunc()}
	if metaApplicable {
		o.pending++
	} else {
		o.res.MetaNotApplicable = true
	}

	return o
}

// settle applies one part's outcome and posts the result, without blocking (the channel is the
// caller's, buffered), when no part is left open.
func (o *subtreeStoreOutcome) settle(apply func(r *subtreeprocessor.StoreResult)) {
	if o == nil {
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.pending == 0 {
		return
	}

	apply(&o.res)

	o.pending--
	if o.pending == 0 {
		// before the result is posted, so a waiter that sees the result finds the root in the set
		if o.set != nil {
			o.set.Finish(o.root, o.res, o.retrying, time.Since(o.started))
		}

		if o.result != nil {
			select {
			case o.result <- o.res:
			default:
			}
		}

		if o.settled != nil {
			o.settled()
		}
	}
}

// track makes the outcome report to set when it settles. Nil-safe.
func (o *subtreeStoreOutcome) track(set *subtreeprocessor.StoredSet, root chainhash.Hash) {
	if o == nil {
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	o.set, o.root, o.started = set, root, time.Now()
}

// toRetry notes that a write was handed to the retry processor, which the stored set counts until
// the request settles. Nil-safe.
func (o *subtreeStoreOutcome) toRetry() {
	if o == nil {
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.set != nil && !o.retrying && o.pending > 0 {
		o.retrying = true

		o.set.RetryStarted()
	}
}

func (o *subtreeStoreOutcome) subtreeStored(existed bool) {
	o.settle(func(r *subtreeprocessor.StoreResult) {
		r.SubtreeStored = true
		r.ExistedAlready = existed
	})
}

// metaAbsent settles the meta part without a meta: the subtree existed and its meta did not.
func (o *subtreeStoreOutcome) metaAbsent() {
	o.settle(func(*subtreeprocessor.StoreResult) {})
}

func (o *subtreeStoreOutcome) metaStored() {
	o.settle(func(r *subtreeprocessor.StoreResult) { r.MetaStored = true })
}

// fail settles a part as failed for good, and counts the request once.
func (o *subtreeStoreOutcome) fail(err error) {
	o.settle(func(r *subtreeprocessor.StoreResult) {
		if r.Err == nil {
			r.Err = err
		}

		if !o.failed {
			o.failed = true

			prometheusBlockAssemblySubtreeStoreFailed.Inc()
		}
	})
}

// abort settles every part still open as failed, for an exit before the writes started.
func (o *subtreeStoreOutcome) abort(err error) {
	if o == nil {
		return
	}

	o.mu.Lock()
	open := o.pending
	o.mu.Unlock()

	for range open {
		o.fail(err)
	}
}

// cancel settles a part as cancelled: by shutdown, or because a reset superseded its request.
func (o *subtreeStoreOutcome) cancel(err error) {
	o.settle(func(r *subtreeprocessor.StoreResult) {
		r.Cancelled = true

		if r.Err == nil {
			r.Err = err
		}
	})
}

// exhausted settles every part the entry still holds as failed.
func (r *subtreeRetrySend) exhausted() {
	err := r.lastErr
	if err == nil {
		err = errors.NewStorageError("subtree store retries exhausted")
	}

	if len(r.subtreeBytes) > 0 {
		r.outcome.fail(err)
	}

	if len(r.subtreeMetaBytes) > 0 {
		r.outcome.fail(err)
	}
}

// announceLate announces a subtree a retry stored, through the notification sender so that it
// honours the request's SkipNotification. It is out of order by nature: it can come after the
// announcements of subtrees stored later.
func (ba *BlockAssembly) announceLate(ctx context.Context, subtreeRetry *subtreeRetrySend) {
	if o := subtreeRetry.outcome; o != nil {
		if o.skip && (o.wants == nil || !o.wants()) {
			return
		}

		if o.late != nil {
			select {
			case o.late <- subtreeRetry.subtreeHash:
			case <-ctx.Done():
			}

			return
		}
	}

	// No sender runs: an entry made without a request.
	ba.sendSubtreeNotification(ctx, subtreeRetry.subtreeHash)
}

// buildSubtreeMetaWithRetry builds the meta of a request, again after a short pause when the
// build failed on a read error of a disk tx map, which can clear. It is inline and short, and does not go
// through the retry channel, because that carries bytes, not the request, whose subtree and map must
// not be read long after the caller was answered. A read error that does not clear delays ErrChan by
// the sum of the pauses (about 0.7 s).
func (ba *BlockAssembly) buildSubtreeMetaWithRetry(ctx context.Context, request subtreeprocessor.NewSubtreeRequest) ([]byte, bool) {
	backoff := metaBuildBackoff

	for attempt := 0; ; attempt++ {
		var readErr bool

		if metaBytes, ok := ba.buildSubtreeMetaReportingReadError(request, &readErr); ok {
			return metaBytes, true
		}

		// A parent that is not found is deterministic and fails at once; only a read error of the
		// disk tx map can clear.
		if !readErr || attempt == metaBuildRetries || request.Superseded() {
			return nil, false
		}

		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return nil, false
		}
	}
}
