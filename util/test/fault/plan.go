// Package fault injects faults into the stores and clients a service under test depends on.
//
// A Plan holds rules and an ordered log of every call made through the decorators built on it.
// The decorators (NewUTXOStore, NewBlobStore, NewBlockchainClient, NewBlockchainStore) wrap a
// real implementation and consult the plan on every call:
//
//	plan := fault.NewPlan()
//	plan.On("InvalidateBlock").FirstN(2).Fail(errors.NewStorageError("injected"))
//	client := fault.NewBlockchainClient(realClient, plan)
//
// Build every rule before the decorated values are used: a rule takes effect once its action
// is set, and rules are not safe to modify while calls are in flight. The plan itself (its log
// and match counts) is safe for concurrent calls.
//
// One plan may be shared by several decorators, so a test can assert the order of calls across
// stores. Rules are tried in the order they were added; the first rule whose trigger fires on a
// call decides what happens to it. A call no rule fires on is passed through unchanged.
package fault

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// Store names, as recorded in Call.Store and accepted by Rule.In.
const (
	StoreUTXO            = "utxo.Store"
	StoreBlob            = "blob.Store"
	StoreBlockchain      = "blockchain.ClientI"
	StoreBlockchainStore = "blockchain.Store"
)

// Call is one call made through a decorator.
type Call struct {
	// Seq is the call's position in the plan's log, starting at 1.
	Seq int
	// Store is the decorated interface, one of the Store* constants.
	Store string
	// Method is the interface method name.
	Method string
	// Args are the call's arguments, excluding a leading context.Context. A variadic
	// argument is recorded as its slice.
	Args []any
	// Err is the error the decorator returned (nil when it succeeded or returns no error).
	Err error
	// Injected reports whether a rule fired on the call.
	Injected bool
}

type action int

const (
	actNone action = iota
	actFail
	actApplyThenFail
	actDelay
	actBlock
	actPanic
	actObserve
)

// Rule is one fault, built with Plan.On. Its trigger methods (Always, Nth, FirstN, Every,
// Probability) and filters (In, Where) return the rule for chaining; an action method (Fail,
// ApplyThenFail, Delay, Block, Panic, Observe) completes it.
type Rule struct {
	plan   *Plan
	method string
	store  string
	where  func(Call) bool

	// trigger: fires on matching call number n (1-based) when fires(n) is true.
	fires   func(n int) bool
	prob    float64
	rnd     *rand.Rand
	matched int

	act     action
	err     error
	delay   time.Duration
	release <-chan struct{}
	panicV  any
	observe func(Call)
}

// Plan holds the rules and the call log shared by the decorators built on it.
type Plan struct {
	mu    sync.Mutex
	rules []*Rule
	calls []Call
}

// NewPlan returns an empty plan: every call passes through and is logged.
func NewPlan() *Plan {
	return &Plan{}
}

// On starts a rule for the named method. The rule matches that method on every decorator
// sharing the plan unless narrowed with In. On panics if no decorated interface has a method
// of that name, so a typo fails the test instead of silently injecting nothing.
func (p *Plan) On(method string) *Rule {
	if _, ok := methodErrors[method]; !ok {
		panic(fmt.Sprintf("fault: no decorated interface has a method %q", method))
	}

	r := &Rule{plan: p, method: method, fires: func(int) bool { return true }}

	p.mu.Lock()
	p.rules = append(p.rules, r)
	p.mu.Unlock()

	return r
}

// In restricts the rule to one decorated interface (a Store* constant).
func (r *Rule) In(store string) *Rule {
	if _, ok := methodErrors[r.method][store]; !ok {
		panic(fmt.Sprintf("fault: %s has no method %q", store, r.method))
	}

	r.store = store

	return r
}

// Where restricts the rule to calls for which match returns true. A call the filter rejects
// does not count towards the rule's trigger.
func (r *Rule) Where(match func(Call) bool) *Rule {
	r.where = match
	return r
}

// Always fires on every matching call. It is the default trigger.
func (r *Rule) Always() *Rule {
	r.fires = func(int) bool { return true }
	return r
}

// Nth fires on the nth matching call only (1-based).
func (r *Rule) Nth(n int) *Rule {
	r.fires = func(i int) bool { return i == n }
	return r
}

// FirstN fires on the first n matching calls, then lets every later call through.
func (r *Rule) FirstN(n int) *Rule {
	r.fires = func(i int) bool { return i <= n }
	return r
}

// AfterN lets the first n matching calls through, then fires on every later call.
func (r *Rule) AfterN(n int) *Rule {
	r.fires = func(i int) bool { return i > n }
	return r
}

// Every fires on every nth matching call (the nth, 2nth, ...).
func (r *Rule) Every(n int) *Rule {
	r.fires = func(i int) bool { return i%n == 0 }
	return r
}

// Probability fires on each matching call with probability p, from a source seeded with seed
// so a failing run can be replayed.
func (r *Rule) Probability(p float64, seed int64) *Rule {
	r.prob = p
	r.rnd = rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test randomness
	r.fires = func(int) bool { return r.rnd.Float64() < r.prob }

	return r
}

// Fail returns err without calling the wrapped implementation. The method must return an error.
func (r *Rule) Fail(err error) *Rule {
	r.requireErrorReturn("Fail")
	r.act, r.err = actFail, err

	return r
}

// ApplyThenFail calls the wrapped implementation and then returns err in place of its error:
// the effect happened but the caller is told it did not (an acknowledgement lost after commit).
// The method must return an error.
func (r *Rule) ApplyThenFail(err error) *Rule {
	r.requireErrorReturn("ApplyThenFail")
	r.act, r.err = actApplyThenFail, err

	return r
}

// Delay sleeps for d before calling the wrapped implementation. A cancelled context ends the
// sleep early and is returned as the call's error when the method returns one.
func (r *Rule) Delay(d time.Duration) *Rule {
	r.act, r.delay = actDelay, d
	return r
}

// Block holds the call until release is closed, then calls the wrapped implementation. A
// cancelled context ends the wait and is returned as the call's error when the method
// returns one.
func (r *Rule) Block(release <-chan struct{}) *Rule {
	r.act, r.release = actBlock, release
	return r
}

// Panic panics with v instead of calling the wrapped implementation.
func (r *Rule) Panic(v any) *Rule {
	r.act, r.panicV = actPanic, v
	return r
}

// Observe calls fn with the call, then calls the wrapped implementation unchanged. Use it to
// signal a test when a call starts. fn runs on the caller's goroutine, outside the plan's lock.
func (r *Rule) Observe(fn func(Call)) *Rule {
	r.act, r.observe = actObserve, fn
	return r
}

func (r *Rule) requireErrorReturn(what string) {
	for store, hasErr := range methodErrors[r.method] {
		if (r.store == "" || r.store == store) && !hasErr {
			panic(fmt.Sprintf("fault: %s on %s.%s, which returns no error", what, store, r.method))
		}
	}
}

// Calls returns a copy of the log, in call order.
func (p *Plan) Calls() []Call {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]Call, len(p.calls))
	copy(out, p.calls)

	return out
}

// CallsTo returns the logged calls to method, in call order.
func (p *Plan) CallsTo(method string) []Call {
	p.mu.Lock()
	defer p.mu.Unlock()

	var out []Call

	for _, c := range p.calls {
		if c.Method == method {
			out = append(out, c)
		}
	}

	return out
}

// Count returns the number of logged calls to method.
func (p *Plan) Count(method string) int {
	return len(p.CallsTo(method))
}

// Methods returns the distinct method names in the log, sorted.
func (p *Plan) Methods() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	seen := map[string]struct{}{}
	for _, c := range p.calls {
		seen[c.Method] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for m := range seen {
		out = append(out, m)
	}

	sort.Strings(out)

	return out
}

// Reset clears the log and every rule's match count; the rules stay.
func (p *Plan) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.calls = nil

	for _, r := range p.rules {
		r.matched = 0
	}
}

// call is the decorator-side handle for one call in flight.
type call struct {
	plan   *Plan
	ctx    context.Context
	idx    int // index into plan.calls
	rule   *Rule
	hasErr bool
}

// enter logs the call and picks the rule that fires on it, if any.
func (p *Plan) enter(ctx context.Context, store, method string, hasErr bool, args ...any) *call {
	p.mu.Lock()
	defer p.mu.Unlock()

	c := Call{Seq: len(p.calls) + 1, Store: store, Method: method, Args: args}

	var fired *Rule

	for _, r := range p.rules {
		// A rule with no action yet is still being built; it neither counts nor fires.
		if r.act == actNone || r.method != method || (r.store != "" && r.store != store) {
			continue
		}

		if r.where != nil && !r.where(c) {
			continue
		}

		r.matched++

		if fired == nil && r.fires(r.matched) {
			fired = r
		}
	}

	c.Injected = fired != nil
	p.calls = append(p.calls, c)

	return &call{plan: p, ctx: ctx, idx: len(p.calls) - 1, rule: fired, hasErr: hasErr}
}

// before runs the rule's pre-call action. A non-nil error means: return it without calling the
// wrapped implementation.
func (c *call) before() error {
	if c.rule == nil {
		return nil
	}

	switch c.rule.act {
	case actFail:
		return c.rule.err
	case actPanic:
		panic(c.rule.panicV)
	case actObserve:
		c.rule.observe(c.snapshot())
	case actDelay:
		timer := time.NewTimer(c.rule.delay)
		defer timer.Stop()

		return c.wait(timer.C, nil)
	case actBlock:
		return c.wait(nil, c.rule.release)
	case actNone, actApplyThenFail:
	}

	return nil
}

// wait returns when the timer fires, release is closed or the call's context is done. A nil
// channel never fires.
func (c *call) wait(timer <-chan time.Time, release <-chan struct{}) error {
	var ctxDone <-chan struct{}
	if c.ctx != nil {
		ctxDone = c.ctx.Done()
	}

	select {
	case <-timer:
		return nil
	case <-release:
		return nil
	case <-ctxDone:
		if c.hasErr {
			return c.ctx.Err()
		}

		return nil
	}
}

// after records the call's outcome and applies ApplyThenFail.
func (c *call) after(err error) error {
	if c.rule != nil && c.rule.act == actApplyThenFail {
		err = c.rule.err
	}

	c.plan.mu.Lock()
	c.plan.calls[c.idx].Err = err
	c.plan.mu.Unlock()

	return err
}

func (c *call) snapshot() Call {
	c.plan.mu.Lock()
	defer c.plan.mu.Unlock()

	return c.plan.calls[c.idx]
}
