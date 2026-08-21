// Copyright (c) 2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package connmgr

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// maxFailedAttempts is the maximum number of successive failed connection
// attempts after which network failure is assumed and new connections will
// be delayed by the configured retry duration.
const maxFailedAttempts = 25

var (
	// ErrDialNil is used to indicate that Dial cannot be nil in the configuration.
	ErrDialNil = errors.New("Config: Dial cannot be nil")

	// maxRetryDuration is the max duration of time retrying of a persistent
	// connection is allowed to grow to.  This is necessary since the retry
	// logic uses a backoff mechanism which increases the interval base times
	// the number of retries that have been done.
	maxRetryDuration = time.Minute * 5

	// defaultRetryDuration is the default duration of time for retrying
	// persistent connections.
	defaultRetryDuration = time.Second * 5

	// defaultTargetOutbound is the default number of outbound connections to
	// maintain.
	defaultTargetOutbound = uint32(8)

	// defaultReplenishInterval is the cadence of the outbound replenishment
	// loop when Config.ReplenishInterval is left unset. One minute is the
	// historical value, so leaving the setting at zero is the rollback lever
	// for the continuous-replenish change: it restores the old CADENCE and
	// disables the event-driven wake. It does not restore the old behaviour,
	// which is materially worse — the previous periodic pass bounded its dial
	// loop by a monotonic request counter against the target, so once startup
	// had pushed that counter to the target the loop never dialled again.
	defaultReplenishInterval = time.Minute
)

// replenishWakeDebounce is the minimum spacing between two wake-driven
// replenishment passes. The wake channel exists so that a freed outbound slot
// is patched in about a second instead of waiting up to a full tick, but a
// single network event can disconnect many peers at once. Without this window
// each of those disconnects would drive its own pass, and because a dial that
// has been launched but has not yet registered as pending is invisible for a
// few microseconds, those passes can each over-count the deficit by one and
// dial an extra address. The window collapses a burst into one pass; anything
// it swallows is picked up by the next tick.
const replenishWakeDebounce = 100 * time.Millisecond

// ConnState represents the state of the requested connection.
type ConnState uint8

// ConnState can be either pending, established, disconnected or failed.  When
// a new connection is requested, it is attempted and categorized as
// established or failed depending on the connection result.  An established
// connection which was disconnected is categorized as disconnected.
const (
	// ConnPending indicates a connection attempt is in progress but not yet completed.
	ConnPending ConnState = iota

	// ConnFailing indicates a connection attempt has failed and may be retried.
	ConnFailing

	// ConnCanceled indicates a connection attempt was canceled before completion.
	ConnCanceled

	// ConnEstablished indicates a connection has been successfully established.
	ConnEstablished

	// ConnDisconnected indicates a previously established connection has been disconnected.
	ConnDisconnected
)

// ConnReq is the connection request to a network address. If permanent, the
// connection will be retried on disconnection.
type ConnReq struct {
	// The following variables must only be used atomically.
	id uint64

	Permanent bool

	addr       net.Addr
	conn       net.Conn
	state      ConnState
	stateMtx   sync.RWMutex
	retryCount atomic.Uint32
}

// updateState updates the state of the connection request.
func (c *ConnReq) updateState(state ConnState) {
	c.stateMtx.Lock()
	c.state = state
	c.stateMtx.Unlock()
}

// ID returns a unique identifier for the connection request.
func (c *ConnReq) ID() uint64 {
	return atomic.LoadUint64(&c.id)
}

// State is the connection state of the requested connection.
func (c *ConnReq) State() ConnState {
	c.stateMtx.RLock()
	state := c.state
	c.stateMtx.RUnlock()

	return state
}

// String returns a human-readable string for the connection request.
func (c *ConnReq) String() string {
	addr := c.GetAddr()
	if addr == nil || addr.String() == "" {
		return fmt.Sprintf("reqid %d", atomic.LoadUint64(&c.id))
	}

	return fmt.Sprintf("%s (reqid %d)", addr, atomic.LoadUint64(&c.id))
}

// SetAddr sets the network address for the connection request.
// This method is thread-safe and can be called concurrently.
func (c *ConnReq) SetAddr(addr net.Addr) {
	c.stateMtx.Lock()
	defer c.stateMtx.Unlock()

	c.addr = addr
}

func (c *ConnReq) GetAddr() net.Addr {
	c.stateMtx.Lock()
	defer c.stateMtx.Unlock()

	return c.addr
}

// Config holds the configuration options related to the connection manager.
type Config struct {
	// Listeners defines a slice of listeners for which the connection
	// manager will take ownership of and accept connections.  When a
	// connection is accepted, the OnAccept handler will be invoked with the
	// connection.  Since the connection manager takes ownership of these
	// listeners, they will be closed when the connection manager is
	// stopped.
	//
	// This field will not have any effect if the OnAccept field is not
	// also specified.  It may be nil if the caller does not wish to listen
	// for incoming connections.
	Listeners []net.Listener

	// OnAccept is a callback that is fired when an inbound connection is
	// accepted.  It is the caller's responsibility to close the connection.
	// Failure to close the connection will result in the connection manager
	// believing the connection is still active and thus have undesirable
	// side effects such as still counting toward maximum connection limits.
	//
	// This field will not have any effect if the Listeners field is not
	// also specified since there couldn't possibly be any accepted
	// connections in that case.
	OnAccept func(net.Conn)

	// TargetOutbound is the number of outbound network connections to
	// maintain. Defaults to 8.
	//
	// The REPLENISHMENT PASS applies this target to the automatic outbound
	// tier only: automaticCounts excludes permanent (addnode) requests from
	// both of its counts, so a permanent peer never causes the pass to dial
	// one fewer automatic address. The reconnect decision in handleDisconnected
	// is deliberately not scoped that way — it compares the whole conns book
	// against the target — so a permanent peer does hold a place there. That is
	// the pre-existing behaviour and is left alone.
	//
	// Callers that dial outside the connection manager entirely (for example a
	// feeler probe) are invisible here by construction: the connection
	// manager's books are the sole authority for the automatic slots, so
	// anything that must not count against the target must not be handed to
	// Connect or NewConnReq.
	TargetOutbound uint32

	// ReplenishInterval is how often the connection manager re-checks its
	// outbound count and dials to close any deficit. The old fixed value was
	// one minute, which meant a peer lost early in a tick left its slot empty
	// for the best part of a minute — during initial block download, with
	// peers churning constantly, that is a large share of the download window
	// spent below target. A short interval (a couple of seconds) patches the
	// hole roughly as fast as svnode's continuously running connection thread
	// does.
	//
	// Defaults to defaultReplenishInterval (one minute) when zero, which is
	// the rollback lever: it restores the previous cadence and disables the
	// event-driven wake, but not the previous behaviour — see
	// defaultReplenishInterval for why the old periodic pass never dialled.
	ReplenishInterval time.Duration

	// RetryDuration is the duration to wait before retrying connection
	// requests. Defaults to 5s.
	RetryDuration time.Duration

	// OnConnection is a callback that is fired when a new outbound
	// connection is established.
	OnConnection func(*ConnReq, net.Conn)

	// OnDisconnection is a callback that is fired when an outbound
	// connection is disconnected.
	OnDisconnection func(*ConnReq)

	// GetNewAddress is a way to get an address to make a network connection
	// to.  If nil, no new connections will be made automatically.
	GetNewAddress func() (net.Addr, error)

	// Dial connects to the address on the named network. It cannot be nil.
	Dial func(net.Addr) (net.Conn, error)
}

// registerPending is used to register a pending connection attempt. By
// registering pending connection attempts we allow callers to cancel pending
// connection attempts before their successful or in the case they're not
// longer wanted.
type registerPending struct {
	c    *ConnReq
	done chan struct{}
}

// handleConnected is used to queue a successful connection.
type handleConnected struct {
	c    *ConnReq
	conn net.Conn
}

// handleDisconnected is used to remove a connection.
type handleDisconnected struct {
	id    uint64
	retry bool
}

// handleFailed is used to remove a pending connection.
type handleFailed struct {
	c   *ConnReq
	err error
}

// ConnManager provides a manager to handle network connections.
type ConnManager struct {
	// The following variables must only be used atomically.
	connReqCount uint64
	start        int32
	stop         int32

	cfg            Config
	wg             sync.WaitGroup
	failedAttempts uint64
	requests       chan interface{}
	quit           chan struct{}

	// pending holds all registered conn requests that have yet to
	// succeed.
	pending *txmap.SyncedMap[uint64, *ConnReq]

	// conns represents the set of all actively connected peers.
	conns *txmap.SyncedMap[uint64, *ConnReq]

	// replenishWake nudges the replenishment loop to run before its next
	// tick. It is a coalescing signal and never a queue: the buffer is one
	// deep and sends are non-blocking, so any number of connection events
	// arriving between two passes collapse into a single wake-up.
	replenishWake chan struct{}

	// dialMu serialises "measure the automatic tier, then claim the slots that
	// measurement justified". Both halves must happen under it or the count is
	// stale by the time it is acted on. It guards only bookkeeping — never a
	// dial, and never a send on cm.requests — so connHandler can take it without
	// risking a cycle.
	dialMu sync.Mutex

	// replenishBackoffUntil (UnixNano; 0 = no backoff) suspends replenishment
	// passes while consecutive dial failures say the NETWORK is down rather than
	// a peer. Written from connHandler, read from replenishHandler, hence atomic.
	replenishBackoffUntil atomic.Int64

	// teranode addition
	logger ulogger.Logger
}

// signalReplenish asks the replenishment loop to run now rather than waiting
// for its next tick, so a slot freed by a disconnect is refilled immediately
// instead of leaving the node below target for the rest of the interval.
//
// The send is non-blocking on purpose. This is called from connHandler, which
// is the single serialisation point for all connection state; if it ever
// blocked, a burst of disconnects would stall every other connection event
// behind it.
//
// ReplenishInterval <= 0 is the rollback lever, and it must back out the whole
// change, not just the cadence: with the event-driven path still live the lever
// could not isolate "dials too often" from "dials on every connection event", so
// an operator suspecting a dial storm would have no way to test the hypothesis.
func (cm *ConnManager) signalReplenish() {
	if cm.cfg.ReplenishInterval <= 0 {
		return
	}

	select {
	case cm.replenishWake <- struct{}{}:
	default:
	}
}

// automaticCounts returns how many established and pending connection requests
// belong to the automatic outbound tier.
//
// Permanent (addnode) requests are excluded from both counts. They have their
// own retry and backoff path and are not subject to the target: counting them
// here would let a single addnode peer silently occupy one of the automatic
// slots, which quietly costs the node one of the independent address groups
// that the outbound diversity rules are trying to buy.
func (cm *ConnManager) automaticCounts() (established, pending int) {
	cm.conns.Iterate(func(_ uint64, connReq *ConnReq) bool {
		if !connReq.Permanent {
			established++
		}

		return true
	})

	cm.pending.Iterate(func(_ uint64, connReq *ConnReq) bool {
		if connReq.Permanent {
			return true
		}

		// A failed request that cannot be replaced is dead, not pending. With
		// no address source there is nothing to retry it with and nothing to
		// dial in its place, so it stays in the book until an operator removes
		// it — and it is kept there deliberately, because that entry is the
		// only handle Remove has for cancelling it.
		//
		// Counting it would be the bug. `addnode <ip> onetry` against a host
		// that is down builds exactly such a request, so without this every
		// failed one-shot dial would retire one automatic slot for the life of
		// the process, and enough of them would starve the tier outright.
		if cm.cfg.GetNewAddress == nil && connReq.State() == ConnFailing {
			return true
		}

		pending++

		return true
	})

	return established, pending
}

// AutomaticOutboundCount returns the number of currently established automatic
// outbound connections, excluding permanent (addnode) peers. This is the number
// the target applies to, so it is the number to watch when judging whether the
// node is actually holding its outbound quota during initial block download.
func (cm *ConnManager) AutomaticOutboundCount() int {
	established, _ := cm.automaticCounts()

	return established
}

// handleFailedConn handles a connection failed due to a disconnect or any
// other failure. If permanent, it retries the connection after the configured
// retry duration. Otherwise, if required, it makes a new connection request.
// After maxFailedConnectionAttempts new connections will be retried after the
// configured retry duration.
func (cm *ConnManager) handleFailedConn(c *ConnReq) {
	if atomic.LoadInt32(&cm.stop) != 0 {
		return
	}

	if c.Permanent {
		c.retryCount.Add(1)

		d := time.Duration(c.retryCount.Load()) * cm.cfg.RetryDuration
		if d > maxRetryDuration {
			d = maxRetryDuration
		}

		cm.logger.Debugf("Retrying connection to %v in %v", c, d)
		time.AfterFunc(d, func() {
			cm.Connect(c)
		})
	} else if cm.cfg.GetNewAddress != nil {
		cm.failedAttempts++

		// Retire the dead request and claim its replacement in one step. Held
		// apart, a replenishment pass can land in between, see the slot as free,
		// and dial a second address for a slot this path has already taken
		// responsibility for — which is exactly what used to happen on every
		// disconnect.
		cm.dialMu.Lock()
		cm.pending.Delete(c.id)

		var replacement *ConnReq
		if cm.failedAttempts < maxFailedAttempts {
			replacement = cm.reserveSlot()
		}
		cm.dialMu.Unlock()

		if replacement == nil {
			cm.logger.Debugf("Max failed connection attempts reached: [%d] -- retrying connection in: %v", maxFailedAttempts, cm.cfg.RetryDuration)
			time.AfterFunc(cm.cfg.RetryDuration, func() {
				cm.NewConnReq()
			})

			// Once this many attempts have failed in a row we assume the network,
			// not the peer, is down, and the whole point of the delay above is to
			// stop hammering it. Withholding the wake is not enough on its own:
			// the periodic pass is now far shorter than RetryDuration, and a dial
			// that fails instantly (ENETUNREACH) leaves cm.pending empty, so every
			// tick would see the full deficit and launch a fresh round — burning
			// and penalising an address per dial until the address book erodes
			// below the re-seed floor. Suspend the periodic pass for the same
			// RetryDuration so both paths back off together.
			cm.replenishBackoffUntil.Store(time.Now().Add(cm.cfg.RetryDuration).UnixNano())

			return
		}

		go cm.dialReserved(replacement)

		// The replacement above covers this slot, but wake the replenishment
		// loop anyway: if this failure was one of several, the loop is the only
		// thing that reconciles the whole book back to target rather than one
		// slot at a time. The wake is now safe to send early — the reservation
		// is already visible, so the pass it triggers cannot double-count.
		cm.signalReplenish()
	}
}

// connHandler handles all connection related requests.  It must be run as a
// goroutine.
//
// The connection handler makes sure that we maintain a pool of active outbound
// connections so that we remain connected to the network.  Connection requests
// are processed and mapped by their assigned ids.
func (cm *ConnManager) connHandler() {
	var ()

out:
	for {
		select {
		case req := <-cm.requests:
			switch msg := req.(type) {
			case registerPending:
				connReq := msg.c
				connReq.updateState(ConnPending)
				cm.pending.Set(msg.c.id, connReq)
				close(msg.done)

			case handleConnected:
				connReq := msg.c

				if _, ok := cm.pending.Get(connReq.id); !ok {
					if msg.conn != nil {
						_ = msg.conn.Close()
					}
					cm.logger.Debugf("Ignoring connection for canceled connreq=%v", connReq)
					continue
				}

				connReq.updateState(ConnEstablished)
				connReq.conn = msg.conn

				// Promote the request from the pending book to the connected
				// book as one step, for the same reason handleDisconnected
				// retires and re-pends under one lock. A slot counts as
				// occupied only while its request is visible in one book or the
				// other, and automaticCounts walks conns first and pending
				// second: split apart, a promotion landing between those two
				// walks is counted by NEITHER, the pass reads a deficit that
				// does not exist, and it dials a second address for a slot that
				// has just been filled. dialMu is what makes the promotion and
				// the measurement mutually exclusive; the two map mutexes
				// cannot, because they are separate locks.
				cm.dialMu.Lock()
				cm.conns.Set(connReq.id, connReq)
				cm.pending.Delete(connReq.id)
				cm.dialMu.Unlock()

				cm.logger.Debugf("Connected to %v", connReq)
				connReq.retryCount.Store(0)
				cm.failedAttempts = 0
				// A dial got through, so the network is evidently not down. Lift
				// the replenishment backoff immediately rather than making the
				// node wait out a RetryDuration it no longer needs.
				cm.replenishBackoffUntil.Store(0)

				if cm.cfg.OnConnection != nil {
					go cm.cfg.OnConnection(connReq, msg.conn)
				}

			case handleDisconnected:
				connReq, ok := cm.conns.Get(msg.id)
				if !ok {
					connReq, ok = cm.pending.Get(msg.id)
					if !ok {
						cm.logger.Errorf("Unknown connid=%d", msg.id)
						continue
					}

					// Pending connection was found, remove
					// it from pending map if we should
					// ignore a later, successful
					// connection.
					connReq.updateState(ConnCanceled)
					cm.logger.Debugf("Canceling: %v", connReq)
					cm.pending.Delete(msg.id)
					cm.signalReplenish()

					continue
				}

				// An existing connection was located, mark as
				// disconnected and execute disconnection
				// callback.
				cm.logger.Debugf("Disconnected from %v", connReq)

				// Retire the connection and, when it is to be retried, re-pend
				// it in the same step. A slot counts as occupied while its
				// request is visible in either book, so if the two halves are
				// held apart the request belongs to neither for as long as the
				// steps between them take — and a replenishment pass landing
				// there sees a free slot that this path is already about to
				// fill, and dials a second address for it.
				//
				// The decision is taken here rather than below because it is
				// the same measurement problem: whether there is room to
				// reconnect has to be read and acted on without another pass
				// slipping in between.
				cm.dialMu.Lock()
				cm.conns.Delete(msg.id)

				repend := msg.retry &&
					(cm.conns.Length() < int(cm.cfg.TargetOutbound) || connReq.Permanent)
				if repend {
					connReq.updateState(ConnPending)
					cm.pending.Set(msg.id, connReq)
				}
				cm.dialMu.Unlock()

				// Closing a socket is a syscall and the callback is the
				// server's, so both are deliberately kept outside dialMu, which
				// guards bookkeeping only.
				if connReq.conn != nil {
					connReq.conn.Close()
				}

				if cm.cfg.OnDisconnection != nil {
					go cm.cfg.OnDisconnection(connReq)
				}

				// All internal state has been cleaned up, if
				// this connection is being removed, we will
				// make no further attempts with this request.
				if !msg.retry {
					connReq.updateState(ConnDisconnected)

					// Nothing here will ever dial again for this slot — this is
					// the path taken by Remove, including the server's eviction
					// of phantom connections. Without a wake the slot would sit
					// empty until the next tick.
					cm.signalReplenish()

					continue
				}

				// Otherwise, we will attempt a reconnection if
				// we do not have enough peers, or if this is a
				// persistent peer. The connection request was
				// re added to the pending map above, so that
				// subsequent processing of connections and
				// failures do not ignore the request.
				if repend {
					cm.logger.Debugf("Reconnecting to %v", connReq)

					cm.handleFailedConn(connReq)
				}

				// Signalled last, once this connection's state has settled, so
				// the replenishment loop reads a book that already reflects the
				// re-pend above and cannot double-count the freed slot.
				cm.signalReplenish()

			case handleFailed:
				connReq := msg.c

				if _, ok := cm.pending.Get(connReq.id); !ok {
					cm.logger.Debugf("Ignoring connection for canceled conn req: %v", connReq)
					continue
				}

				connReq.updateState(ConnFailing)
				cm.logger.Debugf("Failed to connect to %v: %v", connReq, msg.err)
				cm.handleFailedConn(connReq)
			}

		case <-cm.quit:
			break out
		}
	}

	cm.wg.Done()
	cm.logger.Infof("Connection handler done")
}

// reserveSlot claims one automatic outbound slot by registering an
// address-less request in cm.pending, and returns it ready to be dialed.
//
// Reserving is the whole point. A slot is only safe from being dialed twice
// once it is visible in cm.pending, and the previous code made it visible by
// sending registerPending on cm.requests — a channel serviced by connHandler.
// Every replacement dial is decided inside connHandler, so that registration
// could not be serviced until the handler returned, leaving a window in which a
// replenishment pass saw the slot as free and dialed a second address for it.
// Writing the reservation directly closes the window: the map is
// mutex-protected, and the decision and the record now happen in one step.
//
// Callers must hold dialMu across the measurement that justified the
// reservation.
func (cm *ConnManager) reserveSlot() *ConnReq {
	c := &ConnReq{}
	atomic.StoreUint64(&c.id, atomic.AddUint64(&cm.connReqCount, 1))
	c.updateState(ConnPending)
	cm.pending.Set(c.id, c)

	return c
}

// dialReserved resolves an address for an already-reserved slot and dials it.
//
// This is the slow half of a dial and is deliberately kept off both connHandler
// and dialMu: GetNewAddress consults the address manager and the dedup scans
// walk both books. Only the reservation has to be prompt; choosing what to dial
// does not.
//
// Every path that gives up must release the reservation, or the slot is held by
// a dial that will never happen — the same leak this PR fixes on the dedup
// returns.
func (cm *ConnManager) dialReserved(c *ConnReq) {
	if atomic.LoadInt32(&cm.stop) != 0 || cm.cfg.GetNewAddress == nil {
		cm.pending.Delete(c.id)

		return
	}

	addr, err := cm.cfg.GetNewAddress()
	if err != nil {
		select {
		case cm.requests <- handleFailed{c, err}:
		case <-cm.quit:
		}

		return
	}

	if addr != nil {
		// check whether we already have this connected to this address
		existingConns := false

		// we use Iterate() instead of Range() to avoid reading and writing the map at the same time
		cm.conns.Iterate(func(_ uint64, connReq *ConnReq) bool {
			connReqAddr := connReq.GetAddr()
			if connReqAddr != nil && connReqAddr.String() == addr.String() {
				cm.logger.Debugf("Ignoring connection to %v, already connected", addr)

				existingConns = true

				return false
			}

			return true
		})

		if existingConns {
			// Release the registration made above: this entry has no address and
			// never dials, so none of the dial-lifecycle handlers can ever reap
			// it — without this delete it sits in cm.pending forever (mainnet
			// accumulated 22 such entries, each also silently ending its
			// replacement-dial chain).
			cm.pending.Delete(c.id)

			return
		}

		// check whether we already have this pending to this address
		existingPendingTx := false

		// we use Iterate() instead of Range() to avoid reading and writing the map at the same time
		cm.pending.Iterate(func(_ uint64, connReq *ConnReq) bool {
			connReqAddr := connReq.GetAddr()
			if connReqAddr != nil && connReqAddr.String() == addr.String() {
				cm.logger.Debugf("Ignoring connection to %v, already pending (state: %d, retries: %d)", addr, connReq.State(), connReq.retryCount.Load())

				existingPendingTx = true

				return false
			}

			return true
		})

		if existingPendingTx {
			// Same abandoned-registration release as the already-connected
			// return above.
			cm.pending.Delete(c.id)

			return
		}
	}

	// we need a lock on the object here to set it
	c.SetAddr(addr)

	cm.Connect(c)
}

// reserveSlotIfBelowTarget claims one automatic outbound slot, but only when
// the tier is actually short of one. It returns nil when the books already
// account for the target, meaning the caller must not dial.
//
// The measurement and the claim are taken together under dialMu for the same
// reason replenish does it: a deficit read a moment ago is not a licence to
// dial now.
func (cm *ConnManager) reserveSlotIfBelowTarget() *ConnReq {
	cm.dialMu.Lock()
	defer cm.dialMu.Unlock()

	established, pending := cm.automaticCounts()
	if replenishDeficit(established, pending, int(cm.cfg.TargetOutbound)) == 0 {
		return nil
	}

	return cm.reserveSlot()
}

// NewConnReq creates a new connection request and connects to the
// corresponding address, provided the automatic outbound tier is below target.
//
// The target check is not optional. This used to be an unconditional dial, and
// that was harmless while the only caller that mattered was a strict
// one-for-one replacement inside handleFailedConn — the request it replaced had
// just been retired, so the slot really was free. It stopped being harmless
// once the replenishment pass became a second, independent dial driver. The
// max-failed-attempts arm of handleFailedConn retires its request, arms the
// replenishment backoff for RetryDuration, and schedules NewConnReq for the
// same instant the backoff expires; whichever of the two runs first fills the
// slot, and an unconditional NewConnReq then filled it a second time. Nothing
// sheds the extra connection afterwards, so each such episode left the node
// permanently one connection above TargetOutbound.
func (cm *ConnManager) NewConnReq() {
	if atomic.LoadInt32(&cm.stop) != 0 {
		return
	}

	if cm.cfg.GetNewAddress == nil {
		return
	}

	c := cm.reserveSlotIfBelowTarget()
	if c == nil {
		return
	}

	cm.dialReserved(c)
}

// Connect assigns an id and dials a connection to the address of the
// connection request.
func (cm *ConnManager) Connect(c *ConnReq) {
	// During the time we wait for retry there is a chance that
	// this connection was already cancelled.
	if c.State() == ConnCanceled {
		cm.logger.Debugf("Ignoring connect for canceled connreq=%v", c)
		return
	}

	if atomic.LoadInt32(&cm.stop) != 0 {
		return
	}

	if atomic.LoadUint64(&c.id) == 0 {
		atomic.StoreUint64(&c.id, atomic.AddUint64(&cm.connReqCount, 1))

		// Submit a request of a pending connection attempt to the
		// connection manager. By registering the id before the
		// connection is even established, we'll be able to later
		// cancel the connection via the Remove method.
		done := make(chan struct{})
		select {
		case cm.requests <- registerPending{c, done}:
		case <-cm.quit:
			return
		}

		// Wait for the registration to successfully add the pending
		// conn req to the conn manager's internal state.
		select {
		case <-done:
		case <-cm.quit:
			return
		}
	}

	cm.logger.Debugf("Attempting to connect to %v", c)

	conn, err := cm.cfg.Dial(c.GetAddr())
	if err != nil {
		select {
		case cm.requests <- handleFailed{c, err}:
		case <-cm.quit:
		}

		return
	}

	select {
	case cm.requests <- handleConnected{c, conn}:
	case <-cm.quit:
	}
}

// Disconnect disconnects the connection corresponding to the given connection
// id. If permanent, the connection will be retried with an increasing backoff
// duration.
func (cm *ConnManager) Disconnect(id uint64) {
	if atomic.LoadInt32(&cm.stop) != 0 {
		return
	}

	select {
	case cm.requests <- handleDisconnected{id, true}:
	case <-cm.quit:
	}
}

// Remove removes the connection corresponding to the given connection id from
// known connections.
//
// NOTE: This method can also be used to cancel a lingering connection attempt
// that hasn't yet succeeded.
func (cm *ConnManager) Remove(id uint64) {
	if atomic.LoadInt32(&cm.stop) != 0 {
		return
	}

	select {
	case cm.requests <- handleDisconnected{id, false}:
	case <-cm.quit:
	}
}

// listenHandler accepts incoming connections on a given listener.  It must be
// run as a goroutine.
func (cm *ConnManager) listenHandler(listener net.Listener) {
	cm.logger.Infof("Server listening on %s", listener.Addr())

	for atomic.LoadInt32(&cm.stop) == 0 {
		conn, err := listener.Accept()
		if err != nil {
			// Only log the error if not forcibly shutting down.
			if atomic.LoadInt32(&cm.stop) == 0 {
				cm.logger.Errorf("Can't accept connection: %v", err)
			}

			continue
		}

		go cm.cfg.OnAccept(conn)
	}

	cm.wg.Done()
	cm.logger.Infof("Listener handler done for %s", listener.Addr())
}

// Start launches the connection manager and begins connecting to the network.
func (cm *ConnManager) Start() {
	// Already started?
	if atomic.AddInt32(&cm.start, 1) != 1 {
		return
	}

	cm.logger.Infof("Connection manager started")
	cm.wg.Add(1)

	go cm.connHandler()

	// Start all the listeners so long as the caller requested them and
	// provided a callback to be invoked when connections are accepted.
	if cm.cfg.OnAccept != nil {
		for _, listner := range cm.cfg.Listeners {
			cm.wg.Add(1)
			go cm.listenHandler(listner)
		}
	}

	for i := atomic.LoadUint64(&cm.connReqCount); i < uint64(cm.cfg.TargetOutbound); i++ {
		go cm.NewConnReq()
	}

	// Tracked so Wait blocks until the replenishment loop has actually stopped.
	// Untracked, Wait can return while a pass is still in flight and the caller
	// believes shutdown is complete while a fresh dial is being launched. The
	// Done is wrapped here rather than placed inside replenishHandler because
	// the tests start that loop directly and would then unbalance the group.
	cm.wg.Add(1)

	go func() {
		defer cm.wg.Done()

		cm.replenishHandler()
	}()
}

// replenishSnapshot is the set of values reported by one replenishment pass. It
// exists only so the loop can tell whether anything actually changed since the
// last line it logged.
type replenishSnapshot struct {
	established int
	pending     int
	target      int
	deficit     int
}

// replenishHandler keeps the automatic outbound tier topped up. It must be run
// as a goroutine, and is the only goroutine that runs a replenishment pass, so
// its logging state needs no locking.
//
// It runs on a ticker and also whenever a connection event frees a slot. The
// ticker is the backstop that copes with newly learned addresses and with any
// wake that the debounce window swallowed; the wake is what makes a lost peer
// cost about a second rather than most of an interval.
func (cm *ConnManager) replenishHandler() {
	interval := cm.cfg.ReplenishInterval
	if interval <= 0 {
		// Rollback path: an unset interval reproduces the original fixed
		// one-minute ticker exactly.
		interval = defaultReplenishInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var (
		lastLogged  replenishSnapshot
		lastLogTime time.Time
		lastWake    time.Time
	)

	for {
		select {
		case <-cm.quit:
			return

		case <-ticker.C:
			cm.replenish(&lastLogged, &lastLogTime)

		case <-cm.replenishWake:
			if time.Since(lastWake) < replenishWakeDebounce {
				continue
			}

			lastWake = time.Now()

			cm.replenish(&lastLogged, &lastLogTime)
		}
	}
}

// replenish runs a single replenishment pass: it measures the automatic
// outbound tier and launches enough dials to close the gap to the target.
func (cm *ConnManager) replenish(lastLogged *replenishSnapshot, lastLogTime *time.Time) {
	// Honour the network-down backoff set by handleFailedConn. Without this the
	// periodic pass dials straight through the very backoff that exists to stop
	// the node hammering a dead network and shredding its address book.
	if until := cm.replenishBackoffUntil.Load(); until != 0 {
		if time.Now().UnixNano() < until {
			cm.logger.Debugf("[connmgr] replenish check skipped: backing off after %d consecutive dial failures", maxFailedAttempts)
			return
		}

		// Clear only the expired deadline that was just read. connHandler can
		// arm a fresher one between the load and here, and an unconditional
		// store would discard it and dial straight through the very backoff
		// that exists to stop the node hammering a dead network. If the swap
		// loses, something newer is in force, so skip this pass and let the
		// next one re-read it.
		if !cm.replenishBackoffUntil.CompareAndSwap(until, 0) {
			return
		}
	}

	target := int(cm.cfg.TargetOutbound)

	// Measure and claim under one lock. A deficit is a statement about slots
	// that were free a moment ago; acting on it after another path has taken one
	// of them is how a top-up turns into an over-dial.
	cm.dialMu.Lock()
	established, pending := cm.automaticCounts()
	deficit := replenishDeficit(established, pending, target)

	var reserved []*ConnReq
	if cm.cfg.GetNewAddress != nil {
		reserved = make([]*ConnReq, 0, deficit)
		for i := 0; i < deficit; i++ {
			reserved = append(reserved, cm.reserveSlot())
		}
	}
	cm.dialMu.Unlock()

	// The replenish line is the measurement instrument for outbound health, so
	// it has to survive a two-second cadence without burying every other log.
	// The rule chosen here is: log at Infof whenever the numbers differ from the
	// last line emitted, plus a heartbeat at roughly the old one-minute cadence
	// so a long steady state still leaves a trail. Everything else goes to
	// Debugf. That means every transition is visible at Infof while a node
	// sitting healthily at target — or stuck below it — emits about one line a
	// minute rather than thirty.
	now := replenishSnapshot{established: established, pending: pending, target: target, deficit: deficit}
	if now != *lastLogged || time.Since(*lastLogTime) >= defaultReplenishInterval {
		cm.logger.Infof("[connmgr] replenish check: conns=%d pending=%d target=%d dialing=%d",
			established, pending, target, deficit)

		*lastLogged = now
		*lastLogTime = time.Now()
	} else {
		cm.logger.Debugf("[connmgr] replenish check: conns=%d pending=%d target=%d dialing=%d",
			established, pending, target, deficit)
	}

	// deficit is guaranteed non-negative and bounded to at most TargetOutbound.
	for _, c := range reserved {
		go cm.dialReserved(c)
	}
}

// replenishDeficit returns the number of new outbound dials a replenishment
// pass should launch, given the number of established automatic outbound
// connections, the number of automatic dials still in flight, and the target.
// It returns max(0, target-(established+pending)): never negative, and never
// more than target. This guards against the uint32 underflow of the original
// code, where open > target wrapped the subtraction to ~4 billion, and replaces
// the broken monotonic-counter loop bound that stopped the ticker dialing at
// all after startup.
//
// In-flight dials must be counted. A connection is only entered in the
// established book once its TCP connect returns, and reaching an unresponsive
// peer can take the full dial timeout — tens of seconds. At the old one-minute
// cadence that barely mattered, but at a two-second cadence a dial that has not
// yet completed would otherwise look like an empty slot on every pass and be
// re-dialled dozens of times before it either succeeded or failed, turning the
// replenishment loop into a dial storm against the whole address table.
func replenishDeficit(established, pending, target int) int {
	open := established + pending
	if open >= target {
		return 0
	}

	return target - open
}

// Wait blocks until the connection manager halts gracefully.
func (cm *ConnManager) Wait() {
	cm.wg.Wait()
}

// Stop gracefully shuts down the connection manager.
func (cm *ConnManager) Stop() {
	if atomic.AddInt32(&cm.stop, 1) != 1 {
		cm.logger.Warnf("Connection manager already stopped")
		return
	}

	// Stop all the listeners.  There will not be any listeners if
	// listening is disabled.
	for _, listener := range cm.cfg.Listeners {
		// Ignore the error since this is shutdown and there is no way
		// to recover anyways.
		_ = listener.Close()
	}

	close(cm.quit)
	cm.logger.Infof("Connection manager stopped")
}

// New returns a new connection manager.
// Use Start to start connecting to the network.
func New(logger ulogger.Logger, cfg *Config) (*ConnManager, error) {
	if cfg.Dial == nil {
		return nil, ErrDialNil
	}

	// Default to sane values
	if cfg.RetryDuration <= 0 {
		cfg.RetryDuration = defaultRetryDuration
	}

	if cfg.TargetOutbound == 0 {
		cfg.TargetOutbound = defaultTargetOutbound
	}

	cm := ConnManager{
		logger:   logger,
		cfg:      *cfg, // Copy so caller can't mutate
		requests: make(chan interface{}),
		quit:     make(chan struct{}),
		pending:  txmap.NewSyncedMap[uint64, *ConnReq](),
		conns:    txmap.NewSyncedMap[uint64, *ConnReq](),

		// Depth one: this is a coalescing signal, not a queue of work.
		replenishWake: make(chan struct{}, 1),
	}

	return &cm, nil
}
