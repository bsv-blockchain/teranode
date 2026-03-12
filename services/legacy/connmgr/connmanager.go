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

	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
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
)

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
	TargetOutbound uint32

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

	// teranode addition
	logger ulogger.Logger
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
		// check whether this connection was pending, remove from there, before reconnecting
		if _, ok := cm.pending.Get(c.id); ok {
			cm.pending.Delete(c.id)
		}

		cm.failedAttempts++
		if cm.failedAttempts >= maxFailedAttempts {
			cm.logger.Debugf("Max failed connection attempts reached: [%d] -- retrying connection in: %v", maxFailedAttempts, cm.cfg.RetryDuration)
			time.AfterFunc(cm.cfg.RetryDuration, func() {
				cm.NewConnReq()
			})
		} else {
			go cm.NewConnReq()
		}
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

				cm.conns.Set(connReq.id, connReq)

				cm.logger.Debugf("Connected to %v", connReq)
				connReq.retryCount.Store(0)
				cm.failedAttempts = 0

				cm.pending.Delete(connReq.id)

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
					continue
				}

				// An existing connection was located, mark as
				// disconnected and execute disconnection
				// callback.
				cm.logger.Debugf("Disconnected from %v", connReq)

				cm.conns.Delete(msg.id)

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
					continue
				}

				// Otherwise, we will attempt a reconnection if
				// we do not have enough peers, or if this is a
				// persistent peer. The connection request is
				// re added to the pending map, so that
				// subsequent processing of connections and
				// failures do not ignore the request.
				if cm.conns.Length() < int(cm.cfg.TargetOutbound) || connReq.Permanent {
					connReq.updateState(ConnPending)
					cm.logger.Debugf("Reconnecting to %v", connReq)

					cm.pending.Set(msg.id, connReq)

					cm.handleFailedConn(connReq)
				}

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

// NewConnReq creates a new connection request and connects to the
// corresponding address.
func (cm *ConnManager) NewConnReq() {
	if atomic.LoadInt32(&cm.stop) != 0 {
		return
	}

	if cm.cfg.GetNewAddress == nil {
		return
	}

	c := &ConnReq{}
	atomic.StoreUint64(&c.id, atomic.AddUint64(&cm.connReqCount, 1))

	// Submit a request of a pending connection attempt to the connection
	// manager. By registering the id before the connection is even
	// established, we'll be able to later cancel the connection via the
	// Remove method.
	done := make(chan struct{})
	select {
	case cm.requests <- registerPending{c, done}:
	case <-cm.quit:
		return
	}

	// Wait for the registration to successfully add the pending conn req to
	// the conn manager's internal state.
	select {
	case <-done:
	case <-cm.quit:
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
			return
		}
	}

	// we need a lock on the object here to set it
	c.SetAddr(addr)

	cm.Connect(c)
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

	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-cm.quit:
				return
			case <-ticker.C:
				// try to connect to new address every minute, we might have disconnected or have new addresses
				connsLengthUint32, err := safeconversion.IntToUint32(cm.conns.Length())
				if err != nil {
					cm.logger.Errorf("could not convert conns length to uint32: %v", err)
					continue
				}

				connectionsOpen := connsLengthUint32
				cm.logger.Debugf("checking active connections: %d", connectionsOpen)

				for i := atomic.LoadUint64(&cm.connReqCount); i < uint64(cm.cfg.TargetOutbound-connectionsOpen); i++ {
					go cm.NewConnReq()
				}
			}
		}
	}()
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
	}

	return &cm, nil
}
