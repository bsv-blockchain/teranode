package uaerospike

import (
	"encoding/binary"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

const (
	// DefaultConnectionQueueSize is the default size for the connection queue
	// if not specified in the client policy
	DefaultConnectionQueueSize = 128

	// semaphoreTimeoutFraction is the fraction of TotalTimeout to use for semaphore acquisition
	// This ensures the total operation time (semaphore wait + actual operation) stays within bounds
	semaphoreTimeoutFraction = 0.1 // 10% of total timeout

	// minSemaphoreTimeout is the minimum timeout for semaphore acquisition
	minSemaphoreTimeout = 100 * time.Millisecond

	// defaultSemaphoreMultiplier preserves the original semaphore sizing
	// (one slot per ConnectionQueueSize-derived permit) when no option is
	// supplied by the caller.
	defaultSemaphoreMultiplier = 1.0

	// maxSemaphoreCapacity bounds the buffer of the connection-semaphore
	// channel to keep a misconfigured multiplier (typo, NaN/Inf, runaway
	// value from external config) from allocating a multi-GB channel.
	// 1 << 20 (≈1M slots) is far above any legitimate connection-queue
	// size while keeping worst-case channel-buffer overhead bounded.
	maxSemaphoreCapacity = 1 << 20
)

// getConnectionQueueSize returns the connection queue size from the given policy
// or falls back to DefaultConnectionQueueSize if the policy is nil or returns 0
func getConnectionQueueSize(policy *aerospike.ClientPolicy) int {
	if policy != nil && policy.ConnectionQueueSize > 0 {
		return policy.ConnectionQueueSize
	}
	return DefaultConnectionQueueSize
}

// clientConfig holds optional construction-time settings for Client. Populated
// by applying ClientOption values; obtain a defaults-applied instance via
// newClientConfig.
type clientConfig struct {
	// semaphoreMultiplier scales the connection-queue-derived semaphore
	// capacity. A value of 0 (or negative) disables the semaphore entirely
	// — every acquirePermit becomes a no-op and the underlying aerospike
	// client governs concurrency on its own. Default: 1.0.
	semaphoreMultiplier float64
}

// ClientOption configures a Client at construction time.
type ClientOption func(*clientConfig)

// WithSemaphoreMultiplier scales the connection-queue-derived semaphore
// capacity for the constructed Client.
//
//	multiplier <= 0  disables the semaphore entirely. All permit acquires
//	                 become no-ops; only the underlying aerospike client's
//	                 own connection pool governs concurrency.
//	multiplier  NaN  treated as garbage input and disables the semaphore.
//	multiplier  > 0  scales the queue size derived from the policy (or
//	                 DefaultConnectionQueueSize):
//	                     scaledQueue = max(1, round(queueSize * multiplier))
//	                 clamped to maxSemaphoreCapacity (1<<20) to bound the
//	                 worst-case channel allocation. e.g. 2.0 doubles
//	                 capacity, 0.5 halves it.
//
// Typical uses:
//   - 0 to opt out of the in-process throttle when the workload is already
//     bounded upstream and the parking overhead is undesirable.
//   - <1 to over-restrict (sharing the aerospike server with other clients).
//   - >1 when the deployment has been verified to handle more concurrent
//     operations than the default queue size implies.
func WithSemaphoreMultiplier(multiplier float64) ClientOption {
	return func(c *clientConfig) {
		c.semaphoreMultiplier = multiplier
	}
}

func newClientConfig(opts []ClientOption) *clientConfig {
	cfg := &clientConfig{
		semaphoreMultiplier: defaultSemaphoreMultiplier,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}

	return cfg
}

// buildConnSemaphore returns the buffered channel used as the connection
// semaphore, or nil when the multiplier disables it. nil is the documented
// signal to acquirePermit / releasePermit that the throttle is off.
//
// NaN and non-positive multipliers disable the semaphore (NaN is treated as
// garbage input, not a "default"). Positive +Inf and any scaled value above
// maxSemaphoreCapacity are clamped to maxSemaphoreCapacity so a misconfig
// can't trigger a runaway channel allocation.
func buildConnSemaphore(queueSize int, multiplier float64) chan struct{} {
	if math.IsNaN(multiplier) || multiplier <= 0 {
		return nil
	}

	var scaled int

	scaledF := float64(queueSize) * multiplier
	switch {
	case math.IsInf(scaledF, 1) || scaledF >= float64(maxSemaphoreCapacity):
		scaled = maxSemaphoreCapacity
	default:
		scaled = int(math.Round(scaledF))
	}

	if scaled < 1 {
		scaled = 1
	}

	if scaled > maxSemaphoreCapacity {
		scaled = maxSemaphoreCapacity
	}

	return make(chan struct{}, scaled)
}

// ClientStats holds the statistics for Aerospike operations
type ClientStats struct {
	stat             *gocore.Stat
	operateStat      *gocore.Stat
	batchOperateStat *gocore.Stat
}

// NewClientStats creates a new ClientStats instance
func NewClientStats() *ClientStats {
	stat := gocore.NewStat("Aerospike")
	return &ClientStats{
		stat:             stat,
		operateStat:      stat.NewStat("Operate").AddRanges(0, 1, 100, 1_000, 10_000, 100_000),
		batchOperateStat: stat.NewStat("BatchOperate").AddRanges(0, 1, 100, 1_000, 10_000, 100_000),
	}
}

// Client is a wrapper around aerospike.Client that provides a semaphore to limit concurrent connections.
type Client struct {
	*aerospike.Client
	connSemaphore chan struct{} // Simple channel-based semaphore; nil when disabled.
	// connQueueSize is the underlying aerospike client's connection-queue
	// size (post-policy resolution). GetConnectionQueueSize reports this
	// when connSemaphore is nil so external heuristics still see a non-zero
	// pool capacity.
	connQueueSize int
	stats         *ClientStats // Always initialized, never nil
}

// NewClient creates a new Aerospike client with the specified hostname and port.
// Optional ClientOptions (e.g. WithSemaphoreMultiplier) tune behaviour.
func NewClient(hostname string, port int, opts ...ClientOption) (*Client, error) {
	client, err := aerospike.NewClient(hostname, port)
	if err != nil {
		return nil, err
	}

	cfg := newClientConfig(opts)

	// Get queue size from default policy
	policy := aerospike.NewClientPolicy()
	queueSize := getConnectionQueueSize(policy)

	return &Client{
		Client:        client,
		connSemaphore: buildConnSemaphore(queueSize, cfg.semaphoreMultiplier),
		connQueueSize: queueSize,
		stats:         NewClientStats(),
	}, nil
}

// NewClientWithPolicyAndHost creates a new Aerospike client with the specified policy and hosts.
// Optional ClientOptions (e.g. WithSemaphoreMultiplier) tune behaviour and must be supplied via
// NewClientWithPolicyAndHostOpts to keep the existing variadic-host signature intact.
func NewClientWithPolicyAndHost(policy *aerospike.ClientPolicy, hosts ...*aerospike.Host) (*Client, aerospike.Error) {
	return NewClientWithPolicyAndHostOpts(policy, hosts, nil)
}

// NewClientWithPolicyAndHostOpts is the option-aware variant of
// NewClientWithPolicyAndHost. hosts and opts are accepted as explicit slices
// (rather than variadic) so the two slice arguments don't collide.
func NewClientWithPolicyAndHostOpts(policy *aerospike.ClientPolicy, hosts []*aerospike.Host, opts []ClientOption) (*Client, aerospike.Error) {
	var (
		client *aerospike.Client
		err    aerospike.Error
	)

	// Default retry settings
	maxRetries := 3
	retryDelay := 1 * time.Second

	// If timeout is very short (indicating test mode), don't retry
	if policy != nil && policy.Timeout > 0 && policy.Timeout <= 200*time.Millisecond {
		maxRetries = 1 // No retries for short timeouts
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		client, err = aerospike.NewClientWithPolicyAndHost(policy, hosts...)
		if err == nil {
			// Connection successful
			break
		}

		// Use the Matches method to check against transient error codes
		isTransientError := err.Matches(
			types.INVALID_NODE_ERROR,
			types.TIMEOUT,
			types.NO_RESPONSE,
			types.NETWORK_ERROR,
			types.SERVER_NOT_AVAILABLE,
			types.NO_AVAILABLE_CONNECTIONS_TO_NODE,
		)

		if !isTransientError {
			// Error is not transient, don't retry
			break
		}

		// Log the retry attempt (optional, but useful for debugging)
		// log.Printf("Aerospike connection attempt %d failed with transient error (%d): %v. Retrying in %v...", attempt, asAeroErr.ResultCode(), err, retryDelay)

		if attempt < maxRetries {
			time.Sleep(retryDelay)
		}
	}

	if err != nil {
		return nil, err
	}

	cfg := newClientConfig(opts)
	queueSize := getConnectionQueueSize(policy)

	return &Client{
		Client:        client,
		connSemaphore: buildConnSemaphore(queueSize, cfg.semaphoreMultiplier),
		connQueueSize: queueSize,
		stats:         NewClientStats(),
	}, nil
}

// Put is a wrapper around aerospike.Client.Put that uses semaphore to limit concurrent connections.
func (c *Client) Put(policy *aerospike.WritePolicy, key *aerospike.Key, binMap aerospike.BinMap) aerospike.Error {
	if err := c.acquirePermit(policy); err != nil {
		return err
	}
	defer c.releasePermit()

	start := gocore.CurrentTime()

	defer func() {

		// Extract keys from binMap
		keys := make([]string, len(binMap))

		var i int

		for k := range binMap {
			keys[i] = k
			i++
		}

		// Sort the keys
		sort.Strings(keys)

		// Build the query string with sorted keys
		var sb strings.Builder

		sb.WriteString("Put: ")

		for i, k := range keys {
			if i > 0 {
				sb.WriteString(",")
			}

			sb.WriteString(k)
		}

		c.stats.stat.NewStat(sb.String()).AddTime(start)
	}()

	return c.Client.Put(policy, key, binMap)
}

// PutBins is a wrapper around aerospike.Client.PutBins that uses semaphore to limit concurrent connections.
func (c *Client) PutBins(policy *aerospike.WritePolicy, key *aerospike.Key, bins ...*aerospike.Bin) aerospike.Error {
	if err := c.acquirePermit(policy); err != nil {
		return err
	}
	defer c.releasePermit()

	start := gocore.CurrentTime()

	defer func() {

		// Extract keys from binMap
		keys := make([]string, len(bins))
		for i, bin := range bins {
			keys[i] = bin.Name
		}

		// Build the query string with sorted keys
		var sb strings.Builder

		sb.WriteString("PutBins: ")

		for i, k := range keys {
			if i > 0 {
				sb.WriteString(",")
			}

			sb.WriteString(k)
		}

		c.stats.stat.NewStat(sb.String()).AddTime(start)
	}()

	return c.Client.PutBins(policy, key, bins...)
}

// Delete is a wrapper around aerospike.Client.Delete that uses semaphore to limit concurrent connections.
func (c *Client) Delete(policy *aerospike.WritePolicy, key *aerospike.Key) (bool, aerospike.Error) {
	if err := c.acquirePermit(policy); err != nil {
		return false, err
	}
	defer c.releasePermit()

	start := gocore.CurrentTime()

	defer func() {
		c.stats.stat.NewStat("Delete").AddTime(start)
	}()

	return c.Client.Delete(policy, key)
}

// Get is a wrapper around aerospike.Client.Get that uses semaphore to limit concurrent connections.
func (c *Client) Get(policy *aerospike.BasePolicy, key *aerospike.Key, binNames ...string) (*aerospike.Record, aerospike.Error) {
	if err := c.acquirePermit(policy); err != nil {
		return nil, err
	}
	defer c.releasePermit()

	start := gocore.CurrentTime()

	defer func() {

		// Build the query string with sorted keys
		var sb strings.Builder

		sb.WriteString("Get: ")

		for i, k := range binNames {
			if i > 0 {
				sb.WriteString(",")
			}

			sb.WriteString(k)
		}

		c.stats.stat.NewStat(sb.String()).AddTime(start)
	}()

	return c.Client.Get(policy, key, binNames...)
}

// Operate is a wrapper around aerospike.Client.Operate that uses semaphore to limit concurrent connections.
func (c *Client) Operate(policy *aerospike.WritePolicy, key *aerospike.Key, operations ...*aerospike.Operation) (*aerospike.Record, aerospike.Error) {
	if err := c.acquirePermit(policy); err != nil {
		return nil, err
	}
	defer c.releasePermit()

	start := gocore.CurrentTime()
	defer func() {
		c.stats.operateStat.AddTimeForRange(start, len(operations))
	}()

	return c.Client.Operate(policy, key, operations...)
}

// BatchOperate is a wrapper around aerospike.Client.BatchOperate that uses semaphore to limit concurrent connections.
func (c *Client) BatchOperate(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
	if err := c.acquirePermit(policy); err != nil {
		return err
	}
	defer c.releasePermit()

	start := gocore.CurrentTime()
	defer func() {
		c.stats.batchOperateStat.AddTimeForRange(start, len(records))
	}()

	return c.Client.BatchOperate(policy, records)
}

// GetConnectionQueueSize returns the size of the connection semaphore. When
// the semaphore is disabled (multiplier <= 0) the in-process throttle is gone
// and concurrency is governed only by the underlying aerospike-client-go
// connection pool — in that case it returns the resolved underlying
// connection-queue size so callers using this as a pool-capacity hint
// (e.g. pruner heuristics) keep seeing a meaningful value instead of 0.
func (c *Client) GetConnectionQueueSize() int {
	if c.connSemaphore == nil {
		return c.connQueueSize
	}

	return cap(c.connSemaphore)
}

// acquirePermit attempts to acquire a permit from the connection semaphore with an optional timeout.
// The policy parameter can be nil, in which case no timeout is used (blocks until available).
// If the policy has a TotalTimeout > 0, a fraction of that timeout (semaphoreTimeoutFraction)
// is used for permit acquisition to ensure the total operation time stays within bounds.
// Returns an error if the timeout expires before a permit becomes available.
//
// Accepts any Aerospike policy type (BasePolicy, WritePolicy, BatchPolicy) as they all
// embed BasePolicy which contains TotalTimeout.
//
// When the client was constructed with WithSemaphoreMultiplier(0) (or any
// non-positive multiplier) the semaphore is disabled and acquirePermit is
// an unconditional no-op — releasePermit mirrors that and skips the receive.
func (c *Client) acquirePermit(policy any) aerospike.Error {
	if c.connSemaphore == nil {
		return nil
	}

	totalTimeout := time.Duration(0)

	// Extract timeout from policy if available
	if policy != nil {
		switch p := policy.(type) {
		case *aerospike.BasePolicy:
			if p != nil && p.TotalTimeout > 0 {
				totalTimeout = p.TotalTimeout
			}
		case *aerospike.WritePolicy:
			if p != nil && p.TotalTimeout > 0 {
				totalTimeout = p.TotalTimeout
			}
		case *aerospike.BatchPolicy:
			if p != nil && p.TotalTimeout > 0 {
				totalTimeout = p.TotalTimeout
			}
		}
	}

	if totalTimeout <= 0 {
		// No timeout - block until available
		c.connSemaphore <- struct{}{}
		return nil
	}

	// Calculate semaphore timeout as a fraction of total timeout
	// This ensures total operation time (semaphore wait + actual operation) stays within bounds
	semaphoreTimeout := time.Duration(float64(totalTimeout) * semaphoreTimeoutFraction)
	if semaphoreTimeout < minSemaphoreTimeout {
		semaphoreTimeout = minSemaphoreTimeout
	}

	timer := time.NewTimer(semaphoreTimeout)
	defer timer.Stop()

	select {
	case c.connSemaphore <- struct{}{}:
		return nil
	case <-timer.C:
		return aerospike.ErrTimeout
	}
}

// releasePermit releases a permit back to the connection semaphore. No-op
// when the semaphore was disabled at construction time (multiplier <= 0).
func (c *Client) releasePermit() {
	if c.connSemaphore == nil {
		return
	}

	<-c.connSemaphore
}

// CalculateKeySource generates a key source based on the transaction hash, vout, and batch size.
func CalculateKeySource(hash *chainhash.Hash, vout uint32, batchSize int) []byte {
	if batchSize <= 0 {
		return nil
	}

	num := vout / uint32(batchSize)

	return CalculateKeySourceInternal(hash, num)
}

func CalculateKeySourceInternal(hash *chainhash.Hash, num uint32) []byte {
	if num == 0 {
		// Fast path: just return cloned hash bytes
		return hash.CloneBytes()
	}

	// Optimized path: pre-allocate slice with exact capacity to avoid reallocation
	keySource := make([]byte, chainhash.HashSize+4)
	copy(keySource[:chainhash.HashSize], hash[:])

	// Directly write little-endian uint32 to avoid intermediate allocation
	binary.LittleEndian.PutUint32(keySource[chainhash.HashSize:], num)

	return keySource
}
