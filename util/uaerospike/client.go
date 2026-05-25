package uaerospike

import (
	"encoding/binary"
	"sort"
	"strings"
	"sync"
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
)

// getConnectionQueueSize returns the connection queue size from the given policy
// or falls back to DefaultConnectionQueueSize if the policy is nil or returns 0
func getConnectionQueueSize(policy *aerospike.ClientPolicy) int {
	if policy != nil && policy.ConnectionQueueSize > 0 {
		return policy.ConnectionQueueSize
	}
	return DefaultConnectionQueueSize
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

// ConnectionBudgetReport summarises declared connection use across services so
// operators can see when configured concurrency over-subscribes the pool.
//
// Recommended is computed as int(PoolSize * Threshold). Exceeded is true when
// TotalBudget > Recommended. The breakdown is a snapshot of all currently
// registered service budgets, safe to read or mutate by the caller.
type ConnectionBudgetReport struct {
	TotalBudget int
	PoolSize    int
	Threshold   float64
	Recommended int
	Exceeded    bool
	Breakdown   map[string]int
}

// Client is a wrapper around aerospike.Client that limits concurrent connections
// via a single connSemaphore sized to ConnectionQueueSize and exposes a
// connection-budget registry so each long-running query consumer can declare
// its max concurrent use. The registry is diagnostic only -- it does not
// throttle; it lets operators see when configured concurrency would
// over-subscribe the pool (see RegisterConnectionBudget).
type Client struct {
	*aerospike.Client
	connSemaphore chan struct{}
	stats         *ClientStats

	budgetMu sync.Mutex
	budgets  map[string]int
}

// NewClient creates a new Aerospike client with the specified hostname and port.
func NewClient(hostname string, port int) (*Client, error) {
	client, err := aerospike.NewClient(hostname, port)
	if err != nil {
		return nil, err
	}

	// Get queue size from default policy
	policy := aerospike.NewClientPolicy()
	queueSize := getConnectionQueueSize(policy)

	return &Client{
		Client:        client,
		connSemaphore: make(chan struct{}, queueSize),
		stats:         NewClientStats(),
		budgets:       make(map[string]int),
	}, nil
}

// NewClientWithPolicyAndHost creates a new Aerospike client with the specified policy and hosts.
func NewClientWithPolicyAndHost(policy *aerospike.ClientPolicy, hosts ...*aerospike.Host) (*Client, aerospike.Error) {
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

	queueSize := getConnectionQueueSize(policy)

	return &Client{
		Client:        client,
		connSemaphore: make(chan struct{}, queueSize),
		stats:         NewClientStats(),
		budgets:       make(map[string]int),
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

// Execute is a wrapper around aerospike.Client.Execute that uses the connection semaphore
// to limit concurrent connections. Execute runs a server-side UDF on a single key.
func (c *Client) Execute(policy *aerospike.WritePolicy, key *aerospike.Key, packageName string, functionName string, args ...aerospike.Value) (any, aerospike.Error) {
	if err := c.acquirePermit(policy); err != nil {
		return nil, err
	}
	defer c.releasePermit()

	start := gocore.CurrentTime()
	defer func() {
		c.stats.stat.NewStat("Execute").AddTime(start)
	}()

	return c.Client.Execute(policy, key, packageName, functionName, args...)
}

// RegisterConnectionBudget records a service's expected max concurrent connection
// use and returns a report covering all currently registered services. The
// registry is diagnostic only: callers use the returned report to emit an
// operator-facing log when Exceeded is true. It does not throttle.
//
// Re-registering the same service replaces the prior value (use this when a
// service's worker count is re-computed). Passing a budget of 0 removes the
// service from the breakdown.
//
// Concurrent calls from different services are safe. The threshold parameter
// is applied to the returned report only; it is not persisted.
func (c *Client) RegisterConnectionBudget(service string, budget int, threshold float64) ConnectionBudgetReport {
	c.budgetMu.Lock()
	defer c.budgetMu.Unlock()

	if c.budgets == nil {
		c.budgets = make(map[string]int)
	}
	if budget <= 0 {
		delete(c.budgets, service)
	} else {
		c.budgets[service] = budget
	}

	return c.connectionBudgetReportLocked(threshold)
}

// ConnectionBudget returns the current cumulative report without changing any
// registration. Use this for periodic diagnostics or in tests.
func (c *Client) ConnectionBudget(threshold float64) ConnectionBudgetReport {
	c.budgetMu.Lock()
	defer c.budgetMu.Unlock()
	return c.connectionBudgetReportLocked(threshold)
}

func (c *Client) connectionBudgetReportLocked(threshold float64) ConnectionBudgetReport {
	poolSize := cap(c.connSemaphore)
	total := 0
	breakdown := make(map[string]int, len(c.budgets))
	for k, v := range c.budgets {
		total += v
		breakdown[k] = v
	}

	recommended := int(float64(poolSize) * threshold)
	return ConnectionBudgetReport{
		TotalBudget: total,
		PoolSize:    poolSize,
		Threshold:   threshold,
		Recommended: recommended,
		Exceeded:    total > recommended,
		Breakdown:   breakdown,
	}
}

// GetConnectionQueueSize returns the size of the connection semaphore.
// This represents the maximum number of concurrent Aerospike operations allowed.
func (c *Client) GetConnectionQueueSize() int {
	return cap(c.connSemaphore)
}

// extractTimeout extracts the TotalTimeout from any Aerospike policy type.
// Returns 0 if the policy is nil or does not have a TotalTimeout set.
func extractTimeout(policy any) time.Duration {
	if policy == nil {
		return 0
	}
	switch p := policy.(type) {
	case *aerospike.BasePolicy:
		if p != nil && p.TotalTimeout > 0 {
			return p.TotalTimeout
		}
	case *aerospike.WritePolicy:
		if p != nil && p.TotalTimeout > 0 {
			return p.TotalTimeout
		}
	case *aerospike.BatchPolicy:
		if p != nil && p.TotalTimeout > 0 {
			return p.TotalTimeout
		}
	case *aerospike.QueryPolicy:
		if p != nil && p.TotalTimeout > 0 {
			return p.TotalTimeout
		}
	}
	return 0
}

// acquireSemaphore attempts to acquire a permit from the given semaphore with an optional timeout.
// If totalTimeout > 0, a fraction of that timeout (semaphoreTimeoutFraction) is used for
// acquisition to ensure the total operation time stays within bounds.
func acquireSemaphore(sem chan struct{}, totalTimeout time.Duration) aerospike.Error {
	if totalTimeout <= 0 {
		// No timeout - block until available
		sem <- struct{}{}
		return nil
	}

	// Calculate semaphore timeout as a fraction of total timeout
	semaphoreTimeout := time.Duration(float64(totalTimeout) * semaphoreTimeoutFraction)
	if semaphoreTimeout < minSemaphoreTimeout {
		semaphoreTimeout = minSemaphoreTimeout
	}

	timer := time.NewTimer(semaphoreTimeout)
	defer timer.Stop()

	select {
	case sem <- struct{}{}:
		return nil
	case <-timer.C:
		return aerospike.ErrTimeout
	}
}

// acquirePermit attempts to acquire a permit from the connection semaphore with an optional timeout.
// The policy parameter can be nil, in which case no timeout is used (blocks until available).
// If the policy has a TotalTimeout > 0, a fraction of that timeout (semaphoreTimeoutFraction)
// is used for permit acquisition to ensure the total operation time stays within bounds.
// Returns an error if the timeout expires before a permit becomes available.
//
// Accepts any Aerospike policy type (BasePolicy, WritePolicy, BatchPolicy, QueryPolicy) as they all
// embed BasePolicy which contains TotalTimeout.
func (c *Client) acquirePermit(policy any) aerospike.Error {
	return acquireSemaphore(c.connSemaphore, extractTimeout(policy))
}

// releasePermit releases a permit back to the connection semaphore.
func (c *Client) releasePermit() {
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
