package uaerospike

import (
	"encoding/binary"
	"sort"
	"strings"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

var (
	stat                       = gocore.NewStat("Aerospike")
	operateStat                = stat.NewStat("Operate").AddRanges(0, 1, 100, 1_000, 10_000, 100_000)
	batchOperateStat           = stat.NewStat("BatchOperate").AddRanges(0, 1, 100, 1_000, 10_000, 100_000)
	defaultConnectionQueueSize int
)

func init() {
	policy := aerospike.NewClientPolicy()
	if policy.ConnectionQueueSize == 0 {
		panic("Aerospike connection queue size is 0")
	}

	defaultConnectionQueueSize = policy.ConnectionQueueSize
}

type Client struct {
	*aerospike.Client
	connSemaphore chan struct{} // Simple channel-based semaphore
}

func NewClient(hostname string, port int) (*Client, error) {
	client, err := aerospike.NewClient(hostname, port)
	if err != nil {
		return nil, err
	}

	return &Client{
		Client:        client,
		connSemaphore: make(chan struct{}, defaultConnectionQueueSize),
	}, nil
}

func NewClientWithPolicyAndHost(policy *aerospike.ClientPolicy, hosts ...*aerospike.Host) (*Client, aerospike.Error) {
	client, err := aerospike.NewClientWithPolicyAndHost(policy, hosts...)

	if err != nil {
		return nil, err
	}

	queueSize := policy.ConnectionQueueSize
	if queueSize == 0 {
		queueSize = defaultConnectionQueueSize
	}

	return &Client{
		Client:        client,
		connSemaphore: make(chan struct{}, queueSize),
	}, nil
}

func (c *Client) Put(policy *aerospike.WritePolicy, key *aerospike.Key, binMap aerospike.BinMap) aerospike.Error {
	c.connSemaphore <- struct{}{}        // Acquire
	defer func() { <-c.connSemaphore }() // Release

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

		stat.NewStat(sb.String()).AddTime(start)
	}()

	return c.Client.Put(policy, key, binMap)
}

func (c *Client) PutBins(policy *aerospike.WritePolicy, key *aerospike.Key, bins ...*aerospike.Bin) aerospike.Error {
	c.connSemaphore <- struct{}{}        // Acquire
	defer func() { <-c.connSemaphore }() // Release

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

		stat.NewStat(sb.String()).AddTime(start)
	}()

	return c.Client.PutBins(policy, key, bins...)
}

func (c *Client) Delete(policy *aerospike.WritePolicy, key *aerospike.Key) (bool, aerospike.Error) {
	c.connSemaphore <- struct{}{}        // Acquire
	defer func() { <-c.connSemaphore }() // Release

	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat("Delete").AddTime(start)
	}()

	return c.Client.Delete(policy, key)
}

func (c *Client) Get(policy *aerospike.BasePolicy, key *aerospike.Key, binNames ...string) (*aerospike.Record, aerospike.Error) {
	c.connSemaphore <- struct{}{}        // Acquire
	defer func() { <-c.connSemaphore }() // Release

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

		stat.NewStat(sb.String()).AddTime(start)
	}()

	return c.Client.Get(policy, key, binNames...)
}

func (c *Client) Operate(policy *aerospike.WritePolicy, key *aerospike.Key, operations ...*aerospike.Operation) (*aerospike.Record, aerospike.Error) {
	c.connSemaphore <- struct{}{}        // Acquire
	defer func() { <-c.connSemaphore }() // Release

	start := gocore.CurrentTime()
	defer func() {
		operateStat.AddTimeForRange(start, len(operations))
	}()

	return c.Client.Operate(policy, key, operations...)
}

func (c *Client) BatchOperate(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
	c.connSemaphore <- struct{}{}        // Acquire
	defer func() { <-c.connSemaphore }() // Release

	start := gocore.CurrentTime()
	defer func() {
		batchOperateStat.AddTimeForRange(start, len(records))
	}()

	return c.Client.BatchOperate(policy, records)
}

func CalculateKeySource(hash *chainhash.Hash, num uint32) []byte {
	// The key is normally the hash of the transaction
	keySource := hash.CloneBytes()
	if num == 0 {
		return keySource
	}

	// Convert the offset to int64 little ending
	batchOffsetLE := make([]byte, 4)
	binary.LittleEndian.PutUint32(batchOffsetLE, num)

	keySource = append(keySource, batchOffsetLE...)

	return keySource
}
