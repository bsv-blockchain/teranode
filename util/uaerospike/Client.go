package uaerospike

import (
	"sort"
	"strings"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/ordishs/gocore"
)

var (
	stat             = gocore.NewStat("Aerospike")
	operateStat      = stat.NewStat("Operate").AddRanges(0, 1, 100, 1_000, 10_000, 100_000)
	batchOperateStat = stat.NewStat("BatchOperate").AddRanges(0, 1, 100, 1_000, 10_000, 100_000)
)

type Client struct {
	*aerospike.Client
}

func NewClientWithPolicyAndHost(policy *aerospike.ClientPolicy, hosts ...*aerospike.Host) (*Client, aerospike.Error) {
	client, err := aerospike.NewClientWithPolicyAndHost(policy, hosts...)
	return &Client{client}, err
}

func (c *Client) Put(policy *aerospike.WritePolicy, key *aerospike.Key, binMap aerospike.BinMap) aerospike.Error {
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
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat("Delete").AddTime(start)
	}()

	return c.Client.Delete(policy, key)
}

func (c *Client) Get(policy *aerospike.BasePolicy, key *aerospike.Key, binNames ...string) (*aerospike.Record, aerospike.Error) {
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
	start := gocore.CurrentTime()
	defer func() {
		operateStat.AddTimeForRange(start, len(operations))
	}()

	return c.Client.Operate(policy, key, operations...)
}

func (c *Client) BatchOperate(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
	start := gocore.CurrentTime()
	defer func() {
		batchOperateStat.AddTimeForRange(start, len(records))
	}()

	return c.Client.BatchOperate(policy, records)
}
