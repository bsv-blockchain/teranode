package util

import (
	"fmt"
	"github.com/ordishs/gocore"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v6"
)

var aerospikeConnectionMutex sync.Mutex
var aerospikeConnections map[string]*aerospike.Client
var logger = gocore.Log("uaero", gocore.NewLogLevelFromString("DEBUG"))

func init() {
	aerospikeConnections = make(map[string]*aerospike.Client)
}

func GetAerospikeClient(url *url.URL) (*aerospike.Client, error) {
	aerospikeConnectionMutex.Lock()
	defer aerospikeConnectionMutex.Unlock()

	var err error
	client, found := aerospikeConnections[url.Host]
	if !found {
		logger.Infof("[AEROSPIKE] Creating aerospike client for host: %v", url.Host)
		client, err = getAerospikeClient(url)
		if err != nil {
			return nil, err
		}
		aerospikeConnections[url.Host] = client
	} else {
		logger.Infof("[AEROSPIKE] Reusing aerospike client: %v", url.Host)
	}

	return client, nil
}

func getAerospikeClient(url *url.URL) (*aerospike.Client, error) {
	if len(url.Path) < 1 {
		return nil, fmt.Errorf("aerospike namespace not found")
	}

	policy := aerospike.NewClientPolicy()
	// todo optimize these https://github.com/aerospike/aerospike-client-go/issues/256#issuecomment-479964112
	// todo optimize read policies
	// todo optimize write policies
	policy.LimitConnectionsToQueueSize = false
	policy.ConnectionQueueSize = 1024
	policy.MaxErrorRate = 0

	if url.User != nil {
		policy.AuthMode = 2

		policy.User = url.User.Username()
		var ok bool
		policy.Password, ok = url.User.Password()
		if !ok {
			policy.User = ""
			policy.Password = ""
		}
	}

	var hosts []*aerospike.Host
	urlHosts := strings.Split(url.Host, ",")
	for _, host := range urlHosts {
		hostParts := strings.Split(host, ":")
		if len(hostParts) == 2 {
			port, err := strconv.ParseInt(hostParts[1], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid port %v", hostParts[1])
			}

			hosts = append(hosts, &aerospike.Host{
				Name: hostParts[0],
				Port: int(port),
			})
		} else if len(hostParts) == 1 {
			hosts = append(hosts, &aerospike.Host{
				Name: hostParts[0],
				Port: 3000,
			})
		} else {
			return nil, fmt.Errorf("invalid host %v", host)
		}
	}

	fmt.Printf("url %v policy %v\n", url, policy)

	client, err := aerospike.NewClientWithPolicyAndHost(policy, hosts...)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// AerospikeReadPolicyOptions represents functional options for modifying Aerospike read policies.
type AerospikeReadPolicyOptions func(*aerospike.BasePolicy)

// WithTotalTimeout sets the total timeout for the Aerospike read policy.
func WithTotalTimeout(timeout time.Duration) AerospikeReadPolicyOptions {
	return func(policy *aerospike.BasePolicy) {
		policy.TotalTimeout = timeout
	}
}

// WithSocketTimeout sets the socket timeout for the Aerospike read policy.
func WithSocketTimeout(timeout time.Duration) AerospikeReadPolicyOptions {
	return func(policy *aerospike.BasePolicy) {
		policy.SocketTimeout = timeout
	}
}

// WithMaxRetries sets the maximum number of retries for the Aerospike read policy.
func WithMaxRetries(retries int) AerospikeReadPolicyOptions {
	return func(policy *aerospike.BasePolicy) {
		policy.MaxRetries = retries
	}
}

// GetAerospikeReadPolicy creates a new Aerospike read policy with the provided options applied. Used to manage
// default connection parameters
// If no options are provided, the policy will use the default values:
//   - TotalTimeout:     50 milliseconds
//   - SocketTimeout:    50 milliseconds
//   - MaxRetries:       1
func GetAerospikeReadPolicy(options ...AerospikeReadPolicyOptions) *aerospike.BasePolicy {
	readPolicy := aerospike.NewPolicy()
	readPolicy.MaxRetries = 1

	// Apply the provided options
	for _, opt := range options {
		opt(readPolicy)
	}

	return readPolicy
}

// AerospikeWritePolicyOptions represents functional options for modifying Aerospike write policies.
type AerospikeWritePolicyOptions func(*aerospike.WritePolicy)

// WithTotalTimeoutWrite sets the total timeout for the Aerospike write policy.
func WithTotalTimeoutWrite(timeout time.Duration) AerospikeWritePolicyOptions {
	return func(policy *aerospike.WritePolicy) {
		policy.BasePolicy.TotalTimeout = timeout
	}
}

// WithSocketTimeoutWrite sets the socket timeout for the Aerospike write policy.
func WithSocketTimeoutWrite(timeout time.Duration) AerospikeWritePolicyOptions {
	return func(policy *aerospike.WritePolicy) {
		policy.BasePolicy.SocketTimeout = timeout
	}
}

// WithMaxRetriesWrite sets the maximum number of retries for the Aerospike write policy.
func WithMaxRetriesWrite(retries int) AerospikeWritePolicyOptions {
	return func(policy *aerospike.WritePolicy) {
		policy.BasePolicy.MaxRetries = retries
	}
}

// GetAerospikeWritePolicy creates a new Aerospike write policy with the provided options applied. Used to manage
// default connection parameters
// If no options are provided, the policy will use the default values:
//   - TotalTimeout:     50 milliseconds
//   - SocketTimeout:    25 milliseconds
func GetAerospikeWritePolicy(generation, expiration uint32, options ...AerospikeWritePolicyOptions) *aerospike.WritePolicy {
	writePolicy := aerospike.NewWritePolicy(generation, expiration)

	// Apply the provided options
	for _, opt := range options {
		opt(writePolicy)
	}

	return writePolicy
}
