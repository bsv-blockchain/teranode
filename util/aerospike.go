package util

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/aerospike/aerospike-client-go/v6"
)

var aerospikeConnectionMutex sync.Mutex
var aerospikeConnections map[string]*aerospike.Client

func init() {
	aerospikeConnections = make(map[string]*aerospike.Client)
}

func GetAerospikeClient(url *url.URL) (*aerospike.Client, error) {
	aerospikeConnectionMutex.Lock()
	defer aerospikeConnectionMutex.Unlock()

	var err error
	client, ok := aerospikeConnections[url.Host]
	if !ok {
		client, err = getAerospikeClient(url)
		if err != nil {
			return nil, err
		}
		aerospikeConnections[url.Host] = client
	} else {
		fmt.Printf("[AEROSPIKE] Reusing aerospike connection: %v\n", url.Host)
	}

	return client, nil
}

func getAerospikeClient(url *url.URL) (*aerospike.Client, error) {
	if len(url.Path) < 1 {
		return nil, fmt.Errorf("aerospike namespace not found")
	}

	policy := aerospike.NewClientPolicy()
	policy.LimitConnectionsToQueueSize = false
	policy.ConnectionQueueSize = 1024
	policy.MaxErrorRate = 0
	policy.MinConnectionsPerNode = 200
	policy.IdleTimeout = 250000 // server will keep connections alive for 5min or 300000

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
