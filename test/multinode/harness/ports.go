//go:build network_chaos

// Package harness provides Go helpers for driving compose/multinode.sh from
// tests. Port math in this file mirrors gennodes' host-port allocation so
// callers never have to hard-code port numbers.
package harness

// HostBase is the host-port base for node n (1-indexed), matching
// compose/cmd/gennodes/main.go: HostBase = 20000 + (n-1)*2000.
func HostBase(node int) int {
	return 20000 + (node-1)*2000
}

// HealthPort maps to the container's 8000 health endpoint.
func HealthPort(node int) int { return HostBase(node) }

// PropagationPort maps to the container's 8084 propagation gRPC.
func PropagationPort(node int) int { return HostBase(node) + 84 }

// DashboardPort maps to the container's 8090 asset HTTP.
func DashboardPort(node int) int { return HostBase(node) + 90 }

// PrometheusPort maps to the container's 9091 metrics endpoint.
func PrometheusPort(node int) int { return HostBase(node) + 1091 }

// RPCPort maps to the container's 9292 JSON-RPC endpoint.
func RPCPort(node int) int { return HostBase(node) + 1292 }

// P2PPort maps to the container's 9905 libp2p TCP port.
func P2PPort(node int) int { return HostBase(node) + 1905 }
