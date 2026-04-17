// gennodes renders a self-contained docker-compose stack for a multinode teranode
// network ({{N}} ∈ [3, 10]) under -o <outdir>. Outputs:
//
//	<outdir>/docker-compose-multinode.yml
//	<outdir>/settings_multinode.conf
//	<outdir>/postgres/init-multinode.sql
//	<outdir>/aerospike/aerospike-{1..N}.conf
//
// Bring it up with:
//
//	docker compose -f <outdir>/docker-compose-multinode.yml up -d
//
// The libp2p keypair pool is embedded from compose/gen/peer_keys.json so runs
// are reproducible without regenerating keys.
package main

import (
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/bsv-blockchain/teranode/errors"
)

//go:embed all:templates
var templatesFS embed.FS

//go:embed peer_keys.json
var peerKeysJSON []byte

// Container ports exposed by each teranode for host access. Host port =
// node.HostBase + (container - 8000). NodeStride must exceed the span of these
// ports (max 9907 - min 8000 = 1907) to avoid host-port collisions across
// nodes; we use 2000.
var hostExposedContainerPorts = []int{
	8000, // health
	8081, // validator grpc
	8083, // block persister http
	8085, // block assembly grpc
	8087, // blockchain grpc
	8090, // asset http
	8093, // coinbase grpc
	8099, // legacy grpc
	9091, // profile / metrics
	9292, // teranode RPC
	9905, // p2p tcp
	9906, // p2p http
}

const (
	minNodes          = 3
	maxNodes          = 10
	hostBaseFloor     = 20000
	nodeStride        = 2000
	aerospikeBase     = 3000
	aerospikePerNd    = 10 // node 1: 3010/3011/3012, node 10: 3100/3101/3102
	dashboardContPort = 8090
	rpcContPort       = 9292
)

type peerKey struct {
	Index      int    `json:"index"`
	PeerID     string `json:"peer_id"`
	PrivateKey string `json:"private_key_hex"`
}

type hostPort struct {
	Host      int
	Container int
}

type node struct {
	Index                int
	PeerID               string
	PrivateKey           string
	AerospikeServicePort int
	AerospikeFabricPort  int
	AerospikeHeartPort   int
	HostBase             int
	HostPorts            []hostPort
	DashboardPort        int
	RPCPort              int
	StaticPeers          string // pipe-separated multiaddrs, mesh excluding self
}

type spec struct {
	NodeCount int
	Nodes     []node
}

func main() {
	n := flag.Int("n", 0, "number of teranode instances (3-10)")
	out := flag.String("o", "compose/generated", "output directory (relative or absolute)")
	flag.Parse()

	if *n < minNodes || *n > maxNodes {
		fmt.Fprintf(os.Stderr, "-n must be in [%d, %d], got %d\n", minNodes, maxNodes, *n)
		os.Exit(2)
	}

	keys, err := loadKeys(*n)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load peer keys: %v\n", err)
		os.Exit(1)
	}

	s := buildSpec(keys)

	if err := writeAll(*out, s); err != nil {
		fmt.Fprintf(os.Stderr, "write outputs: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("generated %d-node stack under %s\n", s.NodeCount, *out)
	fmt.Printf("  docker compose -f %s/docker-compose-multinode.yml up -d\n", *out)
	fmt.Printf("  %s/open-dashboards.sh\n", *out)
}

func loadKeys(n int) ([]peerKey, error) {
	var all []peerKey
	if err := json.Unmarshal(peerKeysJSON, &all); err != nil {
		return nil, err
	}
	if len(all) < n {
		return nil, errors.NewError("peer key pool has %d entries, need %d", len(all), n)
	}
	return all[:n], nil
}

func buildSpec(keys []peerKey) spec {
	nodes := make([]node, len(keys))
	for i, k := range keys {
		idx := i + 1
		nodes[i] = node{
			Index:                idx,
			PeerID:               k.PeerID,
			PrivateKey:           k.PrivateKey,
			AerospikeServicePort: aerospikeBase + idx*aerospikePerNd,
			AerospikeFabricPort:  aerospikeBase + idx*aerospikePerNd + 1,
			AerospikeHeartPort:   aerospikeBase + idx*aerospikePerNd + 2,
			HostBase:             hostBaseFloor + (idx-1)*nodeStride,
		}
		nodes[i].DashboardPort = nodes[i].HostBase + (dashboardContPort - 8000)
		nodes[i].RPCPort = nodes[i].HostBase + (rpcContPort - 8000)
		nodes[i].HostPorts = make([]hostPort, len(hostExposedContainerPorts))
		for j, cp := range hostExposedContainerPorts {
			nodes[i].HostPorts[j] = hostPort{
				Host:      nodes[i].HostBase + (cp - 8000),
				Container: cp,
			}
		}
	}
	for i := range nodes {
		nodes[i].StaticPeers = meshPeers(keys, i+1)
	}
	return spec{NodeCount: len(nodes), Nodes: nodes}
}

// meshPeers returns the pipe-separated static-peers list for node `selfIdx`
// (1-based), one multiaddr per other node, using the docker DNS name.
func meshPeers(keys []peerKey, selfIdx int) string {
	var parts []string
	for _, k := range keys {
		if k.Index == selfIdx {
			continue
		}
		parts = append(parts, fmt.Sprintf("/dns/teranode%d/tcp/${P2P_PORT}/p2p/%s", k.Index, k.PeerID))
	}
	return strings.Join(parts, " | ")
}

func writeAll(outDir string, s spec) error {
	if err := os.MkdirAll(filepath.Join(outDir, "postgres"), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(outDir, "aerospike"), 0o755); err != nil {
		return err
	}

	if err := renderToFile("templates/docker-compose.yml.tmpl",
		filepath.Join(outDir, "docker-compose-multinode.yml"), s); err != nil {
		return err
	}
	if err := renderToFile("templates/settings.conf.tmpl",
		filepath.Join(outDir, "settings_multinode.conf"), s); err != nil {
		return err
	}
	if err := renderToFile("templates/init.sql.tmpl",
		filepath.Join(outDir, "postgres", "init-multinode.sql"), s); err != nil {
		return err
	}
	for _, nd := range s.Nodes {
		dst := filepath.Join(outDir, "aerospike", fmt.Sprintf("aerospike-%d.conf", nd.Index))
		if err := renderToFile("templates/aerospike.conf.tmpl", dst, struct {
			ServicePort   int
			FabricPort    int
			HeartbeatPort int
		}{
			ServicePort:   nd.AerospikeServicePort,
			FabricPort:    nd.AerospikeFabricPort,
			HeartbeatPort: nd.AerospikeHeartPort,
		}); err != nil {
			return err
		}
	}

	for _, name := range []string{"open-dashboards.sh", "generate-blocks.sh"} {
		p := filepath.Join(outDir, name)
		if err := renderToFile("templates/"+name+".tmpl", p, s); err != nil {
			return err
		}
		if err := os.Chmod(p, 0o755); err != nil {
			return err
		}
	}
	return nil
}

func renderToFile(tmplPath, dst string, data any) error {
	tmpl, err := template.ParseFS(templatesFS, tmplPath)
	if err != nil {
		return errors.NewError("parse %s: %v", tmplPath, err)
	}
	f, err := os.Create(dst)
	if err != nil {
		return errors.NewError("create %s: %v", dst, err)
	}
	defer f.Close()
	if err := tmpl.Execute(f, data); err != nil {
		return errors.NewError("execute %s: %v", tmplPath, err)
	}
	return nil
}
