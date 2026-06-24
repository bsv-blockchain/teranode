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
	8084, // propagation grpc
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
	// AllInOne controls topology: true emits one teranode container per node
	// running all services in a single process; false emits one container per
	// microservice (chaos-tester mode) so individual services can be killed,
	// paused, or isolated without taking down the whole node.
	AllInOne bool
	// SplitServices is the per-node service definition iterated by the split
	// compose template. Populated only when AllInOne is false. Single
	// source of truth for split-mode service shape — adding a new entry here
	// adds one container per node end-to-end.
	SplitServices []splitService
}

// splitService describes one microservice container in the split topology.
// Each entry produces one container per node when the template iterates
// {{range .Nodes}}{{range $.SplitServices}}.
type splitService struct {
	// Name is the suffix used in container/service names ("blockchain" ->
	// teranodeN-blockchain) and in the daemon flag ("-blockchain=1").
	Name string

	// EntrypointFlags are the per-service flags passed to teranode.run
	// after "-all=0". Most services have one ("-blockchain=1"); the core
	// sidecar bundles several so it carries the full list.
	EntrypointFlags []string

	// NeedsNetAdmin grants the container NET_ADMIN. Only the p2p container
	// gets this — it's the netns target for chaos isolate/slow primitives.
	NeedsNetAdmin bool

	// HostPorts is the list of container ports mapped to the host at
	// HostBase + (port - 8000). One service may expose multiple host ports
	// (e.g. p2p has both 9905 tcp and 9906 http; core has six).
	HostPorts []int

	// ExposePorts is the full set of ports surfaced to the docker network
	// (typically a superset of HostPorts — e.g. propagation also exposes
	// 8833 HTTP for in-cluster proxy use, not mapped to the host).
	ExposePorts []int

	// DependsOnBlockchain biases startup ordering (compose `depends_on`).
	// False only for the blockchain service itself; everything else dials
	// blockchain on boot so getting it ready first cuts retry noise.
	DependsOnBlockchain bool

	// DataMounts lists the subdirectories under data/multinode/teranodeN/
	// that this service container needs bind-mounted to /app/data/. Each
	// entry becomes one `volumes:` line. The list should be the SMALLEST
	// set the service actually accesses — the whole point of the split
	// topology is that chaos-killing one container doesn't perturb data
	// owned by services it has no reason to share with. Source for each
	// service's set: daemon/daemon_services.go's start*Service functions
	// (which Get*Store calls they make), plus subtreevalidation's
	// QuorumPath setting (the only consumer of subtree_quorum).
	DataMounts []string

	// HealthcheckPort is the TCP port probed by the container's
	// healthcheck (`nc -z localhost <port>`). Zero means no healthcheck
	// is emitted. Picking the primary listener port is enough: grpc-go's
	// Server.Serve binds the listener AND wires the handler in one call,
	// so a successful TCP accept is a reliable proxy for "ready to serve".
	// Dependents that want to wait on readiness reference this via
	// `condition: service_healthy` in their compose `depends_on` block.
	HealthcheckPort int
}

// buildSplitServices returns the single, version-controlled definition of
// the split-mode service set. Order in the returned slice is the order
// services appear in the generated compose file.
//
// DataMounts notes (per daemon/daemon_services.go start*Service):
//
//	blockchain          - postgres only, no blob stores
//	blockassembly       - txStore + subtreeStore + utxoStore (external)
//	blockvalidation     - subtreeStore + txStore + utxoStore (external)
//	subtreevalidation   - subtreeStore + txStore + utxoStore + quorum_path
//	validator           - utxoStore only (external for aerospike client overflow)
//	propagation         - txStore only (no utxo, no blockchain blob)
//	p2p                 - no blob stores (blockchain client + kafka only)
//	asset               - utxoStore + txStore + subtreeStore (serves them over HTTP)
//	core                - rpc/alert/bp/utxop/pruner/legacy → needs blockstore
//	                       (blockpersister + utxopersister), subtreestore
//	                       (blockpersister + legacy), txstore (rpc), and
//	                       external (everything calling utxoStore)
//
// The `external` subdir is the aerospike-client side overflow for
// large UTXO records (set in utxostore URL: externalStore=file://…/external).
// Any service that opens utxoStore needs it mounted.
//
// HostPorts / ExposePorts entries must be >= 8000 — the split compose
// template's host-port arithmetic (HostBase + container - 8000) silently
// breaks otherwise. See templates/docker-compose-split.yml.tmpl.
func buildSplitServices() []splitService {
	return []splitService{
		{
			Name:            "blockchain",
			EntrypointFlags: []string{"-blockchain=1"},
			HostPorts:       []int{8087},
			ExposePorts:     []int{8087},
			DataMounts:      nil,
			HealthcheckPort: 8087, // gRPC; depended on by 8 sibling services
		},
		{
			Name:                "blockassembly",
			EntrypointFlags:     []string{"-blockassembly=1"},
			HostPorts:           []int{8085},
			ExposePorts:         []int{8085},
			DependsOnBlockchain: true,
			DataMounts:          []string{"txstore", "subtreestore", "external"},
			HealthcheckPort:     8085,
		},
		{
			Name:                "blockvalidation",
			EntrypointFlags:     []string{"-blockvalidation=1"},
			HostPorts:           []int{8088},
			ExposePorts:         []int{8088},
			DependsOnBlockchain: true,
			DataMounts:          []string{"txstore", "subtreestore", "external"},
			HealthcheckPort:     8088,
		},
		{
			Name:                "subtreevalidation",
			EntrypointFlags:     []string{"-subtreevalidation=1"},
			HostPorts:           []int{8086},
			ExposePorts:         []int{8086},
			DependsOnBlockchain: true,
			DataMounts:          []string{"txstore", "subtreestore", "subtree_quorum", "external"},
			HealthcheckPort:     8086,
		},
		{
			Name:                "validator",
			EntrypointFlags:     []string{"-validator=1"},
			HostPorts:           []int{8081},
			ExposePorts:         []int{8081},
			DependsOnBlockchain: true,
			DataMounts:          []string{"external"},
			HealthcheckPort:     8081,
		},
		{
			Name:                "propagation",
			EntrypointFlags:     []string{"-propagation=1"},
			HostPorts:           []int{8084},
			ExposePorts:         []int{8084, 8833},
			DependsOnBlockchain: true,
			DataMounts:          []string{"txstore"},
			HealthcheckPort:     8084,
		},
		{
			Name:                "p2p",
			EntrypointFlags:     []string{"-p2p=1"},
			NeedsNetAdmin:       true,
			HostPorts:           []int{9905, 9906},
			ExposePorts:         []int{9904, 9905, 9906},
			DependsOnBlockchain: true,
			DataMounts:          nil,
			HealthcheckPort:     9904, // p2p gRPC (control plane), not 9905 (libp2p tcp)
		},
		{
			Name:                "asset",
			EntrypointFlags:     []string{"-asset=1"},
			HostPorts:           []int{8090},
			ExposePorts:         []int{8090},
			DependsOnBlockchain: true,
			DataMounts:          []string{"txstore", "subtreestore", "external"},
			HealthcheckPort:     8090,
		},
		{
			Name: "core",
			EntrypointFlags: []string{
				"-rpc=1", "-alert=1", "-blockpersister=1",
				"-utxopersister=1", "-pruner=1", "-legacy=1",
			},
			HostPorts:           []int{8000, 8083, 8093, 8099, 9091, 9292},
			ExposePorts:         []int{8000, 8083, 8093, 8099, 9091, 9292},
			DependsOnBlockchain: true,
			DataMounts:          []string{"txstore", "subtreestore", "blockstore", "external"},
			HealthcheckPort:     9292, // RPC; the 6 bundled daemons all expose ports but RPC is the one external tooling probes
		},
	}
}

func main() {
	n := flag.Int("n", 0, "number of teranode instances (3-10)")
	out := flag.String("o", "compose/generated", "output directory (relative or absolute)")
	allInOne := flag.Int("allinone", 1, "1 = single container per node (default), 0 = split per microservice (chaos topology)")
	flag.Parse()

	if *n < minNodes || *n > maxNodes {
		fmt.Fprintf(os.Stderr, "-n must be in [%d, %d], got %d\n", minNodes, maxNodes, *n)
		os.Exit(2)
	}
	if *allInOne != 0 && *allInOne != 1 {
		fmt.Fprintf(os.Stderr, "-allinone must be 0 or 1, got %d\n", *allInOne)
		os.Exit(2)
	}

	keys, err := loadKeys(*n)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load peer keys: %v\n", err)
		os.Exit(1)
	}

	s := buildSpec(keys)
	s.AllInOne = *allInOne == 1

	if err := writeAll(*out, s); err != nil {
		fmt.Fprintf(os.Stderr, "write outputs: %v\n", err)
		os.Exit(1)
	}

	topology := "all-in-one"
	if !s.AllInOne {
		topology = "split-per-service"
	}
	fmt.Printf("generated %d-node stack (%s) under %s\n", s.NodeCount, topology, *out)
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
	// buildSpec returns an all-in-one spec by default; callers that want
	// the split topology must flip AllInOne to false after construction.
	// This keeps the historical behaviour (and the existing gen_test.go)
	// working without forcing every caller to know about the new field.
	// SplitServices is populated unconditionally — it's a static list and
	// the split template ignores it when AllInOne is true. Populating
	// here (not in main) means every test/caller gets the slice for free.
	return spec{
		NodeCount:     len(nodes),
		Nodes:         nodes,
		AllInOne:      true,
		SplitServices: buildSplitServices(),
	}
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

	composeTmpl := "templates/docker-compose.yml.tmpl"
	if !s.AllInOne {
		composeTmpl = "templates/docker-compose-split.yml.tmpl"
	}
	if err := renderToFile(composeTmpl,
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
	tmpl, err := template.New(filepath.Base(tmplPath)).Funcs(template.FuncMap{
		// add / sub let templates compute host ports from HostBase. The split
		// template builds host:container mappings as
		// {{ add $node.HostBase (sub $port 8000) }}:{{ $port }} so the
		// container-port list in splitService is the single source of truth
		// (host port is derived, never hardcoded).
		"add": func(a, b int) int { return a + b },
		"sub": func(a, b int) int { return a - b },
	}).ParseFS(templatesFS, tmplPath)
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
