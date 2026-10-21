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
	"slices"
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

	// metricsContPort is the pprof + /metrics listener every teranode
	// process binds (PROFILE_PORT in settings.conf).
	metricsContPort = 9091

	// metricsHostOffset is the first host offset handed out for per-service
	// metrics ports: service i in buildSplitServices gets
	// metricsHostOffset + i. 1910 sits above the highest derived offset in
	// use (container 9906 -> 1906) and leaves room below nodeStride (2000)
	// for the ten services the split topology allows, so no mapping can
	// collide with a derived port or with the next node's range.
	//
	// Node 1 blockassembly therefore publishes :9091 on host 21911.
	metricsHostOffset = 1910
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
	// StaticPeersSplit is StaticPeers with the split-topology DNS names
	// (teranodeN-p2p). In split mode the bare "teranodeN" name does not
	// resolve — there is no node-level container — so the all-in-one list
	// would leave every peer undiallable ("no good addresses") and blocks
	// would never propagate. Emitted only when AllInOne is false.
	StaticPeersSplit string
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

	// Retention selects the pruner/retention overrides emitted into
	// settings_multinode.conf. Defaults to productionRetention, which emits
	// nothing and leaves settings.conf's shipped values in force.
	Retention retentionProfile
}

// retentionProfile is THE place to change pruning aggressiveness for the
// generated multinode rig. Swap the values in aggressiveRetention for a
// production set and regenerate; nothing else needs to change.
//
// Invariant, documented in settings.conf and enforced by validate():
// UnminedTxRetention < BlockHeightRetention < ParentPreservationBlocks.
type retentionProfile struct {
	Name string
	// Emit false means "write no overrides at all" — the stack inherits
	// settings.conf. Every other field is ignored.
	Emit                     bool
	BlockHeightRetention     uint32 // -> global_blockHeightRetention
	UnminedTxRetention       uint32 // -> utxostore_unminedTxRetention
	ParentPreservationBlocks uint32 // -> utxostore_parentPreservationBlocks
	BlockTrigger             string // -> pruner_block_trigger
	BAWaitTimeout            string // -> pruner_blockAssemblyWaitTimeout
	BlobSafetyWindow         uint32 // -> pruner_blobDeletionSafetyWindow
	PersistSleep             string // -> blockpersister_persistSleep

	// Block-assembly shape, not retention, but carried here so one profile
	// describes one environment. Empty/zero means "do not emit", leaving
	// settings.conf in force.
	//
	// These matter for reproducing a block-assembly stall: moveForwardBlock's
	// work scales with how much a subtree holds, so a rig running the shipped
	// 32768 items per subtree does far less work per mined block than a
	// production node running 1048576, and stalls correspondingly less.
	InitialSubtreeItems   uint32 // -> initial_merkle_items_per_subtree
	MaximumSubtreeItems   uint32 // -> maximum_merkle_items_per_subtree
	DynamicSubtreeSize    string // -> blockassembly_useDynamicSubtreeSize ("true"/"false")
	RestartValidateParent string // -> blockassembly_onRestartValidateParentChain
}

// productionRetention emits nothing: the stack inherits settings.conf's
// 288 / 144 / 1440 / OnBlockPersisted / 10m. This is the default, so the
// generator's behaviour is unchanged for every existing caller.
var productionRetention = retentionProfile{Name: "production", Emit: false}

// aggressiveRetention makes pruning observable within tens of blocks on a
// fresh regtest chain instead of the 288 a production profile would need.
// Production equivalents are in the trailing comments.
//
// BAWaitTimeout is a deliberate divergence: at the production 10m the pruner
// parks silently through a block-assembly freeze and the run tells you
// nothing. At 60s it either proceeds or increments
// teranode_pruner_skipped_total{reason="block_assembly_timeout"}, which is an
// observable. Re-run at 10m once a defect reproduces.
var aggressiveRetention = retentionProfile{
	Name: "aggressive", Emit: true,
	BlockHeightRetention:     20,                 // settings.conf ships 288
	UnminedTxRetention:       10,                 // code default is BlockHeightRetention/2
	ParentPreservationBlocks: 40,                 // settings.conf ships 1440
	BlockTrigger:             "OnBlockPersisted", // core runs -blockpersister=1
	BAWaitTimeout:            "60s",              // settings.conf ships 10m — see above
	BlobSafetyWindow:         5,                  // settings.conf ships 10
	PersistSleep:             "2s",               // settings.conf ships 10s
}

// prodScalingRetention mirrors an observed production scaling deployment,
// so the rig can be run against the configuration
// the defect is actually reported on rather than an invented one.
//
// The headline is BlockHeightRetention = 6. That is ~48x more aggressive
// than the shipped default of 288, and it matters beyond raw eagerness:
// pruner_skipDuringCatchup's own documentation states it is "only safe with
// retention >= 288". Any node that falls into CATCHINGBLOCKS under load is
// therefore pruning outside the window its own settings call safe.
//
// UnminedTxRetention is set explicitly to 3, the value the loader would
// derive (globalBlockHeightRetention/2) and so what production gets. It
// cannot be left unset: the template emits every field, so 0 would be written
// and the derivation would never run. Note the struct tag in settings/utxostore_settings.go advertises
// default 144 while the loader uses global/2 - the tag is wrong, not the code.
var prodScalingRetention = retentionProfile{
	Name: "prod-scaling", Emit: true,
	BlockHeightRetention:     6,
	UnminedTxRetention:       3, // = BlockHeightRetention/2, as the loader derives
	ParentPreservationBlocks: 1440,
	BlockTrigger:             "OnBlockPersisted",
	BAWaitTimeout:            "10m",
	BlobSafetyWindow:         10,
	PersistSleep:             "10s",

	InitialSubtreeItems:   1048576, // prod; settings.conf ships 32768
	MaximumSubtreeItems:   4194304, // prod; settings.conf ships 32768
	DynamicSubtreeSize:    "false", // prod; settings.conf ships true
	RestartValidateParent: "false", // prod; settings.conf ships true
}

// retentionProfiles maps the -retention flag value to a profile.
var retentionProfiles = map[string]retentionProfile{
	"production":   productionRetention,
	"aggressive":   aggressiveRetention,
	"prod-scaling": prodScalingRetention,
}

// validate checks the retention invariant. A profile that emits nothing is
// trivially valid.
func (r retentionProfile) validate() error {
	if !r.Emit {
		return nil
	}
	if !(r.UnminedTxRetention < r.BlockHeightRetention &&
		r.BlockHeightRetention < r.ParentPreservationBlocks) {
		return errors.NewError(
			"retention profile %q violates unminedTxRetention (%d) < blockHeightRetention (%d) < parentPreservationBlocks (%d)",
			r.Name, r.UnminedTxRetention, r.BlockHeightRetention, r.ParentPreservationBlocks)
	}
	return nil
}

// extraHostPort publishes a container port on an EXPLICIT host offset rather
// than the derived (container - 8000) one.
//
// HostOffset must be in [metricsHostOffset, nodeStride): above the highest
// derived offset in use (container 9906 -> offset 1906) and below the
// per-node stride (2000), so a mapping can collide neither with a derived
// port inside its own node nor with any port of the next node.
type extraHostPort struct {
	HostOffset int
	Container  int
}

// resourceLimits is the optional cgroup cap applied to one split-mode
// container. The zero value means "unlimited" and renders nothing, so a
// default generation is byte-identical to one without limits support.
type resourceLimits struct {
	CPUs     string // compose `cpus:` value, e.g. "2" or "1.5"
	MemLimit string // compose `mem_limit:` value, e.g. "8g"
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

	// ExtraHostPorts publishes container ports on EXPLICIT host offsets
	// rather than the derived (container - 8000) one. Needed wherever two
	// services in the same node listen on the same container port: every
	// split service serves :9091 (pprof + /metrics), but the derived
	// arithmetic would hand them all HostBase+1091 and compose would refuse
	// to start. See metricsHostOffset.
	ExtraHostPorts []extraHostPort

	// Limits caps this container's CPU and memory. Populated only from the
	// -limits flag; the zero value renders nothing, so default output is
	// byte-identical to a build without this field.
	Limits resourceLimits

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
//	core                - rpc/bp/utxop/pruner/legacy → needs blockstore
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
	svcs := []splitService{
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
				"-rpc=1", "-blockpersister=1",
				"-utxopersister=1", "-pruner=1", "-legacy=1",
			},
			HostPorts:           []int{8000, 8083, 8093, 8099, 9091, 9292},
			ExposePorts:         []int{8000, 8083, 8093, 8099, 9091, 9292},
			DependsOnBlockchain: true,
			DataMounts:          []string{"txstore", "subtreestore", "blockstore", "external"},
			HealthcheckPort:     9292, // RPC; the 6 bundled daemons all expose ports but RPC is the one external tooling probes
		},
	}

	// Publish every service's :9091 on its own host offset. Assigned by
	// index rather than written into each literal above so the offsets
	// cannot drift out of step with the service order. core keeps its
	// derived HostBase+1091 mapping too — publishing one container port on
	// two host ports is legal, and it keeps the metrics port mapping
	// uniform across all nine services.
	for i := range svcs {
		svcs[i].ExtraHostPorts = append(svcs[i].ExtraHostPorts, extraHostPort{
			HostOffset: metricsHostOffset + i,
			Container:  metricsContPort,
		})
		if !slices.Contains(svcs[i].ExposePorts, metricsContPort) {
			svcs[i].ExposePorts = append(svcs[i].ExposePorts, metricsContPort)
		}
	}

	return svcs
}

// applyLimits parses a -limits spec ("svc=cpus[:mem][,svc=cpus[:mem]...]")
// and stamps the caps onto the matching entries of svcs. Unknown service
// names are rejected rather than silently ignored, so a typo surfaces at
// generation time instead of as a mysteriously unconstrained container.
func applyLimits(svcs []splitService, spec string) error {
	if strings.TrimSpace(spec) == "" {
		return nil
	}
	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		name, caps, ok := strings.Cut(entry, "=")
		if !ok {
			return errors.NewError("-limits entry %q is not of the form svc=cpus[:mem]", entry)
		}
		name = strings.TrimSpace(name)
		cpus, mem, _ := strings.Cut(caps, ":")
		cpus, mem = strings.TrimSpace(cpus), strings.TrimSpace(mem)
		if cpus == "" && mem == "" {
			return errors.NewError("-limits entry %q sets neither cpus nor mem", entry)
		}

		idx := slices.IndexFunc(svcs, func(s splitService) bool { return s.Name == name })
		if idx < 0 {
			known := make([]string, len(svcs))
			for i, s := range svcs {
				known[i] = s.Name
			}
			return errors.NewError("-limits names unknown service %q (known: %s)", name, strings.Join(known, ", "))
		}
		svcs[idx].Limits = resourceLimits{CPUs: cpus, MemLimit: mem}
	}
	return nil
}

func main() {
	n := flag.Int("n", 0, "number of teranode instances (3-10)")
	out := flag.String("o", "compose/generated", "output directory (relative or absolute)")
	allInOne := flag.Int("allinone", 1, "1 = single container per node (default), 0 = split per microservice (chaos topology)")
	limits := flag.String("limits", "", "split-mode per-service resource caps: 'svc=cpus[:mem][,...]' (e.g. 'blockassembly=2:8g,validator=4'); empty = unlimited")
	retention := flag.String("retention", "production", "pruner/retention profile: production (inherit settings.conf), aggressive (prune within tens of blocks), or prod-scaling (mirror a production scaling deployment)")
	flag.Parse()

	if *n < minNodes || *n > maxNodes {
		fmt.Fprintf(os.Stderr, "-n must be in [%d, %d], got %d\n", minNodes, maxNodes, *n)
		os.Exit(2)
	}
	if *allInOne != 0 && *allInOne != 1 {
		fmt.Fprintf(os.Stderr, "-allinone must be 0 or 1, got %d\n", *allInOne)
		os.Exit(2)
	}
	if *limits != "" && *allInOne == 1 {
		fmt.Fprintln(os.Stderr, "-limits applies to the split topology only; pass -allinone=0")
		os.Exit(2)
	}
	profile, ok := retentionProfiles[*retention]
	if !ok {
		fmt.Fprintf(os.Stderr, "-retention must be one of production, aggressive, prod-scaling; got %q\n", *retention)
		os.Exit(2)
	}
	if err := profile.validate(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}

	keys, err := loadKeys(*n)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load peer keys: %v\n", err)
		os.Exit(1)
	}

	s := buildSpec(keys)
	s.AllInOne = *allInOne == 1
	s.Retention = profile

	if err := applyLimits(s.SplitServices, *limits); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}

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
		nodes[i].StaticPeersSplit = meshPeersWithSuffix(keys, i+1, "-p2p")
	}
	// buildSpec returns an all-in-one spec by default; callers that want
	// the split topology must flip AllInOne to false after construction.
	// This keeps the historical behaviour (and the existing gen_test.go)
	// working without forcing every caller to know about the new field.
	// SplitServices is populated unconditionally — it's a static list and
	// the split template ignores it when AllInOne is true. Populating
	// here (not in main) means every test/caller gets the slice for free.
	return spec{
		NodeCount: len(nodes),
		Nodes:     nodes,
		AllInOne:  true,
		// Retention defaults to the production profile, which emits no
		// overrides — callers that want aggressive pruning set it in main.
		Retention:     productionRetention,
		SplitServices: buildSplitServices(),
	}
}

// meshPeers returns the pipe-separated static-peers list for node `selfIdx`
// (1-based), one multiaddr per other node, using the docker DNS name.
func meshPeers(keys []peerKey, selfIdx int) string {
	return meshPeersWithSuffix(keys, selfIdx, "")
}

// meshPeersWithSuffix builds the static-peer list against the container DNS
// name "teranode<N><suffix>". The all-in-one topology wants "" (the node
// container is literally teranodeN); the split topology wants "-p2p",
// because there is no teranodeN container there — only nine per-service
// siblings — and dialling the bare name fails with "no good addresses".
func meshPeersWithSuffix(keys []peerKey, selfIdx int, suffix string) string {
	var parts []string
	for _, k := range keys {
		if k.Index == selfIdx {
			continue
		}
		parts = append(parts, fmt.Sprintf("/dns/teranode%d%s/tcp/${P2P_PORT}/p2p/%s", k.Index, suffix, k.PeerID))
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
		if err := os.Chmod(p, 0o750); err != nil {
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
