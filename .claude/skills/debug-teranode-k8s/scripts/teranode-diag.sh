#!/usr/bin/env bash
# teranode-diag.sh — Comprehensive diagnostic snapshot for Teranode on Kubernetes
#
# Discovery is automatic and deployment-agnostic: namespaces, pods, the Aerospike
# server container, the Aerospike DB namespace, and pprof ports are all discovered
# at runtime (via labels, the Teranode operator CRs, and the datastore operators'
# CRs). Nothing about a particular cluster is hardcoded. Anything discovery can't
# resolve can be overridden with the environment variables listed under --help.
#
# This snapshot reads only. The one exception is the host-level deep dive
# (--host-probes), which creates short-lived privileged pods on datastore nodes;
# it is OFF by default and skips gracefully where it isn't permitted.
#
# Usage: ./teranode-diag.sh [OPTIONS]
#   --quick          Skip 2-second deltas (faster but no rate data)
#   --sample-pct N   Percentage of pods to sample for goroutine profiles (1-100, default 10)
#   --host-probes    Enable host-level deep dive: privileged hostNetwork/hostPID probe
#                    pods on Aerospike nodes (perf, ss, ethtool, nvme-cli, iostat) plus
#                    node-level bandwidth probes. Only works on clusters that allow
#                    privileged host-network pods (e.g. bare-metal k0s); auto-skips on
#                    managed clusters (EKS/GKE) that forbid it. OFF by default.
#   --help           Show this help, including the discovery override variables.
#
# Discovery override env vars (set any to skip / pin discovery):
#   TERANODE_NAMESPACES    space/comma list of teranode workload namespaces
#   AEROSPIKE_NAMESPACES   space/comma list of Aerospike namespaces
#   POSTGRES_NAMESPACES    space/comma list of PostgreSQL namespaces
#   KAFKA_NAMESPACES       space/comma list of Kafka/Redpanda namespaces
#   AEROSPIKE_CONTAINER    Aerospike server container name (default: discovered, else aerospike-server)
#   AEROSPIKE_DB_NS        Aerospike DB namespace holding UTXOs (default: discovered via 'asinfo -v namespaces')
#   PPROF_PORT             teranode service pprof port (default: 9091)
#   BLASTER_PPROF_PORT     tx-blaster pprof port (default: discovered named port 'profiler', else 9092)
#   TERANODE_PART_OF_LABEL discovery label (default: teranode.bsvblockchain.org/part-of=true)
#
# Outputs a structured text report covering:
#   1. Cluster overview (pod counts, node placement, UTXO backend, FSM state)
#   2. Aerospike health (latencies, cache, objects, ops/s)
#   2B. PostgreSQL health (connections, long queries, replication lag)
#   2C. Kafka/Redpanda health (consumer-group lag per topic)
#   3. Aerospike disk I/O (NVMe IOPS, queue depth, iowait)         [needs --quick off]
#   3B. Aerospike host deep dive (perf, ss, NVMe SMART, ethtool)   [needs --host-probes]
#   4. Propagation pods (CPU, throttling, aggregated goroutine profiles)
#   5. Blaster pods (CPU, schedstat, aggregated goroutine profiles)  [if tx-blaster present]
#   6. Block assembly & block validator (CPU, goroutines)
#   7. Networking (node bandwidth [--host-probes], pod throughput, TCP retransmits, softnet)
#   8. Summary with directional hints (NOT absolute SLAs — deployments vary)

set -o pipefail

# ── Parse arguments ──────────────────────────────────────────────────────────
QUICK=0
SAMPLE_PCT=10
HOST_PROBES=0

print_help() { sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --quick) QUICK=1; shift ;;
        --host-probes) HOST_PROBES=1; shift ;;
        --help|-h) print_help; exit 0 ;;
        --sample-pct)
            SAMPLE_PCT="$2"
            if ! [[ "$SAMPLE_PCT" =~ ^[0-9]+$ ]] || [ "$SAMPLE_PCT" -lt 1 ] || [ "$SAMPLE_PCT" -gt 100 ]; then
                echo "Error: --sample-pct must be 1-100" >&2; exit 1
            fi
            shift 2 ;;
        *) echo "Unknown option: $1 (try --help)" >&2; exit 1 ;;
    esac
done

TIMESTAMP=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
DELTA_SECS=2

# ── Temp directory for parallel output ───────────────────────────────────────
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# ── Discovery (deployment-agnostic) ──────────────────────────────────────────
# Everything here is discovered from labels and operator CRs, or taken from an
# override env var. Nothing about a specific cluster is hardcoded. Each discovery
# is best-effort and falls back gracefully so the script never aborts on a miss.

norm_list() { echo "$1" | tr ',' ' ' | xargs; }
ns_of_crd() { kubectl get "$1" -A --no-headers -o custom-columns=':metadata.namespace' 2>/dev/null | sort -u | xargs; }
ns_by_image() { kubectl get pods -A -o jsonpath='{range .items[*]}{.metadata.namespace}{"\t"}{range .spec.containers[*]}{.image}{" "}{end}{"\n"}{end}' 2>/dev/null | grep -iE "$1" | awk '{print $1}' | grep -v operator | sort -u | xargs; }

PART_OF_LABEL="${TERANODE_PART_OF_LABEL:-teranode.bsvblockchain.org/part-of=true}"

# Teranode workload namespaces: by the operator "part-of" label, then the cluster CR, then app=blockchain.
if [ -n "${TERANODE_NAMESPACES:-}" ]; then
    WORKLOAD_NAMESPACES=$(norm_list "$TERANODE_NAMESPACES")
else
    WORKLOAD_NAMESPACES=$(kubectl get pods -A -l "$PART_OF_LABEL" --no-headers -o custom-columns=':metadata.namespace' 2>/dev/null | sort -u | xargs)
    [ -z "$WORKLOAD_NAMESPACES" ] && WORKLOAD_NAMESPACES=$(ns_of_crd clusters.teranode.bsvblockchain.org)
    [ -z "$WORKLOAD_NAMESPACES" ] && WORKLOAD_NAMESPACES=$(kubectl get pods -A -l app=blockchain --no-headers -o custom-columns=':metadata.namespace' 2>/dev/null | sort -u | xargs)
fi

# Aerospike namespaces: by the Aerospike operator CR, then by image.
if [ -n "${AEROSPIKE_NAMESPACES:-}" ]; then
    AERO_NAMESPACES=$(norm_list "$AEROSPIKE_NAMESPACES")
else
    AERO_NAMESPACES=$(ns_of_crd aerospikeclusters.asdb.aerospike.com)
    [ -z "$AERO_NAMESPACES" ] && AERO_NAMESPACES=$(ns_by_image 'aerospike')
fi

# PostgreSQL namespaces: by CloudNativePG cluster CR, then by image.
if [ -n "${POSTGRES_NAMESPACES:-}" ]; then
    PG_NAMESPACES=$(norm_list "$POSTGRES_NAMESPACES")
else
    PG_NAMESPACES=$(ns_of_crd clusters.postgresql.cnpg.io)
    [ -z "$PG_NAMESPACES" ] && PG_NAMESPACES=$(ns_by_image 'postgres|cnpg')
fi

# Kafka/Redpanda namespaces: by image (operator CRDs vary by distro).
if [ -n "${KAFKA_NAMESPACES:-}" ]; then
    KAFKA_NAMESPACES=$(norm_list "$KAFKA_NAMESPACES")
else
    KAFKA_NAMESPACES=$(ns_by_image 'redpanda|kafka|strimzi')
fi

# Aerospike server container + DB namespace, discovered from a live pod.
AERO_CONTAINER="${AEROSPIKE_CONTAINER:-}"
AERO_DB_NS="${AEROSPIKE_DB_NS:-}"
_first_aero_ns=$(echo "$AERO_NAMESPACES" | awk '{print $1}')
if [ -n "$_first_aero_ns" ]; then
    _first_aero_pod=$(kubectl get pods -n "$_first_aero_ns" --no-headers -o custom-columns=':metadata.name' 2>/dev/null | grep -v operator | grep -v '^$' | head -1)
    if [ -z "$AERO_CONTAINER" ] && [ -n "$_first_aero_pod" ]; then
        AERO_CONTAINER=$(kubectl get pod -n "$_first_aero_ns" "$_first_aero_pod" -o jsonpath='{range .spec.containers[*]}{.name}{"\n"}{end}' 2>/dev/null | grep -i aerospike | grep -vi export | head -1)
    fi
    [ -z "$AERO_CONTAINER" ] && AERO_CONTAINER="aerospike-server"
    if [ -z "$AERO_DB_NS" ] && [ -n "$_first_aero_pod" ]; then
        AERO_DB_NS=$(kubectl exec -n "$_first_aero_ns" "$_first_aero_pod" -c "$AERO_CONTAINER" -- asinfo -v 'namespaces' 2>/dev/null | tr ';' '\n' | grep -v '^$' | head -1 | tr -d '\r')
    fi
fi
[ -z "$AERO_CONTAINER" ] && AERO_CONTAINER="aerospike-server"
[ -z "$AERO_DB_NS" ] && AERO_DB_NS="utxo-store"

# pprof ports. Teranode services default to 9091. The tx-blaster's pprof port varies
# by deployment, so discover it from the pod's named 'profiler' port (fallback 9092).
PPROF_PORT="${PPROF_PORT:-9091}"
if [ -z "${BLASTER_PPROF_PORT:-}" ]; then
    for _ns in $WORKLOAD_NAMESPACES; do
        _bp=$(kubectl get pods -n "$_ns" -l app=tx-blaster --no-headers -o custom-columns=':metadata.name' 2>/dev/null | head -1)
        if [ -n "$_bp" ]; then
            BLASTER_PPROF_PORT=$(kubectl get pod -n "$_ns" "$_bp" -o jsonpath='{range .spec.containers[*].ports[?(@.name=="profiler")]}{.containerPort}{end}' 2>/dev/null)
            [ -n "$BLASTER_PPROF_PORT" ] && break
        fi
    done
fi
[ -z "${BLASTER_PPROF_PORT:-}" ] && BLASTER_PPROF_PORT=9092

# UTXO store backend (informational): read the utxostore setting from a teranode pod.
UTXO_BACKEND="unknown"
_first_tn_ns=$(echo "$WORKLOAD_NAMESPACES" | awk '{print $1}')
if [ -n "$_first_tn_ns" ]; then
    _tn_pod=$(kubectl get pods -n "$_first_tn_ns" -l app=propagation --no-headers -o custom-columns=':metadata.name' 2>/dev/null | head -1)
    [ -z "$_tn_pod" ] && _tn_pod=$(kubectl get pods -n "$_first_tn_ns" -l app=blockchain --no-headers -o custom-columns=':metadata.name' 2>/dev/null | head -1)
    if [ -n "$_tn_pod" ]; then
        _scheme=$(kubectl exec -n "$_first_tn_ns" "$_tn_pod" -- sh -c 'printf "%s\n" "$utxostore"; grep -ihoE "utxostore[^=]*=[a-z]+://" /app/settings*.conf 2>/dev/null' 2>/dev/null | grep -oE '[a-z]+://' | head -1)
        case "$_scheme" in
            aerospike://) UTXO_BACKEND="aerospike" ;;
            postgres://)  UTXO_BACKEND="postgres" ;;
            sqlite://)    UTXO_BACKEND="sqlite" ;;
            memory://)    UTXO_BACKEND="memory" ;;
        esac
    fi
fi
[ "$UTXO_BACKEND" = "unknown" ] && [ -n "$AERO_NAMESPACES" ] && UTXO_BACKEND="aerospike (assumed from namespaces)"

# ── Build node lookup table: hostname → IP role ─────────────────────────────
# 'role' is an optional label; absent on many clusters (shows <none>) — harmless.
kubectl get nodes -o wide -L role --no-headers 2>/dev/null | awk '{print $1, $6, $NF}' > "$TMPDIR/node_lookup.txt"

# ── Helper functions ─────────────────────────────────────────────────────────
header() {
    echo ""
    echo "================================================================================"
    echo "  $1"
    echo "  $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    echo "================================================================================"
}

subheader() {
    echo ""
    echo "── $1 ──"
}

# Select a sample of pods based on SAMPLE_PCT
# Usage: sample_pods "$POD_LIST"
# Reads SAMPLE_PCT from global. Returns sampled pod names, one per line.
sample_pods() {
    local pods="$1"
    local total
    total=$(echo "$pods" | wc -l | tr -d ' ')
    local count=$(( (total * SAMPLE_PCT + 99) / 100 ))
    [ "$count" -lt 1 ] && count=1
    [ "$count" -gt "$total" ] && count=$total
    if command -v gshuf &>/dev/null; then
        echo "$pods" | gshuf -n "$count" | sort
    elif command -v shuf &>/dev/null; then
        echo "$pods" | shuf -n "$count" | sort
    else
        echo "$pods" | head -n "$count"
    fi
}

node_info() {
    # Lookup node IP and role from the pre-fetched table
    local node="$1"
    local info
    info=$(grep "^${node} " "$TMPDIR/node_lookup.txt" 2>/dev/null | head -1)
    if [ -n "$info" ]; then
        echo "$info" | awk '{printf "%-14s %-12s", $2, $3}'
    else
        printf "%-14s %-12s" "?" "?"
    fi
}

parse_latencies() {
    python3 -c "
import sys
raw = sys.stdin.read().strip()
if not raw:
    print('  (no data)')
    sys.exit(0)
entries = raw.split(';')
print(f'  {\"Operation\":<30} {\"rate/s\":>10} {\"  >1ms%\":>8} {\"  >2ms%\":>8} {\"  >4ms%\":>8} {\"  >8ms%\":>8} {\" >16ms%\":>8}')
print(f'  {\"-\"*30} {\"-\"*10} {\"-\"*8} {\"-\"*8} {\"-\"*8} {\"-\"*8} {\"-\"*8}')
for entry in entries:
    entry = entry.strip()
    if not entry or ':' not in entry:
        continue
    parts = entry.split(':')
    if len(parts) < 2:
        continue
    op = parts[0]
    rest = ':'.join(parts[1:])
    vals = rest.split(',')
    if len(vals) < 2:
        continue
    try:
        rate = float(vals[1])
    except (ValueError, IndexError):
        continue
    if rate < 0.1:
        continue
    pcts = []
    for i in range(2, min(7, len(vals))):
        try:
            pcts.append(float(vals[i]))
        except ValueError:
            pcts.append(0.0)
    while len(pcts) < 5:
        pcts.append(0.0)
    print(f'  {op:<30} {rate:>10.1f} {pcts[0]:>7.2f}% {pcts[1]:>7.2f}% {pcts[2]:>7.2f}% {pcts[3]:>7.2f}% {pcts[4]:>7.2f}%')
"
}

# Parse a single goroutine profile into structured lines for aggregation
# Output format: one line per group: COUNT\tFUNCTION\tFILE
# This is meant to be collected across pods and then aggregated
parse_goroutines_raw() {
    python3 -c "
import sys, re

lines = sys.stdin.read().strip().split('\n')
if not lines or 'goroutine profile' not in lines[0]:
    sys.exit(0)

m = re.search(r'total (\d+)', lines[0])
total = int(m.group(1)) if m else 0
print(f'TOTAL\t{total}')

i = 1
while i < len(lines):
    line = lines[i]
    m = re.match(r'^(\d+)\s+@\s+', line)
    if m:
        count = int(m.group(1))
        func_name = '(unknown)'
        file_loc = ''
        j = i + 1
        while j < len(lines) and lines[j].startswith('#'):
            fm = re.match(r'^#\s+\S+\s+(\S+)\s+(\S+)', lines[j])
            if fm:
                fn = fm.group(1)
                loc = fm.group(2)
                skip = ('runtime/', 'internal/', 'sync.', 'sync/', 'bufio/', 'io.', 'net.', 'net/',
                        'golang.org/x/net/', 'context.')
                grpc_infra = ('google.golang.org/grpc/internal/transport.',
                              'google.golang.org/grpc.(*Server).serveStreams',
                              'google.golang.org/grpc.(*Server).handleRawConn')
                if any(fn.startswith(s) for s in skip):
                    j += 1
                    continue
                if any(fn.startswith(s) for s in grpc_infra):
                    func_name = fn.split('(')[0] if '(' in fn else fn
                    func_name = func_name.split('/')[-1] if '/' in func_name else func_name
                    file_loc = loc.split('/')[-1] if '/' in loc else loc
                    break
                short = fn
                for prefix in ['github.com/bsv-blockchain/teranode/', 'github.com/bitcoin-sv/teranode-coinbase/', 'github.com/IBM/']:
                    short = short.replace(prefix, '')
                func_name = short
                file_loc = loc.split('/')[-1] if '/' in loc else loc
                break
            j += 1
        print(f'{count}\t{func_name}\t{file_loc}')
    i += 1
"
}

# Aggregate multiple raw goroutine files into a summary table
# Usage: aggregate_goroutines <file1> <file2> ... | displayed
# Reads from files passed as arguments
aggregate_goroutines() {
    python3 -c "
import sys, os
from collections import defaultdict

files = sys.argv[1:]
if not files:
    print('  (no goroutine data)')
    sys.exit(0)

totals = []
func_counts = defaultdict(lambda: [0, '', ''])  # [sum_count, func, file]

valid_files = 0
for f in files:
    if not os.path.exists(f) or os.path.getsize(f) == 0:
        continue
    valid_files += 1
    with open(f) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            parts = line.split('\t')
            if parts[0] == 'TOTAL':
                totals.append(int(parts[1]))
                continue
            if len(parts) >= 2:
                count = int(parts[0])
                func = parts[1]
                floc = parts[2] if len(parts) > 2 else ''
                key = func
                func_counts[key][0] += count
                func_counts[key][1] = func
                func_counts[key][2] = floc

if not valid_files:
    print('  (no goroutine data)')
    sys.exit(0)

avg_total = sum(totals) / len(totals) if totals else 0
fleet_total = sum(totals)

print(f'  Sampled {valid_files} pods  |  Avg goroutines/pod: {avg_total:,.0f}')
print()

results = sorted(func_counts.values(), key=lambda x: -x[0])

aero_blocked = sum(c for c, f, _ in results if 'aerospike' in f.lower())
ba_blocked = sum(c for c, f, _ in results if 'blockassembly' in f.lower() or 'block_assembly' in f.lower())
kafka_blocked = sum(c for c, f, _ in results if 'sarama' in f.lower() or 'kafka' in f.lower())
grpc_infra = sum(c for c, f, _ in results if 'transport.' in f or 'grpc.' in f)
active = fleet_total - grpc_infra

print(f'  Across sampled pods: active={active}  gRPC-infra={grpc_infra}')
print(f'  Blocked on: Aerospike={aero_blocked}  BlockAssembly={ba_blocked}  Kafka={kafka_blocked}')
print()
print(f'  {\"Avg/pod\":>8} {\"Total\":>8} {\"% total\":>8}  Function')
print(f'  {\"-\"*8} {\"-\"*8} {\"-\"*8}  {\"-\"*60}')
for count, func_name, file_loc in results[:15]:
    avg = count / valid_files
    pct = count / fleet_total * 100 if fleet_total > 0 else 0
    loc_str = f'  ({file_loc})' if file_loc else ''
    display = func_name[:65]
    print(f'  {avg:>8.0f} {count:>8} {pct:>7.1f}%  {display}{loc_str}')
" "$@"
}

parse_cpu_delta() {
    python3 -c "
import sys
lines = sys.stdin.read().strip().split('\n')
cpu_lines = [l for l in lines if l.startswith('cpu ')]
if len(cpu_lines) < 2:
    print('  (insufficient data)')
    sys.exit(0)
a = list(map(int, cpu_lines[0].split()[1:]))
b = list(map(int, cpu_lines[1].split()[1:]))
d = [b[i]-a[i] for i in range(min(len(a),len(b)))]
total = sum(d)
if total == 0:
    print('  (no CPU activity)')
    sys.exit(0)
labels = ['user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq', 'steal']
parts = []
for i, l in enumerate(labels):
    if i < len(d):
        pct = d[i] / total * 100
        if pct >= 0.01:
            parts.append(f'{l}={pct:.1f}%')
ncpu = total / ($DELTA_SECS * 100)
print(f'  CPU: {\" | \".join(parts)}  [{ncpu:.0f} cores]')
"
}

# Fetch goroutine profile for a pod and save raw parsed output
# Usage: fetch_goroutine_profile <namespace> <pod> <pprof_port> <output_file>
fetch_goroutine_profile() {
    local ns="$1" pod="$2" port="$3" outfile="$4"
    kubectl exec -n "$ns" "$pod" -- sh -c "wget -qO- 'http://localhost:${port}/debug/pprof/goroutine?debug=1' 2>/dev/null || curl -s 'http://localhost:${port}/debug/pprof/goroutine?debug=1' 2>/dev/null" 2>/dev/null \
        | parse_goroutines_raw > "$outfile" 2>/dev/null
}

# Fetch network stats delta for a pod and save results
# Usage: fetch_pod_net_stats <namespace> <pod> <output_file>
fetch_pod_net_stats() {
    local ns="$1" pod="$2" outfile="$3"
    kubectl exec -n "$ns" "$pod" -- sh -c "
        cat /proc/net/dev | grep eth0 > /tmp/_net1
        cat /proc/net/snmp | grep '^Tcp:' > /tmp/_tcp1
        sleep $DELTA_SECS
        cat /proc/net/dev | grep eth0 > /tmp/_net2
        cat /proc/net/snmp | grep '^Tcp:' > /tmp/_tcp2
        wc -l < /proc/net/tcp6 2>/dev/null || echo 0
        echo '===NET1==='; cat /tmp/_net1
        echo '===NET2==='; cat /tmp/_net2
        echo '===TCP1==='; cat /tmp/_tcp1
        echo '===TCP2==='; cat /tmp/_tcp2
        echo '===SOFTNET==='; cat /proc/net/softnet_stat
    " 2>/dev/null > "$outfile"
}

# Fetch node-level network stats via a hostNetwork probe pod
# Usage: fetch_node_net_stats <node_name> <output_file>
fetch_node_net_stats() {
    local node="$1" outfile="$2"
    local probe_name="net-probe-${node}-$$"

    # Create a short-lived hostNetwork pod on the target node
    kubectl run "$probe_name" --image=busybox --restart=Never \
        --overrides="{\"spec\":{\"nodeName\":\"$node\",\"hostNetwork\":true,\"tolerations\":[{\"operator\":\"Exists\"}]}}" \
        -- sleep 30 >/dev/null 2>&1

    # Wait for it to be running (max 10s)
    for _i in $(seq 1 10); do
        local phase
        phase=$(kubectl get pod "$probe_name" -o jsonpath='{.status.phase}' 2>/dev/null)
        [ "$phase" = "Running" ] && break
        sleep 1
    done

    # Collect data
    kubectl exec "$probe_name" -- sh -c "
        cat /proc/net/dev | grep -E 'bond1|bond0|enp' > /tmp/_n1
        sleep $DELTA_SECS
        cat /proc/net/dev | grep -E 'bond1|bond0|enp' > /tmp/_n2
        echo '===SPEED==='
        cat /sys/class/net/bond1/speed 2>/dev/null || cat /sys/class/net/bond0/speed 2>/dev/null || echo 0
        echo '===NET1==='; cat /tmp/_n1
        echo '===NET2==='; cat /tmp/_n2
    " 2>/dev/null > "$outfile"

    # Cleanup
    kubectl delete pod "$probe_name" --force --grace-period=0 >/dev/null 2>&1
}

# Create a privileged probe pod on a node for deep diagnostics
# Usage: create_probe_pod <node_name> <probe_name>
create_probe_pod() {
    local node="$1" name="$2"
    kubectl run "$name" --image=ubuntu:24.04 --restart=Never --privileged \
        --overrides="{\"spec\":{\"nodeName\":\"$node\",\"hostNetwork\":true,\"hostPID\":true,\"containers\":[{\"name\":\"$name\",\"image\":\"ubuntu:24.04\",\"command\":[\"sleep\",\"300\"],\"securityContext\":{\"privileged\":true}}]}}" >/dev/null 2>&1
    for _i in $(seq 1 15); do
        local phase
        phase=$(kubectl get pod "$name" -o jsonpath='{.status.phase}' 2>/dev/null)
        [ "$phase" = "Running" ] && return 0
        sleep 1
    done
    return 1
}

# Install tools in a probe pod (iproute2 for ss, nvme-cli, ethtool, sysstat for iostat)
install_probe_tools() {
    local name="$1"
    kubectl exec "$name" -- sh -c 'apt-get update -qq >/dev/null 2>&1; apt-get install -y -qq iproute2 nvme-cli ethtool sysstat >/dev/null 2>&1' 2>/dev/null
}

# Collect deep diagnostics from an Aerospike node via probe pod
# Usage: collect_aero_deep <probe_pod_name> <output_file>
collect_aero_deep() {
    local probe="$1" outfile="$2"
    kubectl exec "$probe" -- bash -c '
        ASD_PID=$(nsenter -t 1 -m -u -i -n -p pgrep -x asd 2>/dev/null | head -1)

        echo "===PERF_STAT==="
        if [ -n "$ASD_PID" ]; then
            nsenter -t 1 -m -u -i -n -p perf stat -p $ASD_PID -e cycles,instructions,cache-misses,cache-references,context-switches,cpu-migrations,L1-dcache-load-misses --timeout 2000 2>&1
        else
            echo "(asd not found)"
        fi

        echo "===SS_SUMMARY==="
        # ss runs in the probe pod which has hostNetwork=true, so it sees host sockets directly
        ss -tn "sport = :3000" 2>/dev/null | grep ESTAB | awk "{
            recvq=\$2; sendq=\$3;
            total++
            if (recvq > 0) recv_nz++
            if (sendq > 0) send_nz++
            recv_sum += recvq; send_sum += sendq
            if (recvq > recv_max) recv_max = recvq
            if (sendq > send_max) send_max = sendq
        } END {
            printf \"connections=%d recv_nonzero=%d recv_max=%d send_nonzero=%d send_max=%d recv_avg=%.0f send_avg=%.0f\\n\",
                total, recv_nz+0, recv_max+0, send_nz+0, send_max+0, total>0?recv_sum/total:0, total>0?send_sum/total:0
        }"

        echo "===SS_RTT==="
        ss -tnip "sport = :3000" | grep -oP "rtt:[0-9.]+" | awk -F: "{
            rtt=\$2
            if (rtt < 0.1) b[\"  <0.1ms\"]++
            else if (rtt < 1) b[\" 0.1-1ms\"]++
            else if (rtt < 5) b[\"   1-5ms\"]++
            else if (rtt < 10) b[\"  5-10ms\"]++
            else b[\"   >10ms\"]++
            total++
        } END {
            for (b in buckets) printf \"%s: %d\\n\", b, buckets[b]
            PROCINFO[\"sorted_in\"]=\"@ind_str_asc\"
            for (k in b) printf \"%s: %d (%.1f%%)\\n\", k, b[k], b[k]/total*100
        }"

        echo "===SS_RETRANS==="
        ss -tnip "sport = :3000" | grep -oP "retrans:\d+/\d+" | awk -F"[:/]" "BEGIN{c=0;t=0}{c+=\$2;t+=\$3}END{printf \"current=%d total=%d\\n\", c, t}"

        echo "===SS_RWND_LIMITED==="
        ss -tnip "sport = :3000" | grep -c "rwnd_limited" || echo "0"

        echo "===ETHTOOL==="
        BOND_SLAVES=$(cat /proc/net/bonding/bond1 2>/dev/null | grep "Slave Interface" | head -1 | awk "{print \$NF}")
        if [ -n "$BOND_SLAVES" ]; then
            echo "slave=$BOND_SLAVES"
            ethtool -S $BOND_SLAVES 2>/dev/null | grep -E "rx_dropped|tx_dropped|rx_errors|tx_errors|rx_pause|tx_pause|rx_out_of_buffer|rx_discards"
            echo "---ring---"
            ethtool -g $BOND_SLAVES 2>/dev/null | grep -A1 "Current" | tail -4
            echo "---coalesce---"
            ethtool -c $BOND_SLAVES 2>/dev/null | grep -E "Adaptive|rx-usecs:|rx-frames:|tx-usecs:|tx-frames:"
        fi

        echo "===NVME_SMART==="
        for dev in /dev/nvme0 /dev/nvme1 /dev/nvme2 /dev/nvme3 /dev/nvme6 /dev/nvme7; do
            if [ -e "$dev" ]; then
                name=$(basename $dev)
                temp=$(nvme smart-log $dev 2>/dev/null | grep "^temperature" | awk "{print \$3}")
                spare=$(nvme smart-log $dev 2>/dev/null | grep "available_spare[^_]" | awk "{print \$NF}")
                pct_used=$(nvme smart-log $dev 2>/dev/null | grep "percentage_used" | awk "{print \$NF}")
                media_err=$(nvme smart-log $dev 2>/dev/null | grep "media_errors" | awk "{print \$NF}")
                printf "%s: temp=%s spare=%s used=%s media_errors=%s\n" "$name" "$temp" "$spare" "$pct_used" "$media_err"
            fi
        done

        echo "===IOSTAT==="
        iostat -x -d -y 1 1 2>/dev/null | grep nvme | grep -v "p[0-9]"
    ' 2>/dev/null > "$outfile"
}

# Parse the deep dive output file
parse_aero_deep() {
    local f="$1" node="$2"
    python3 -c "
import sys, os, re

content = open(sys.argv[1]).read()
node = sys.argv[2]
sections = {}
current = None
for line in content.split('\n'):
    if line.startswith('===') and line.endswith('==='):
        current = line.strip('=')
        sections[current] = []
    elif current is not None:
        sections.setdefault(current, []).append(line)

print(f'  Node: {node}')
print()

# ── perf stat ──
perf_lines = sections.get('PERF_STAT', [])
print('  CPU Performance (perf stat, 2s sample of asd process):')
ipc = '?'
cache_miss_pct = '?'
ctx_sw = '?'
cpu_mig = '?'
l1_miss = '?'
for line in perf_lines:
    line = line.strip()
    if 'insn per cycle' in line:
        m = re.search(r'#\s+([\d.]+)\s+insn per cycle', line)
        if m: ipc = m.group(1)
    if '% of all cache refs' in line:
        m = re.search(r'#\s+([\d.]+)%\s+of all cache refs', line)
        if m: cache_miss_pct = m.group(1)
    if 'context-switches' in line and '#' not in line:
        m = re.search(r'([\d,]+)\s+context-switches', line)
        if m: ctx_sw = m.group(1)
    if 'cpu-migrations' in line:
        m = re.search(r'([\d,]+)\s+cpu-migrations', line)
        if m: cpu_mig = m.group(1)
    if 'L1-dcache-load-misses' in line:
        m = re.search(r'([\d,]+)\s+L1-dcache-load-misses', line)
        if m: l1_miss = m.group(1)
print(f'    IPC: {ipc}  (instructions per cycle; >1.0 is good, <0.5 is memory-stalled)')
print(f'    Cache miss rate: {cache_miss_pct}%  (of cache refs; >10% indicates memory pressure)')
print(f'    Context switches: {ctx_sw}  CPU migrations: {cpu_mig}  L1 misses: {l1_miss}')
print()

# ── ss socket summary ──
ss_lines = sections.get('SS_SUMMARY', [])
print('  Socket Queues (ss, Aerospike port 3000):')
for line in ss_lines:
    if 'connections=' in line:
        parts = dict(p.split('=') for p in line.split() if '=' in p)
        print(f'    Connections: {parts.get(\"connections\",\"?\")}')
        print(f'    Recv-Q:  non-zero={parts.get(\"recv_nonzero\",\"0\")}  max={parts.get(\"recv_max\",\"0\")}  avg={parts.get(\"recv_avg\",\"0\")}')
        print(f'    Send-Q:  non-zero={parts.get(\"send_nonzero\",\"0\")}  max={parts.get(\"send_max\",\"0\")}  avg={parts.get(\"send_avg\",\"0\")}')

# ss RTT
rtt_lines = [l for l in sections.get('SS_RTT', []) if l.strip()]
if rtt_lines:
    print('    RTT distribution:')
    for line in rtt_lines:
        if line.strip():
            print(f'      {line.strip()}')

# ss retrans
retrans_lines = sections.get('SS_RETRANS', [])
for line in retrans_lines:
    if 'current=' in line:
        print(f'    Retransmits: {line.strip()}')

# rwnd limited
rwnd_lines = sections.get('SS_RWND_LIMITED', [])
rwnd_count = '0'
for line in rwnd_lines:
    line = line.strip()
    if line.isdigit():
        rwnd_count = line
if int(rwnd_count) > 0:
    print(f'    [!] {rwnd_count} connections are rwnd_limited (receiver window limiting throughput)')
print()

# ── ethtool ──
eth_lines = sections.get('ETHTOOL', [])
print('  NIC Hardware (ethtool):')
for line in eth_lines:
    line = line.strip()
    if line.startswith('slave='):
        print(f'    Interface: {line.split(\"=\")[1]}')
    elif ':' in line and any(k in line for k in ['dropped', 'errors', 'pause', 'buffer', 'discards']):
        key, _, val = line.partition(':')
        val = val.strip()
        if val != '0':
            print(f'    [!] {key.strip()}: {val}')
    elif line.startswith('Adaptive'):
        print(f'    {line}')
    elif line.startswith('rx-') or line.startswith('tx-') or line.startswith('RX:') or line.startswith('TX:'):
        print(f'    {line}')
# Print "clean" if no issues found
eth_issues = [l for l in eth_lines if ':' in l and any(k in l for k in ['dropped', 'errors', 'pause', 'buffer', 'discards']) and not l.strip().endswith(': 0')]
if not eth_issues:
    print('    (no drops, errors, or pause frames)')
print()

# ── NVMe SMART ──
nvme_lines = [l for l in sections.get('NVME_SMART', []) if l.strip()]
if nvme_lines:
    print('  NVMe Health (SMART):')
    print(f'    {\"Device\":<10} {\"Temp\":>6} {\"Spare\":>8} {\"Used\":>6} {\"MediaErr\":>10}')
    print(f'    {\"-\"*10} {\"-\"*6} {\"-\"*8} {\"-\"*6} {\"-\"*10}')
    any_issues = False
    for line in nvme_lines:
        parts = line.split()
        if len(parts) >= 1 and ':' in parts[0]:
            dev = parts[0].rstrip(':')
            fields = dict(p.split('=') for p in parts[1:] if '=' in p)
            temp = fields.get('temp', '?').replace('°C','').strip()
            spare = fields.get('spare', '?')
            used = fields.get('used', '?')
            merr = fields.get('media_errors', '?')
            print(f'    {dev:<10} {temp:>6} {spare:>8} {used:>6} {merr:>10}')
            if merr != '0' and merr != '?':
                any_issues = True
            try:
                if int(spare.replace(\"%\",\"\")) < 20:
                    any_issues = True
            except: pass
    if any_issues:
        print('    [!] WARNING: NVMe health issues detected above')
print()

# ── iostat ──
io_lines = [l for l in sections.get('IOSTAT', []) if l.strip()]
if io_lines:
    print('  iostat -x (1s snapshot):')
    print(f'    {\"Device\":<12} {\"r/s\":>10} {\"rkB/s\":>12} {\"r_await\":>8} {\"w/s\":>10} {\"wkB/s\":>12} {\"w_await\":>8} {\"aqu-sz\":>8} {\"%util\":>7}')
    print(f'    {\"-\"*12} {\"-\"*10} {\"-\"*12} {\"-\"*8} {\"-\"*10} {\"-\"*12} {\"-\"*8} {\"-\"*8} {\"-\"*7}')
    for line in io_lines:
        fields = line.split()
        if len(fields) >= 22:
            dev = fields[0]
            rs = fields[1]
            rkbs = fields[2]
            r_await = fields[5]
            ws = fields[7]
            wkbs = fields[8]
            w_await = fields[11]
            aqu = fields[20]
            util = fields[21]
            print(f'    {dev:<12} {rs:>10} {rkbs:>12} {r_await:>8} {ws:>10} {wkbs:>12} {w_await:>8} {aqu:>8} {util:>7}')
print()
" "$f" "$node"
}

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1: CLUSTER OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 1: CLUSTER OVERVIEW"

echo "  Timestamp: $TIMESTAMP"
echo "  Sample percentage: ${SAMPLE_PCT}%"
echo "  Host probes: $([ "$HOST_PROBES" -eq 1 ] && echo enabled || echo 'disabled (--host-probes to enable)')"
echo ""
echo "  Discovered:"
echo "    Teranode namespaces:  ${WORKLOAD_NAMESPACES:-(none)}"
echo "    Aerospike namespaces: ${AERO_NAMESPACES:-(none)}"
echo "    Postgres namespaces:  ${PG_NAMESPACES:-(none)}"
echo "    Kafka/Redpanda ns:    ${KAFKA_NAMESPACES:-(none)}"
echo "    UTXO store backend:   ${UTXO_BACKEND}"
[ -n "$AERO_NAMESPACES" ] && echo "    Aerospike: container=$AERO_CONTAINER  db-namespace=$AERO_DB_NS"
echo "    pprof port=$PPROF_PORT  blaster pprof port=$BLASTER_PPROF_PORT"
echo ""

# FSM state is the highest-signal check for a node that seems inert (IDLE = does nothing;
# CATCHINGBLOCKS = syncing, no mining). Served by the Asset server; best-effort probe.
if [ -n "$WORKLOAD_NAMESPACES" ]; then
    echo "  FSM state (per teranode namespace):"
    for NS in $WORKLOAD_NAMESPACES; do
        ASSET_POD=$(kubectl get pods -n "$NS" -l app=asset --no-headers -o custom-columns=':metadata.name' 2>/dev/null | head -1)
        STATE=""
        if [ -n "$ASSET_POD" ]; then
            STATE=$(kubectl exec -n "$NS" "$ASSET_POD" -- sh -c 'wget -qO- http://localhost:8090/api/v1/fsm/state 2>/dev/null || curl -s http://localhost:8090/api/v1/fsm/state 2>/dev/null' 2>/dev/null \
                | grep -oE 'IDLE|RUNNING|CATCHINGBLOCKS|LEGACYSYNCING' | head -1)
        fi
        printf "    %-32s %s\n" "$NS" "${STATE:-(unknown — check asset pod / FSM endpoint)}"
    done
    echo ""
fi

for NS in $WORKLOAD_NAMESPACES; do
    subheader "Namespace: $NS"
    kubectl get pods -n "$NS" --no-headers -o custom-columns=':metadata.labels.app' 2>/dev/null \
        | grep -v '^$' | grep -v '<none>' | sort | uniq -c | sort -rn | while read count name; do
        printf "  %-30s %d pods\n" "$name" "$count"
    done

    echo ""
    printf "  %-25s %-14s %-12s %s\n" "Node" "IP" "Role" "Pods"
    printf "  %-25s %-14s %-12s %s\n" "----" "--" "----" "----"
    kubectl get pods -n "$NS" -o wide --no-headers 2>/dev/null | awk '{print $7}' | sort | uniq -c | sort -rn | while read count node; do
        info=$(node_info "$node")
        printf "  %-25s %s %d\n" "$node" "$info" "$count"
    done
done

for NS in $AERO_NAMESPACES; do
    subheader "Namespace: $NS"
    kubectl get pods -n "$NS" -o wide --no-headers 2>/dev/null | awk '{
        printf "  %-50s %s  age=%s  node=%s\n", $1, $3, $5, $7
    }'
done

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2: AEROSPIKE HEALTH
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 2: AEROSPIKE HEALTH"

for NS in $AERO_NAMESPACES; do
    subheader "Cluster: $NS"

    AERO_PODS=$(kubectl get pods -n "$NS" --no-headers -o custom-columns=':metadata.name' 2>/dev/null | grep -v operator | grep -v '^$' | sort)
    if [ -z "$AERO_PODS" ]; then
        echo "  (no pods found)"
        continue
    fi

    AERO_COUNT=$(echo "$AERO_PODS" | wc -l | tr -d ' ')
    SAMPLED_AERO=$(sample_pods "$AERO_PODS")
    SAMPLED_AERO_COUNT=$(echo "$SAMPLED_AERO" | wc -l | tr -d ' ')

    # CPU usage
    echo "  CPU & Memory (kubectl top):"
    kubectl top pods -n "$NS" --containers --no-headers 2>/dev/null | grep "$AERO_CONTAINER" | \
        awk '{printf "    %-45s CPU=%-8s Mem=%s\n", $1, $3, $4}'

    # Per-node stats (always all pods — fast)
    echo ""
    printf "  %-25s %12s %12s %8s %10s %10s\n" "Pod" "Objects" "Master" "Cache%" "RW-InProg" "Clients"
    printf "  %-25s %12s %12s %8s %10s %10s\n" "---" "-------" "------" "------" "---------" "-------"

    for POD in $AERO_PODS; do
        NS_STATS=$(kubectl exec -n "$NS" "$POD" -c "$AERO_CONTAINER" -- asinfo -v "namespace/$AERO_DB_NS" 2>/dev/null | tr ';' '\n')
        SVC_STATS=$(kubectl exec -n "$NS" "$POD" -c "$AERO_CONTAINER" -- asinfo -v 'statistics' 2>/dev/null | tr ';' '\n')

        OBJECTS=$(echo "$NS_STATS" | grep '^objects=' | cut -d= -f2)
        MASTER=$(echo "$NS_STATS" | grep '^master_objects=' | cut -d= -f2)
        CACHE=$(echo "$NS_STATS" | grep '^cache_read_pct=' | cut -d= -f2)
        RW=$(echo "$SVC_STATS" | grep '^rw_in_progress=' | cut -d= -f2)
        CLIENTS=$(echo "$SVC_STATS" | grep '^client_connections=' | cut -d= -f2)

        SHORT=$(echo "$POD" | tail -c 26)
        printf "  %-25s %12s %12s %7s%% %10s %10s\n" "$SHORT" "$OBJECTS" "$MASTER" "$CACHE" "$RW" "$CLIENTS"
    done

    # Latencies + ticker from sampled pods
    echo ""
    echo "  Latency histograms (sampled $SAMPLED_AERO_COUNT/$AERO_COUNT pods):"
    for POD in $SAMPLED_AERO; do
        echo "    [$POD]:"
        kubectl exec -n "$NS" "$POD" -c "$AERO_CONTAINER" -- asinfo -v 'latencies:' 2>/dev/null | parse_latencies
    done

    # Ticker delta from first sampled pod
    if [ "$QUICK" -eq 0 ]; then
        FIRST_POD=$(echo "$SAMPLED_AERO" | head -1)
        echo ""
        echo "  Throughput (ticker delta from $FIRST_POD):"
        TICKER_OUTPUT=$(kubectl exec -n "$NS" "$FIRST_POD" -c "$AERO_CONTAINER" -- sh -c '
            tail -200 /var/log/aerospike/aerospike.log | grep "batch-sub:" | tail -2
        ' 2>/dev/null)
        echo "$TICKER_OUTPUT" | python3 -c "
import sys, re
lines = sys.stdin.read().strip().split('\n')
lines = [l for l in lines if 'batch-sub:' in l]
if len(lines) < 2:
    print('    (need 2 ticker lines, got', len(lines), ')')
    sys.exit(0)
def parse_ticker(line):
    ops = {}
    for op in ['read', 'write', 'delete', 'udf']:
        m = re.search(rf'{op}\s+\((\d+)', line)
        if m:
            ops[op] = int(m.group(1))
    return ops
a = parse_ticker(lines[0])
b = parse_ticker(lines[1])
if not a or not b:
    print('    (could not parse ticker)')
    sys.exit(0)
interval = 10
total = 0
for op in ['read', 'write', 'delete', 'udf']:
    if op in a and op in b:
        delta = b[op] - a[op]
        rate = delta / interval
        total += rate
        print(f'    batch-sub-{op}: {rate:>12,.0f}/s  (delta={delta:,})')
print(f'    TOTAL:          {total:>12,.0f}/s')
" 2>/dev/null
    fi

    # Config summary (from first pod)
    FIRST_POD=$(echo "$AERO_PODS" | head -1)
    echo ""
    echo "  Config:"
    NS_CFG=$(kubectl exec -n "$NS" "$FIRST_POD" -c "$AERO_CONTAINER" -- asinfo -v "get-config:context=namespace;id=$AERO_DB_NS" 2>/dev/null | tr ';' '\n')
    REPL=$(echo "$NS_CFG" | grep '^replication-factor=' | cut -d= -f2)
    RPC=$(echo "$NS_CFG" | grep '^storage-engine.read-page-cache=' | cut -d= -f2)
    PWC=$(echo "$NS_CFG" | grep '^storage-engine.post-write-cache=' | cut -d= -f2)
    MWC=$(echo "$NS_CFG" | grep '^storage-engine.max-write-cache=' | cut -d= -f2)
    echo "    replication-factor=$REPL  read-page-cache=${RPC:-?}"
    echo "    post-write-cache=$(( ${PWC:-0} / 1048576 ))MB  max-write-cache=$(( ${MWC:-0} / 1048576 ))MB"
    DSLEEP=$(echo "$NS_CFG" | grep '^storage-engine.defrag-sleep=' | cut -d= -f2)
    DLWM=$(echo "$NS_CFG" | grep '^storage-engine.defrag-lwm-pct=' | cut -d= -f2)
    FLUSH=$(echo "$NS_CFG" | grep '^storage-engine.flush-size=' | cut -d= -f2)
    echo "    defrag-sleep=${DSLEEP}us  defrag-lwm-pct=${DLWM}%  flush-size=$(( ${FLUSH:-0} / 1024 ))KB"

    SVC_CFG=$(kubectl exec -n "$NS" "$FIRST_POD" -c "$AERO_CONTAINER" -- asinfo -v 'get-config:context=service' 2>/dev/null | tr ';' '\n')
    ST=$(echo "$SVC_CFG" | grep '^service-threads=' | cut -d= -f2)
    BIT=$(echo "$SVC_CFG" | grep '^batch-index-threads=' | cut -d= -f2)
    AP=$(echo "$SVC_CFG" | grep '^auto-pin=' | cut -d= -f2)
    BMBPQ=$(echo "$SVC_CFG" | grep '^batch-max-buffers-per-queue=' | cut -d= -f2)
    echo "    service-threads=$ST  batch-index-threads=$BIT  auto-pin=$AP"
    echo "    batch-max-buffers-per-queue=$BMBPQ"
    PTS=$(kubectl exec -n "$NS" "$FIRST_POD" -c "$AERO_CONTAINER" -- asinfo -v "get-config:context=namespace;id=$AERO_DB_NS" 2>/dev/null | tr ';' '\n' | grep '^partition-tree-sprigs=' | cut -d= -f2)
    echo "    partition-tree-sprigs=$PTS"
done

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2B: POSTGRESQL HEALTH
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 2B: POSTGRESQL HEALTH"

if [ -z "$PG_NAMESPACES" ]; then
    echo "  (no PostgreSQL namespaces discovered — skipping. Set POSTGRES_NAMESPACES to override.)"
else
    for NS in $PG_NAMESPACES; do
        subheader "PostgreSQL: $NS"

        # Operator-level view (CloudNativePG), if present. Pin fields by custom-columns
        # so column order/age don't shift the values; phase may contain spaces.
        kubectl get clusters.postgresql.cnpg.io -n "$NS" --no-headers 2>/dev/null \
            -o custom-columns='NAME:.metadata.name,INSTANCES:.spec.instances,READY:.status.readyInstances,STATUS:.status.phase' \
            | awk 'NF{name=$1; inst=$2; rdy=$3; $1=$2=$3=""; sub(/^ +/,""); printf "  cluster=%-40s instances=%s ready=%s status=%s\n", name, inst, rdy, $0}'

        # Find a primary pod (cnpg labels it), else any postgres pod
        PGPOD=$(kubectl get pods -n "$NS" -l cnpg.io/instanceRole=primary --no-headers -o custom-columns=':metadata.name' 2>/dev/null | head -1)
        [ -z "$PGPOD" ] && PGPOD=$(kubectl get pods -n "$NS" -l role=primary --no-headers -o custom-columns=':metadata.name' 2>/dev/null | head -1)
        [ -z "$PGPOD" ] && PGPOD=$(kubectl get pods -n "$NS" --no-headers -o custom-columns=':metadata.name' 2>/dev/null | grep -iE 'postgres|pg-|-pg' | grep -v operator | head -1)
        if [ -z "$PGPOD" ]; then echo "  (no postgres pod found)"; continue; fi
        PGC=$(kubectl get pod -n "$NS" "$PGPOD" -o jsonpath='{range .spec.containers[*]}{.name}{"\n"}{end}' 2>/dev/null | grep -iE 'postgres' | head -1)
        [ -z "$PGC" ] && PGC=$(kubectl get pod -n "$NS" "$PGPOD" -o jsonpath='{.spec.containers[0].name}' 2>/dev/null)

        echo "  Connections by state:"
        kubectl exec -n "$NS" "$PGPOD" -c "$PGC" -- sh -c "psql -tAX -U postgres -c \"select state, count(*) from pg_stat_activity group by 1 order by 2 desc\"" 2>/dev/null \
            | awk -F'|' 'NF>=2{printf "    %-22s %s\n", $1, $2}'
        echo "  Max-connections setting:"
        kubectl exec -n "$NS" "$PGPOD" -c "$PGC" -- sh -c "psql -tAX -U postgres -c \"show max_connections\"" 2>/dev/null | awk 'NF{printf "    max_connections=%s\n", $1}'
        echo "  Longest active queries (top 3 by duration):"
        kubectl exec -n "$NS" "$PGPOD" -c "$PGC" -- sh -c "psql -tAX -U postgres -c \"select round(extract(epoch from now()-query_start)) secs, left(query,60) q from pg_stat_activity where state='active' and pid<>pg_backend_pid() order by 1 desc limit 3\"" 2>/dev/null \
            | awk -F'|' 'NF>=2{printf "    %6ss  %s\n", $1, $2}'
        echo "  Replication:"
        kubectl exec -n "$NS" "$PGPOD" -c "$PGC" -- sh -c "psql -tAX -U postgres -c \"select application_name, coalesce(replay_lag::text,'n/a') from pg_stat_replication\"" 2>/dev/null \
            | awk -F'|' 'NF>=2{printf "    %-28s replay_lag=%s\n", $1, $2}'
    done
fi

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2C: KAFKA / REDPANDA HEALTH
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 2C: KAFKA / REDPANDA HEALTH"

if [ -z "$KAFKA_NAMESPACES" ]; then
    echo "  (no Kafka/Redpanda namespaces discovered — skipping. Set KAFKA_NAMESPACES to override.)"
else
    for NS in $KAFKA_NAMESPACES; do
        subheader "Kafka/Redpanda: $NS"
        KPOD=$(kubectl get pods -n "$NS" --no-headers -o custom-columns=':metadata.name' 2>/dev/null | grep -iE 'redpanda|kafka|broker' | grep -viE 'operator|console|exporter|connect' | head -1)
        if [ -z "$KPOD" ]; then echo "  (no broker pod found)"; continue; fi

        if kubectl exec -n "$NS" "$KPOD" -- sh -c 'command -v rpk' >/dev/null 2>&1; then
            echo "  Consumer-group lag (rpk; lag = how far each consumer is behind, highest first):"
            # Run list + per-group describe INSIDE the pod in a single exec, emitting
            # group<TAB>lag. ('rpk group list' columns are BROKER GROUP STATE → name=$2;
            # 'rpk group describe' has a TOTAL-LAG summary line.) Doing the loop host-side
            # with repeated kubectl-exec mishandles word-splitting, so keep it in-pod.
            LAGS=$(kubectl exec -n "$NS" "$KPOD" -- sh -c 'rpk group list 2>/dev/null | awk "NR>1 && NF>=2{print \$2}" | while read g; do lag=$(rpk group describe "$g" 2>/dev/null | awk "/^TOTAL-LAG/{print \$2; exit}"); printf "%s\t%s\n" "$g" "${lag:-?}"; done' 2>/dev/null)
            if [ -z "$LAGS" ]; then
                echo "    (no consumer groups)"
            else
                echo "$LAGS" | sort -t"$(printf '\t')" -k2,2 -nr | awk -F'\t' 'NF>=2{printf "    %-54s total-lag=%s\n", $1, $2}'
            fi
        else
            echo "  (rpk not found in broker container — consumer-group lag unavailable here;"
            echo "   use kafka-consumer-groups.sh or another Kafka admin tool against this cluster)"
        fi
    done
fi

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3: AEROSPIKE DISK I/O
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 3: AEROSPIKE DISK I/O"

for NS in $AERO_NAMESPACES; do
    subheader "Cluster: $NS"

    AERO_PODS=$(kubectl get pods -n "$NS" --no-headers -o custom-columns=':metadata.name' 2>/dev/null | grep -v operator | grep -v '^$' | sort)
    SAMPLED_AERO=$(sample_pods "$AERO_PODS")

    if [ "$QUICK" -eq 0 ]; then
        for POD in $SAMPLED_AERO; do
            echo "  Disk I/O + CPU delta (${DELTA_SECS}s from $POD):"

            DELTA_RAW=$(kubectl exec -n "$NS" "$POD" -c "$AERO_CONTAINER" -- sh -c "
                grep nvme /proc/diskstats | grep -v 'p[0-9]' > /tmp/_d1
                cat /proc/stat | head -1 > /tmp/_c1
                sleep $DELTA_SECS
                grep nvme /proc/diskstats | grep -v 'p[0-9]' > /tmp/_d2
                cat /proc/stat | head -1 > /tmp/_c2
                echo '===DISK1==='; cat /tmp/_d1
                echo '===DISK2==='; cat /tmp/_d2
                echo '===CPU==='; cat /tmp/_c1; cat /tmp/_c2
            " 2>/dev/null)

            echo "$DELTA_RAW" | python3 -c "
import sys

content = sys.stdin.read()
sections = {}
current = None
for line in content.split('\n'):
    if line.startswith('===') and line.endswith('==='):
        current = line.strip('=')
        sections[current] = []
    elif current:
        sections.setdefault(current, []).append(line)

def parse_diskstats(lines):
    devs = {}
    for line in lines:
        parts = line.split()
        if len(parts) < 14:
            continue
        name = parts[2]
        devs[name] = {
            'reads': int(parts[3]), 'rd_sectors': int(parts[5]), 'rd_ms': int(parts[6]),
            'writes': int(parts[7]), 'wr_sectors': int(parts[9]), 'wr_ms': int(parts[10]),
            'inflight': int(parts[11]), 'io_ms': int(parts[12]), 'weighted_ms': int(parts[13]),
        }
    return devs

d1 = parse_diskstats(sections.get('DISK1', []))
d2 = parse_diskstats(sections.get('DISK2', []))

delta = $DELTA_SECS
data_drives = [n for n in d1 if n in d2 and ((d2[n]['reads'] - d1[n]['reads']) > 100 or (d2[n]['writes'] - d1[n]['writes']) > 100)]
if not data_drives:
    data_drives = [n for n in d1 if n in d2 and ((d2[n]['reads'] - d1[n]['reads']) > 0 or (d2[n]['writes'] - d1[n]['writes']) > 0)]

if data_drives:
    print(f'  {\"Drive\":<12} {\"rd IOPS\":>10} {\"wr IOPS\":>10} {\"rd MB/s\":>10} {\"wr MB/s\":>10} {\"avg_rd_us\":>10} {\"avg_wr_us\":>10} {\"queuedepth\":>11}')
    print(f'  {\"-\"*12} {\"-\"*10} {\"-\"*10} {\"-\"*10} {\"-\"*10} {\"-\"*10} {\"-\"*10} {\"-\"*11}')
    total_rd = total_wr = 0
    for name in sorted(data_drives):
        dr = (d2[name]['reads'] - d1[name]['reads']) / delta
        dw = (d2[name]['writes'] - d1[name]['writes']) / delta
        drs = (d2[name]['rd_sectors'] - d1[name]['rd_sectors']) * 512 / delta / 1024 / 1024
        dws = (d2[name]['wr_sectors'] - d1[name]['wr_sectors']) * 512 / delta / 1024 / 1024
        d_reads = d2[name]['reads'] - d1[name]['reads']
        d_writes = d2[name]['writes'] - d1[name]['writes']
        avg_rd = (d2[name]['rd_ms'] - d1[name]['rd_ms']) * 1000 / d_reads if d_reads > 0 else 0
        avg_wr = (d2[name]['wr_ms'] - d1[name]['wr_ms']) * 1000 / d_writes if d_writes > 0 else 0
        wms = d2[name]['weighted_ms'] - d1[name]['weighted_ms']
        avgqd = wms / (delta * 1000)
        total_rd += dr; total_wr += dw
        print(f'  {name:<12} {dr:>10.0f} {dw:>10.0f} {drs:>10.1f} {dws:>10.1f} {avg_rd:>10.1f} {avg_wr:>10.1f} {avgqd:>11.1f}')
    print(f'  {\"TOTAL\":<12} {total_rd:>10.0f} {total_wr:>10.0f}')
else:
    print('  (no active NVMe drives detected)')

cpu_lines = sections.get('CPU', [])
cpu_lines = [l for l in cpu_lines if l.startswith('cpu ')]
if len(cpu_lines) >= 2:
    a = list(map(int, cpu_lines[0].split()[1:]))
    b = list(map(int, cpu_lines[1].split()[1:]))
    d = [b[i]-a[i] for i in range(min(len(a),len(b)))]
    total = sum(d)
    if total > 0:
        labels = ['user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq', 'steal']
        parts = []
        for i, l in enumerate(labels):
            if i < len(d):
                pct = d[i] / total * 100
                if pct >= 0.01:
                    parts.append(f'{l}={pct:.1f}%')
        ncpu = total / ($DELTA_SECS * 100)
        print()
        print(f'  Kernel CPU: {\" | \".join(parts)}  [{ncpu:.0f} cores]')
" 2>/dev/null
            echo ""
        done
    else
        echo "  (skipped — use without --quick for disk I/O data)"
    fi
done

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3B: AEROSPIKE DEEP DIVE
# Uses privileged probe pods on Aerospike nodes to collect:
#   - perf stat (IPC, cache misses, context switches)
#   - ss socket queue depths and RTT distribution
#   - ethtool NIC hardware counters
#   - NVMe SMART health data (temperature, spare, wear, media errors)
#   - iostat -x per-device extended stats
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 3B: AEROSPIKE DEEP DIVE"

if [ "$HOST_PROBES" -ne 1 ]; then
    echo "  (skipped — needs --host-probes. This creates privileged hostNetwork/hostPID pods"
    echo "   on Aerospike nodes for perf/ss/ethtool/nvme-cli/iostat, and only works where the"
    echo "   cluster permits such pods, e.g. bare-metal k0s. Managed clusters like EKS/GKE"
    echo "   typically forbid it. Re-run with --host-probes on a cluster that allows it.)"
elif ! kubectl auth can-i create pods >/dev/null 2>&1; then
    echo "  (skipped — this kube context lacks permission to create probe pods)"
elif [ -z "$AERO_NAMESPACES" ]; then
    echo "  (skipped — no Aerospike namespaces discovered)"
else
    # Collect unique Aerospike nodes across all clusters
    AERO_NODE_LIST=""
    for NS in $AERO_NAMESPACES; do
        AERO_PODS=$(kubectl get pods -n "$NS" --no-headers -o custom-columns=':metadata.name' 2>/dev/null | grep -v operator | grep -v '^$' | sort)
        [ -z "$AERO_PODS" ] && continue
        SAMPLED_AERO=$(sample_pods "$AERO_PODS")
        for POD in $SAMPLED_AERO; do
            NODE=$(kubectl get pod -n "$NS" "$POD" -o jsonpath='{.spec.nodeName}' 2>/dev/null)
            # Deduplicate nodes (in case both clusters share a node)
            if ! echo "$AERO_NODE_LIST" | grep -qw "$NODE"; then
                AERO_NODE_LIST="$AERO_NODE_LIST $NODE"
            fi
        done
    done
    AERO_NODE_LIST=$(echo "$AERO_NODE_LIST" | xargs)
    AERO_NODE_COUNT=$(echo "$AERO_NODE_LIST" | wc -w | tr -d ' ')

    echo "  Sampling $AERO_NODE_COUNT Aerospike nodes: $AERO_NODE_LIST"
    echo "  Creating privileged probe pods..."

    # Create probe pods in parallel
    PROBE_NAMES=""
    PROBE_PIDS=""
    for NODE in $AERO_NODE_LIST; do
        PROBE_NAME="aero-deep-${NODE}-$$"
        PROBE_NAMES="$PROBE_NAMES $PROBE_NAME"
        create_probe_pod "$NODE" "$PROBE_NAME" &
        PROBE_PIDS="$PROBE_PIDS $!"
    done
    for PID in $PROBE_PIDS; do wait "$PID" 2>/dev/null; done

    # Install tools in parallel
    INSTALL_PIDS=""
    for PROBE_NAME in $PROBE_NAMES; do
        install_probe_tools "$PROBE_NAME" &
        INSTALL_PIDS="$INSTALL_PIDS $!"
    done
    for PID in $INSTALL_PIDS; do wait "$PID" 2>/dev/null; done

    # Collect data in parallel
    COLLECT_PIDS=""
    IDX=0
    for NODE in $AERO_NODE_LIST; do
        PROBE_NAME=$(echo "$PROBE_NAMES" | awk -v i=$((IDX+1)) '{print $i}')
        collect_aero_deep "$PROBE_NAME" "$TMPDIR/aerodeep_${NODE}.txt" &
        COLLECT_PIDS="$COLLECT_PIDS $!"
        IDX=$((IDX+1))
    done
    for PID in $COLLECT_PIDS; do wait "$PID" 2>/dev/null; done

    # Parse and display results
    for NODE in $AERO_NODE_LIST; do
        f="$TMPDIR/aerodeep_${NODE}.txt"
        if [ -s "$f" ]; then
            subheader "Deep dive: $NODE"
            parse_aero_deep "$f" "$NODE"
        fi
    done

    # Cleanup probe pods
    for PROBE_NAME in $PROBE_NAMES; do
        kubectl delete pod "$PROBE_NAME" --force --grace-period=0 >/dev/null 2>&1 &
    done
    wait
fi

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4: PROPAGATION PODS
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 4: PROPAGATION PODS"

for NS in $WORKLOAD_NAMESPACES; do
    PROP_PODS=$(kubectl get pods -n "$NS" -l app=propagation --no-headers -o custom-columns=':metadata.name' 2>/dev/null | grep -v '^$' | sort)
    PROP_COUNT=0
    [ -n "$PROP_PODS" ] && PROP_COUNT=$(echo "$PROP_PODS" | wc -l | tr -d ' ')
    [ "$PROP_COUNT" -eq 0 ] && continue

    SAMPLED=$(sample_pods "$PROP_PODS")
    SAMPLED_COUNT=$(echo "$SAMPLED" | wc -l | tr -d ' ')

    subheader "Namespace: $NS ($PROP_COUNT propagation pods, sampling $SAMPLED_COUNT)"

    # CPU summary (always all pods)
    CPU_DATA=$(kubectl top pods -n "$NS" --no-headers 2>/dev/null | grep propagation)
    if [ -n "$CPU_DATA" ]; then
        echo "$CPU_DATA" | awk '{gsub(/m$/,"",$2); cpu=$2; sum+=cpu; count++; if(count==1||cpu>max)max=cpu; if(count==1||cpu<min)min=cpu} END {printf "  CPU: avg=%dm  min=%dm  max=%dm  total=%dm  pods=%d\n", sum/count, min, max, sum, count}'
    fi

    FIRST_PROP=$(echo "$SAMPLED" | head -1)
    LIMITS=$(kubectl get pod -n "$NS" "$FIRST_PROP" -o jsonpath='{.spec.containers[0].resources.limits.cpu}' 2>/dev/null)
    REQUESTS=$(kubectl get pod -n "$NS" "$FIRST_PROP" -o jsonpath='{.spec.containers[0].resources.requests.cpu}' 2>/dev/null)
    echo "  Limits: cpu=$LIMITS  Requests: cpu=$REQUESTS"

    # Throttling from first sampled pod
    THROTTLE=$(kubectl exec -n "$NS" "$FIRST_PROP" -- cat /sys/fs/cgroup/cpu.stat 2>/dev/null)
    NR_THROTTLED=$(echo "$THROTTLE" | grep '^nr_throttled' | awk '{print $2}')
    THROTTLED_US=$(echo "$THROTTLE" | grep '^throttled_usec' | awk '{print $2}')
    echo "  Throttling (sample=$FIRST_PROP): nr_throttled=$NR_THROTTLED  throttled_usec=$THROTTLED_US"

    # Node kernel CPU
    if [ "$QUICK" -eq 0 ]; then
        NODE_CPU=$(kubectl exec -n "$NS" "$FIRST_PROP" -- sh -c "cat /proc/stat | head -1; sleep $DELTA_SECS; cat /proc/stat | head -1" 2>/dev/null)
        echo -n "  Node kernel "; echo "$NODE_CPU" | parse_cpu_delta
    fi

    # Parallel goroutine fetch for all sampled pods
    echo ""
    echo "  Goroutine profile (aggregated from $SAMPLED_COUNT/$PROP_COUNT pods):"
    PIDS=""
    for POD in $SAMPLED; do
        fetch_goroutine_profile "$NS" "$POD" "$PPROF_PORT" "$TMPDIR/prop_${NS}_${POD}.txt" &
        PIDS="$PIDS $!"
    done
    for PID in $PIDS; do wait "$PID" 2>/dev/null; done

    PROFILE_FILES=""
    for POD in $SAMPLED; do
        f="$TMPDIR/prop_${NS}_${POD}.txt"
        [ -s "$f" ] && PROFILE_FILES="$PROFILE_FILES $f"
    done
    aggregate_goroutines $PROFILE_FILES
done

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5: BLASTER PODS
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 5: BLASTER PODS"

for NS in $WORKLOAD_NAMESPACES; do
    BLASTER_PODS=$(kubectl get pods -n "$NS" -l app=tx-blaster --no-headers -o custom-columns=':metadata.name' 2>/dev/null | grep -v '^$' | sort)
    BLASTER_COUNT=0
    [ -n "$BLASTER_PODS" ] && BLASTER_COUNT=$(echo "$BLASTER_PODS" | wc -l | tr -d ' ')
    [ "$BLASTER_COUNT" -eq 0 ] && continue

    SAMPLED=$(sample_pods "$BLASTER_PODS")
    SAMPLED_COUNT=$(echo "$SAMPLED" | wc -l | tr -d ' ')

    subheader "Namespace: $NS ($BLASTER_COUNT blaster pods, sampling $SAMPLED_COUNT)"

    # CPU summary
    CPU_DATA=$(kubectl top pods -n "$NS" --no-headers 2>/dev/null | grep blaster)
    if [ -n "$CPU_DATA" ]; then
        echo "$CPU_DATA" | awk '{gsub(/m$/,"",$2); cpu=$2; sum+=cpu; count++; if(count==1||cpu>max)max=cpu; if(count==1||cpu<min)min=cpu} END {printf "  CPU: avg=%dm  min=%dm  max=%dm  total=%dm  pods=%d\n", sum/count, min, max, sum, count}'
    fi

    # Node placement
    echo "  Node placement:"
    kubectl get pods -n "$NS" -l app=tx-blaster -o wide --no-headers 2>/dev/null | awk '{print $7}' | sort | uniq -c | sort -rn | while read count node; do
        printf "    %-25s %d blasters\n" "$node" "$count"
    done

    FIRST_BLASTER=$(echo "$SAMPLED" | head -1)

    # Schedstat from first sampled pod
    SCHED=$(kubectl exec -n "$NS" "$FIRST_BLASTER" -- cat /proc/1/schedstat 2>/dev/null)
    if [ -n "$SCHED" ]; then
        echo "$SCHED" | BLASTER_NAME="$FIRST_BLASTER" python3 -c "
import sys, os
parts = sys.stdin.read().strip().split()
name = os.environ.get('BLASTER_NAME', '?')
if len(parts) >= 2:
    run = int(parts[0])
    wait = int(parts[1])
    total = run + wait
    if total > 0:
        print(f'  schedstat (sample={name}): run={run/1e9:.1f}s  wait={wait/1e9:.1f}s  wait_ratio={wait/total*100:.1f}%')
" 2>/dev/null
    fi

    # Node kernel CPU
    if [ "$QUICK" -eq 0 ]; then
        NODE_CPU=$(kubectl exec -n "$NS" "$FIRST_BLASTER" -- sh -c "cat /proc/stat | head -1; sleep $DELTA_SECS; cat /proc/stat | head -1" 2>/dev/null)
        echo -n "  Node kernel "; echo "$NODE_CPU" | parse_cpu_delta
    fi

    # Parallel goroutine fetch
    echo ""
    echo "  Goroutine profile (aggregated from $SAMPLED_COUNT/$BLASTER_COUNT pods):"
    PIDS=""
    for POD in $SAMPLED; do
        fetch_goroutine_profile "$NS" "$POD" "$BLASTER_PPROF_PORT" "$TMPDIR/blaster_${NS}_${POD}.txt" &
        PIDS="$PIDS $!"
    done
    for PID in $PIDS; do wait "$PID" 2>/dev/null; done

    PROFILE_FILES=""
    for POD in $SAMPLED; do
        f="$TMPDIR/blaster_${NS}_${POD}.txt"
        [ -s "$f" ] && PROFILE_FILES="$PROFILE_FILES $f"
    done
    aggregate_goroutines $PROFILE_FILES
done

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6: BLOCK ASSEMBLY & BLOCK VALIDATOR
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 6: BLOCK ASSEMBLY & BLOCK VALIDATOR"

# NOTE: A block validator that is catching up (FSM = CATCHINGBLOCKS) re-validates
# historical blocks and will legitimately show high CPU and many I/O-blocked
# goroutines. That is expected while syncing — not a bug. A validator on a node in
# RUNNING state only validates locally-produced or freshly-announced blocks and is
# typically much lighter. Correlate the numbers below with the per-namespace FSM
# state from Section 1 before treating high usage here as a problem.

echo "  # A catching-up block validator (FSM=CATCHINGBLOCKS) re-validates historical blocks"
echo "  # and will show high CPU / many I/O-blocked goroutines — expected while syncing."
echo "  # Compare against the per-namespace FSM state in Section 1 before flagging it."

for NS in $WORKLOAD_NAMESPACES; do
    # ── Block Assembly ──
    BA_PODS=$(kubectl get pods -n "$NS" -l app=block-assembly --no-headers -o custom-columns=':metadata.name' 2>/dev/null | grep -v '^$' | sort)
    BA_COUNT=0
    [ -n "$BA_PODS" ] && BA_COUNT=$(echo "$BA_PODS" | wc -l | tr -d ' ')

    if [ "$BA_COUNT" -gt 0 ]; then
        SAMPLED_BA=$(sample_pods "$BA_PODS")
        SAMPLED_BA_COUNT=$(echo "$SAMPLED_BA" | wc -l | tr -d ' ')

        subheader "$NS — Block Assembly ($BA_COUNT pods, sampling $SAMPLED_BA_COUNT)"
        kubectl top pods -n "$NS" --no-headers 2>/dev/null | grep block-assembly | awk '{printf "  CPU=%s  Mem=%s  (%s)\n", $2, $3, $1}'

        # Parallel goroutine fetch
        PIDS=""
        for POD in $SAMPLED_BA; do
            fetch_goroutine_profile "$NS" "$POD" "$PPROF_PORT" "$TMPDIR/ba_${NS}_${POD}.txt" &
            PIDS="$PIDS $!"
        done
        for PID in $PIDS; do wait "$PID" 2>/dev/null; done

        echo ""
        echo "  Goroutine profile (aggregated from $SAMPLED_BA_COUNT/$BA_COUNT pods):"
        PROFILE_FILES=""
        for POD in $SAMPLED_BA; do
            f="$TMPDIR/ba_${NS}_${POD}.txt"
            [ -s "$f" ] && PROFILE_FILES="$PROFILE_FILES $f"
        done
        aggregate_goroutines $PROFILE_FILES
    fi

    # ── Block Validator ──
    BV_PODS=$(kubectl get pods -n "$NS" -l app=block-validator --no-headers -o custom-columns=':metadata.name' 2>/dev/null | grep -v '^$' | sort)
    BV_COUNT=0
    [ -n "$BV_PODS" ] && BV_COUNT=$(echo "$BV_PODS" | wc -l | tr -d ' ')

    if [ "$BV_COUNT" -gt 0 ]; then
        SAMPLED_BV=$(sample_pods "$BV_PODS")
        SAMPLED_BV_COUNT=$(echo "$SAMPLED_BV" | wc -l | tr -d ' ')

        subheader "$NS — Block Validator ($BV_COUNT pods, sampling $SAMPLED_BV_COUNT)"
        kubectl top pods -n "$NS" --no-headers 2>/dev/null | grep block-validator | awk '{printf "  CPU=%s  Mem=%s  (%s)\n", $2, $3, $1}'

        # Resource limits
        FIRST_BV=$(echo "$SAMPLED_BV" | head -1)
        BV_LIMITS=$(kubectl get pod -n "$NS" "$FIRST_BV" -o jsonpath='{.spec.containers[0].resources.limits.cpu}' 2>/dev/null)
        BV_REQUESTS=$(kubectl get pod -n "$NS" "$FIRST_BV" -o jsonpath='{.spec.containers[0].resources.requests.cpu}' 2>/dev/null)
        echo "  Limits: cpu=${BV_LIMITS:-none}  Requests: cpu=${BV_REQUESTS:-none}"

        # Throttling
        BV_THROTTLE=$(kubectl exec -n "$NS" "$FIRST_BV" -- cat /sys/fs/cgroup/cpu.stat 2>/dev/null)
        BV_NR_T=$(echo "$BV_THROTTLE" | grep '^nr_throttled' | awk '{print $2}')
        BV_T_US=$(echo "$BV_THROTTLE" | grep '^throttled_usec' | awk '{print $2}')
        echo "  Throttling: nr_throttled=${BV_NR_T:-0}  throttled_usec=${BV_T_US:-0}"

        # Parallel goroutine fetch
        PIDS=""
        for POD in $SAMPLED_BV; do
            fetch_goroutine_profile "$NS" "$POD" "$PPROF_PORT" "$TMPDIR/bv_${NS}_${POD}.txt" &
            PIDS="$PIDS $!"
        done
        for PID in $PIDS; do wait "$PID" 2>/dev/null; done

        echo ""
        echo "  Goroutine profile (aggregated from $SAMPLED_BV_COUNT/$BV_COUNT pods):"
        PROFILE_FILES=""
        for POD in $SAMPLED_BV; do
            f="$TMPDIR/bv_${NS}_${POD}.txt"
            [ -s "$f" ] && PROFILE_FILES="$PROFILE_FILES $f"
        done
        aggregate_goroutines $PROFILE_FILES
    fi
done

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7: NETWORKING
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 7: NETWORKING"

if [ "$QUICK" -eq 1 ]; then
    echo "  (skipped — use without --quick for network data)"
else
    # ── Node-level bandwidth (one debug pod per unique node role) ──
    if [ "$HOST_PROBES" -ne 1 ]; then
        subheader "Node-level bandwidth"
        echo "  (skipped — needs --host-probes; it creates hostNetwork probe pods on the"
        echo "   data-carrying nodes. Pod-level throughput below uses exec only and still runs.)"
    else
    subheader "Node-level bandwidth (${DELTA_SECS}s sample per role)"

    # Prefer the 'role' node label; fall back to the nodes that actually host propagation +
    # aerospike pods when a cluster has no recognizable role labels.
    awk '!seen[$3]++ {print $3, $1}' "$TMPDIR/node_lookup.txt" | grep -E '^(txblaster|teranode|aerospike|blocks)' > "$TMPDIR/role_nodes.txt"
    if [ ! -s "$TMPDIR/role_nodes.txt" ]; then
        for _ns in $WORKLOAD_NAMESPACES; do
            kubectl get pods -n "$_ns" -l app=propagation -o wide --no-headers 2>/dev/null | awk '{print "teranode", $7}'
        done > "$TMPDIR/role_nodes.txt"
        for _ns in $AERO_NAMESPACES; do
            kubectl get pods -n "$_ns" -o wide --no-headers 2>/dev/null | grep -v operator | awk '{print "aerospike", $7}'
        done >> "$TMPDIR/role_nodes.txt"
        awk 'NF>=2 && !seen[$2]++' "$TMPDIR/role_nodes.txt" > "$TMPDIR/role_nodes.tmp" && mv "$TMPDIR/role_nodes.tmp" "$TMPDIR/role_nodes.txt"
    fi

    # Launch probe pods in parallel — read nodes into array first to avoid stdin conflicts
    NODE_ROLES=()
    NODE_NAMES=()
    while read role node; do
        [ -z "$role" ] || [ -z "$node" ] && continue
        NODE_ROLES+=("$role")
        NODE_NAMES+=("$node")
    done < "$TMPDIR/role_nodes.txt"

    NODE_PIDS=""
    for i in "${!NODE_NAMES[@]}"; do
        fetch_node_net_stats "${NODE_NAMES[$i]}" "$TMPDIR/nodenet_${NODE_ROLES[$i]}_${NODE_NAMES[$i]}.txt" &
        NODE_PIDS="$NODE_PIDS $!"
    done
    for PID in $NODE_PIDS; do wait "$PID" 2>/dev/null; done

    printf "  %-14s %-25s %10s %12s %12s %10s\n" "Role" "Node" "Link(Gbps)" "RX MB/s" "TX MB/s" "% Used"
    printf "  %-14s %-25s %10s %12s %12s %10s\n" "----" "----" "----------" "-------" "-------" "------"

    while read role node; do
        [ -z "$role" ] || [ -z "$node" ] && continue
        f="$TMPDIR/nodenet_${role}_${node}.txt"
        [ ! -s "$f" ] && continue
        ROLE_V="$role" NODE_V="$node" DELTA="$DELTA_SECS" python3 -c "
import sys, os
role = os.environ.get('ROLE_V', '?')
node = os.environ.get('NODE_V', '?')
delta = int(os.environ.get('DELTA', '2'))
content = open(sys.argv[1]).read()
sections = {}
current = None
for line in content.split('\n'):
    if line.startswith('===') and line.endswith('==='):
        current = line.strip('=')
        sections[current] = []
    elif current:
        sections.setdefault(current, []).append(line)

speed_mbps = 0
for l in sections.get('SPEED', []):
    l = l.strip()
    if l.isdigit():
        speed_mbps = int(l)
        break

def parse_net(lines):
    best = None
    for line in lines:
        parts = line.split()
        if len(parts) >= 10:
            iface = parts[0].rstrip(':')
            rx = int(parts[1])
            tx = int(parts[9])
            if best is None or (rx + tx) > (best['rx'] + best['tx']):
                best = {'iface': iface, 'rx': rx, 'tx': tx}
    return best

n1 = parse_net(sections.get('NET1', []))
n2 = parse_net(sections.get('NET2', []))

if n1 and n2:
    rx_bps = (n2['rx'] - n1['rx']) / delta
    tx_bps = (n2['tx'] - n1['tx']) / delta
    rx_mbps = rx_bps / 1024 / 1024
    tx_mbps = tx_bps / 1024 / 1024
    speed_gbps = speed_mbps / 1000 if speed_mbps else 0
    max_bytes_sec = speed_mbps * 1000000 / 8 if speed_mbps else 0
    pct_used = max(rx_bps, tx_bps) / max_bytes_sec * 100 if max_bytes_sec else 0
    print(f'  {role:<14} {node:<25} {speed_gbps:>10.0f} {rx_mbps:>12.1f} {tx_mbps:>12.1f} {pct_used:>9.1f}%')
else:
    print(f'  {role:<14} {node:<25} (no data)')
" "$f" 2>/dev/null
    done < "$TMPDIR/role_nodes.txt"
    fi  # end node-level bandwidth (--host-probes)

    # ── Per-pod network throughput + TCP health ──
    subheader "Pod network throughput + TCP health (${DELTA_SECS}s sample)"

    # Sample one propagation pod per workload namespace
    for NS in $WORKLOAD_NAMESPACES; do
        FIRST_PROP=$(kubectl get pods -n "$NS" -l app=propagation --no-headers -o custom-columns=':metadata.name' 2>/dev/null | grep -v '^$' | head -1)
        if [ -n "$FIRST_PROP" ]; then
            fetch_pod_net_stats "$NS" "$FIRST_PROP" "$TMPDIR/podnet_prop_${NS}.txt" &
        fi

        FIRST_BLASTER=$(kubectl get pods -n "$NS" -l app=tx-blaster --no-headers -o custom-columns=':metadata.name' 2>/dev/null | grep -v '^$' | head -1)
        if [ -n "$FIRST_BLASTER" ]; then
            fetch_pod_net_stats "$NS" "$FIRST_BLASTER" "$TMPDIR/podnet_blaster_${NS}.txt" &
        fi
    done

    # Sample one aerospike pod per cluster — Aerospike uses hostNetwork so read bond1
    for NS in $AERO_NAMESPACES; do
        FIRST_AERO=$(kubectl get pods -n "$NS" --no-headers -o custom-columns=':metadata.name' 2>/dev/null | grep -v operator | grep -v '^$' | head -1)
        if [ -n "$FIRST_AERO" ]; then
            (kubectl exec -n "$NS" "$FIRST_AERO" -c "$AERO_CONTAINER" -- sh -c "
                cat /proc/net/dev | grep -E 'bond1|bond0|eth0' | head -1 > /tmp/_net1
                cat /proc/net/snmp | grep '^Tcp:' > /tmp/_tcp1
                sleep $DELTA_SECS
                cat /proc/net/dev | grep -E 'bond1|bond0|eth0' | head -1 > /tmp/_net2
                cat /proc/net/snmp | grep '^Tcp:' > /tmp/_tcp2
                wc -l < /proc/net/tcp6 2>/dev/null || echo 0
                echo '===NET1==='; cat /tmp/_net1
                echo '===NET2==='; cat /tmp/_net2
                echo '===TCP1==='; cat /tmp/_tcp1
                echo '===TCP2==='; cat /tmp/_tcp2
            " 2>/dev/null > "$TMPDIR/podnet_aero_${NS}.txt") &
        fi
    done

    wait

    # Parse all pod net stats
    printf "\n  %-14s %-25s %12s %12s %12s %10s %8s\n" "Type" "Pod (namespace)" "RX MB/s" "TX MB/s" "Retrans/s" "RetransRatio" "Conns"
    printf "  %-14s %-25s %12s %12s %12s %10s %8s\n" "----" "---" "-------" "-------" "---------" "----------" "-----"

    for f in "$TMPDIR"/podnet_*.txt; do
        [ ! -s "$f" ] && continue
        fname=$(basename "$f" .txt)
        python3 -c "
import sys, os

fname = '$fname'
# Extract type and namespace from filename: podnet_TYPE_NAMESPACE
parts = fname.replace('podnet_', '').split('_', 1)
ptype = parts[0] if parts else '?'
pns = parts[1] if len(parts) > 1 else '?'

content = open('$f').read()
sections = {}
current = None
for line in content.split('\n'):
    if line.startswith('===') and line.endswith('==='):
        current = line.strip('=')
        sections[current] = []
    elif current is not None:
        sections.setdefault(current, []).append(line)
    elif current is None and not line.startswith('==='):
        # First line before any section is the connection count
        l = line.strip()
        if l.isdigit():
            sections['CONNS'] = [l]

def parse_net_line(lines):
    for line in lines:
        parts = line.split()
        if len(parts) >= 10:
            return {'rx': int(parts[1]), 'tx': int(parts[9])}
    return None

def parse_tcp(lines):
    # Two lines: header and values
    header = None
    values = None
    for line in lines:
        if line.startswith('Tcp:'):
            fields = line.split()
            if fields[1].isalpha():
                header = fields[1:]
            else:
                values = fields[1:]
    if header and values:
        d = {}
        for h, v in zip(header, values):
            try:
                d[h] = int(v)
            except ValueError:
                pass
        return d
    return {}

n1 = parse_net_line(sections.get('NET1', []))
n2 = parse_net_line(sections.get('NET2', []))
tcp1 = parse_tcp(sections.get('TCP1', []))
tcp2 = parse_tcp(sections.get('TCP2', []))
conns_lines = sections.get('CONNS', ['0'])
conns = conns_lines[0].strip() if conns_lines else '0'

rx_mbps = tx_mbps = 0
if n1 and n2:
    rx_mbps = (n2['rx'] - n1['rx']) / $DELTA_SECS / 1024 / 1024
    tx_mbps = (n2['tx'] - n1['tx']) / $DELTA_SECS / 1024 / 1024

retrans_s = 0
retrans_ratio = 0
if tcp1 and tcp2:
    retrans_d = tcp2.get('RetransSegs', 0) - tcp1.get('RetransSegs', 0)
    outsegs_d = tcp2.get('OutSegs', 0) - tcp1.get('OutSegs', 0)
    retrans_s = retrans_d / $DELTA_SECS
    retrans_ratio = retrans_d / outsegs_d * 100 if outsegs_d > 0 else 0

short_ns = (pns[:23] + '…') if len(pns) > 24 else pns
label = f'{short_ns}'
print(f'  {ptype:<14} {label:<25} {rx_mbps:>12.1f} {tx_mbps:>12.1f} {retrans_s:>12.1f} {retrans_ratio:>9.4f}% {conns:>8}')
" 2>/dev/null
    done

    # ── Softnet drops ──
    subheader "Softnet drops (host-level, from sampled pod)"

    # Read from any propagation pod (softnet_stat is host-level)
    FIRST_NS=$(echo "$WORKLOAD_NAMESPACES" | awk '{print $1}')
    if [ -n "$FIRST_NS" ]; then
        FIRST_PROP=$(kubectl get pods -n "$FIRST_NS" -l app=propagation --no-headers -o custom-columns=':metadata.name' 2>/dev/null | grep -v '^$' | head -1)
        if [ -n "$FIRST_PROP" ]; then
            SOFTNET=$(kubectl exec -n "$FIRST_NS" "$FIRST_PROP" -- cat /proc/net/softnet_stat 2>/dev/null)
            echo "$SOFTNET" | python3 -c "
import sys
lines = sys.stdin.read().strip().split('\n')
total_processed = 0
total_dropped = 0
total_squeezed = 0
for line in lines:
    fields = line.split()
    if len(fields) >= 3:
        total_processed += int(fields[0], 16)
        total_dropped += int(fields[1], 16)
        total_squeezed += int(fields[2], 16)
cpus = len(lines)
print(f'  CPUs: {cpus}  Packets processed: {total_processed:,}  Dropped: {total_dropped:,}  Time-squeezed: {total_squeezed:,}')
if total_dropped > 0:
    print(f'  [!] WARNING: {total_dropped:,} softnet drops detected — kernel is dropping packets due to backlog overflow')
if total_squeezed > 100:
    print(f'  [i] {total_squeezed:,} time-squeeze events — kernel ran out of budget processing packets (consider increasing netdev_budget)')
" 2>/dev/null
        fi
    fi
fi

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8: SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 8: SUMMARY"

echo ""
echo "  Collecting summary metrics..."
echo ""

for NS in $AERO_NAMESPACES; do
    FIRST_POD=$(kubectl get pods -n "$NS" --no-headers -o custom-columns=':metadata.name' 2>/dev/null | grep -v operator | grep -v '^$' | head -1)
    [ -z "$FIRST_POD" ] && continue

    CACHE=$(kubectl exec -n "$NS" "$FIRST_POD" -c "$AERO_CONTAINER" -- asinfo -v "namespace/$AERO_DB_NS" 2>/dev/null | tr ';' '\n' | grep '^cache_read_pct=' | cut -d= -f2)
    RW=$(kubectl exec -n "$NS" "$FIRST_POD" -c "$AERO_CONTAINER" -- asinfo -v 'statistics' 2>/dev/null | tr ';' '\n' | grep '^rw_in_progress=' | cut -d= -f2)

    LATENCY_RAW=$(kubectl exec -n "$NS" "$FIRST_POD" -c "$AERO_CONTAINER" -- asinfo -v 'latencies:' 2>/dev/null)
    BATCH_GT1=$(echo "$LATENCY_RAW" | python3 -c "
import sys
raw = sys.stdin.read().strip()
for entry in raw.split(';'):
    if entry.startswith('batch-index:'):
        vals = entry.split(',')
        if len(vals) > 2:
            print(vals[2])
            break
" 2>/dev/null)

    echo "  Aerospike [$NS]:"
    echo "    cache-read-pct=$CACHE  rw-in-progress=$RW  batch-index >1ms=${BATCH_GT1:-?}%"
done

echo ""
echo "  ──────────────────────────────────────────────────────────"
echo "  Directional hints — signals worth investigating, NOT pass/fail thresholds."
echo "  Teranode throughput targets vary by deployment; diagnose by comparison, not absolutes."
echo ""

HINTS=""

for NS in $AERO_NAMESPACES; do
    FIRST_POD=$(kubectl get pods -n "$NS" --no-headers -o custom-columns=':metadata.name' 2>/dev/null | grep -v operator | grep -v '^$' | head -1)
    [ -z "$FIRST_POD" ] && continue
    CACHE=$(kubectl exec -n "$NS" "$FIRST_POD" -c "$AERO_CONTAINER" -- asinfo -v "namespace/$AERO_DB_NS" 2>/dev/null | tr ';' '\n' | grep '^cache_read_pct=' | cut -d= -f2)
    CACHE_INT=$(echo "$CACHE" | cut -d. -f1)
    if [ -n "$CACHE_INT" ] && [ "$CACHE_INT" -lt 50 ] 2>/dev/null; then
        HINTS="${HINTS}  [!] $NS: cache-read-pct=${CACHE}% — Aerospike is disk-bound. Consider read-page-cache=true or larger post-write-cache.\n"
    fi
done

for NS in $WORKLOAD_NAMESPACES; do
    FIRST_PROP=$(kubectl get pods -n "$NS" -l app=propagation --no-headers -o custom-columns=':metadata.name' 2>/dev/null | grep -v '^$' | head -1)
    if [ -n "$FIRST_PROP" ]; then
        NR_T=$(kubectl exec -n "$NS" "$FIRST_PROP" -- cat /sys/fs/cgroup/cpu.stat 2>/dev/null | grep '^nr_throttled' | awk '{print $2}')
        if [ -n "$NR_T" ] && [ "$NR_T" -gt 1000 ] 2>/dev/null; then
            HINTS="${HINTS}  [!] $NS propagation: nr_throttled=$NR_T — CPU throttling detected. Increase CPU limit.\n"
        fi
    fi
done

# Check perf data for IPC issues
for f in "$TMPDIR"/aerodeep_*.txt; do
    [ ! -s "$f" ] && continue
    NODE=$(basename "$f" .txt | sed 's/aerodeep_//')
    IPC=$(grep "insn per cycle" "$f" 2>/dev/null | sed -n 's/.*# *\([0-9.]*\) *insn per cycle.*/\1/p')
    if [ -n "$IPC" ]; then
        IPC_INT=$(echo "$IPC" | awk '{printf "%d", $1 * 100}')
        if [ "$IPC_INT" -lt 60 ] 2>/dev/null; then
            HINTS="${HINTS}  [!] Aerospike node $NODE: IPC=${IPC} — CPU is memory-stalled. auto-pin=numa and larger partition-tree-sprigs would help.\n"
        fi
    fi
done

if [ -z "$HINTS" ]; then
    echo "  (no obvious storage/CPU signals from automated checks)"
    echo "  Next: review the goroutine profiles above for the busiest pipeline stage,"
    echo "  confirm FSM state (Section 1), and check Kafka/Redpanda consumer lag (Section 2C)"
    echo "  for any service that appears stalled. See the debug-teranode skill for interpretation."
else
    echo -e "$HINTS"
fi

echo ""
echo "  Diagnostic complete at $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo ""
