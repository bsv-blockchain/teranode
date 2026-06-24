#!/usr/bin/env bash
# teranode-diag-docker.sh — Diagnostic snapshot for Teranode on Docker / docker-compose.
#
# Mirrors the Kubernetes diag tool but uses docker primitives. Discovery is automatic:
# the compose project, its containers, the datastore backends, pprof ports, the settings
# context, and FSM state are all detected at runtime. Read-only.
#
# Teranode docker stacks vary (operator single-node, multi-node dev/CI, split single-node);
# this script adapts to whichever is running. pprof (port 9091) is NOT host-published in the
# operator stack, so profiles are pulled by exec'ing inside the container; in dev/CI stacks
# the host port is used automatically.
#
# Usage: ./teranode-diag-docker.sh [OPTIONS]
#   --quick          Skip log scans and the longer collections
#   --project NAME   Compose project to target (default: auto-detected)
#   --since DURATION Log window for the error scan (default: 10m)
#   --help           Show this help and the override env vars
#
# Override env vars:
#   COMPOSE_PROJECT      pin the compose project name
#   TERANODE_CONTAINERS  space/comma list of teranode service containers (skip discovery)
#   AEROSPIKE_CONTAINER  Aerospike container name (default: discovered)
#   AEROSPIKE_DB_NS      Aerospike DB namespace (default: discovered via asinfo -v namespaces)
#   POSTGRES_CONTAINER   Postgres container name (default: discovered)
#   KAFKA_CONTAINER      Kafka/Redpanda broker container (default: discovered)
#   PPROF_PORT           teranode pprof container port (default: 9091)

set -o pipefail

QUICK=0
SINCE=10m
COMPOSE_PROJECT="${COMPOSE_PROJECT:-}"
PPROF_PORT="${PPROF_PORT:-9091}"

print_help() { sed -n '2,33p' "$0" | sed 's/^# \{0,1\}//'; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --quick) QUICK=1; shift ;;
        --project) COMPOSE_PROJECT="$2"; shift 2 ;;
        --since) SINCE="$2"; shift 2 ;;
        --help|-h) print_help; exit 0 ;;
        *) echo "Unknown option: $1 (try --help)" >&2; exit 1 ;;
    esac
done

command -v docker >/dev/null 2>&1 || { echo "Error: docker not found in PATH" >&2; exit 1; }
DC="docker compose"; docker compose version >/dev/null 2>&1 || DC="docker-compose"

TIMESTAMP=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
norm_list() { echo "$1" | tr ',' ' ' | xargs; }
# Aerospike in-container service port (3000 by default; multinode uses 30N0). asinfo
# defaults to :3000, so on non-default deployments it must be told the real port.
aero_port() { docker port "$1" 2>/dev/null | sed -E 's#/.*##' | grep -E '^30[0-9]0$' | sort -u | head -1; }

header()    { echo ""; echo "================================================================================"; echo "  $1"; echo "================================================================================"; }
subheader() { echo ""; echo "── $1 ──"; }

# ── Discovery ────────────────────────────────────────────────────────────────
# Compose project: the project label most common among containers running a teranode image.
if [ -z "$COMPOSE_PROJECT" ]; then
    COMPOSE_PROJECT=$(docker ps --format '{{.Label "com.docker.compose.project"}}\t{{.Image}}' 2>/dev/null \
        | grep -iE 'teranode' | awk -F'\t' '{print $1}' | grep -v '^$' | sort | uniq -c | sort -rn | awk 'NR==1{print $2}')
fi
if [ -z "$COMPOSE_PROJECT" ]; then
    echo "No teranode compose project detected. Is the stack running? (docker compose ls)" >&2
    docker compose ls 2>/dev/null
    exit 1
fi

# All containers in the project: name<TAB>image<TAB>status<TAB>service
PROJ_FILTER="label=com.docker.compose.project=$COMPOSE_PROJECT"
# (read loop rather than mapfile/readarray — those are bash 4+, macOS ships bash 3.2)
ALL_ROWS=()
while IFS= read -r _row; do [ -n "$_row" ] && ALL_ROWS+=("$_row"); done < <(docker ps -a --filter "$PROJ_FILTER" --format '{{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Label "com.docker.compose.service"}}' 2>/dev/null)

# Classify containers by image. Teranode node services use the teranode image but NOT the
# coinbase/blaster image (teranode-coinbase). Datastores by their image.
TERANODE_CTRS=""; AERO_CTRS=""; PG_CTRS=""; KAFKA_CTRS=""; BLASTER_CTRS=""
for row in "${ALL_ROWS[@]}"; do
    name=$(echo "$row" | cut -f1); image=$(echo "$row" | cut -f2)
    case "$image" in
        *teranode-coinbase*|*coinbase*|*blaster*) BLASTER_CTRS="$BLASTER_CTRS $name" ;;
        *teranode*) [[ "$name" == *builder* ]] || TERANODE_CTRS="$TERANODE_CTRS $name" ;;
    esac
    case "$image" in
        *aerospike*) [[ "$image" == *exporter* ]] || AERO_CTRS="$AERO_CTRS $name" ;;
        *postgres*|*cnpg*) PG_CTRS="$PG_CTRS $name" ;;
        *redpanda*|*kafka*) [[ "$name" == *console* ]] || KAFKA_CTRS="$KAFKA_CTRS $name" ;;
    esac
done
[ -n "${TERANODE_CONTAINERS:-}" ] && TERANODE_CTRS=$(norm_list "$TERANODE_CONTAINERS")
TERANODE_CTRS=$(echo "$TERANODE_CTRS" | xargs)
AERO_CTRS=$(echo "$AERO_CTRS" | xargs);   [ -n "${AEROSPIKE_CONTAINER:-}" ] && AERO_CTRS="$AEROSPIKE_CONTAINER"
PG_CTRS=$(echo "$PG_CTRS" | xargs);       [ -n "${POSTGRES_CONTAINER:-}" ] && PG_CTRS="$POSTGRES_CONTAINER"
KAFKA_CTRS=$(echo "$KAFKA_CTRS" | xargs); [ -n "${KAFKA_CONTAINER:-}" ] && KAFKA_CTRS="$KAFKA_CONTAINER"
BLASTER_CTRS=$(echo "$BLASTER_CTRS" | xargs)

FIRST_TN=$(echo "$TERANODE_CTRS" | awk '{print $1}')
FIRST_AERO=$(echo "$AERO_CTRS" | awk '{print $1}')
FIRST_PG=$(echo "$PG_CTRS" | awk '{print $1}')
FIRST_KAFKA=$(echo "$KAFKA_CTRS" | awk '{print $1}')

# Settings context + UTXO backend, from a teranode container.
SETTINGS_CTX=""; UTXO_BACKEND="unknown"
if [ -n "$FIRST_TN" ]; then
    SETTINGS_CTX=$(docker exec "$FIRST_TN" sh -c 'printf "%s" "$SETTINGS_CONTEXT"' 2>/dev/null)
    _scheme=$(docker exec "$FIRST_TN" sh -c 'printf "%s\n" "$utxostore"; grep -ihoE "utxostore[^=]*=[a-z]+://" /app/settings*.conf 2>/dev/null' 2>/dev/null | grep -oE '[a-z]+://' | head -1)
    case "$_scheme" in
        aerospike://) UTXO_BACKEND="aerospike" ;;
        postgres://)  UTXO_BACKEND="postgres" ;;
        sqlite://)    UTXO_BACKEND="sqlite" ;;
        memory://)    UTXO_BACKEND="memory" ;;
    esac
fi
[ "$UTXO_BACKEND" = "unknown" ] && [ -n "$FIRST_AERO" ] && UTXO_BACKEND="aerospike (assumed — aerospike container present)"
[ "$UTXO_BACKEND" = "unknown" ] && [ -z "$FIRST_AERO" ] && [ -n "$FIRST_PG" ] && UTXO_BACKEND="postgres (assumed — no aerospike, postgres present)"

# Aerospike container + DB namespace.
AERO_CONTAINER="${AEROSPIKE_CONTAINER:-$FIRST_AERO}"
AERO_DB_NS="${AEROSPIKE_DB_NS:-}"
if [ -z "$AERO_DB_NS" ] && [ -n "$AERO_CONTAINER" ]; then
    _ap=$(aero_port "$AERO_CONTAINER"); _ap=${_ap:-3000}
    AERO_DB_NS=$(docker exec "$AERO_CONTAINER" asinfo -p "$_ap" -v 'namespaces' 2>/dev/null | tr ';' '\n' | grep -v '^$' | head -1 | tr -d '\r')
fi
[ -z "$AERO_DB_NS" ] && AERO_DB_NS="utxo-store"

# ── Helpers ──────────────────────────────────────────────────────────────────
# Pull an HTTP endpoint from a container: prefer a host-published port, else exec inside.
fetch_http() {
    local ctr="$1" port="$2" path="$3" hostport
    hostport=$(docker port "$ctr" "$port/tcp" 2>/dev/null | head -1 | sed 's/.*://')
    if [ -n "$hostport" ] && command -v curl >/dev/null 2>&1; then
        curl -s --max-time 15 "http://localhost:${hostport}${path}" 2>/dev/null
    else
        docker exec "$ctr" sh -c "curl -s http://localhost:${port}${path} 2>/dev/null || wget -qO- http://localhost:${port}${path} 2>/dev/null" 2>/dev/null
    fi
}

# Parse one goroutine profile (debug=1) into COUNT<TAB>FUNC<TAB>FILE lines for aggregation.
parse_goroutines_raw() {
    python3 -c "
import sys, re
lines = sys.stdin.read().strip().split('\n')
if not lines or 'goroutine profile' not in lines[0]:
    sys.exit(0)
m = re.search(r'total (\d+)', lines[0]); print(f'TOTAL\t{int(m.group(1)) if m else 0}')
i = 1
while i < len(lines):
    mm = re.match(r'^(\d+)\s+@\s+', lines[i])
    if mm:
        count = int(mm.group(1)); func_name='(unknown)'; file_loc=''
        j = i + 1
        while j < len(lines) and lines[j].startswith('#'):
            fm = re.match(r'^#\s+\S+\s+(\S+)\s+(\S+)', lines[j])
            if fm:
                fn, loc = fm.group(1), fm.group(2)
                skip = ('runtime/','internal/','sync.','sync/','bufio/','io.','net.','net/','golang.org/x/net/','context.')
                if any(fn.startswith(s) for s in skip): j += 1; continue
                short = fn
                for p in ['github.com/bsv-blockchain/teranode/','github.com/bitcoin-sv/teranode-coinbase/','github.com/IBM/']:
                    short = short.replace(p, '')
                func_name = short; file_loc = loc.split('/')[-1] if '/' in loc else loc
                break
            j += 1
        print(f'{count}\t{func_name}\t{file_loc}')
    i += 1
"
}

# Aggregate raw goroutine files into a summary (avg/container, totals, blocked-on categories).
aggregate_goroutines() {
    python3 -c "
import sys, os
from collections import defaultdict
files = sys.argv[1:]; totals=[]; fc=defaultdict(lambda:[0,'',''])
valid=0
for f in files:
    if not os.path.exists(f) or os.path.getsize(f)==0: continue
    valid+=1
    for line in open(f):
        line=line.strip()
        if not line: continue
        p=line.split('\t')
        if p[0]=='TOTAL': totals.append(int(p[1])); continue
        if len(p)>=2:
            fc[p[1]][0]+=int(p[0]); fc[p[1]][1]=p[1]; fc[p[1]][2]=p[2] if len(p)>2 else ''
if not valid: print('  (no goroutine data)'); sys.exit(0)
res=sorted(fc.values(), key=lambda x:-x[0]); fleet=sum(totals)
aero=sum(c for c,f,_ in res if 'aerospike' in f.lower())
ba=sum(c for c,f,_ in res if 'blockassembly' in f.lower())
kafka=sum(c for c,f,_ in res if 'sarama' in f.lower() or 'kafka' in f.lower())
print(f'  Sampled {valid} container(s)  |  Avg goroutines: {sum(totals)/len(totals) if totals else 0:,.0f}')
print(f'  Blocked on: Aerospike={aero}  BlockAssembly={ba}  Kafka={kafka}')
print(); print(f'  {\"Avg\":>8} {\"Total\":>8} {\"%\":>6}  Function')
for c,fn,loc in res[:15]:
    pct=c/fleet*100 if fleet else 0
    print(f'  {c/valid:>8.0f} {c:>8} {pct:>5.1f}%  {fn[:60]}{\"  (\"+loc+\")\" if loc else \"\"}')
" "$@"
}

# Parse Aerospike latency histograms.
parse_latencies() {
    python3 -c "
import sys
raw=sys.stdin.read().strip()
if not raw: print('  (no data)'); sys.exit(0)
print(f'  {\"Operation\":<28} {\"rate/s\":>9} {\">1ms%\":>8} {\">2ms%\":>8} {\">8ms%\":>8}')
for e in raw.split(';'):
    e=e.strip()
    if not e or ':' not in e: continue
    op=e.split(':')[0]; vals=':'.join(e.split(':')[1:]).split(',')
    if len(vals)<2: continue
    try: rate=float(vals[1])
    except: continue
    if rate<0.1: continue
    def g(i):
        try: return float(vals[i])
        except: return 0.0
    print(f'  {op:<28} {rate:>9.1f} {g(2):>7.2f}% {g(3):>7.2f}% {g(5):>7.2f}%')
"
}

TMPDIR=$(mktemp -d); trap 'rm -rf "$TMPDIR"' EXIT

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1: STACK OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 1: STACK OVERVIEW"
echo "  Timestamp: $TIMESTAMP"
echo "  Compose project: $COMPOSE_PROJECT"
echo "  Settings context: ${SETTINGS_CTX:-(unknown)}"
echo "  UTXO store backend: $UTXO_BACKEND"
[ -n "$AERO_CONTAINER" ] && echo "  Aerospike: container=$AERO_CONTAINER  db-namespace=$AERO_DB_NS"
echo "  pprof port: $PPROF_PORT (host-published per container if mapped, else via docker exec)"
echo ""
echo "  Containers:"
printf "  %-34s %-26s %-22s %s\n" "NAME" "SERVICE" "STATUS" "PORTS"
for row in "${ALL_ROWS[@]}"; do
    name=$(echo "$row" | cut -f1); svc=$(echo "$row" | cut -f4); status=$(echo "$row" | cut -f3)
    ports=$(docker port "$name" 2>/dev/null | tr '\n' ',' | sed 's/,$//')
    printf "  %-34s %-26s %-22s %s\n" "$name" "${svc:--}" "${status:0:21}" "${ports:0:40}"
done
echo ""
echo "  Roles: teranode=[$TERANODE_CTRS]"
echo "         aerospike=[${AERO_CTRS:--}]  postgres=[${PG_CTRS:--}]  kafka=[${KAFKA_CTRS:--}]"
[ -n "$BLASTER_CTRS" ] && echo "         blaster/coinbase=[$BLASTER_CTRS]  (load tooling)"
echo ""
# FSM state: try the asset service first, else any teranode container serving 8090.
echo "  FSM state:"
_fsm_any=0
for ctr in $TERANODE_CTRS; do
    state=$(fetch_http "$ctr" 8090 "/api/v1/fsm/state" | grep -oE 'IDLE|RUNNING|CATCHINGBLOCKS|LEGACYSYNCING' | head -1)
    if [ -n "$state" ]; then printf "    %-34s %s\n" "$ctr" "$state"; _fsm_any=1; fi
done
[ "$_fsm_any" -eq 0 ] && echo "    (unknown — no container answered the asset FSM endpoint on :8090)"

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2: RESOURCE USAGE (docker stats)
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 2: RESOURCE USAGE"
STATS_TARGETS=$(echo "$TERANODE_CTRS $AERO_CTRS $PG_CTRS $KAFKA_CTRS $BLASTER_CTRS" | xargs)
if [ -n "$STATS_TARGETS" ]; then
    # shellcheck disable=SC2086
    docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}' $STATS_TARGETS 2>/dev/null \
        | sed 's/^/  /'
else
    echo "  (no containers to sample)"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3: AEROSPIKE HEALTH
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 3: AEROSPIKE HEALTH"
if [ -z "$AERO_CTRS" ]; then
    echo "  (no Aerospike container — UTXO backend is '$UTXO_BACKEND'. Skipping.)"
else
    for ctr in $AERO_CTRS; do
        AP=$(aero_port "$ctr"); AP=${AP:-3000}
        subheader "Aerospike: $ctr (port $AP, db namespace: $AERO_DB_NS)"
        STATS=$(docker exec "$ctr" asinfo -p "$AP" -v statistics 2>/dev/null | tr ';' '\n')
        NSS=$(docker exec "$ctr" asinfo -p "$AP" -v "namespace/$AERO_DB_NS" 2>/dev/null | tr ';' '\n')
        printf "    rw_in_progress=%s  client_connections=%s  objects=%s  cache_read_pct=%s\n" \
            "$(echo "$STATS" | grep '^rw_in_progress=' | cut -d= -f2)" \
            "$(echo "$STATS" | grep '^client_connections=' | cut -d= -f2)" \
            "$(echo "$NSS" | grep '^objects=' | cut -d= -f2)" \
            "$(echo "$NSS" | grep '^cache_read_pct=' | cut -d= -f2)"
        echo "    Latencies:"
        docker exec "$ctr" asinfo -p "$AP" -v 'latencies:' 2>/dev/null | parse_latencies
    done
fi

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4: POSTGRESQL HEALTH
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 4: POSTGRESQL HEALTH"
if [ -z "$PG_CTRS" ]; then
    echo "  (no PostgreSQL container detected — skipping)"
else
    for ctr in $PG_CTRS; do
        subheader "PostgreSQL: $ctr"
        echo "  Connections by state:"
        docker exec "$ctr" sh -c "psql -tAX -U postgres -c \"select state, count(*) from pg_stat_activity group by 1 order by 2 desc\"" 2>/dev/null \
            | awk -F'|' 'NF>=2{printf "    %-22s %s\n", $1, $2}'
        echo "  Longest active queries (top 3):"
        docker exec "$ctr" sh -c "psql -tAX -U postgres -c \"select round(extract(epoch from now()-query_start)) secs, left(query,60) q from pg_stat_activity where state='active' and pid<>pg_backend_pid() order by 1 desc limit 3\"" 2>/dev/null \
            | awk -F'|' 'NF>=2{printf "    %6ss  %s\n", $1, $2}'
    done
fi

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5: KAFKA / REDPANDA HEALTH
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 5: KAFKA / REDPANDA HEALTH"
if [ -z "$KAFKA_CTRS" ]; then
    echo "  (no Kafka/Redpanda container detected — skipping)"
else
    for ctr in $KAFKA_CTRS; do
        subheader "Kafka/Redpanda: $ctr"
        if docker exec "$ctr" sh -c 'command -v rpk' >/dev/null 2>&1; then
            echo "  Consumer-group lag (lag = how far each consumer is behind, highest first):"
            # Same in-container single-exec approach as the k8s skill: list + describe
            # in one shell, emit group<TAB>lag. (list cols BROKER GROUP STATE → name=$2;
            # describe has a TOTAL-LAG line.)
            LAGS=$(docker exec "$ctr" sh -c 'rpk group list 2>/dev/null | awk "NR>1 && NF>=2{print \$2}" | while read g; do lag=$(rpk group describe "$g" 2>/dev/null | awk "/^TOTAL-LAG/{print \$2; exit}"); printf "%s\t%s\n" "$g" "${lag:-?}"; done' 2>/dev/null)
            if [ -z "$LAGS" ]; then
                echo "    (no consumer groups)"
            else
                echo "$LAGS" | sort -t"$(printf '\t')" -k2,2 -nr | awk -F'\t' 'NF>=2{printf "    %-54s total-lag=%s\n", $1, $2}'
            fi
        else
            echo "  (rpk not found in container — use a Kafka admin tool for consumer-group lag)"
        fi
    done
fi

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6: GOROUTINE PROFILES
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 6: GOROUTINE PROFILES (teranode services)"
echo "  The busiest pipeline stage here is usually the local bottleneck."
echo "  See the debug-teranode skill (debugging-playbook.md §3) to read these."
for ctr in $TERANODE_CTRS; do
    fetch_http "$ctr" "$PPROF_PORT" "/debug/pprof/goroutine?debug=1" | parse_goroutines_raw > "$TMPDIR/gr_${ctr}.txt" 2>/dev/null
done
for ctr in $TERANODE_CTRS; do
    f="$TMPDIR/gr_${ctr}.txt"
    if [ -s "$f" ]; then
        subheader "$ctr"
        aggregate_goroutines "$f"
    fi
done
# Blaster profiles (if present): port varies — try named 9092 then 7092.
if [ -n "$BLASTER_CTRS" ] && [ "$QUICK" -eq 0 ]; then
    for ctr in $BLASTER_CTRS; do
        out=""
        for p in 9092 7092 6060; do
            out=$(fetch_http "$ctr" "$p" "/debug/pprof/goroutine?debug=1" | parse_goroutines_raw)
            [ -n "$out" ] && break
        done
        if [ -n "$out" ]; then subheader "blaster: $ctr"; echo "$out" > "$TMPDIR/bl_${ctr}.txt"; aggregate_goroutines "$TMPDIR/bl_${ctr}.txt"; fi
    done
fi

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7: LOG ERROR SCAN
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 7: LOG ERROR SCAN (last $SINCE)"
if [ "$QUICK" -eq 1 ]; then
    echo "  (skipped in --quick mode)"
else
    for ctr in $TERANODE_CTRS; do
        n=$(docker logs --since "$SINCE" "$ctr" 2>&1 | grep -cE '\| ERROR \||"level":"error"|level=error')
        printf "  %-34s %s ERROR lines\n" "$ctr" "${n:-0}"
        if [ "${n:-0}" -gt 0 ]; then
            docker logs --since "$SINCE" "$ctr" 2>&1 | grep -E '\| ERROR \||"level":"error"|level=error' | tail -3 | sed 's/^/      /'
        fi
    done
fi

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8: SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
header "SECTION 8: SUMMARY"
echo "  Project=$COMPOSE_PROJECT  context=${SETTINGS_CTX:-?}  utxo-backend=$UTXO_BACKEND"
echo "  teranode containers: $(echo "$TERANODE_CTRS" | wc -w | tr -d ' ')   aerospike: $(echo "$AERO_CTRS" | wc -w | tr -d ' ')   postgres: $(echo "$PG_CTRS" | wc -w | tr -d ' ')   kafka: $(echo "$KAFKA_CTRS" | wc -w | tr -d ' ')"
echo ""
echo "  Directional next steps (NOT pass/fail thresholds — deployments vary):"
echo "   • Inert node? Check the FSM state in Section 1 (IDLE / CATCHINGBLOCKS) first."
echo "   • Throughput problem? Find the busiest stage in Section 6 and follow the"
echo "     debug-teranode playbook's bottleneck method."
echo "   • A stalled service often means unhealthy Kafka (Section 5 lag) — teranode pauses"
echo "     on unhealthy Kafka by design."
echo "   • Errors in Section 7 point at the failing service; cross-reference architecture.md."
echo ""
echo "  Diagnostic complete at $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
