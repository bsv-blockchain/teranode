package aerospike_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	astore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	apruner "github.com/bsv-blockchain/teranode/stores/utxo/aerospike/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// TestPrunerThroughput measures one Aerospike prune pass against a real
// container, so the same file can be run on upstream main and on a branch and
// the numbers compared. It drives only the store and pruner API both share.
//
// It is skipped unless TERANODE_PRUNER_BENCH=1. TERANODE_PRUNER_BENCH_CHILDREN
// sets the prunable children per workload (default 5000) and
// TERANODE_PRUNER_BENCH_RUNS the passes per workload (default 3).
// TERANODE_PRUNER_BENCH_CHUNK overrides pruner_utxoChunkSize, so a pass can be
// given many chunks per partition worker without seeding millions of records.
// TERANODE_PRUNER_BENCH_DELAY (for example "1ms") adds that one-way delay to
// the Aerospike container's network after seeding, through a sidecar running
// tc netem in the container's network namespace; it needs the docker CLI and
// the alpine image.
//
// Each workload seeds chains of parent, child and grandchild, mined, so that
// only the children are fully spent and due for deletion. The parents keep an
// unspent output and so survive, which is what makes the pruner write a replay
// marker on them. Per pass it reports wall time, children deleted per second,
// and the server's own batch counters, which count every branch's calls the
// same way.
//
// Without a delay the container's round trip is well under a millisecond,
// which hides exactly the cost of extra serial batches, so compare wall times
// with a delay set. The batch counts compare the same either way.
func TestPrunerThroughput(t *testing.T) {
	if os.Getenv("TERANODE_PRUNER_BENCH") != "1" {
		t.Skip("set TERANODE_PRUNER_BENCH=1 to run the pruner throughput benchmark")
	}

	children := envInt(t, "TERANODE_PRUNER_BENCH_CHILDREN", 5000)
	runs := envInt(t, "TERANODE_PRUNER_BENCH_RUNS", 3)

	for _, w := range []pruneWorkload{
		// Every child spends one output of its own parent.
		{name: "one_parent", parentsPerChild: 1, inputsPerParent: 1},
		// Every child spends one output of each of four parents.
		{name: "four_parents", parentsPerChild: 4, inputsPerParent: 1},
		// Every child spends eight outputs of one parent, a consolidation.
		{name: "consolidation", parentsPerChild: 1, inputsPerParent: 8},
	} {
		t.Run(w.name, func(t *testing.T) {
			results := make([]pruneResult, 0, runs)

			for run := 0; run < runs; run++ {
				results = append(results, runPruneWorkload(t, w, children))
			}

			reportPruneResults(t, w, results)
		})
	}
}

type pruneWorkload struct {
	name            string
	parentsPerChild int
	inputsPerParent int
}

type pruneResult struct {
	deleted  int64
	elapsed  time.Duration
	counters map[string]int64
}

// pruneCounters are the server statistics read around a pass: batch requests
// from the node statistics, batch sub-transactions from the namespace.
var pruneCounters = []string{
	"batch_index_initiate",
	"batch_sub_read_success",
	"batch_sub_write_success",
	"batch_sub_delete_success",
	"batch_sub_udf_complete",
	"batch_sub_read_not_found",
	"batch_sub_write_filtered_out",
}

func runPruneWorkload(t *testing.T, w pruneWorkload, children int) pruneResult {
	t.Helper()

	logger := ulogger.New("pruner-throughput")
	s := test.CreateBaseTestSettings(t)
	s.UtxoStore.DisableDAHCleaner = false

	if chunk := envInt(t, "TERANODE_PRUNER_BENCH_CHUNK", 0); chunk > 0 {
		s.Pruner.UTXOChunkSize = chunk
	}

	client, store, ctx, cleanup := initAerospike(t, s, logger)
	defer cleanup()

	require.NoError(t, store.SetBlockHeight(1000))

	seedPruneWorkload(t, ctx, store, w, children)

	astore.ResetPrunerServiceForTests()
	defer astore.ResetPrunerServiceForTests()

	require.NoError(t, store.CreateIndexIfNotExists(ctx, apruner.IndexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC))
	require.NoError(t, store.WaitForIndexReady(ctx, apruner.IndexName))

	svc, err := store.GetPrunerService()
	require.NoError(t, err)

	pruner := svc.(*apruner.Service)
	pruner.Start(ctx)

	// Prune refuses until Start has seen the DAH index ready; height 1 has
	// nothing due, so this only waits for readiness.
	require.Eventually(t, func() bool {
		_, err := pruner.Prune(ctx, 1, "pruner-throughput-ready")
		return err == nil
	}, 30*time.Second, 50*time.Millisecond)

	if delay := os.Getenv("TERANODE_PRUNER_BENCH_DELAY"); delay != "" {
		addNetworkDelay(t, client, delay)
	}

	before := readPruneCounters(t, client, store.GetNamespace())
	start := time.Now()

	deleted, err := pruner.Prune(ctx, 1300, "pruner-throughput")
	require.NoError(t, err)

	elapsed := time.Since(start)
	after := readPruneCounters(t, client, store.GetNamespace())

	require.Equal(t, int64(children), deleted, "every seeded child must be pruned and nothing else")

	delta := make(map[string]int64, len(pruneCounters))
	for _, name := range pruneCounters {
		delta[name] = after[name] - before[name]
	}

	return pruneResult{deleted: deleted, elapsed: elapsed, counters: delta}
}

// seedPruneWorkload writes `children` chains. For each child it creates its
// parents with one output more than the child spends, so every parent keeps
// an unspent output and survives; the child has one output, spent by a
// grandchild that also survives. Parents and children are mined at 1000, the
// grandchildren at 1001, so only the children become due.
func seedPruneWorkload(t *testing.T, ctx context.Context, store *astore.Store, w pruneWorkload, children int) {
	t.Helper()

	const workers = 32

	var (
		mu       sync.Mutex
		mined    []*chainhash.Hash
		minedTop []*chainhash.Hash
	)

	jobs := make(chan int)
	errs := make(chan error, workers)

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for n := range jobs {
				hashes, grandchild, err := seedChain(ctx, store, w, n)
				if err != nil {
					errs <- err
					return
				}

				mu.Lock()
				mined = append(mined, hashes...)
				minedTop = append(minedTop, grandchild)
				mu.Unlock()
			}
		}()
	}

	for n := 0; n < children; n++ {
		select {
		case jobs <- n:
		case err := <-errs:
			close(jobs)
			wg.Wait()
			require.NoError(t, err)
		}
	}

	close(jobs)
	wg.Wait()

	select {
	case err := <-errs:
		require.NoError(t, err)
	default:
	}

	setMinedInBatches(t, ctx, store, mined, 1000)
	setMinedInBatches(t, ctx, store, minedTop, 1001)
}

func seedChain(ctx context.Context, store *astore.Store, w pruneWorkload, n int) ([]*chainhash.Hash, *chainhash.Hash, error) {
	child := bt.NewTx()
	hashes := make([]*chainhash.Hash, 0, w.parentsPerChild+1)

	for p := 0; p < w.parentsPerChild; p++ {
		parent := bt.NewTx()

		seed := chainhash.HashH([]byte(fmt.Sprintf("pruner-throughput-%d-%d", n, p)))
		if err := parent.From(seed.String(), 0, "51", 1_000_000); err != nil {
			return nil, nil, err
		}

		for o := 0; o <= w.inputsPerParent; o++ {
			if err := parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000); err != nil {
				return nil, nil, err
			}
		}

		if _, err := store.Create(ctx, parent, 1000); err != nil {
			return nil, nil, err
		}

		hashes = append(hashes, parent.TxIDChainHash())

		for o := 0; o < w.inputsPerParent; o++ {
			if err := child.From(parent.TxID(), uint32(o), parent.Outputs[o].LockingScript.String(), parent.Outputs[o].Satoshis); err != nil {
				return nil, nil, err
			}
		}
	}

	if err := child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000); err != nil {
		return nil, nil, err
	}

	if _, _, err := store.SpendAndCreate(ctx, child, 1000); err != nil {
		return nil, nil, err
	}

	hashes = append(hashes, child.TxIDChainHash())

	grandchild := bt.NewTx()
	if err := grandchild.From(child.TxID(), 0, child.Outputs[0].LockingScript.String(), child.Outputs[0].Satoshis); err != nil {
		return nil, nil, err
	}

	if err := grandchild.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2000); err != nil {
		return nil, nil, err
	}

	if _, _, err := store.SpendAndCreate(ctx, grandchild, 1001); err != nil {
		return nil, nil, err
	}

	return hashes, grandchild.TxIDChainHash(), nil
}

func setMinedInBatches(t *testing.T, ctx context.Context, store *astore.Store, hashes []*chainhash.Hash, height uint32) {
	t.Helper()

	const batch = 500

	for i := 0; i < len(hashes); i += batch {
		end := min(i+batch, len(hashes))

		_, err := store.SetMinedMulti(ctx, hashes[i:end], utxo.MinedBlockInfo{BlockID: height, BlockHeight: height, OnLongestChain: true})
		require.NoError(t, err)
	}
}

// readPruneCounters sums the named statistics over every node, from the node
// statistics and the namespace statistics.
func readPruneCounters(t *testing.T, client *uaerospike.Client, namespace string) map[string]int64 {
	t.Helper()

	totals := make(map[string]int64, len(pruneCounters))
	policy := aerospike.NewInfoPolicy()

	for _, node := range client.GetNodes() {
		info, err := node.RequestInfo(policy, "statistics", "namespace/"+namespace)
		require.NoError(t, err)

		for _, section := range info {
			for _, pair := range strings.Split(section, ";") {
				name, value, ok := strings.Cut(pair, "=")
				if !ok {
					continue
				}

				for _, want := range pruneCounters {
					if name == want {
						if v, err := strconv.ParseInt(value, 10, 64); err == nil {
							totals[name] += v
						}
					}
				}
			}
		}
	}

	return totals
}

func reportPruneResults(t *testing.T, w pruneWorkload, results []pruneResult) {
	t.Helper()

	elapsed := make([]time.Duration, 0, len(results))
	for _, r := range results {
		elapsed = append(elapsed, r.elapsed)
	}

	sort.Slice(elapsed, func(i, j int) bool { return elapsed[i] < elapsed[j] })

	median := elapsed[len(elapsed)/2]
	deleted := results[0].deleted

	var b strings.Builder

	fmt.Fprintf(&b, "PRUNEBENCH delay=%s chunk=%s workload=%s children=%d parents_per_child=%d inputs_per_parent=%d runs=%d", os.Getenv("TERANODE_PRUNER_BENCH_DELAY"), os.Getenv("TERANODE_PRUNER_BENCH_CHUNK"), w.name, deleted, w.parentsPerChild, w.inputsPerParent, len(results))
	fmt.Fprintf(&b, " median=%s min=%s max=%s children_per_s=%.0f", median.Round(time.Millisecond), elapsed[0].Round(time.Millisecond), elapsed[len(elapsed)-1].Round(time.Millisecond), float64(deleted)/median.Seconds())

	for _, name := range pruneCounters {
		v := results[0].counters[name]
		fmt.Fprintf(&b, " %s=%d", name, v)
	}

	fmt.Fprintf(&b, " batches_per_1000_children=%.1f", 1000*float64(results[0].counters["batch_index_initiate"])/float64(deleted))

	t.Log(b.String())
}

// addNetworkDelay finds the Aerospike container by its published port and adds
// a netem delay to its interface from a sidecar sharing its network namespace.
func addNetworkDelay(t *testing.T, client *uaerospike.Client, delay string) {
	t.Helper()

	nodes := client.GetNodes()
	require.NotEmpty(t, nodes)

	port := nodes[0].GetHost().Port

	out, err := exec.Command("docker", "ps", "-q", "--filter", fmt.Sprintf("publish=%d", port)).Output()
	require.NoError(t, err)

	id := strings.TrimSpace(string(out))
	require.NotEmpty(t, id, "no container publishes port %d", port)

	cmd := exec.Command("docker", "run", "--rm", "--net", "container:"+id, "--cap-add", "NET_ADMIN", "alpine:latest",
		"sh", "-c", "apk add -q iproute2 >/dev/null 2>&1 && tc qdisc add dev eth0 root netem delay "+delay)

	combined, err := cmd.CombinedOutput()
	require.NoError(t, err, string(combined))
}

func envInt(t *testing.T, name string, fallback int) int {
	t.Helper()

	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}

	v, err := strconv.Atoi(raw)
	require.NoError(t, err, name)

	return v
}
