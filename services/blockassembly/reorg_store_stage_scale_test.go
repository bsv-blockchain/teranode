//go:build reorgscale

package blockassembly

// REORG_SCALE_CASE=store of the reorgscale suite (see subtreeprocessor/reorg_scale_test.go): the store
// step of a forward block, N subtrees of 1M leaves through the old worker path and through the staged
// path at several writer widths. It lives here because the store workers are in this package. Run it
// with
//
//	REORG_SCALE_CASE=store REORG_SCALE_BLOB=file:///data/scale make reorg-scale
//
// Settings, all optional:
//
//	REORG_SCALE_BLOB            file:// path the subtree files are written under (default: TMPDIR); one
//	                            directory per run, removed after it
//	REORG_SCALE_STORE_SUBTREES  subtrees to store (default 16; a 500M pending set is about 500)
//	REORG_SCALE_SUBTREE         leaves per subtree (default 1M)
//	REORG_SCALE_STORE_WORKERS   workers of the old path, comma separated (default 4,16,64)
//	REORG_SCALE_STORE_WRITERS   writer widths of the staged path, comma separated (default 8,16,64,128)
//	REORG_SCALE_OUT             path of the JSON report (default: logged only)

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type storeScaleRow struct {
	Path           string  `json:"path"`
	Workers        int     `json:"workers"`
	Writers        int     `json:"writers,omitempty"`
	Seconds        float64 `json:"seconds"`
	SubtreesPerSec float64 `json:"subtrees_per_second"`
}

type storeScaleReport struct {
	Subtrees    int             `json:"subtrees"`
	SubtreeSize int             `json:"subtree_size"`
	GOMAXPROCS  int             `json:"gomaxprocs"`
	Rows        []storeScaleRow `json:"rows"`
}

func scaleIntList(t *testing.T, key string, def []int) []int {
	t.Helper()

	v := os.Getenv(key)
	if v == "" {
		return def
	}

	var out []int

	for _, field := range strings.Split(v, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(field))
		require.NoError(t, err, key)

		out = append(out, n)
	}

	return out
}

func TestReorgScale(t *testing.T) {
	if os.Getenv("REORG_SCALE_CASE") != "store" {
		t.Skip("the other cases are in subtreeprocessor")
	}

	subtrees := scaleIntList(t, "REORG_SCALE_STORE_SUBTREES", []int{16})[0]
	leaves := scaleIntList(t, "REORG_SCALE_SUBTREE", []int{1 << 20})[0]

	base := strings.TrimPrefix(os.Getenv("REORG_SCALE_BLOB"), "file://")
	if base == "" {
		base = t.TempDir()
	}

	fx := newStoreStageFixture(t, subtrees, leaves, t.TempDir())
	report := storeScaleReport{Subtrees: subtrees, SubtreeSize: leaves, GOMAXPROCS: runtime.GOMAXPROCS(0)}

	record := func(name string, path storeStagePath) {
		dir := filepath.Join(base, name)
		require.NoError(t, os.MkdirAll(dir, 0o755))

		seconds := fx.runner(t, path)(dir).Seconds()
		require.NoError(t, os.RemoveAll(dir))

		report.Rows = append(report.Rows, storeScaleRow{Path: name, Workers: path.workers, Writers: path.writers, Seconds: seconds, SubtreesPerSec: float64(subtrees) / seconds})
		t.Logf("%s workers=%d writers=%d: %.2fs, %.2f subtrees/s", name, path.workers, path.writers, seconds, float64(subtrees)/seconds)
	}

	for _, workers := range scaleIntList(t, "REORG_SCALE_STORE_WORKERS", []int{4, 16, 64}) {
		record("old", storeStagePath{workers: workers})
	}

	for _, writers := range scaleIntList(t, "REORG_SCALE_STORE_WRITERS", []int{8, 16, 64, 128}) {
		record("staged", storeStagePath{staged: true, workers: 4, writers: writers})
	}

	out, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)
	t.Logf("reorgscale store report:\n%s", out)

	if path := os.Getenv("REORG_SCALE_OUT"); path != "" {
		require.NoError(t, os.WriteFile(path, out, 0o644))
	}
}
