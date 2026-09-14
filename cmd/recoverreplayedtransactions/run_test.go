package recoverreplayedtransactions

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/replayrecovery"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestRunRejectsUnsafeOptionsBeforeConnecting(t *testing.T) {
	for _, resume := range []bool{false, true} {
		err := Run(context.Background(), ulogger.TestLogger{}, &settings.Settings{}, Options{WorkDir: t.TempDir(), Apply: true, Resume: resume, Timeout: time.Minute, Concurrency: 1}, io.Discard, io.Discard)
		require.ErrorContains(t, err, "maintenance")
	}
}
func TestExitCodesRemainDistinctWhenWrapped(t *testing.T) {
	require.Equal(t, 0, ExitCode(nil))
	require.Equal(t, 1, ExitCode(commandError("ordinary failure")))
	require.Equal(t, 2, ExitCode(commandError("audit: %w", replayrecovery.ErrIncomplete)))
	require.Equal(t, 2, ExitCode(commandError("audit: %w", ErrFindings)))
}
func TestOptionsRequireExplicitApplyAndResume(t *testing.T) {
	base := Options{WorkDir: t.TempDir(), Timeout: time.Minute, Concurrency: 1}
	require.NoError(t, base.validate())
	base.Apply = true
	require.Error(t, base.validate())
	base.Maintenance = true
	require.NoError(t, base.validate())
	base.Apply = false
	base.Resume = true
	require.Error(t, base.validate())
	base.Resume = false
	base.WorkDir = ""
	require.Error(t, base.validate())
}

func TestVerificationFailurePreservesCompletedMutationReport(t *testing.T) {
	applied := replayrecovery.Summary{Applied: true, RestartRequired: true, Repaired: 2, Stage: "applied-verification-pending"}
	failed := replayrecovery.Summary{}
	report := retainMutationStatus(failed, applied)
	require.True(t, report.Applied)
	require.True(t, report.RestartRequired)
	require.EqualValues(t, 2, report.Repaired)
	require.False(t, report.Complete)
}

func TestAuditResultReturnsNonzeroForRepairableRecords(t *testing.T) {
	err := auditResult(replayrecovery.Summary{Complete: true, FullySpent: 2}, nil)
	require.ErrorContains(t, err, "repairable records")
	require.Equal(t, 2, ExitCode(err))
	require.NoError(t, auditResult(replayrecovery.Summary{Complete: true}, nil))
	require.ErrorIs(t, auditResult(replayrecovery.Summary{FullySpent: 2}, replayrecovery.ErrIncomplete), replayrecovery.ErrIncomplete)
	failure := commandError("scan failed")
	require.ErrorIs(t, auditResult(replayrecovery.Summary{FullySpent: 2}, failure), failure)
}

func TestRunRejectsZeroRetentionBeforeConnecting(t *testing.T) {
	s := &settings.Settings{}
	s.UtxoStore.UtxoStore = &url.URL{Scheme: "aerospike", Host: "127.0.0.1:1", Path: "/test"}
	err := Run(t.Context(), ulogger.TestLogger{}, s, Options{WorkDir: t.TempDir(), Timeout: time.Second, Concurrency: 1}, io.Discard, io.Discard)
	require.ErrorContains(t, err, "block height retention must be positive")
}

type cancelInventoryBackend struct {
	replayrecovery.Backend
	cancel context.CancelFunc
}

func (b cancelInventoryBackend) Identity() string { return "cancel-inventory" }

func (b cancelInventoryBackend) Inventory(ctx context.Context, _ func(replayrecovery.InventoryRecord) error) error {
	b.cancel()
	return ctx.Err()
}

func (b cancelInventoryBackend) SpendReferences(ctx context.Context, _ func(replayrecovery.SpendReference) error) error {
	return ctx.Err()
}

func TestRunJobCancellationWritesIncompleteReport(t *testing.T) {
	u, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	s := test.CreateBaseTestSettings(t)
	chain, err := blockchainsql.New(ulogger.TestLogger{}, u, s)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, chain.Close()) })
	require.NoError(t, chain.SetState(t.Context(), "fsm_state", []byte("IDLE")))
	guard, err := newIdleGuard(t.Context(), chain)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	backend := cancelInventoryBackend{cancel: cancel}
	opts := Options{WorkDir: t.TempDir(), Timeout: time.Minute, Concurrency: 1}
	require.NoError(t, os.Chmod(opts.WorkDir, 0700))
	var out bytes.Buffer
	err = runJob(ctx, backend, nil, chain, memory.New(), guard, opts, s.ChainCfgParams.GenesisActivationHeight, &out, io.Discard)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, ExitCode(err))
	var report replayrecovery.Summary
	require.NoError(t, json.Unmarshal(out.Bytes(), &report))
	require.False(t, report.Complete)
	require.False(t, report.Applied)
	require.Equal(t, "inventory", report.Stage)
	durable, err := os.ReadFile(filepath.Join(opts.WorkDir, "report.json"))
	require.NoError(t, err)
	require.JSONEq(t, out.String(), string(durable))
}

// Count actual canonical block lookups while keeping all reads on sqlitememory.
type historyReadCounter struct {
	HistoryChain
	blocks int
}

func (c *historyReadCounter) GetBlockInChainByHeightHash(ctx context.Context, height uint32, tip *chainhash.Hash) (*model.Block, bool, error) {
	c.blocks++
	return c.HistoryChain.GetBlockInChainByHeightHash(ctx, height, tip)
}

func TestBuildJobHistoryResume(t *testing.T) {
	for _, mode := range []string{"sealed", "missing-target", "unsealed", "changed-consensus"} {
		t.Run(mode, func(t *testing.T) {
			u, err := url.Parse("sqlitememory:///")
			require.NoError(t, err)
			s := test.CreateBaseTestSettings(t)
			chain, err := blockchainsql.New(ulogger.TestLogger{}, u, s)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, chain.Close()) })
			require.NoError(t, chain.SetState(t.Context(), "fsm_state", []byte("IDLE")))
			guard, err := newIdleGuard(t.Context(), chain)
			require.NoError(t, err)
			counted := &historyReadCounter{HistoryChain: chain}
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			manifest := filepath.Join(dir, "manifest.sqlite")
			census, err := sql.Open("sqlite", manifest)
			require.NoError(t, err)
			defer census.Close()
			_, err = census.Exec("CREATE TABLE targets(id TEXT PRIMARY KEY)")
			require.NoError(t, err)
			path := filepath.Join(dir, "history.sqlite")
			opts := HistoryOptions{EndHeight: guard.tip.Height, TargetsPath: manifest, Guard: guard.Check, GenesisActivationHeight: s.ChainCfgParams.GenesisActivationHeight}
			archive := memory.New()
			history, err := NewHistory(t.Context(), path, counted, archive, opts)
			require.NoError(t, err)
			if mode != "unsealed" {
				require.NoError(t, history.Build(t.Context(), guard.tip))
			}
			require.NoError(t, history.Close())
			counted.blocks = 0
			if mode == "missing-target" {
				_, err = census.Exec("INSERT INTO targets VALUES(?)", strings.Repeat("a", 64))
				require.NoError(t, err)
			}
			if mode == "changed-consensus" {
				opts.GenesisActivationHeight++
			}
			history, err = buildJobHistory(t.Context(), path, counted, archive, opts, guard.tip, true)
			if mode == "changed-consensus" {
				require.ErrorContains(t, err, "consensus settings changed")
				require.Zero(t, counted.blocks)
				return
			}
			require.NoError(t, err)
			require.NoError(t, history.Close())
			if mode == "sealed" {
				require.Zero(t, counted.blocks, "resuming sealed history must not repeat canonical body lookups")
			} else {
				require.Positive(t, counted.blocks, "incomplete evidence must rebuild before discovery")
			}
		})
	}
}

type emptyInventoryBackend struct{ replayrecovery.Backend }

func (emptyInventoryBackend) Identity() string { return "empty-inventory" }
func (emptyInventoryBackend) Inventory(ctx context.Context, _ func(replayrecovery.InventoryRecord) error) error {
	return ctx.Err()
}
func (emptyInventoryBackend) SpendReferences(ctx context.Context, _ func(replayrecovery.SpendReference) error) error {
	return ctx.Err()
}

func TestRunJobResumePreservesSealedHistory(t *testing.T) {
	u, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	s := test.CreateBaseTestSettings(t)
	chain, err := blockchainsql.New(ulogger.TestLogger{}, u, s)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, chain.Close()) })
	require.NoError(t, chain.SetState(t.Context(), "fsm_state", []byte("IDLE")))
	guard, err := newIdleGuard(t.Context(), chain)
	require.NoError(t, err)
	counted := &historyReadCounter{HistoryChain: chain}
	backend := emptyInventoryBackend{}
	opts := Options{WorkDir: t.TempDir(), Apply: true, Maintenance: true, Resume: true, Timeout: time.Minute, Concurrency: 1}
	require.NoError(t, os.Chmod(opts.WorkDir, 0700))
	manifest := filepath.Join(opts.WorkDir, "manifest.sqlite")
	_, err = replayrecovery.Inventory(t.Context(), backend, manifest, guard.Check)
	require.NoError(t, err)
	archive := memory.New()
	history, err := buildJobHistory(t.Context(), filepath.Join(opts.WorkDir, "history.sqlite"), counted, archive, HistoryOptions{EndHeight: guard.tip.Height, TargetsPath: manifest, Guard: guard.Check, GenesisActivationHeight: s.ChainCfgParams.GenesisActivationHeight}, guard.tip, false)
	require.NoError(t, err)
	require.NoError(t, history.Close())
	require.NoError(t, writeJobJSON(filepath.Join(opts.WorkDir, "job.json"), jobState{Version: 3, Identity: backend.Identity(), Tip: guard.tip, Apply: true, Phase: "history"}))
	counted.blocks = 0
	var out bytes.Buffer
	err = runJob(t.Context(), backend, nil, counted, archive, guard, opts, s.ChainCfgParams.GenesisActivationHeight, &out, io.Discard)
	require.NoError(t, err)
	require.Zero(t, counted.blocks)
	var report replayrecovery.Summary
	require.NoError(t, json.Unmarshal(out.Bytes(), &report))
	require.True(t, report.Complete)
}
