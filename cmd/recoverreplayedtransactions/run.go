// Package recoverreplayedtransactions provides the IDLE-only store recovery job.
package recoverreplayedtransactions

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/replayrecovery"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	astore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"golang.org/x/sys/unix"
)

type Options struct {
	WorkDir     string
	Apply       bool
	Maintenance bool
	Resume      bool
	Timeout     time.Duration
	Concurrency int
}

func (o Options) validate() error {
	if o.WorkDir == "" {
		return commandError("work-dir is required")
	}
	if o.Apply && !o.Maintenance {
		return commandError("apply requires --maintenance acknowledgement that all shared-store writers are stopped")
	}
	if o.Resume && !o.Apply {
		return commandError("resume requires --apply")
	}
	if o.Timeout <= 0 || o.Concurrency < 1 || o.Concurrency > 64 {
		return commandError("timeout must be positive and concurrency between 1 and 64")
	}
	return nil
}

type historyClient struct{ blockchain.ClientI }

func (c historyClient) GetBlockInChainByHeightHash(ctx context.Context, height uint32, tip *chainhash.Hash) (*model.Block, bool, error) {
	header, _, err := c.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, false, err
	}
	if header == nil || !header.Hash().IsEqual(tip) {
		return nil, false, commandError("canonical tip moved during history lookup")
	}
	b, err := c.GetBlockByHeight(ctx, height)
	if err != nil {
		return nil, false, err
	}
	if b == nil || b.Height != height {
		return nil, false, commandError("canonical block height mismatch")
	}
	// LocalHistory authenticates every header's ancestry back from the pinned tip.
	return b, false, nil
}

func readOnlyArchive(logger ulogger.Logger, configured *url.URL) (blob.Store, error) {
	if configured == nil {
		return nil, commandError("subtree store URL is not configured")
	}
	u := *configured
	q := u.Query()
	q.Set("disableDAH", "true")
	q.Del("batch")
	u.RawQuery = q.Encode()
	if u.Scheme == "file" {
		path := u.Path
		if u.Host == "." {
			path = strings.TrimPrefix(path, "/")
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			return nil, commandError("archive root is not a directory")
		}
	}
	return blob.NewStore(logger, &u, options.WithHashPrefix(2))
}

// Run opens read clients only. It never changes FSM state or starts store cleaners.
func Run(ctx context.Context, logger ulogger.Logger, s *settings.Settings, o Options, out, progress io.Writer) (err error) {
	if err = o.validate(); err != nil {
		return err
	}
	ctx, timeoutCancel := context.WithTimeout(ctx, o.Timeout)
	defer timeoutCancel()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	if s == nil || s.UtxoStore.UtxoStore == nil || s.UtxoStore.UtxoStore.Scheme != "aerospike" {
		return commandError("replay recovery supports only the Aerospike UTXO backend")
	}
	chain, err := blockchain.NewClient(ctx, logger, s, "replay-recovery")
	if err != nil {
		return err
	}
	if closer, ok := chain.(io.Closer); ok {
		defer func() { err = combineErrors(err, closer.Close()) }()
	}
	guard, err := newIdleGuard(ctx, chain)
	if err != nil {
		return err
	}
	watcher := guard.watch(ctx, cancel)
	defer func() {
		cancel(nil)
		<-watcher
		if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
			err = combineErrors(err, cause)
		}
	}()
	lock, err := lockWorkDir(o.WorkDir)
	if err != nil {
		return err
	}
	defer func() { err = combineErrors(err, lock.Close()) }()
	u := s.UtxoStore.UtxoStore
	namespace := strings.Trim(u.Path, "/")
	set := u.Query().Get("set")
	if set == "" {
		set = "txmeta"
	}
	if namespace == "" || strings.Contains(namespace, "/") {
		return commandError("UTXO URL must name one namespace and optional set query parameter")
	}
	client, err := util.GetAerospikeClient(logger, u, s)
	if err != nil {
		return err
	}
	defer util.CloseAerospikeClient(u.Host)
	native, err := astore.NewRecoveryBackend(client.Client, namespace, set, s.UtxoStore.UtxoBatchSize, u.Host)
	if err != nil {
		return err
	}
	native.ScanConcurrency = o.Concurrency
	retained, closeRetained, err := openExternalTransactionReader(ctx, logger, u)
	if err != nil {
		return err
	}
	defer func() { err = combineErrors(err, closeRetained()) }()
	rawBackend := replayrecovery.NewEvidenceBackend(native, nil, guard.tip, retained)
	archive, err := readOnlyArchive(logger, s.SubtreeValidation.SubtreeStore)
	if err != nil {
		return err
	}
	defer func() { err = combineErrors(err, archive.Close(ctx)) }()
	return runJob(ctx, native, rawBackend.Transaction, historyClient{chain}, archive, guard, o, s.ChainCfgParams.GenesisActivationHeight, out, progress)
}

type jobState struct {
	Version  int                `json:"version"`
	Identity string             `json:"identity"`
	Tip      replayrecovery.Tip `json:"tip"`
	Apply    bool               `json:"apply"`
	Phase    string             `json:"phase"`
}

func lockWorkDir(path string) (*os.File, error) {
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	dir := os.NewFile(uintptr(fd), path)
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil {
		_ = dir.Close()
		return nil, err
	}
	if stat.Mode&0077 != 0 || int64(stat.Uid) != int64(os.Geteuid()) {
		_ = dir.Close()
		return nil, commandError("work directory must be owned and private (0700)")
	}
	if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = dir.Close()
		return nil, commandError("recovery work directory already in use: %w", err)
	}
	return dir, nil
}
func writeJobJSON(path string, value any) (err error) {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".recovery-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(append(data, '\n')); err == nil {
		err = f.Sync()
	}
	err = combineErrors(err, f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return combineErrors(dir.Sync(), dir.Close())
}
func readJobState(path string) (state jobState, err error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return state, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil {
		return state, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0077 != 0 || int64(stat.Uid) != int64(os.Geteuid()) {
		return state, commandError("unsafe recovery state file")
	}
	err = json.NewDecoder(io.LimitReader(f, 65536)).Decode(&state)
	return state, err
}

func runJob(ctx context.Context, native replayrecovery.Backend, retained replayrecovery.TransactionReader, chain HistoryChain, archive blob.Store, guard *idleGuard, o Options, genesis uint32, out, progress io.Writer) (err error) {
	manifest := filepath.Join(o.WorkDir, "manifest.sqlite")
	historyPath := filepath.Join(o.WorkDir, "history.sqlite")
	journal := filepath.Join(o.WorkDir, "journal.sqlite")
	statePath := filepath.Join(o.WorkDir, "job.json")
	state := jobState{Version: 2, Identity: native.Identity(), Tip: guard.tip, Apply: o.Apply, Phase: "inventory"}
	var summary, mutations replayrecovery.Summary
	reportAllowed := false
	defer func() {
		summary = retainMutationStatus(summary, mutations)
		if err != nil {
			summary.Complete = false
		}
		if reportAllowed {
			err = combineErrors(err, writeJobJSON(filepath.Join(o.WorkDir, "report.json"), summary))
		}
		err = combineErrors(err, json.NewEncoder(out).Encode(summary))
	}()
	if o.Resume {
		saved, e := readJobState(statePath)
		if e != nil {
			return e
		}
		if saved.Version != state.Version || saved.Identity != state.Identity || saved.Tip != state.Tip || !saved.Apply {
			return commandError("resume identity, version, mode or pinned tip mismatch")
		}
		state = saved
	} else {
		entries, e := os.ReadDir(o.WorkDir)
		if e != nil {
			return e
		}
		if len(entries) != 0 {
			return commandError("work directory contains an existing run; use a new directory or explicit --resume")
		}
		if err = writeJobJSON(statePath, state); err != nil {
			return err
		}
	}
	reportAllowed = true
	if o.Resume && (state.Phase == "apply" || state.Phase == "verify" || state.Phase == "done") {
		_, e := os.Lstat(journal)
		mutations.RestartRequired = e == nil
		mutations.Applied = state.Phase == "verify" || state.Phase == "done"
	}
	switch state.Phase {
	case "inventory", "history", "discover", "apply", "verify", "done":
	default:
		return commandError("invalid recovery phase")
	}
	beforeApply := state.Phase == "inventory" || state.Phase == "history" || state.Phase == "discover"
	if beforeApply {
		if _, e := os.Lstat(journal); e == nil {
			return commandError("unexpected mutation journal in an unsealed run")
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
		if o.Resume {
			// Only unsealed, read-only artifacts owned by this job can be rebuilt.
			for _, path := range []string{manifest, historyPath} {
				for _, suffix := range []string{"", ".lock", "-journal"} {
					if e := os.Remove(path + suffix); e != nil && !errors.Is(e, os.ErrNotExist) {
						return e
					}
				}
			}
		}
		census, ok := native.(replayrecovery.CensusBackend)
		if !ok {
			return commandError("backend lacks full store census")
		}
		summary, err = replayrecovery.Inventory(ctx, census, manifest, guard.Check)
		if err != nil {
			return err
		}
		state.Phase = "history"
		if err = writeJobJSON(statePath, state); err != nil {
			return err
		}
	}
	options := HistoryOptions{StartHeight: 0, EndHeight: guard.tip.Height, Guard: guard.Check, TargetsPath: manifest, GenesisActivationHeight: genesis, Unconfirmed: retained}
	last := time.Time{}
	options.Progress = func(c HistoryCoverage) {
		if time.Since(last) >= 5*time.Second {
			_, _ = fmt.Fprintf(progress, "History: %d blocks, %d gaps\n", c.Scanned, c.GapCount)
			last = time.Now()
		}
	}
	var history *LocalHistory
	if beforeApply {
		history, err = NewHistory(ctx, historyPath, chain, archive, options)
		if err != nil {
			return err
		}
		defer func() { err = combineErrors(err, history.Close()) }()
		if err = history.Build(ctx, guard.tip); err != nil {
			return err
		}
	} else {
		history, err = OpenHistory(historyPath, chain, options)
		if err != nil {
			return err
		}
		defer func() { err = combineErrors(err, history.Close()) }()
	}
	backend := replayrecovery.NewEvidenceBackend(native, history, guard.tip, retained)
	if beforeApply {
		state.Phase = "discover"
		if err = writeJobJSON(statePath, state); err != nil {
			return err
		}
		summary, err = replayrecovery.Discover(ctx, backend, history, manifest, guard.Check, func(p replayrecovery.Summary) {
			if time.Since(last) >= 5*time.Second {
				_, _ = fmt.Fprintf(progress, "%s: %d records\n", p.Stage, p.Scanned)
				last = time.Now()
			}
		})
		if err != nil && !errors.Is(err, replayrecovery.ErrIncomplete) {
			return err
		}
		if !o.Apply {
			return err
		}
		state.Phase = "apply"
		if e := writeJobJSON(statePath, state); e != nil {
			return e
		}
	}
	_, statErr := os.Lstat(journal)
	resumeJournal := statErr == nil
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	summary, err = replayrecovery.Apply(ctx, backend, history, manifest, journal, replayrecovery.ApplyOptions{Guard: guard.Check, Maintenance: o.Maintenance, Resume: resumeJournal, Tip: guard.tip})
	mutations = retainMutationStatus(summary, mutations)
	if err != nil && !errors.Is(err, replayrecovery.ErrIncomplete) {
		return err
	}
	state.Phase = "verify"
	if e := writeJobJSON(statePath, state); e != nil {
		return e
	}
	summary, err = replayrecovery.Verify(ctx, backend, history, manifest, journal, guard.Check)
	if err == nil {
		state.Phase = "done"
		err = writeJobJSON(statePath, state)
	}
	return err
}

func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	if errors.Is(err, replayrecovery.ErrIncomplete) {
		return 2
	}
	return 1
}

// Mutation status is sticky across failures in later read-only phases.
func retainMutationStatus(report, previous replayrecovery.Summary) replayrecovery.Summary {
	report.RestartRequired = report.RestartRequired || previous.RestartRequired
	report.Applied = report.Applied || previous.Applied
	report.Repaired = max(report.Repaired, previous.Repaired)
	return report
}
