package alarm

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
)

const writeInterval = time.Second

var ErrCorruptStore = errors.NewProcessingError("probe store is corrupt")

type storeFile struct {
	Baseline *BaselineState       `json:"baseline,omitempty"`
	Snoozes  map[string]time.Time `json:"snoozes,omitempty"`
	Alarms   []snapshot.Alarm     `json:"alarms,omitempty"`
}

type StoreOption func(*Store)

func WithClock(now func() time.Time) StoreOption {
	return func(s *Store) { s.clock = now }
}

func withTimer(after func(d time.Duration, f func()) func() bool) StoreOption {
	return func(s *Store) { s.afterFunc = after }
}

func defaultAfterFunc(d time.Duration, f func()) func() bool {
	return time.AfterFunc(d, f).Stop
}

type Store struct {
	mu           sync.Mutex
	path         string
	clock        func() time.Time
	afterFunc    func(d time.Duration, f func()) func() bool
	stopTimer    func() bool
	timerGen     uint64
	timerErr     error
	data         storeFile
	dirty        bool
	lastWrite    time.Time
	beforeRename func() error
}

func OpenStore(path string, opts ...StoreOption) (*Store, error) {
	s := &Store{path: path, clock: time.Now, afterFunc: defaultAfterFunc}

	for _, o := range opts {
		o(s)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, errors.NewStorageError("probe store: create directory", err)
	}

	raw, err := os.ReadFile(path)

	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return nil, errors.NewStorageError("probe store: read %s", path, err)
	case len(raw) > 0:
		if err := json.Unmarshal(raw, &s.data); err != nil {
			return nil, errors.NewProcessingError("probe store: %s is corrupt", path, err)
		}
	}

	if s.data.Snoozes == nil {
		s.data.Snoozes = map[string]time.Time{}
	}

	return s, nil
}

func (s *Store) SaveBaseline(state BaselineState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := BaselineState{Metrics: make(map[string]MetricState, len(state.Metrics))}

	for id, m := range state.Metrics {
		cp.Metrics[id] = m
	}

	s.data.Baseline = &cp

	return s.markDirtyLocked()
}

func (s *Store) LoadBaseline() (BaselineState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.data.Baseline == nil {
		return BaselineState{}, false, nil
	}

	out := BaselineState{Metrics: make(map[string]MetricState, len(s.data.Baseline.Metrics))}

	for id, m := range s.data.Baseline.Metrics {
		out.Metrics[id] = m
	}

	return out, true, nil
}

func (s *Store) SaveSnooze(id string, until time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data.Snoozes[id] = until
	s.dirty = true

	return s.persistLocked()
}

func (s *Store) ClearSnooze(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data.Snoozes[id]; !ok {
		return nil
	}

	delete(s.data.Snoozes, id)
	s.dirty = true

	return s.persistLocked()
}

func (s *Store) Snoozes(now time.Time) map[string]time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[string]time.Time, len(s.data.Snoozes))

	for id, until := range s.data.Snoozes {
		if !until.After(now) {
			delete(s.data.Snoozes, id)

			s.dirty = true

			continue
		}

		out[id] = until
	}

	return out
}

func (s *Store) AppendAlarm(a snapshot.Alarm) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data.Alarms = append(s.data.Alarms, a)

	sort.SliceStable(s.data.Alarms, func(i, j int) bool {
		return s.data.Alarms[i].RaisedAt.Before(s.data.Alarms[j].RaisedAt)
	})

	return s.markDirtyLocked()
}

func (s *Store) AlarmsSince(t time.Time) []snapshot.Alarm {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]snapshot.Alarm, 0, len(s.data.Alarms))

	for _, a := range s.data.Alarms {
		if !a.RaisedAt.Before(t) {
			out = append(out, a)
		}
	}

	return out
}

func (s *Store) Prune(now time.Time, retention time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := now.Add(-retention)
	kept := s.data.Alarms[:0]

	for _, a := range s.data.Alarms {
		if !a.RaisedAt.Before(cutoff) {
			kept = append(kept, a)
		}
	}

	if len(kept) == len(s.data.Alarms) {
		return nil
	}

	s.data.Alarms = kept

	return s.markDirtyLocked()
}

func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.flushLocked()
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.disarmLocked()

	return s.flushLocked()
}

func (s *Store) flushLocked() error {
	err := s.persistLocked()
	pending := s.timerErr
	s.timerErr = nil

	if err != nil {
		return err
	}

	return pending
}

func (s *Store) markDirtyLocked() error {
	s.dirty = true

	elapsed := s.clock().Sub(s.lastWrite)
	if elapsed >= writeInterval {
		return s.persistLocked()
	}

	if s.stopTimer != nil {
		return nil
	}

	s.timerGen++
	gen := s.timerGen
	s.stopTimer = s.afterFunc(writeInterval-elapsed, func() { s.trailingFlush(gen) })

	return nil
}

func (s *Store) trailingFlush(gen uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if gen != s.timerGen || s.stopTimer == nil {
		return
	}

	s.stopTimer = nil

	if err := s.persistLocked(); err != nil {
		s.timerErr = err
	}
}

func (s *Store) disarmLocked() {
	if s.stopTimer != nil {
		s.stopTimer()
		s.stopTimer = nil
	}

	s.timerGen++
}

func (s *Store) persistLocked() error {
	if !s.dirty {
		return nil
	}

	raw, err := json.Marshal(s.data)
	if err != nil {
		return errors.NewStorageError("probe store: encode", err)
	}

	if err := s.writeAtomic(raw); err != nil {
		return err
	}

	s.dirty = false
	s.lastWrite = s.clock()

	s.disarmLocked()

	return nil
}

func (s *Store) writeAtomic(raw []byte) error {
	dir := filepath.Dir(s.path)

	tmp, err := os.CreateTemp(dir, filepath.Base(s.path)+".*.tmp")
	if err != nil {
		return errors.NewStorageError("probe store: create temp file", err)
	}

	tmpName := tmp.Name()

	fail := func(err error) error {
		_ = tmp.Close()
		_ = os.Remove(tmpName)

		return errors.NewStorageError("probe store: write %s", s.path, err)
	}

	if err := tmp.Chmod(0o600); err != nil {
		return fail(err)
	}

	if _, err := tmp.Write(raw); err != nil {
		return fail(err)
	}

	if err := tmp.Sync(); err != nil {
		return fail(err)
	}

	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return errors.NewStorageError("probe store: write %s", s.path, err)
	}

	if s.beforeRename != nil {
		if err := s.beforeRename(); err != nil {
			return errors.NewStorageError("probe store: write %s", s.path, err)
		}
	}

	if err := os.Rename(tmpName, s.path); err != nil {
		_ = os.Remove(tmpName)
		return errors.NewStorageError("probe store: rename into %s", s.path, err)
	}

	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}

	return nil
}
