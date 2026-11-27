// Package blobfault wraps a blob.Store and injects faults into its writes, for tests that need a
// store write to fail, to fail a number of times and then succeed, or to hang.
//
// A hang ignores the write's context on purpose. The file blob store's write, fsync and rename are
// synchronous syscalls that a context cannot interrupt, so a test of a deadline around a store
// write must not be able to pass because the fake gave up when its context ended.
//
// Only tests import this package.
package blobfault

import (
	"context"
	"io"
	"sync"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
)

// Always makes a Rule fail every write it matches.
const Always = -1

// Rule is the fault a write meets. FailTimes fails that many matching writes (Always for every
// one) before writes reach the inner store. Block, when not nil, holds each matching write until
// the channel is closed, whatever the write's context does; Entered, when not nil, receives a
// value without blocking as each write starts to wait on Block.
type Rule struct {
	FailTimes int
	Block     <-chan struct{}
	Entered   chan<- struct{}
}

type ruleKey struct {
	key      string
	fileType fileformat.FileType
}

// Store is a blob.Store whose Set and SetFromReader meet the configured faults. Every other method
// goes straight to the inner store.
type Store struct {
	blob.Store

	mu       sync.Mutex
	rules    map[ruleKey]*Rule
	anyRules map[fileformat.FileType]*Rule
	calls    map[ruleKey]int
}

// New wraps inner.
func New(inner blob.Store) *Store {
	return &Store{
		Store:    inner,
		rules:    make(map[ruleKey]*Rule),
		anyRules: make(map[fileformat.FileType]*Rule),
		calls:    make(map[ruleKey]int),
	}
}

// OnSet applies r to writes of key with fileType. It replaces any earlier rule for them.
func (s *Store) OnSet(key []byte, fileType fileformat.FileType, r Rule) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rules[ruleKey{string(key), fileType}] = &r
}

// OnSetAny applies r to writes of every key with fileType that has no rule of its own.
func (s *Store) OnSetAny(fileType fileformat.FileType, r Rule) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.anyRules[fileType] = &r
}

// SetCalls is the number of Set and SetFromReader calls made for key with fileType, failed ones
// included.
func (s *Store) SetCalls(key []byte, fileType fileformat.FileType) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls[ruleKey{string(key), fileType}]
}

// Set applies the matching rule, then writes to the inner store.
func (s *Store) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	if err := s.fault(key, fileType); err != nil {
		return err
	}

	return s.Store.Set(ctx, key, fileType, value, opts...)
}

// SetFromReader applies the matching rule, then writes to the inner store.
func (s *Store) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	if err := s.fault(key, fileType); err != nil {
		_ = reader.Close()
		return err
	}

	return s.Store.SetFromReader(ctx, key, fileType, reader, opts...)
}

// fault counts the call and applies its rule: it waits on Block first, then fails while the rule
// has failures left.
func (s *Store) fault(key []byte, fileType fileformat.FileType) error {
	k := ruleKey{string(key), fileType}

	s.mu.Lock()
	s.calls[k]++

	r := s.rules[k]
	if r == nil {
		r = s.anyRules[fileType]
	}

	var (
		block   <-chan struct{}
		entered chan<- struct{}
		fail    bool
	)

	if r != nil {
		block, entered = r.Block, r.Entered

		switch {
		case r.FailTimes == Always:
			fail = true
		case r.FailTimes > 0:
			r.FailTimes--
			fail = true
		}
	}
	s.mu.Unlock()

	if block != nil {
		if entered != nil {
			select {
			case entered <- struct{}{}:
			default:
			}
		}

		<-block
	}

	if fail {
		return errors.NewStorageError("blobfault: injected write failure for %x %s", key, fileType)
	}

	return nil
}
