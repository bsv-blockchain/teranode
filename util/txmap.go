package util

import (
	"fmt"
	"sync"

	"github.com/dolthub/swiss"
)

type TxMap interface {
	Put(hash [32]byte, value uint64) error
	Get(hash [32]byte) (uint64, bool)
	Exists(hash [32]byte) bool
	Length() int
}

type SwissMap struct {
	mu     sync.Mutex
	m      *swiss.Map[[32]byte, struct{}]
	length int
}

func NewSwissMap(length int) *SwissMap {
	return &SwissMap{
		m: swiss.NewMap[[32]byte, struct{}](uint32(length)),
	}
}

func (s *SwissMap) Exists(hash [32]byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.m.Get(hash)
	return ok
}

func (s *SwissMap) Get(hash [32]byte) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.m.Get(hash)

	return 0, ok
}

func (s *SwissMap) Put(hash [32]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length++

	s.m.Put(hash, struct{}{})
	return nil
}

func (s *SwissMap) Delete(hash [32]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length--

	s.m.Delete(hash)
	return nil
}

func (s *SwissMap) Length() int {
	return s.length
}

type SwissMapUint64 struct {
	mu     sync.Mutex
	m      *swiss.Map[[32]byte, uint64]
	length int
}

func NewSwissMapUint64(length int) *SwissMapUint64 {
	return &SwissMapUint64{
		m: swiss.NewMap[[32]byte, uint64](uint32(length)),
	}
}

func (s *SwissMapUint64) Exists(hash [32]byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.m.Get(hash)
	return ok
}

func (s *SwissMapUint64) Put(hash [32]byte, n uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	exists := s.m.Has(hash)
	if exists {
		return fmt.Errorf("hash already exists in map")
	}

	s.m.Put(hash, n)
	s.length++

	return nil
}

func (s *SwissMapUint64) Get(hash [32]byte) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length++

	n, ok := s.m.Get(hash)
	if !ok {
		return 0, false
	}

	return n, true
}

func (s *SwissMapUint64) Length() int {
	return s.length
}

type SplitSwissMap struct {
	m map[[1]byte]*SwissMap
	// length int
}

func NewSplitSwissMap(length int) *SplitSwissMap {
	m := &SplitSwissMap{
		m: make(map[[1]byte]*SwissMap, 256),
	}

	for i := 0; i <= 255; i++ {
		m.m[[1]byte{uint8(i)}] = NewSwissMap(length / 256)
	}

	return m
}

func (g *SplitSwissMap) Exists(hash [32]byte) bool {
	return g.m[[1]byte{hash[0]}].Exists(hash)
}

func (g *SplitSwissMap) Get(hash [32]byte) (uint64, bool) {
	return g.m[[1]byte{hash[0]}].Get(hash)
}

func (g *SplitSwissMap) Put(hash [32]byte, n uint64) error {
	return g.m[[1]byte{hash[0]}].Put(hash)
}

func (g *SplitSwissMap) Length() int {
	length := 0
	for i := 0; i <= 255; i++ {
		length += g.m[[1]byte{uint8(i)}].length
	}

	return length
}

type SplitSwissMapUint64 struct {
	m          map[[1]byte]*SwissMapUint64
	splitIndex int
	// length int
}

func NewSplitSwissMapUint64(length int, splitIndex ...int) *SplitSwissMapUint64 {
	m := &SplitSwissMapUint64{
		m:          make(map[[1]byte]*SwissMapUint64, 256),
		splitIndex: 0,
	}

	if len(splitIndex) > 0 {
		m.splitIndex = splitIndex[0]
	}

	for i := 0; i <= 255; i++ {
		m.m[[1]byte{uint8(i)}] = NewSwissMapUint64(length / 256)
	}

	return m
}

func (g *SplitSwissMapUint64) Exists(hash [32]byte) bool {
	return g.m[[1]byte{hash[g.splitIndex]}].Exists(hash)
}

func (g *SplitSwissMapUint64) Put(hash [32]byte, n uint64) error {
	return g.m[[1]byte{hash[g.splitIndex]}].Put(hash, n)
}

func (g *SplitSwissMapUint64) Get(hash [32]byte) (uint64, bool) {
	return g.m[[1]byte{hash[g.splitIndex]}].Get(hash)
}

func (g *SplitSwissMapUint64) Length() int {
	length := 0
	for i := 0; i <= 255; i++ {
		length += g.m[[1]byte{uint8(i)}].length
	}

	return length
}

type Split2SwissMapUint64 struct {
	m map[[1]byte]*SplitSwissMapUint64
	// length int
}

func NewSplit2SwissMapUint64(length int) *Split2SwissMapUint64 {
	m := &Split2SwissMapUint64{
		m: make(map[[1]byte]*SplitSwissMapUint64, 256),
	}

	for i := 0; i <= 255; i++ {
		m.m[[1]byte{uint8(i)}] = NewSplitSwissMapUint64(length/256, 1)
	}

	return m
}

func (g *Split2SwissMapUint64) Exists(hash [32]byte) bool {
	return g.m[[1]byte{hash[0]}].Exists(hash)
}

func (g *Split2SwissMapUint64) Put(hash [32]byte, n uint64) error {
	return g.m[[1]byte{hash[0]}].Put(hash, n)
}

func (g *Split2SwissMapUint64) Get(hash [32]byte) (uint64, bool) {
	return g.m[[1]byte{hash[0]}].Get(hash)
}

func (g *Split2SwissMapUint64) Length() int {
	length := 0
	for i := 0; i <= 255; i++ {
		length += g.m[[1]byte{uint8(i)}].Length()
	}

	return length
}
