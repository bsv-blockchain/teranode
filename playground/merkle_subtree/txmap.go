package main

import (
	"encoding/binary"
	"hash/maphash"
	"sync"

	"github.com/dolthub/swiss"
	"github.com/puzpuzpuz/xsync/v2"
)

type txMap interface {
	Exists(hash [32]byte) bool
	Put(hash [32]byte) error
}

type SwissMap struct {
	m *swiss.Map[[32]byte, struct{}]
}

func NewSwissMap(length int) *SwissMap {
	return &SwissMap{
		m: swiss.NewMap[[32]byte, struct{}](uint32(length)),
	}
}

func (s *SwissMap) Exists(hash [32]byte) bool {
	_, ok := s.m.Get(hash)
	return ok
}

func (s *SwissMap) Put(hash [32]byte) error {
	s.m.Put(hash, struct{}{})
	return nil
}

type GoMap struct {
	m map[[32]byte]struct{}
}

func NewGoMap(length int) *GoMap {
	return &GoMap{
		m: make(map[[32]byte]struct{}, length),
	}
}

func (g *GoMap) Exists(hash [32]byte) bool {
	_, ok := g.m[hash]
	return ok
}

func (g *GoMap) Put(hash [32]byte) error {
	g.m[hash] = struct{}{}
	return nil
}

type SyncMap struct {
	m sync.Map
}

func NewSyncMap(length int) *SyncMap {
	return &SyncMap{
		m: sync.Map{}, // can we set the length of the sync.Map ?
	}
}

func (s *SyncMap) Exists(hash [32]byte) bool {
	_, ok := s.m.Load(hash)
	return ok
}

func (s *SyncMap) Put(hash [32]byte) error {
	s.m.Store(hash, struct{}{})
	return nil
}

type XSyncMap struct {
	m *xsync.MapOf[[32]byte, struct{}]
}

func NewXSyncMap(length int) *XSyncMap {
	return &XSyncMap{
		m: xsync.NewTypedMapOf[[32]byte, struct{}](func(seed maphash.Seed, hash [32]byte) uint64 {
			// provide a hash function when creating the MapOf;
			// we recommend using the hash/maphash package for the function
			var h maphash.Hash
			h.SetSeed(seed)
			_ = binary.Write(&h, binary.LittleEndian, hash)
			return h.Sum64()
		}),
	}
}

func (x *XSyncMap) Exists(hash [32]byte) bool {
	_, ok := x.m.Load(hash)
	return ok
}

func (x *XSyncMap) Put(hash [32]byte) error {
	x.m.Store(hash, struct{}{})
	return nil
}

type GoMutexMap struct {
	m  map[[32]byte]struct{}
	mx sync.Mutex
}

func NewGoMutexMap(length int) *GoMutexMap {
	return &GoMutexMap{
		m: make(map[[32]byte]struct{}, length),
	}
}

func (g *GoMutexMap) Exists(hash [32]byte) bool {
	g.mx.Lock()
	defer g.mx.Unlock()

	_, ok := g.m[hash]
	return ok
}

func (g *GoMutexMap) Put(hash [32]byte) error {
	g.mx.Lock()
	defer g.mx.Unlock()

	g.m[hash] = struct{}{}
	return nil
}

type GoSplitMutexMap struct {
	m map[[1]byte]*GoMutexMap
}

func NewGoSplitMutexMap(length int) *GoSplitMutexMap {
	m := &GoSplitMutexMap{
		m: make(map[[1]byte]*GoMutexMap, 256),
	}

	for i := 0; i <= 255; i++ {
		m.m[[1]byte{uint8(i)}] = NewGoMutexMap(length / 255)
	}

	return m
}

func (g *GoSplitMutexMap) Exists(hash [32]byte) bool {
	return g.m[[1]byte{hash[0]}].Exists(hash)
}

func (g *GoSplitMutexMap) Put(hash [32]byte) error {
	return g.m[[1]byte{hash[0]}].Put(hash)
}
