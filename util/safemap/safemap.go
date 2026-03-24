package safemap

import "sync"

type Safemap[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

func New[K comparable, V any]() *Safemap[K, V] {
	return &Safemap[K, V]{m: make(map[K]V)}
}

func (s *Safemap[K, V]) Get(key K) (V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.m[key]
	return value, ok
}

func (s *Safemap[K, V]) Set(key K, value V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[key] = value
}

func (s *Safemap[K, V]) Delete(key K) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.m, key)
}

func (s *Safemap[K, V]) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.m)
}

func (s *Safemap[K, V]) Keys() []K {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]K, 0, len(s.m))
	for key := range s.m {
		keys = append(keys, key)
	}

	return keys
}

func (s *Safemap[K, V]) Values() []V {
	s.mu.RLock()
	defer s.mu.RUnlock()

	values := make([]V, 0, len(s.m))
	for _, value := range s.m {
		values = append(values, value)
	}

	return values
}

func (s *Safemap[K, V]) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m = make(map[K]V)
}

func (s *Safemap[K, V]) Has(key K) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m[key]

	return ok
}

func (s *Safemap[K, V]) Each(f func(key K, value V)) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for key, value := range s.m {
		f(key, value)
	}
}

func (s *Safemap[K, V]) EachWithBreak(f func(key K, value V) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for key, value := range s.m {
		if f(key, value) {
			break
		}
	}
}
