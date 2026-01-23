package pruner

import (
	"sync"

	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
)

type PrunerEventNotifier struct {
	mu        sync.RWMutex
	observers []pruner.Observer
}

func NewPrunerEventNotifier() *PrunerEventNotifier {
	return &PrunerEventNotifier{
		observers: make([]pruner.Observer, 0),
	}
}

func (n *PrunerEventNotifier) AddObserver(observer pruner.Observer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.observers = append(n.observers, observer)
}

func (n *PrunerEventNotifier) NotifyPruneComplete(height uint32, recordsProcessed int64) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	for _, observer := range n.observers {
		observer.OnPruneComplete(height, recordsProcessed)
	}
}
