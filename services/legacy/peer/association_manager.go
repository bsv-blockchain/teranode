package peer

import (
	"encoding/hex"
	"sync"
)

// AssociationManager tracks multistream associations by their hex-encoded ID.
type AssociationManager struct {
	associations map[string]*Association
	mu           sync.RWMutex
}

// NewAssociationManager creates a new AssociationManager.
func NewAssociationManager() *AssociationManager {
	return &AssociationManager{
		associations: make(map[string]*Association),
	}
}

// Register adds an association to the manager. Returns false if an association
// with the same ID already exists.
func (m *AssociationManager) Register(a *Association) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := hex.EncodeToString(a.RawID())
	if _, exists := m.associations[key]; exists {
		return false
	}
	m.associations[key] = a
	return true
}

// Lookup returns the association for the given raw ID, or nil if not found.
func (m *AssociationManager) Lookup(rawID []byte) *Association {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.associations[hex.EncodeToString(rawID)]
}

// Remove removes an association by its raw ID.
func (m *AssociationManager) Remove(rawID []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.associations, hex.EncodeToString(rawID))
}

// Count returns the number of tracked associations.
func (m *AssociationManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.associations)
}
