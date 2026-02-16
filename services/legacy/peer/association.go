package peer

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-wire"
)

// Stream represents a single TCP stream within a multistream association.
type Stream struct {
	Type wire.StreamType
	Peer *Peer
}

// Association represents a multistream association between two peers.
// A single logical peer connection can have multiple TCP streams,
// each carrying different types of traffic (e.g., blocks vs transactions).
type Association struct {
	id          []byte
	streams     map[wire.StreamType]*Stream
	policyName  string
	primaryPeer *Peer
	mu          sync.RWMutex
}

// NewAssociation creates a new association with the given ID and primary peer.
func NewAssociation(id []byte, primary *Peer) *Association {
	a := &Association{
		id:          id,
		streams:     make(map[wire.StreamType]*Stream),
		primaryPeer: primary,
	}
	// Register the primary peer as the GENERAL stream.
	a.streams[wire.StreamTypeGeneral] = &Stream{
		Type: wire.StreamTypeGeneral,
		Peer: primary,
	}
	return a
}

// ID returns the hex-encoded association ID.
func (a *Association) ID() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return hex.EncodeToString(a.id)
}

// RawID returns the raw association ID bytes.
func (a *Association) RawID() []byte {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.id
}

// PrimaryPeer returns the primary peer of this association.
func (a *Association) PrimaryPeer() *Peer {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.primaryPeer
}

// AddStream adds a stream to the association. Returns false if a stream of
// that type already exists.
func (a *Association) AddStream(streamType wire.StreamType, p *Peer) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.streams[streamType]; exists {
		return false
	}
	a.streams[streamType] = &Stream{
		Type: streamType,
		Peer: p,
	}
	return true
}

// Stream returns the stream of the given type, or nil if not found.
func (a *Association) Stream(streamType wire.StreamType) *Stream {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.streams[streamType]
}

// RemoveStream removes a stream from the association.
func (a *Association) RemoveStream(streamType wire.StreamType) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.streams, streamType)
}

// SetPolicy sets the stream policy name for this association.
func (a *Association) SetPolicy(name string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.policyName = name
}

// Policy returns the stream policy name for this association.
func (a *Association) Policy() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.policyName
}

// StreamCount returns the number of active streams.
func (a *Association) StreamCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.streams)
}

// HasRecentActivity returns true if any stream in the association has
// received a message within the given timeout duration.
func (a *Association) HasRecentActivity(timeout time.Duration) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, stream := range a.streams {
		if stream.Peer != nil && time.Since(stream.Peer.LastRecv()) < timeout {
			return true
		}
	}
	return false
}

// GenerateAssociationID generates a new association ID in the format
// used by Bitcoin SV: [0x00 type byte (UUID)][16 random UUID bytes] = 17 bytes.
func GenerateAssociationID() ([]byte, error) {
	id := make([]byte, 17)
	id[0] = 0x00 // IDType::UUID
	if _, err := rand.Read(id[1:]); err != nil {
		return nil, err
	}
	return id, nil
}
