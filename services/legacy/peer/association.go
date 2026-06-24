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

// StreamInfo holds a snapshot of a single stream's byte counters.
type StreamInfo struct {
	Type      wire.StreamType
	BytesSent uint64
	BytesRecv uint64
}

// Streams returns a snapshot of all streams in the association with their byte counts.
func (a *Association) Streams() []StreamInfo {
	a.mu.RLock()
	defer a.mu.RUnlock()
	result := make([]StreamInfo, 0, len(a.streams))
	for _, s := range a.streams {
		result = append(result, StreamInfo{
			Type:      s.Type,
			BytesSent: s.Peer.BytesSent(),
			BytesRecv: s.Peer.BytesReceived(),
		})
	}
	return result
}

// StreamCount returns the number of active streams.
func (a *Association) StreamCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.streams)
}

// StreamPeers returns a snapshot of the peers backing each stream in the
// association. It is used to fan stall-control signals across all streams so a
// response delivered on one stream (e.g. a block/headers reply on DATA1) clears
// the matching pending-response deadline armed on another stream (e.g. the
// getdata/getheaders sent on GENERAL).
func (a *Association) StreamPeers() []*Peer {
	a.mu.RLock()
	defer a.mu.RUnlock()
	peers := make([]*Peer, 0, len(a.streams))
	for _, s := range a.streams {
		if s != nil && s.Peer != nil {
			peers = append(peers, s.Peer)
		}
	}
	return peers
}

// ReadBytes returns the total number of bytes read off the wire across all
// streams in the association, counted at byte granularity (i.e. updated as a
// message streams in, not only on completion). It is the progress signal used
// to tell an actively-downloading large block apart from a stalled peer, since
// a multi-GB block arriving on the DATA1 stream would otherwise show no
// movement until it fully completes.
func (a *Association) ReadBytes() uint64 {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var total uint64

	for _, s := range a.streams {
		if s != nil && s.Peer != nil {
			total += s.Peer.ReadBytes()
		}
	}

	return total
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
// used by SV Node: [0x00 type byte (UUID)][16 random UUID bytes] = 17 bytes.
func GenerateAssociationID() ([]byte, error) {
	id := make([]byte, 17)
	id[0] = 0x00 // IDType::UUID
	if _, err := rand.Read(id[1:]); err != nil {
		return nil, err
	}
	return id, nil
}
