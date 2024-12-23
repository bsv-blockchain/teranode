package netsync

import (
	"bytes"
	"encoding/binary"

	"github.com/bitcoin-sv/teranode/errors"
	peerpkg "github.com/bitcoin-sv/teranode/services/legacy/peer"
	"github.com/bitcoin-sv/teranode/services/legacy/wire"
)

// invMsg packages a bitcoin inv message and the peer it came from together
// so the block handler has access to that information.
type invMsg struct {
	inv  *wire.MsgInv
	peer *peerpkg.Peer
}

func (i *invMsg) Bytes() []byte {
	peerAddr := i.peer.Addr()
	peerBytes := []byte(peerAddr)

	invBytes := bytes.NewBuffer(make([]byte, 0, 1024))
	_ = i.inv.BsvEncode(invBytes, 0, wire.BaseEncoding)

	msgBytes := make([]byte, 0, 8+len(peerBytes)+8+invBytes.Len())

	lenBytes := make([]byte, 8)

	// first 8 bytes are the length of the peer address
	// nolint: gosec
	binary.LittleEndian.PutUint64(lenBytes, uint64(len(peerBytes)))
	msgBytes = append(msgBytes, lenBytes...)

	// next bytes are the peer address
	msgBytes = append(msgBytes, peerBytes...)

	// next 8 bytes are the length of the inv message
	// nolint: gosec
	binary.LittleEndian.PutUint64(lenBytes, uint64(invBytes.Len()))
	msgBytes = append(msgBytes, lenBytes...)

	// next bytes are the inv message
	msgBytes = append(msgBytes, invBytes.Bytes()...)

	return msgBytes
}

func (sm *SyncManager) newInvFromBytes(invMsgBytes []byte) (*invMsg, error) {
	peerLen := binary.LittleEndian.Uint64(invMsgBytes[:8])
	peerAddr := string(invMsgBytes[8 : 8+peerLen])

	invLen := binary.LittleEndian.Uint64(invMsgBytes[8+peerLen : 16+peerLen])
	invBytes := invMsgBytes[16+peerLen : 16+peerLen+invLen]

	inv := wire.NewMsgInv()

	err := inv.Bsvdecode(bytes.NewReader(invBytes), 0, wire.BaseEncoding)
	if err != nil {
		return nil, err
	}

	// get the peer from our peer list, we should be able to get it by Addr
	for peer := range sm.peerStates {
		if peer.Addr() == peerAddr {
			return &invMsg{
				inv:  inv,
				peer: peer,
			}, nil
		}
	}

	return nil, errors.NewProcessingError("peer could not be found in peer list: %s", peerAddr)
}
