package netsync

import (
	"bytes"
	"encoding/binary"

	"github.com/bitcoin-sv/teranode/errors"
	peerpkg "github.com/bitcoin-sv/teranode/services/legacy/peer"
	kafkamessage "github.com/bitcoin-sv/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/go-wire"
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

	lenUint64, err := safeconversion.IntToUint64(len(peerBytes))
	if err != nil {
		return nil
	}

	binary.LittleEndian.PutUint64(lenBytes, lenUint64)
	msgBytes = append(msgBytes, lenBytes...)

	// next bytes are the peer address
	msgBytes = append(msgBytes, peerBytes...)

	invLenUint64, err := safeconversion.IntToUint64(invBytes.Len())
	if err != nil {
		return nil
	}

	// next 8 bytes are the length of the inv message
	binary.LittleEndian.PutUint64(lenBytes, invLenUint64)
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

	for peer := range sm.peerStates.Range() {
		if peer.Addr() == peerAddr {
			return &invMsg{
				inv:  inv,
				peer: peer,
			}, nil
		}
	}

	return nil, errors.NewProcessingError("peer could not be found in peer list: %s", peerAddr)
}

func (sm *SyncManager) newKafkaMessageFromInv(i *wire.MsgInv, peer *peerpkg.Peer) *kafkamessage.KafkaInvTopicMessage {
	n := kafkamessage.KafkaInvTopicMessage{
		PeerAddress: peer.Addr(),
	}

	for _, invVect := range i.InvList {
		n.Inv = append(n.Inv, &kafkamessage.Inv{
			Type: kafkamessage.InvType(invVect.Type), // nolint:gosec
			Hash: invVect.Hash.String(),
		})
	}

	return &n
}

func (sm *SyncManager) newInvFromKafkaMessage(message *kafkamessage.KafkaInvTopicMessage) (*invMsg, error) {
	invM := wire.NewMsgInv()

	for _, inv := range message.Inv {
		invType := wire.InvType(inv.Type) //nolint:gosec

		hash, err := chainhash.NewHashFromStr(inv.Hash)
		if err != nil {
			return nil, errors.New(errors.ERR_INVALID_ARGUMENT, "Failed to parse inv hash from message", err)
		}

		if err = invM.AddInvVect(wire.NewInvVect(invType, hash)); err != nil {
			return nil, errors.New(errors.ERR_INVALID_ARGUMENT, "Failed to add inv vect from message", err)
		}
	}

	var peer *peerpkg.Peer

	for p := range sm.peerStates.Range() {
		if p.Addr() == message.PeerAddress {
			peer = p
			break
		}
	}

	if peer == nil {
		return nil, errors.NewProcessingError("peer could not be found in peer list: %s", message.PeerAddress)
	}

	return &invMsg{
		inv:  invM,
		peer: peer,
	}, nil
}
