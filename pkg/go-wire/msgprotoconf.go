// Copyright (c) 2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"fmt"
	"io"
	"strings"

	"github.com/libsv/go-bt/v2"
)

// MsgProtoconf implements the Message interface and represents a bitcoin
// protoconf message.  It is sent after verack message directly to inform
// max receive payload length

const (
	MaxProtoconfPayload       = 1024 * 1024
	MaxNumStreamPolicies      = 10
	DefaultStreamPolicy       = "Default"
	BlockPriorityStreamPolicy = "BlockPriority"

	// MaxProtoconfPayload is the maximum number of bytes a protoconf can be.
	// NumberOfFields 8 bytes + DefaultMaxRecvPayloadLength 4 bytes
	// Default Value: DEFAULT_MAX_PROTOCOL_RECV_PAYLOAD_LENGTH which is 2MB (2 * 1024 * 1024 bytes)
	// Legacy Value: LEGACY_MAX_PROTOCOL_PAYLOAD_LENGTH which is 1MB (1 * 1024 * 1024 bytes)
	// Maximum Allowed Value: MAX_PROTOCOL_RECV_PAYLOAD_LENGTH which is 1GB (ONE_GIGABYTE)
	DefaultMaxRecvPayloadLength uint32 = 2 * 1024 * 1024
)

type MsgProtoconf struct {
	NumberOfFields       uint64
	MaxRecvPayloadLength uint32
	StreamPolicies       []string
}

// NewMsgProtoconf returns a new bitcoin protoconf message that conforms to
// the Message interface.  See MsgFeeFilter for details.
func NewMsgProtoconf(maxRecvPayloadLength uint32, allowBlockPriority bool) *MsgProtoconf {
	var length = DefaultMaxRecvPayloadLength
	if maxRecvPayloadLength > 0 {
		length = maxRecvPayloadLength
	}

	var streamPolicies []string
	if allowBlockPriority {
		streamPolicies = append(streamPolicies, BlockPriorityStreamPolicy)
	}

	// Always add the default...
	streamPolicies = append(streamPolicies, DefaultStreamPolicy)

	return &MsgProtoconf{
		NumberOfFields:       2, // numberOfFields is set to 2, increment if new properties are added
		MaxRecvPayloadLength: length,
		StreamPolicies:       streamPolicies,
	}
}

// Bsvdecode decodes r using the bitcoin protocol encoding into the receiver.
// This is part of the Message interface implementation.
func (msg *MsgProtoconf) Bsvdecode(r io.Reader, pver uint32, enc MessageEncoding) error {
	if pver < ProtoconfVersion {
		str := fmt.Sprintf("protoconf message invalid for protocol version %d", pver)
		return messageError("MsgProtoconf.Bsvdecode", str)
	}

	var vi bt.VarInt

	_, err := vi.ReadFrom(r)
	if err != nil {
		return err
	}

	msg.NumberOfFields = uint64(vi)

	if msg.NumberOfFields > 0 {
		// Read the MaxRecvPayloadLength
		msg.MaxRecvPayloadLength, err = binarySerializer.Uint32(r, littleEndian)
		if err != nil {
			return err
		}
	}

	if msg.NumberOfFields > 1 {
		_, err = vi.ReadFrom(r)
		if err != nil {
			return err
		}

		b := make([]byte, vi)

		_, err = io.ReadFull(r, b)
		if err != nil {
			return err
		}

		msg.StreamPolicies = strings.Split(string(b), ",")
	}

	return nil
}

// BsvEncode encodes the receiver to w using the bitcoin protocol encoding.
// This is part of the Message interface implementation.
func (msg *MsgProtoconf) BsvEncode(w io.Writer, pver uint32, enc MessageEncoding) error {
	if pver < ProtoconfVersion {
		str := fmt.Sprintf("protoconf message invalid for protocol version %d", pver)
		return messageError("MsgProtoconf.BsvEncode", str)
	}

	// First write the number of fields as a varint. At the moment, this is always 2 and will write 1 byte (0x02)
	if _, err := w.Write(bt.VarInt(msg.NumberOfFields).Bytes()); err != nil {
		return err
	}

	if err := binarySerializer.PutUint32(w, littleEndian, msg.MaxRecvPayloadLength); err != nil {
		return err
	}

	s := strings.Join(msg.StreamPolicies, ",")

	if _, err := w.Write(bt.VarInt(len(s)).Bytes()); err != nil {
		return err
	}

	if _, err := w.Write([]byte(s)); err != nil {
		return err
	}

	return nil
}

// Command returns the protocol command string for the message.  This is part
// of the Message interface implementation.
func (msg *MsgProtoconf) Command() string {
	return CmdProtoconf
}

// MaxPayloadLength returns the maximum length the payload can be for the
// receiver.  This is part of the Message interface implementation.
func (msg *MsgProtoconf) MaxPayloadLength(pver uint32) uint64 {
	// The protoconf message itself, which is limited to 1.048.576 bytes.
	return MaxProtoconfPayload
}
