// Copyright (c) 2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"fmt"
	"io"
)

// MsgExtMsg implements the Message interface and represents a bitcoin
// extmsg message.
type MsgExtMsg struct {
	NumberOfFields       uint64 // numberOfFields is set to 1, increment if new properties are added
	MaxRecvPayloadLength uint64
}

// MaxExtMsgPayload is the maximum number of bytes a protoconf can be.
// NumberOfFields 8 bytes + MaxRecvPayloadLength 4 bytes
const MaxExtMsgPayload = 0xFFFFFFFFFFFFFFFF

// Bsvdecode decodes r using the bitcoin protocol encoding into the receiver.
// This is part of the Message interface implementation.
func (msg *MsgExtMsg) Bsvdecode(r io.Reader, pver uint32, enc MessageEncoding) error {
	if pver < ProtoconfVersion {
		str := fmt.Sprintf("protoconf message invalid for protocol "+
			"version %d", pver)
		return messageError("MsgExtMsg.Bsvdecode", str)
	}
	// do nothing...
	return nil
}

// BsvEncode encodes the receiver to w using the bitcoin protocol encoding.
// This is part of the Message interface implementation.
func (msg *MsgExtMsg) BsvEncode(w io.Writer, pver uint32, enc MessageEncoding) error {
	if pver < ProtoconfVersion {
		str := fmt.Sprintf("protoconf message invalid for protocol "+
			"version %d", pver)
		return messageError("MsgExtMsg.BsvEncode", str)
	}

	return writeElements(w, msg.NumberOfFields, msg.MaxRecvPayloadLength)
}

// Command returns the protocol command string for the message.  This is part
// of the Message interface implementation.
func (msg *MsgExtMsg) Command() string {
	return CmdProtoconf
}

// MaxPayloadLength returns the maximum length the payload can be for the
// receiver.  This is part of the Message interface implementation.
func (msg *MsgExtMsg) MaxPayloadLength(pver uint32) uint64 {
	return MaxProtoconfPayload
}

// NewMsgExtMsg returns a new bitcoin feefilter message that conforms to
// the Message interface.  See MsgFeeFilter for details.
func NewMsgExtMsg(maxRecvPayloadLength uint64) *MsgExtMsg {
	return &MsgExtMsg{
		NumberOfFields:       1, // numberOfFields is set to 1, increment if new properties are added
		MaxRecvPayloadLength: maxRecvPayloadLength,
	}
}
