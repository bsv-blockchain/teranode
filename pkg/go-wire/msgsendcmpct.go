// Copyright (c) 2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"io"
)

// MsgSendcmpct defines a bitcoin sendcmpct message which is used to negotiate
// the receipt of compact blocks.  It was added in BIP0031.
type MsgSendcmpct struct {
	SendCmpct bool
	Version   uint64
}

// Bsvdecode decodes r using the bitcoin protocol encoding into the receiver.
// This is part of the Message interface implementation.
func (msg *MsgSendcmpct) Bsvdecode(r io.Reader, pver uint32, enc MessageEncoding) error {
	// Read stop hash
	err := readElement(r, &msg.SendCmpct)
	if err != nil {
		return err
	}

	err = readElement(r, &msg.Version)
	if err != nil {
		return err
	}

	return nil
}

// BsvEncode encodes the receiver to w using the bitcoin protocol encoding.
// This is part of the Message interface implementation.
func (msg *MsgSendcmpct) BsvEncode(w io.Writer, pver uint32, enc MessageEncoding) error {
	return writeElements(w, msg.SendCmpct, msg.Version)
}

// Command returns the protocol command string for the message.  This is part
// of the Message interface implementation.
func (msg *MsgSendcmpct) Command() string {
	return CmdSendcmpct
}

// MaxPayloadLength returns the maximum length the payload can be for the
// receiver.  This is part of the Message interface implementation.
func (msg *MsgSendcmpct) MaxPayloadLength(pver uint32) uint64 {
	return 1 + 8
}

// NewMsgSendcmpct returns a new compact blocks negotiation message that conforms to
// the Message interface.  See MsgSendcmpct for details.
func NewMsgSendcmpct(sendcmpct bool) *MsgSendcmpct {
	return &MsgSendcmpct{
		SendCmpct: sendcmpct,
		Version:   1,
	}
}
