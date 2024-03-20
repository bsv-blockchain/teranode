// Copyright (c) 2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"io"
)

// MsgAuthch authentication handshake message
type MsgAuthch struct {
	Version   int32
	Length    uint32
	Challenge []byte
}

// Bsvdecode decodes r using the bitcoin protocol encoding into the receiver.
// This is part of the Message interface implementation.
func (msg *MsgAuthch) Bsvdecode(r io.Reader, pver uint32, enc MessageEncoding) error {
	// Read stop hash
	err := readElement(r, &msg.Version)
	if err != nil {
		return err
	}

	msg.Challenge, err = ReadVarBytes(r, pver, msg.MaxPayloadLength(pver), "challenge")
	if err != nil {
		return err
	}
	msg.Length = uint32(len(msg.Challenge))

	return nil
}

// BsvEncode encodes the receiver to w using the bitcoin protocol encoding.
// This is part of the Message interface implementation.
func (msg *MsgAuthch) BsvEncode(w io.Writer, pver uint32, enc MessageEncoding) error {
	return writeElements(w, msg.Version, msg.Length, msg.Challenge)
}

// Command returns the protocol command string for the message.  This is part
// of the Message interface implementation.
func (msg *MsgAuthch) Command() string {
	return CmdAuthch
}

// MaxPayloadLength returns the maximum length the payload can be for the
// receiver.  This is part of the Message interface implementation.
func (msg *MsgAuthch) MaxPayloadLength(pver uint32) uint64 {
	return 40
}

// NewMsgAuthch returns a new auth challenge message
func NewMsgAuthch(message string) *MsgAuthch {
	return &MsgAuthch{
		Version:   1,
		Length:    uint32(len(message)),
		Challenge: []byte(message),
	}
}
