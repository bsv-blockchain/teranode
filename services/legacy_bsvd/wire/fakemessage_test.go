// Copyright (c) 2013-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import "io"

// fakeMessage implements the Message interface and is used to force encode
// errors in messages.
type fakeMessage struct {
	command        string
	payload        []byte
	forceEncodeErr bool
	forceLenErr    bool
}

// Bsvdecode doesn't do anything.  It just satisfies the wire.Message
// interface.
func (msg *fakeMessage) Bsvdecode(r io.Reader, pver uint32, enc MessageEncoding) error {
	return nil
}

// BsvEncode writes the payload field of the fake message or forces an error
// if the forceEncodeErr flag of the fake message is set.  It also satisfies the
// wire.Message interface.
func (msg *fakeMessage) BsvEncode(w io.Writer, pver uint32, enc MessageEncoding) error {
	if msg.forceEncodeErr {
		err := &MessageError{
			Func:        "fakeMessage.BsvEncode",
			Description: "intentional error",
		}
		return err
	}

	_, err := w.Write(msg.payload)
	return err
}

// Command returns the command field of the fake message and satisfies the
// Message interface.
func (msg *fakeMessage) Command() string {
	return msg.command
}

// MaxPayloadLength returns the length of the payload field of fake message
// or a smaller value if the forceLenErr flag of the fake message is set.  It
// satisfies the Message interface.
func (msg *fakeMessage) MaxPayloadLength(pver uint32) uint64 {
	lenp := uint64(len(msg.payload))
	if msg.forceLenErr {
		return lenp - 1
	}

	return lenp
}
