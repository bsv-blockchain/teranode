// Copyright (c) 2013-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"bytes"
	"fmt"
	"io"

	"github.com/libsv/go-bt/v2/chainhash"
)

// ExtendedTxIn defines an extended bitcoin transaction input.
type ExtendedTxIn struct {
	PreviousOutPoint   OutPoint
	PreviousTxSatoshis uint64
	PreviousTxScript   []byte
	SignatureScript    []byte
	Sequence           uint32
}

// SerializeSize returns the number of bytes it would take to serialize
// the transaction input.
func (t *ExtendedTxIn) SerializeSize() int {
	// Outpoint Hash 32 bytes + Outpoint Index 4 bytes + Sequence 4 bytes +
	// serialized varint size for the length of SignatureScript +
	// SignatureScript bytes.
	return 40 + VarIntSerializeSize(uint64(len(t.SignatureScript))) +
		len(t.SignatureScript) +
		8 + VarIntSerializeSize(uint64(len(t.PreviousTxScript))) + len(t.PreviousTxScript) // 8 bytes for satoshis, var int script size, script
}

// NewExtendedTxIn returns a new bitcoin transaction input with the provided
// previous outpoint point and signature script with a default sequence of
// MaxTxInSequenceNum.
func NewExtendedTxIn(prevOut *OutPoint, signatureScript []byte, previousTxSatoshis uint64, previousTxScript []byte) *ExtendedTxIn {
	return &ExtendedTxIn{
		PreviousOutPoint:   *prevOut,
		PreviousTxSatoshis: previousTxSatoshis,
		PreviousTxScript:   previousTxScript,
		SignatureScript:    signatureScript,
		Sequence:           MaxTxInSequenceNum,
	}
}

// MsgExtendedTx implements the Message interface and represents a bitcoin tx message.
// It is used to deliver transaction information in response to a getdata
// message (MsgGetData) for a given transaction.
//
// Use the AddTxIn and AddTxOut functions to build up the list of transaction
// inputs and outputs.
type MsgExtendedTx struct {
	Version  int32
	TxIn     []*ExtendedTxIn
	TxOut    []*TxOut
	LockTime uint32
}

// AddTxIn adds a transaction input to the message.
func (msg *MsgExtendedTx) AddTxIn(ti *ExtendedTxIn) {
	msg.TxIn = append(msg.TxIn, ti)
}

// AddTxOut adds a transaction output to the message.
func (msg *MsgExtendedTx) AddTxOut(to *TxOut) {
	msg.TxOut = append(msg.TxOut, to)
}

// TxHash generates the Hash for the transaction.
func (msg *MsgExtendedTx) TxHash() chainhash.Hash {
	// Encode the transaction and calculate double sha256 on the result.
	// Ignore the error returns since the only way the encode could fail
	// is being out of memory or due to nil pointers, both of which would
	// cause a run-time panic.
	buf := bytes.NewBuffer(make([]byte, 0, msg.SerializeSize()))
	_ = msg.Serialize(buf)
	return chainhash.DoubleHashH(buf.Bytes())
}

// Copy creates a deep copy of a transaction so that the original does not get
// modified when the copy is manipulated.
func (msg *MsgExtendedTx) Copy() *MsgTx {
	// Create new tx and start by copying primitive values and making space
	// for the transaction inputs and outputs.
	newTx := MsgTx{
		Version:  msg.Version,
		TxIn:     make([]*TxIn, 0, len(msg.TxIn)),
		TxOut:    make([]*TxOut, 0, len(msg.TxOut)),
		LockTime: msg.LockTime,
	}

	// Deep copy the old TxIn data.
	for _, oldTxIn := range msg.TxIn {
		// Deep copy the old previous outpoint.
		oldOutPoint := oldTxIn.PreviousOutPoint
		newOutPoint := OutPoint{}
		_ = newOutPoint.Hash.SetBytes(oldOutPoint.Hash[:])
		newOutPoint.Index = oldOutPoint.Index

		// Deep copy the old signature script.
		var newScript []byte
		oldScript := oldTxIn.SignatureScript
		oldScriptLen := len(oldScript)
		if oldScriptLen > 0 {
			newScript = make([]byte, oldScriptLen)
			copy(newScript, oldScript[:oldScriptLen])
		}

		// Create new txIn with the deep copied data.
		newTxIn := TxIn{
			PreviousOutPoint: newOutPoint,
			SignatureScript:  newScript,
			Sequence:         oldTxIn.Sequence,
		}

		// Finally, append this fully copied txin.
		newTx.TxIn = append(newTx.TxIn, &newTxIn)
	}

	// Deep copy the old TxOut data.
	for _, oldTxOut := range msg.TxOut {
		// Deep copy the old PkScript
		var newScript []byte
		oldScript := oldTxOut.PkScript
		oldScriptLen := len(oldScript)
		if oldScriptLen > 0 {
			newScript = make([]byte, oldScriptLen)
			copy(newScript, oldScript[:oldScriptLen])
		}

		// Create new txOut with the deep copied data and append it to
		// new Tx.
		newTxOut := TxOut{
			Value:    oldTxOut.Value,
			PkScript: newScript,
		}
		newTx.TxOut = append(newTx.TxOut, &newTxOut)
	}

	return &newTx
}

// Bsvdecode decodes r using the bitcoin protocol encoding into the receiver.
// This is part of the Message interface implementation.
// See Deserialize for decoding transactions stored to disk, such as in a
// database, as opposed to decoding transactions from the wire.
func (msg *MsgExtendedTx) Bsvdecode(r io.Reader, pver uint32, enc MessageEncoding) error {
	version, err := binarySerializer.Uint32(r, littleEndian)
	if err != nil {
		return err
	}
	msg.Version = int32(version)

	count, err := ReadVarInt(r, pver)
	if err != nil {
		return err
	}

	// read in the EF header
	efHeader := make([]byte, 5)
	_, err = io.ReadFull(r, efHeader)
	if err != nil {
		return err
	}
	if count != 0 || !bytes.Equal(efHeader, []byte{0x00, 0x00, 0x00, 0x00, 0xEF}) {
		return fmt.Errorf("invalid extended tx EF header")
	}

	count, err = ReadVarInt(r, pver)
	if err != nil {
		return err
	}

	// Prevent more input transactions than could possibly fit into a
	// message.  It would be possible to cause memory exhaustion and panics
	// without a sane upper bound on this count.
	if count > uint64(maxTxInPerMessage()) {
		str := fmt.Sprintf("too many input transactions to fit into "+
			"max message size [count %d, max %d]", count, maxTxInPerMessage())
		return messageError("MsgTx.Bsvdecode", str)
	}

	// returnScriptBuffers is a closure that returns any script buffers that
	// were borrowed from the pool when there are any deserialization
	// errors.  This is only valid to call before the final step which
	// replaces the scripts with the location in a contiguous buffer and
	// returns them.
	returnScriptBuffers := func() {
		for _, txIn := range msg.TxIn {
			if txIn == nil {
				continue
			}

			if txIn.SignatureScript != nil {
				scriptPool.Return(txIn.SignatureScript)
			}
		}
		for _, txOut := range msg.TxOut {
			if txOut == nil || txOut.PkScript == nil {
				continue
			}
			scriptPool.Return(txOut.PkScript)
		}
	}

	// Deserialize the inputs.
	var totalScriptSize uint64
	txIns := make([]ExtendedTxIn, count)
	msg.TxIn = make([]*ExtendedTxIn, count)
	for i := uint64(0); i < count; i++ {
		// The pointer is set now in case a script buffer is borrowed
		// and needs to be returned to the pool on error.
		ti := &txIns[i]
		msg.TxIn[i] = ti
		err = readExtendedTxIn(r, pver, msg.Version, ti)
		if err != nil {
			returnScriptBuffers()
			return err
		}
		totalScriptSize += uint64(len(ti.SignatureScript))
	}

	count, err = ReadVarInt(r, pver)
	if err != nil {
		returnScriptBuffers()
		return err
	}

	// Prevent more output transactions than could possibly fit into a
	// message.  It would be possible to cause memory exhaustion and panics
	// without a sane upper bound on this count.
	if count > uint64(maxTxOutPerMessage()) {
		returnScriptBuffers()
		str := fmt.Sprintf("too many output transactions to fit into "+
			"max message size [count %d, max %d]", count, maxTxOutPerMessage())
		return messageError("MsgTx.Bsvdecode", str)
	}

	// Deserialize the outputs.
	txOuts := make([]TxOut, count)
	msg.TxOut = make([]*TxOut, count)
	for i := uint64(0); i < count; i++ {
		// The pointer is set now in case a script buffer is borrowed
		// and needs to be returned to the pool on error.
		to := &txOuts[i]
		msg.TxOut[i] = to
		err = readTxOut(r, pver, msg.Version, to)
		if err != nil {
			returnScriptBuffers()
			return err
		}
		totalScriptSize += uint64(len(to.PkScript))
	}

	msg.LockTime, err = binarySerializer.Uint32(r, littleEndian)
	if err != nil {
		returnScriptBuffers()
		return err
	}

	// Create a single allocation to house all of the scripts and set each
	// input signature script and output public key script to the
	// appropriate subslice of the overall contiguous buffer.  Then, return
	// each individual script buffer back to the pool so they can be reused
	// for future deserializations.  This is done because it significantly
	// reduces the number of allocations the garbage collector needs to
	// track, which in turn improves performance and drastically reduces the
	// amount of runtime overhead that would otherwise be needed to keep
	// track of millions of small allocations.
	//
	// NOTE: It is no longer valid to call the returnScriptBuffers closure
	// after these blocks of code run because it is already done and the
	// scripts in the transaction inputs and outputs no longer point to the
	// buffers.
	var offset uint64
	scripts := make([]byte, totalScriptSize)
	for i := 0; i < len(msg.TxIn); i++ {
		// Copy the signature script into the contiguous buffer at the
		// appropriate offset.
		signatureScript := msg.TxIn[i].SignatureScript
		copy(scripts[offset:], signatureScript)

		// Reset the signature script of the transaction input to the
		// slice of the contiguous buffer where the script lives.
		scriptSize := uint64(len(signatureScript))
		end := offset + scriptSize
		msg.TxIn[i].SignatureScript = scripts[offset:end:end]
		offset += scriptSize

		// Return the temporary script buffer to the pool.
		scriptPool.Return(signatureScript)
	}
	for i := 0; i < len(msg.TxOut); i++ {
		// Copy the public key script into the contiguous buffer at the
		// appropriate offset.
		pkScript := msg.TxOut[i].PkScript
		copy(scripts[offset:], pkScript)

		// Reset the public key script of the transaction output to the
		// slice of the contiguous buffer where the script lives.
		scriptSize := uint64(len(pkScript))
		end := offset + scriptSize
		msg.TxOut[i].PkScript = scripts[offset:end:end]
		offset += scriptSize

		// Return the temporary script buffer to the pool.
		scriptPool.Return(pkScript)
	}

	return nil
}

// Deserialize decodes a transaction from r into the receiver using a format
// that is suitable for long-term storage such as a database while respecting
// the Version field in the transaction.  This function differs from Bsvdecode
// in that Bsvdecode decodes from the bitcoin wire protocol as it was sent
// across the network.  The wire encoding can technically differ depending on
// the protocol version and doesn't even really need to match the format of a
// stored transaction at all.  As of the time this comment was written, the
// encoded transaction is the same in both instances, but there is a distinct
// difference and separating the two allows the API to be flexible enough to
// deal with changes.
func (msg *MsgExtendedTx) Deserialize(r io.Reader) error {
	// At the current time, there is no difference between the wire encoding
	// at protocol version 0 and the stable long-term storage format.  As
	// a result, make use of Bsvdecode.
	return msg.Bsvdecode(r, 0, BaseEncoding)
}

// BsvEncode encodes the receiver to w using the bitcoin protocol encoding.
// This is part of the Message interface implementation.
// See Serialize for encoding transactions to be stored to disk, such as in a
// database, as opposed to encoding transactions for the wire.
func (msg *MsgExtendedTx) BsvEncode(w io.Writer, pver uint32, enc MessageEncoding) error {
	err := binarySerializer.PutUint32(w, littleEndian, uint32(msg.Version))
	if err != nil {
		return err
	}

	// write EF header
	_, err = w.Write([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0xEF})
	if err != nil {
		return err
	}

	count := uint64(len(msg.TxIn))
	err = WriteVarInt(w, pver, count)
	if err != nil {
		return err
	}

	for _, ti := range msg.TxIn {
		err = writeExtendedTxIn(w, pver, msg.Version, ti)
		if err != nil {
			return err
		}
	}

	count = uint64(len(msg.TxOut))
	err = WriteVarInt(w, pver, count)
	if err != nil {
		return err
	}

	for _, to := range msg.TxOut {
		err = WriteTxOut(w, pver, msg.Version, to)
		if err != nil {
			return err
		}
	}

	return binarySerializer.PutUint32(w, littleEndian, msg.LockTime)
}

// Serialize encodes the transaction to w using a format that suitable for
// long-term storage such as a database while respecting the Version field in
// the transaction.  This function differs from BsvEncode in that BsvEncode
// encodes the transaction to the bitcoin wire protocol in order to be sent
// across the network.  The wire encoding can technically differ depending on
// the protocol version and doesn't even really need to match the format of a
// stored transaction at all.  As of the time this comment was written, the
// encoded transaction is the same in both instances, but there is a distinct
// difference and separating the two allows the API to be flexible enough to
// deal with changes.
func (msg *MsgExtendedTx) Serialize(w io.Writer) error {
	// At the current time, there is no difference between the wire encoding
	// at protocol version 0 and the stable long-term storage format.  As
	// a result, make use of BsvEncode.
	return msg.BsvEncode(w, 0, BaseEncoding)
}

// baseSize returns the serialized size of the transaction without accounting
// for any witness data.
func (msg *MsgExtendedTx) baseSize() int {
	// Version 4 bytes + LockTime 4 bytes + Serialized varint size for the
	// number of transaction inputs and outputs.
	n := 8 + VarIntSerializeSize(uint64(len(msg.TxIn))) +
		VarIntSerializeSize(uint64(len(msg.TxOut)))

	for _, txIn := range msg.TxIn {
		n += txIn.SerializeSize()
	}

	for _, txOut := range msg.TxOut {
		n += txOut.SerializeSize()
	}

	return n
}

// SerializeSize returns the number of bytes it would take to serialize the
// the transaction.
func (msg *MsgExtendedTx) SerializeSize() int {
	return msg.baseSize()
}

// Command returns the protocol command string for the message.  This is part
// of the Message interface implementation.
func (msg *MsgExtendedTx) Command() string {
	return CmdExtendedTx
}

// MaxPayloadLength returns the maximum length the payload can be for the
// receiver.  This is part of the Message interface implementation.
func (msg *MsgExtendedTx) MaxPayloadLength(pver uint32) uint64 {
	return MaxBlockPayload()
}

// PkScriptLocs returns a slice containing the start of each public key script
// within the raw serialized transaction.  The caller can easily obtain the
// length of each script by using len on the script available via the
// appropriate transaction output entry.
func (msg *MsgExtendedTx) PkScriptLocs() []int {
	numTxOut := len(msg.TxOut)
	if numTxOut == 0 {
		return nil
	}

	// The starting offset in the serialized transaction of the first
	// transaction output is:
	//
	// Version 4 bytes + serialized varint size for the number of
	// transaction inputs and outputs + serialized size of each transaction
	// input.
	n := 4 + VarIntSerializeSize(uint64(len(msg.TxIn))) +
		VarIntSerializeSize(uint64(numTxOut))

	for _, txIn := range msg.TxIn {
		n += txIn.SerializeSize()
	}

	// Calculate and set the appropriate offset for each public key script.
	pkScriptLocs := make([]int, numTxOut)
	for i, txOut := range msg.TxOut {
		// The offset of the script in the transaction output is:
		//
		// Value 8 bytes + serialized varint size for the length of
		// PkScript.
		n += 8 + VarIntSerializeSize(uint64(len(txOut.PkScript)))
		pkScriptLocs[i] = n
		n += len(txOut.PkScript)
	}

	return pkScriptLocs
}

// NewMsgExtendedTx returns a new extended bitcoin tx message that conforms to the Message
// interface.  The return instance has a default version of TxVersion and there
// are no transaction inputs or outputs.  Also, the lock time is set to zero
// to indicate the transaction is valid immediately as opposed to some time in
// future.
func NewMsgExtendedTx(version int32) *MsgExtendedTx {
	return &MsgExtendedTx{
		Version: version,
		TxIn:    make([]*ExtendedTxIn, 0, defaultTxInOutAlloc),
		TxOut:   make([]*TxOut, 0, defaultTxInOutAlloc),
	}
}

// readExtendedTxIn reads the next sequence of bytes from r as a transaction input
// (TxIn).
func readExtendedTxIn(r io.Reader, pver uint32, version int32, ti *ExtendedTxIn) error {
	err := readOutPoint(r, pver, version, &ti.PreviousOutPoint)
	if err != nil {
		return err
	}

	ti.SignatureScript, err = readScript(r, pver, maxTxInPerMessage(),
		"transaction input signature script")
	if err != nil {
		return err
	}

	err = readElement(r, &ti.Sequence)
	if err != nil {
		return err
	}

	err = readElement(r, &ti.PreviousTxSatoshis)
	if err != nil {
		return err
	}

	// read the previous tx script
	ti.PreviousTxScript, err = readScript(r, pver, maxTxInPerMessage(),
		"transaction previous tx script")
	if err != nil {
		return err
	}

	return nil
}

// writeExtendedTxIn encodes ti to the bitcoin protocol encoding for a transaction
// input (TxIn) to w.
func writeExtendedTxIn(w io.Writer, pver uint32, version int32, ti *ExtendedTxIn) error {
	err := writeOutPoint(w, pver, version, &ti.PreviousOutPoint)
	if err != nil {
		return err
	}

	err = WriteVarBytes(w, pver, ti.SignatureScript)
	if err != nil {
		return err
	}

	err = binarySerializer.PutUint32(w, littleEndian, ti.Sequence)
	if err != nil {
		return err
	}

	err = binarySerializer.PutUint64(w, littleEndian, ti.PreviousTxSatoshis)
	if err != nil {
		return err
	}

	// write the previous tx script
	err = WriteVarBytes(w, pver, ti.PreviousTxScript)
	if err != nil {
		return err
	}

	return nil
}
