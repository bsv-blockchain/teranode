package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
)

type UTXO struct {
	Txid            []byte
	Index           uint32
	Satoshis        uint64
	UnlockingScript []byte
}

func NewUTXO(txid []byte, index uint32, satoshis uint64, unlockingScript []byte) *UTXO {
	return &UTXO{
		Txid:            txid,
		Index:           index,
		Satoshis:        satoshis,
		UnlockingScript: unlockingScript,
	}
}

func (u *UTXO) KeyHash() [32]byte {
	var buf bytes.Buffer

	buf.Write(u.Txid)
	_ = binary.Write(&buf, binary.LittleEndian, u.Index)
	buf.Write(u.UnlockingScript)
	_ = binary.Write(&buf, binary.LittleEndian, u.Satoshis)

	return sha256.Sum256(buf.Bytes())
}
