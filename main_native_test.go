//go:build native

package main

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	msg, _    = hex.DecodeString("269d204413554cf4099df30554c8060ecc5f28302252167e6cc6c563c28dad7f")
	sig, _    = hex.DecodeString("304402206BA39DD04FCDDF34CA26F79FDD82E6238A1607BE01EB7F64A53CC83C567E46EE022039265C4D4CA4817FECBB42C943BEF51166C63F640DAD0A555A7A23221A894ECB")
	pubkey, _ = hex.DecodeString("0390c85d6d1f222d2780996ca0666c483986e1762fd46be8fe80750285787186fd")
)

func TestVerifySignature(t *testing.T) {
	res := VerifySignature(msg, sig, pubkey)
	assert.True(t, res)
}

// Run this test continuously to see of there is a memory leak
//func TestVerifySignature_Memory(t *testing.T) {
//	for {
//		res := VerifySignature(msg, sig, pubkey)
//		assert.True(t, res)
//	}
//}

func BenchmarkVerifySignature(b *testing.B) {
	for i := 0; i < b.N; i++ {
		VerifySignature(msg, sig, pubkey)
	}
}
