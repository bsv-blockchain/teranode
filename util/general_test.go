package util

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/libsv/go-bk/wif"
	"github.com/stretchr/testify/assert"
)

func TestKeys(t *testing.T) {
	wifKey := "L56TgyTpDdvL3W24SMoALYotibToSCySQeo4pThLKxw6EFR6f93Q"

	w, err := wif.DecodeWIF(wifKey)
	if err != nil {
		t.Fatal(err)
	}

	walletAddress, err := bscript.NewAddressFromPublicKey(w.PrivKey.PubKey(), false)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "myL4TciLD59ESU9MmKH1rvfYb8QXhFHHN6", walletAddress.AddressString)
}
