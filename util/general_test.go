package util

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	primitives "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/stretchr/testify/assert"
)

func TestKeys(t *testing.T) {
	wifKey := "L56TgyTpDdvL3W24SMoALYotibToSCySQeo4pThLKxw6EFR6f93Q"

	pk, err := primitives.PrivateKeyFromWif(wifKey)
	if err != nil {
		t.Fatal(err)
	}

	walletAddress, err := bscript.NewAddressFromPublicKey(pk.PubKey(), false)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "myL4TciLD59ESU9MmKH1rvfYb8QXhFHHN6", walletAddress.AddressString)
}
