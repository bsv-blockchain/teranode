package work

import (
	"fmt"
	"testing"

	"github.com/bitcoin-sv/ubsv/model"
)

func TestTarget(t *testing.T) {
	// bits, _ := hex.DecodeString("1d00ffff")
	bits, _ := model.NewNBitFromString("2000ffff")

	target := bits.CalculateTarget()

	s := fmt.Sprintf("%064x", target.Bytes())

	t.Logf("Target: %s, len: %d", s, len(s)/2)
}

// 00ffff000000000000000000000000000000000000000000000000000000000000
//   00ffff0000000000000000000000000000000000000000000000000000000000
