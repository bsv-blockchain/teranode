package blockassembly

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReorgNeedsReset(t *testing.T) {
	const maturity = 100

	tests := []struct {
		name        string
		moveBack    int
		moveForward int
		height      uint32
		maturity    uint32
		retention   uint32
		want        bool
	}{
		// legacy behaviour: maturity-deep reorgs reset only above height 1000
		{"high height, depth below maturity", 99, 0, 5000, maturity, 288, false},
		{"high height, moveBack at maturity", 100, 0, 5000, maturity, 288, true},
		{"high height, moveForward at maturity", 0, 100, 5000, maturity, 288, true},
		{"height exactly 1000, depth at maturity", 100, 0, 1000, maturity, 288, false},
		{"height 1001, depth at maturity", 100, 0, 1001, maturity, 288, true},
		{"low height, depth above maturity but below retention", 200, 0, 500, maturity, 288, false},
		{"low height, shallow reorg", 1, 1, 10, maturity, 288, false},

		// retention: reorgBlocks is unsafe once the deeper side reaches retention, at any height
		{"low height, moveBack just below retention", 287, 0, 500, maturity, 288, false},
		{"low height, moveBack at retention", 288, 0, 500, maturity, 288, true},
		{"low height, moveForward at retention", 0, 288, 500, maturity, 288, true},
		{"low height, moveForward above retention", 0, 5000, 500, maturity, 288, true},
		{"height 0, depth at retention", 288, 0, 0, maturity, 288, true},
		{"high height, depth above retention", 400, 0, 5000, maturity, 288, true},

		// retention smaller than maturity
		{"retention 1, depth 0", 0, 0, 500, maturity, 1, false},
		{"retention 1, depth 1 low height", 1, 0, 500, maturity, 1, true},
		{"retention 1, depth 1 forward", 0, 1, 5, maturity, 1, true},
		{"retention 5, depth 4 low height", 4, 4, 500, maturity, 5, false},
		{"retention 5, depth 5 low height", 0, 5, 500, maturity, 5, true},
		{"retention 5, depth 5 high height", 5, 0, 5000, maturity, 5, true},
		{"retention 5, depth 4 high height", 4, 0, 5000, maturity, 5, false},

		// retention plus adjustment is the caller's effective value: 288+(-100)=188, 288+200=488
		{"adjusted retention 188, depth 187", 187, 0, 500, maturity, 188, false},
		{"adjusted retention 188, depth 188", 188, 0, 500, maturity, 188, true},
		{"adjusted retention 488, depth 300 low height", 300, 0, 500, maturity, 488, false},
		{"adjusted retention 488, depth 300 high height", 300, 0, 5000, maturity, 488, true},
		{"adjusted retention 488, depth 488 low height", 0, 488, 500, maturity, 488, true},

		// retention 0 means pruning disabled: only the legacy rule applies
		{"retention 0, deep, low height", 5000, 0, 500, maturity, 0, false},
		{"retention 0, deep, high height", 5000, 0, 5000, maturity, 0, true},

		// test-style settings: maturity 1
		{"maturity 1, low height, depth 1", 1, 0, 10, 1, 288, false},
		{"maturity 1, high height, depth 1", 1, 0, 2000, 1, 288, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := reorgNeedsReset(tt.moveBack, tt.moveForward, tt.height, tt.maturity, tt.retention)
			assert.Equal(t, tt.want, got)
		})
	}
}
