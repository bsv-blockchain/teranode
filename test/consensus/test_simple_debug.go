package consensus

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
)

func TestSimpleDebug(t *testing.T) {
	fmt.Println("Starting simple debug test")

	// Create a simple script
	script := &bscript.Script{}
	_ = script.AppendOpcodes(bscript.Op1)

	fmt.Println("Script created")

	// Try to create test builder
	tb := NewTestBuilder(script, "Debug", SCRIPT_VERIFY_NONE, false, 0)

	fmt.Printf("TestBuilder created: %v\n", tb != nil)
}
