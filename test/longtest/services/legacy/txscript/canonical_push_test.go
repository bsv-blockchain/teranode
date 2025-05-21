// Copyright (c) 2013-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package txscript

import (
	"bytes"
	"testing"

	"github.com/bitcoin-sv/teranode/services/legacy/txscript"
)

// go test -v -tags test_txscript ./test/...

// TestHasCanonicalPush ensures the canonicalPush function works as expected.
func TestHasCanonicalPush(t *testing.T) {
	t.Parallel()

	for i := 0; i < 65535; i++ {
		script, err := txscript.NewScriptBuilder().AddInt64(int64(i)).Script()
		if err != nil {
			t.Errorf("Script: test #%d unexpected error: %v\n", i,
				err)
			continue
		}
		if result := txscript.IsPushOnlyScript(script); !result {
			t.Errorf("IsPushOnlyScript: test #%d failed: %x\n", i,
				script)
			continue
		}
		pops, err := txscript.ParseScript(script)
		if err != nil {
			t.Errorf("parseScript: #%d failed: %v", i, err)
			continue
		}
		for _, pop := range pops {
			if result := txscript.CanonicalPush(pop); !result {
				t.Errorf("canonicalPush: test #%d failed: %x\n",
					i, script)
				break
			}
		}
	}
	for i := 0; i <= txscript.MaxScriptElementSize; i++ {
		builder := txscript.NewScriptBuilder()
		builder.AddData(bytes.Repeat([]byte{0x49}, i))
		script, err := builder.Script()
		if err != nil {
			t.Errorf("StandardPushesTests test #%d unexpected error: %v\n", i, err)
			continue
		}
		if result := txscript.IsPushOnlyScript(script); !result {
			t.Errorf("StandardPushesTests IsPushOnlyScript test #%d failed: %x\n", i, script)
			continue
		}
		pops, err := txscript.ParseScript(script)
		if err != nil {
			t.Errorf("StandardPushesTests #%d failed to TstParseScript: %v", i, err)
			continue
		}
		for _, pop := range pops {
			if result := txscript.CanonicalPush(pop); !result {
				t.Errorf("StandardPushesTests TstHasCanonicalPushes test #%d failed: %x\n", i, script)
				break
			}
		}
	}
}
