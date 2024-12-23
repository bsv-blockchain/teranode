// Copyright (c) 2014 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bsvjson_test

import (
	"reflect"
	"testing"

	"github.com/bitcoin-sv/teranode/services/rpc/bsvjson"
)

// TestHelpers tests the various helper functions which create pointers to
// primitive types.
func TestHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		f        func() interface{}
		expected interface{}
	}{
		{
			name: "bool",
			f: func() interface{} {
				return bsvjson.Bool(true)
			},
			expected: func() interface{} {
				val := true
				return &val
			}(),
		},
		{
			name: "int",
			f: func() interface{} {
				return bsvjson.Int(5)
			},
			expected: func() interface{} {
				val := int(5)
				return &val
			}(),
		},
		{
			name: "uint",
			f: func() interface{} {
				return bsvjson.Uint(5)
			},
			expected: func() interface{} {
				val := uint(5)
				return &val
			}(),
		},
		{
			name: "int32",
			f: func() interface{} {
				return bsvjson.Int32(5)
			},
			expected: func() interface{} {
				val := int32(5)
				return &val
			}(),
		},
		{
			name: "uint32",
			f: func() interface{} {
				return bsvjson.Uint32(5)
			},
			expected: func() interface{} {
				val := uint32(5)
				return &val
			}(),
		},
		{
			name: "int64",
			f: func() interface{} {
				return bsvjson.Int64(5)
			},
			expected: func() interface{} {
				val := int64(5)
				return &val
			}(),
		},
		{
			name: "uint64",
			f: func() interface{} {
				return bsvjson.Uint64(5)
			},
			expected: func() interface{} {
				val := uint64(5)
				return &val
			}(),
		},
		{
			name: "string",
			f: func() interface{} {
				return bsvjson.String("abc")
			},
			expected: func() interface{} {
				val := "abc"
				return &val
			}(),
		},
	}

	t.Logf("Running %d tests", len(tests))

	for i, test := range tests {
		result := test.f()
		if !reflect.DeepEqual(result, test.expected) {
			t.Errorf("Test #%d (%s) unexpected value - got %v, "+
				"want %v", i, test.name, result, test.expected)
			continue
		}
	}
}
