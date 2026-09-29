package bsvjson_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
)

// maxRPCFuzzInput caps the request body the fuzz targets will parse.
//
// The RPC server rejects bodies over rpc_maxRequestSize (10 MB by default,
// enforced by http.MaxBytesReader before anything is read), so nothing here
// needs to model an unbounded body. 64 KB keeps the exec rate high while still
// leaving room for the nesting and array sizes that matter: a legitimate
// JSON-RPC request is under 1 KB.
const maxRPCFuzzInput = 64 * 1024

// hostileParams are parameter shapes chosen to attack the reflection in
// UnmarshalCmd rather than the JSON syntax: wrong types where a concrete type
// is expected, numbers that do not fit the destination field, and arrays where
// a scalar is expected. assignField converts between kinds by hand, which is
// where a conversion that is merely unchecked becomes a panic.
var hostileParams = []string{
	`[]`,
	`[null]`,
	`[null,null,null,null,null,null,null,null]`,
	`[true]`,
	`[-1]`,
	`[18446744073709551615]`,
	`[-9223372036854775808]`,
	`[1e309]`,
	`[{}]`,
	`[[]]`,
	`[[[[[[[[[[]]]]]]]]]]`,
	`["",""]`,
	`[""]`,
	`["0000000000000000000000000000000000000000000000000000000000000000"]`,
	`[0,1,2,3,4,5,6,7,8,9,10]`,
	`[{"a":1},{"b":2}]`,
	`["\ud800"]`,
}

// seedRequests returns one well-formed JSON-RPC body per registered method,
// crossed with the hostile parameter shapes. Seeding from the live registry
// rather than a hand-written list means a newly registered command is fuzzed
// the day it lands, with no seed list to forget to update.
func seedRequests() []string {
	methods := bsvjson.RegisteredCmdMethods()
	bodies := make([]string, 0, len(methods)*2+len(hostileParams))

	for _, m := range methods {
		bodies = append(bodies,
			fmt.Sprintf(`{"jsonrpc":"1.0","id":1,"method":%q,"params":[]}`, m),
			fmt.Sprintf(`{"jsonrpc":"1.0","id":1,"method":%q,"params":[null,null,null]}`, m),
		)
	}

	for _, p := range hostileParams {
		bodies = append(bodies,
			fmt.Sprintf(`{"jsonrpc":"1.0","id":1,"method":"getblock","params":%s}`, p))
	}

	return bodies
}

// FuzzParseRPCRequest feeds arbitrary bytes through the exact sequence the RPC
// server applies to an untrusted HTTP body: json.Unmarshal into a
// bsvjson.Request, then UnmarshalCmd on it (services/rpc/Server.go, and
// parseCmd just below it). Anyone who can reach the RPC port controls these
// bytes in full, so a panic here takes the node down from a single request.
//
// UnmarshalCmd is the part worth fuzzing. It is roughly 350 lines of
// reflection over ~145 registered commands — reflect.Value.Field indexed by
// parameter position, pointer chasing through baseType, and hand-rolled
// numeric and string conversions in assignField. Every one of those is driven
// by the method name and parameter list in the request.
func FuzzParseRPCRequest(f *testing.F) {
	for _, body := range seedRequests() {
		f.Add([]byte(body))
	}

	f.Add([]byte(``))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"method":"getblock"}`))
	f.Add([]byte(`{"method":null,"params":null,"id":null}`))
	f.Add([]byte(`[]`))

	f.Fuzz(func(_ *testing.T, data []byte) {
		if len(data) > maxRPCFuzzInput {
			data = data[:maxRPCFuzzInput]
		}

		var request bsvjson.Request
		if err := json.Unmarshal(data, &request); err != nil {
			return // a malformed body is rejected before parseCmd is reached
		}

		// Must not panic. An error is the expected outcome for anything the
		// server would answer with ErrRPCInvalidParams or ErrRPCMethodNotFound.
		_, _ = bsvjson.UnmarshalCmd(&request)
	})
}

// FuzzUnmarshalCmd is the structured companion to FuzzParseRPCRequest. It
// holds the JSON-RPC envelope fixed and mutates only the method name and the
// parameter list, so the budget is spent inside the reflection rather than on
// rediscovering that arbitrary bytes are not valid JSON. In a byte-oriented
// target almost every mutation dies at json.Unmarshal and never reaches a
// registered command at all.
func FuzzUnmarshalCmd(f *testing.F) {
	for _, m := range bsvjson.RegisteredCmdMethods() {
		f.Add(m, `[]`)
		f.Add(m, `[null]`)
	}

	for _, p := range hostileParams {
		f.Add("getblock", p)
		f.Add("getrawtransaction", p)
		f.Add("createrawtransaction", p)
	}

	f.Add("", `[]`)
	f.Add("notaregisteredmethod", `[1,2,3]`)

	f.Fuzz(func(_ *testing.T, method, params string) {
		if len(params) > maxRPCFuzzInput {
			params = params[:maxRPCFuzzInput]
		}

		// Encode the method with json.Marshal rather than %q: %q emits Go
		// escapes such as \xff, which are not valid JSON, so every mutation
		// that produced a non-UTF-8 method name would be thrown away at
		// json.Unmarshal below instead of reaching the registry lookup.
		encodedMethod, err := json.Marshal(method)
		if err != nil {
			return
		}

		body := fmt.Sprintf(`{"jsonrpc":"1.0","id":1,"method":%s,"params":%s}`, encodedMethod, params)

		var request bsvjson.Request
		if err := json.Unmarshal([]byte(body), &request); err != nil {
			return // the fuzzed params were not a valid JSON array
		}

		_, _ = bsvjson.UnmarshalCmd(&request)
	})
}
