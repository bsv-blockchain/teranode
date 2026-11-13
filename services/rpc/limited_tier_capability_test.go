package rpc

// The limited tier is a permission set, not a capability list.
//
// rpcLimited admits a limited credential to 44 method names; nineteen of
// them dispatch to handleUnimplemented (notes section 59). A monitoring
// fleet holding read-only credentials therefore reaches "method not
// implemented" on exactly the calls it is most likely to make - getblockcount
// above all - and nothing in the tier check can see that coming, because the
// check asks only whether a name is allowed.
//
// The recorded decision (fix list F5) is to label the list as permission
// rather than prune it: pruning changes which error text an integration sees
// and is the kind of behaviour change to make deliberately, once. These
// tests pin the mismatch set so the labelling prose in rpc_limit_user's
// longdesc cannot drift out of date silently: if the intersection changes,
// either a method was implemented (good - update the list and the prose) or
// a new allowlisted name went unwired (a finding - fix it before a fleet
// discovers it).

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// limitedButUnimplemented is the intersection the tier check cannot detect:
// names a limited caller may ask for that dispatch to handleUnimplemented.
// Go funcs are uncomparable, so the placeholder is identified by code
// pointer identity against the known function.
func limitedButUnimplemented() map[string]bool {
	placeholder := reflect.ValueOf(handleUnimplemented).Pointer()

	out := map[string]bool{}
	for name, handler := range rpcHandlersBeforeInit {
		if _, allowed := rpcLimited[name]; !allowed {
			continue
		}
		if handler == nil {
			out[name] = true
			continue
		}
		if reflect.ValueOf(handler).Pointer() == placeholder {
			out[name] = true
		}
	}
	return out
}

func TestRpcLimited_UnimplementedIntersectionIsPinned(t *testing.T) {
	// The enumeration from section 59, verbatim, as of this commit.
	want := []string{
		"decoderawtransaction", "decodescript", "estimatefee", "getbestblock",
		"getblockcount", "getcfilter", "getcfilterheader", "getcurrentnet",
		"getheaders", "getnettotals", "getnetworkhashps", "gettxout",
		"gettxoutproof", "searchrawtransactions", "submitblock", "uptime",
		"validateaddress", "verifymessage", "verifytxoutproof",
	}

	got := limitedButUnimplemented()

	for _, name := range want {
		require.True(t, got[name],
			"allowlisted method %s was expected unimplemented; if it has now been implemented, update this test AND keep rpc_limit_user's longdesc honest", name)
	}

	require.Len(t, got, len(want),
		"the set of allowlisted-but-unimplemented methods changed; the permission-vs-capability labelling in settings/rpc_settings.go and this test must be updated together")
}

func TestRpcLimited_IntersectionNamesAreAllAllowlisted(t *testing.T) {
	// Sanity in the other direction: the pinned names above really are the
	// ones a limited credential passes the tier check on.
	pinned := []string{
		"decoderawtransaction", "decodescript", "estimatefee", "getbestblock",
		"getblockcount", "getcfilter", "getcfilterheader", "getcurrentnet",
		"getheaders", "getnettotals", "getnetworkhashps", "gettxout",
		"gettxoutproof", "searchrawtransactions", "submitblock", "uptime",
		"validateaddress", "verifymessage", "verifytxoutproof",
	}

	for _, name := range pinned {
		_, allowed := rpcLimited[name]
		require.True(t, allowed, "pinned name %s must remain in rpcLimited for this test to mean what it says", name)
	}
}
