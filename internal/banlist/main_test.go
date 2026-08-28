package banlist

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if a test leaves a goroutine running. This package
// is gated because it can actually fail: before the store cleanup in
// newTestBanList it reported around 111 leaked goroutines on the committed
// defaults, or around 167 with blockchain_use_in_memory_chain_check enabled
// (4 and 6 per store respectively, across 28 tests), and zero after.
//
// One limit to know before copying this: the check only runs when every test in
// the package passed — VerifyTestMain calls Find only on a zero exit code. A
// package with a flaky test therefore reports no leaks and looks clean, so a
// flake has to be fixed before the result here means anything.
//
// The single ignore is gocore's init-time goroutine, which is a bare
// `go func() { for { ...; time.Sleep(1ms) } }()` with no context and no exit
// path — genuinely unstoppable, so ignoring it is the only option. It must be
// matched by name via IgnoreAnyFunction rather than IgnoreTopFunction: the
// goroutine's top frame is time.Sleep, so ignoring the top frame would blind
// the check to every sleeping goroutine in the tree.
//
// Nothing else is ignored, deliberately. The other background goroutines seen
// across this repo — database pools, ttl caches, gRPC callback serialisers,
// the Aerospike cluster worker — all exit when the object owning them is
// closed. Ignoring those would hide exactly the leak this check exists to
// catch: a long-lived service holding an unclosed store.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/ordishs/gocore.init.0.func1"),
	)
}
