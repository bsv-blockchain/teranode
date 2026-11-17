package addrmgr_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/legacy/addrmgr"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestUnverifiedAddressDrawsOnlyFromNew pins the one thing the feeler probe
// needs from the address book: a draw restricted to addresses the node has
// never confirmed for itself.
//
// GetAddress tosses a coin between the tried and new tables. For a probe whose
// entire purpose is to move addresses INTO tried, roughly half of those draws
// would be wasted on addresses already there. svnode avoids that by passing
// newOnly to CAddrMan::Select (addrman.cpp:337); UnverifiedAddress is the same
// restriction.
func TestUnverifiedAddressDrawsOnlyFromNew(t *testing.T) {
	amgr := addrmgr.New(ulogger.TestLogger{}, "testunverifiedaddress", nil)

	src := wire.NewNetAddressIPPort(net.ParseIP("173.194.115.1"), 8333, wire.SFNodeNetwork)

	added := make([]*wire.NetAddress, 0, 20)

	for i := 0; i < 20; i++ {
		na := wire.NewNetAddressIPPort(net.ParseIP(fmt.Sprintf("8.8.%d.1", i)), 8333, wire.SFNodeNetwork)
		amgr.AddAddress(na, src)
		added = append(added, na)
	}

	require.Equal(t, 20, amgr.NumAddresses())

	// Promote half of them, so the tried table is populated and a 50/50 draw
	// would land there about half the time.
	promoted := make(map[string]struct{}, 10)

	for _, na := range added[:10] {
		amgr.Good(na)
		promoted[addrmgr.NetAddressKey(na)] = struct{}{}
	}

	for i := 0; i < 200; i++ {
		ka := amgr.UnverifiedAddress()
		require.NotNil(t, ka, "the new table still holds ten addresses")
		require.False(t, addrmgr.TstAddressIsTried(amgr, ka.NetAddress),
			"UnverifiedAddress must never return an address that is already in tried")
		require.NotContains(t, promoted, addrmgr.NetAddressKey(ka.NetAddress))
	}

	// Companion assertion: extracting selectNew out of GetAddress must not have
	// cost GetAddress its tried half.
	sawTried := false

	for i := 0; i < 200 && !sawTried; i++ {
		if ka := amgr.GetAddress(); ka != nil && addrmgr.TstKnownAddressTried(ka) {
			sawTried = true
		}
	}

	require.True(t, sawTried, "GetAddress must still be able to return a tried address")
}

// TestUnverifiedAddressReturnsOnEmptyNewTable covers the guard that stops the
// bucket loop spinning.
//
// selectNew loops until it finds an address and has no termination condition of
// its own, so without the nNew check an empty new table hangs the caller — and
// the caller is the feeler goroutine. The timeout is what turns that hang into
// a test failure rather than a stuck run.
func TestUnverifiedAddressReturnsOnEmptyNewTable(t *testing.T) {
	amgr := addrmgr.New(ulogger.TestLogger{}, "testunverifiedaddressempty", nil)

	// An address that has been promoted leaves the new table empty while the
	// manager still knows about it, which is the state that would spin.
	na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
	src := wire.NewNetAddressIPPort(net.ParseIP("173.194.115.1"), 8333, wire.SFNodeNetwork)

	amgr.AddAddress(na, src)
	amgr.Good(na)

	require.Equal(t, 1, amgr.NumAddresses(), "the address is still known, it has just moved table")

	done := make(chan *addrmgr.UnverifiedAddress, 1)

	go func() { done <- amgr.UnverifiedAddress() }()

	select {
	case ka := <-done:
		require.Nil(t, ka, "an empty new table has nothing to probe")
	case <-time.After(5 * time.Second):
		t.Fatal("UnverifiedAddress did not return on an empty new table")
	}
}
