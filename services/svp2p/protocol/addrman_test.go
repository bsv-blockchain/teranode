package protocol

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// The cases in this file are a direct port of
// bitcoin-sv@879fc8b42 src/test/addrman_tests.cpp. Every BOOST_CHECK constant
// is carried over verbatim; the C++ case name and line are named above each
// Go test so a reviewer can diff them side by side.
//
// The C++ fixture is `class CAddrManTest : public CAddrMan` (addrman_tests.cpp:14)
// whose MakeDeterministic() does `nKey.SetNull(); insecure_rand =
// FastRandomContext(true);` and overrides RandomInt with a uint64 hash chain
// seeded at 1. newTestAddrMan below is that fixture.

// resolveIP mirrors the C++ helper `static CNetAddr ResolveIP(const char *ip)`
// (addrman_tests.cpp:41). Every address in these vectors is a numeric literal,
// so a parse stands in for the C++ LookupHost call.
func resolveIP(t *testing.T, ip string) net.IP {
	t.Helper()

	parsed := net.ParseIP(ip)
	require.NotNil(t, parsed, "failed to resolve: %s", ip)

	return parsed
}

// resolveService mirrors `static CService ResolveService(const char *ip, int
// port = 0)` (addrman_tests.cpp:52). The C++ default port is 0 and several
// vectors rely on that, so callers pass it explicitly here.
func resolveService(t *testing.T, ip string, port uint16) Address {
	t.Helper()

	return NewAddress(resolveIP(t, ip), port, 0)
}

// resolveServiceAt is resolveService with an explicit nTime, for the vectors
// that do `addr.nTime = GetAdjustedTime();` before adding. Address is opaque, so
// the timestamp is a constructor argument rather than a field write.
func resolveServiceAt(t *testing.T, ip string, port uint16, nTime int64) Address {
	t.Helper()

	return NewAddressAtTime(resolveIP(t, ip), port, 0, nTime)
}

// hashKeyFromInt reproduces the C++ test's key derivation
// `uint256 nKey1 = (uint256)(CHashWriter(SER_GETHASH, 0) << 1).GetHash();`
// (addrman_tests.cpp:416). A C++ `int` serialises as 4 bytes little-endian.
func hashKeyFromInt(v int32) [32]byte {
	var h addrHasher

	h.int32(v)

	return h.hash()
}

func newTestAddrMan(t *testing.T) *AddrMan {
	t.Helper()

	a := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{})
	a.makeDeterministic()

	return a
}

// ---------------------------------------------------------------------------
// caddrinfo_get_tried_bucket (addrman_tests.cpp:403)
// ---------------------------------------------------------------------------

func TestCAddrInfoGetTriedBucket(t *testing.T) {
	addr1 := resolveService(t, "250.1.1.1", 8333)
	addr2 := resolveService(t, "250.1.1.1", 9999)

	source1 := resolveIP(t, "250.1.1.1")

	info1 := newAddrInfo(addr1, source1)

	nKey1 := hashKeyFromInt(1)
	nKey2 := hashKeyFromInt(2)

	// BOOST_CHECK(info1.GetTriedBucket(nKey1) == 40);
	require.Equal(t, 40, info1.getTriedBucket(nKey1))

	// Test 26: Make sure key actually randomizes bucket placement. A fail on
	//  this test could be a security issue.
	require.NotEqual(t, info1.getTriedBucket(nKey2), info1.getTriedBucket(nKey1))

	// Test 27: Two addresses with same IP but different ports can map to
	//  different buckets because they have different keys.
	info2 := newAddrInfo(addr2, source1)

	require.NotEqual(t, info2.getKey(), info1.getKey())
	require.NotEqual(t, info2.getTriedBucket(nKey1), info1.getTriedBucket(nKey1))

	buckets := make(map[int]struct{})

	for i := 0; i < 255; i++ {
		ip := "250.1.1." + itoa(i)
		infoi := newAddrInfo(resolveService(t, ip, 0), resolveIP(t, ip))
		buckets[infoi.getTriedBucket(nKey1)] = struct{}{}
	}
	// Test 28: IP addresses in the same group (\16 prefix for IPv4) should
	//  never get more than 8 buckets
	require.Len(t, buckets, 8)

	buckets = make(map[int]struct{})

	for j := 0; j < 255; j++ {
		ip := "250." + itoa(j) + ".1.1"
		infoj := newAddrInfo(resolveService(t, ip, 0), resolveIP(t, ip))
		buckets[infoj.getTriedBucket(nKey1)] = struct{}{}
	}
	// Test 29: IP addresses in the different groups should map to more than
	//  8 buckets.
	require.Len(t, buckets, 160)
}

// ---------------------------------------------------------------------------
// caddrinfo_get_new_bucket (addrman_tests.cpp:458)
// ---------------------------------------------------------------------------

func TestCAddrInfoGetNewBucket(t *testing.T) {
	addr1 := resolveService(t, "250.1.2.1", 8333)
	addr2 := resolveService(t, "250.1.2.1", 9999)

	source1 := resolveIP(t, "250.1.2.1")

	info1 := newAddrInfo(addr1, source1)

	nKey1 := hashKeyFromInt(1)
	nKey2 := hashKeyFromInt(2)

	// BOOST_CHECK(info1.GetNewBucket(nKey1) == 786);
	require.Equal(t, 786, info1.getNewBucketFromSource(nKey1))

	// Test 30: Make sure key actually randomizes bucket placement. A fail on
	//  this test could be a security issue.
	require.NotEqual(t, info1.getNewBucketFromSource(nKey2), info1.getNewBucketFromSource(nKey1))

	// Test 31: Ports should not effect bucket placement in the addr
	info2 := newAddrInfo(addr2, source1)
	require.NotEqual(t, info2.getKey(), info1.getKey())
	require.Equal(t, info2.getNewBucketFromSource(nKey1), info1.getNewBucketFromSource(nKey1))

	buckets := make(map[int]struct{})

	for i := 0; i < 255; i++ {
		ip := "250.1.1." + itoa(i)
		infoi := newAddrInfo(resolveService(t, ip, 0), resolveIP(t, ip))
		buckets[infoi.getNewBucketFromSource(nKey1)] = struct{}{}
	}
	// Test 32: IP addresses in the same group (\16 prefix for IPv4) should
	//  always map to the same bucket.
	require.Len(t, buckets, 1)

	buckets = make(map[int]struct{})

	for j := 0; j < 4*255; j++ {
		ip := itoa(250+(j/255)) + "." + itoa(j%256) + ".1.1"
		infoj := newAddrInfo(resolveService(t, ip, 0), resolveIP(t, "251.4.1.1"))
		buckets[infoj.getNewBucketFromSource(nKey1)] = struct{}{}
	}
	// Test 33: IP addresses in the same source groups should map to no more
	//  than 64 buckets.
	require.LessOrEqual(t, len(buckets), 64)

	buckets = make(map[int]struct{})

	for p := 0; p < 255; p++ {
		infoj := newAddrInfo(resolveService(t, "250.1.1.1", 0), resolveIP(t, "250."+itoa(p)+".1.1"))
		buckets[infoj.getNewBucketFromSource(nKey1)] = struct{}{}
	}
	// Test 34: IP addresses in the different source groups should map to more
	//  than 64 buckets.
	require.Greater(t, len(buckets), 64)
}

// ---------------------------------------------------------------------------
// addrman_simple (addrman_tests.cpp:65)
// ---------------------------------------------------------------------------

func TestAddrManSimple(t *testing.T) {
	addrman := newTestAddrMan(t)

	source := resolveIP(t, "252.2.2.2")

	// Test 1: Does Addrman respond correctly when empty.
	require.Equal(t, 0, addrman.Size())

	_, ok := addrman.Select(false)
	require.False(t, ok, "Select on an empty addrman must report no address")

	// Test 2: Does Addrman::Add work as expected.
	addr1 := resolveService(t, "250.1.1.1", 8333)
	addrman.Add(addr1, source, 0)
	require.Equal(t, 1, addrman.Size())

	ret1, ok := addrman.Select(false)
	require.True(t, ok)
	require.Equal(t, "250.1.1.1:8333", ret1.String())

	// Test 3: Does IP address deduplication work correctly.
	//  Expected dup IP should not be added.
	addr1Dup := resolveService(t, "250.1.1.1", 8333)
	addrman.Add(addr1Dup, source, 0)
	require.Equal(t, 1, addrman.Size())

	// Test 5: New table has one addr and we add a diff addr we should
	//  have two addrs.
	addr2 := resolveService(t, "250.1.1.2", 8333)
	addrman.Add(addr2, source, 0)
	require.Equal(t, 2, addrman.Size())

	// Test 6: AddrMan::Clear() should empty the new table.
	addrman.Clear()
	require.Equal(t, 0, addrman.Size())

	_, ok = addrman.Select(false)
	require.False(t, ok)
}

// ---------------------------------------------------------------------------
// addrman_ports (addrman_tests.cpp:104)
// ---------------------------------------------------------------------------

func TestAddrManPorts(t *testing.T) {
	addrman := newTestAddrMan(t)

	source := resolveIP(t, "252.2.2.2")
	now := time.Now().Unix()

	require.Equal(t, 0, addrman.Size())

	// Test 7; Addr with same IP but diff port does not replace existing addr.
	addr1 := resolveService(t, "250.1.1.1", 8333)
	addrman.Add(addr1, source, 0)
	require.Equal(t, 1, addrman.Size())

	addr1Port := resolveService(t, "250.1.1.1", 8334)
	addrman.Add(addr1Port, source, 0)
	require.Equal(t, 1, addrman.Size())

	ret2, ok := addrman.Select(false)
	require.True(t, ok)
	require.Equal(t, "250.1.1.1:8333", ret2.String())

	// Test 8: Add same IP but diff port to tried table, it doesn't get added.
	//  Perhaps this is not ideal behavior but it is the current behavior.
	addrman.Good(addr1Port, now)
	require.Equal(t, 1, addrman.Size())

	ret3, ok := addrman.Select(true)
	require.True(t, ok)
	require.Equal(t, "250.1.1.1:8333", ret3.String())
}

// ---------------------------------------------------------------------------
// addrman_select (addrman_tests.cpp:134)
// ---------------------------------------------------------------------------

func TestAddrManSelect(t *testing.T) {
	addrman := newTestAddrMan(t)

	source := resolveIP(t, "252.2.2.2")
	now := time.Now().Unix()

	// Test 9: Select from new with 1 addr in new.
	addr1 := resolveService(t, "250.1.1.1", 8333)
	addrman.Add(addr1, source, 0)
	require.Equal(t, 1, addrman.Size())

	ret1, ok := addrman.Select(true)
	require.True(t, ok)
	require.Equal(t, "250.1.1.1:8333", ret1.String())

	// Test 10: move addr to tried, select from new expected nothing returned.
	addrman.Good(addr1, now)
	require.Equal(t, 1, addrman.Size())

	_, ok = addrman.Select(true)
	require.False(t, ok)

	ret3, ok := addrman.Select(false)
	require.True(t, ok)
	require.Equal(t, "250.1.1.1:8333", ret3.String())

	require.Equal(t, 1, addrman.Size())

	// Add three addresses to new table.
	addr2 := resolveService(t, "250.3.1.1", 8333)
	addr3 := resolveService(t, "250.3.2.2", 9999)
	addr4 := resolveService(t, "250.3.3.3", 9999)

	addrman.Add(addr2, resolveIP(t, "250.3.1.1"), 0)
	addrman.Add(addr3, resolveIP(t, "250.3.1.1"), 0)
	addrman.Add(addr4, resolveIP(t, "250.4.1.1"), 0)

	// Add three addresses to tried table.
	addr5 := resolveService(t, "250.4.4.4", 8333)
	addr6 := resolveService(t, "250.4.5.5", 7777)
	addr7 := resolveService(t, "250.4.6.6", 8333)

	addrman.Add(addr5, resolveIP(t, "250.3.1.1"), 0)
	addrman.Good(addr5, now)
	addrman.Add(addr6, resolveIP(t, "250.3.1.1"), 0)
	addrman.Good(addr6, now)
	addrman.Add(addr7, resolveIP(t, "250.1.1.3"), 0)
	addrman.Good(addr7, now)

	// Test 11: 6 addrs + 1 addr from last test = 7.
	require.Equal(t, 7, addrman.Size())

	// Test 12: Select pulls from new and tried regardless of port number.
	ports := make(map[uint16]struct{})

	for i := 0; i < 20; i++ {
		sel, ok := addrman.Select(false)
		require.True(t, ok)

		ports[sel.Port()] = struct{}{}
	}

	require.Len(t, ports, 3)
}

// ---------------------------------------------------------------------------
// addrman_new_collisions (addrman_tests.cpp:194)
// ---------------------------------------------------------------------------

func TestAddrManNewCollisions(t *testing.T) {
	addrman := newTestAddrMan(t)

	source := resolveIP(t, "252.2.2.2")

	require.Equal(t, 0, addrman.Size())

	for i := 1; i < 18; i++ {
		addr := resolveService(t, "250.1.1."+itoa(i), 0)
		addrman.Add(addr, source, 0)

		// Test 13: No collision in new table yet.
		require.Equal(t, i, addrman.Size(), "unexpected size at i=%d", i)
	}

	// Test 14: new table collision!
	addr1 := resolveService(t, "250.1.1.18", 0)
	addrman.Add(addr1, source, 0)
	require.Equal(t, 17, addrman.Size())

	addr2 := resolveService(t, "250.1.1.19", 0)
	addrman.Add(addr2, source, 0)
	require.Equal(t, 18, addrman.Size())
}

// ---------------------------------------------------------------------------
// addrman_tried_collisions (addrman_tests.cpp:222)
// ---------------------------------------------------------------------------

func TestAddrManTriedCollisions(t *testing.T) {
	addrman := newTestAddrMan(t)

	source := resolveIP(t, "252.2.2.2")
	now := time.Now().Unix()

	require.Equal(t, 0, addrman.Size())

	for i := 1; i < 80; i++ {
		addr := resolveService(t, "250.1.1."+itoa(i), 0)
		addrman.Add(addr, source, 0)
		addrman.Good(addr, now)

		// Test 15: No collision in tried table yet.
		require.Equal(t, i, addrman.Size(), "unexpected size at i=%d", i)
	}

	// Test 16: tried table collision!
	addr1 := resolveService(t, "250.1.1.80", 0)
	addrman.Add(addr1, source, 0)
	require.Equal(t, 79, addrman.Size())

	addr2 := resolveService(t, "250.1.1.81", 0)
	addrman.Add(addr2, source, 0)
	require.Equal(t, 80, addrman.Size())
}

// ---------------------------------------------------------------------------
// addrman_find (addrman_tests.cpp:251)
// ---------------------------------------------------------------------------

func TestAddrManFind(t *testing.T) {
	addrman := newTestAddrMan(t)

	require.Equal(t, 0, addrman.Size())

	addr1 := resolveService(t, "250.1.2.1", 8333)
	addr2 := resolveService(t, "250.1.2.1", 9999)
	addr3 := resolveService(t, "251.255.2.1", 8333)

	source1 := resolveIP(t, "250.1.2.1")
	source2 := resolveIP(t, "250.1.2.2")

	addrman.Add(addr1, source1, 0)
	addrman.Add(addr2, source2, 0)
	addrman.Add(addr3, source1, 0)

	// Test 17: ensure Find returns an IP matching what we searched on.
	info1 := addrman.find(newNetAddr(addr1.IP()))
	require.NotNil(t, info1)
	require.Equal(t, "250.1.2.1:8333", info1.address().String())

	// Test 18; Find does not discriminate by port number.
	info2 := addrman.find(newNetAddr(addr2.IP()))
	require.NotNil(t, info2)
	require.Equal(t, info1.address().String(), info2.address().String())

	// Test 19: Find returns another IP matching what we searched on.
	info3 := addrman.find(newNetAddr(addr3.IP()))
	require.NotNil(t, info3)
	require.Equal(t, "251.255.2.1:8333", info3.address().String())
}

// ---------------------------------------------------------------------------
// addrman_create (addrman_tests.cpp:286)
// ---------------------------------------------------------------------------

func TestAddrManCreate(t *testing.T) {
	addrman := newTestAddrMan(t)

	require.Equal(t, 0, addrman.Size())

	addr1 := resolveService(t, "250.1.2.1", 8333)
	source1 := resolveIP(t, "250.1.2.1")

	pinfo, _ := addrman.create(addr1, newNetAddr(source1))

	// Test 20: The result should be the same as the input addr.
	require.Equal(t, "250.1.2.1:8333", pinfo.address().String())

	info2 := addrman.find(newNetAddr(addr1.IP()))
	require.NotNil(t, info2)
	require.Equal(t, "250.1.2.1:8333", info2.address().String())
}

// ---------------------------------------------------------------------------
// addrman_delete (addrman_tests.cpp:307)
// ---------------------------------------------------------------------------

func TestAddrManDelete(t *testing.T) {
	addrman := newTestAddrMan(t)

	require.Equal(t, 0, addrman.Size())

	addr1 := resolveService(t, "250.1.2.1", 8333)
	source1 := resolveIP(t, "250.1.2.1")

	_, nID := addrman.create(addr1, newNetAddr(source1))

	// Test 21: Delete should actually delete the addr.
	require.Equal(t, 1, addrman.Size())

	addrman.delete(nID)
	require.Equal(t, 0, addrman.Size())
	require.Nil(t, addrman.find(newNetAddr(addr1.IP())))
}

// ---------------------------------------------------------------------------
// addrman_getaddr (addrman_tests.cpp:329)
// ---------------------------------------------------------------------------

func TestAddrManGetAddr(t *testing.T) {
	addrman := newTestAddrMan(t)

	// Test 22: Sanity check, GetAddr should never return anything if addrman
	//  is empty.
	require.Equal(t, 0, addrman.Size())
	require.Empty(t, addrman.GetAddr())

	now := time.Now().Unix()

	// Times set so isTerrible = false.
	addr1 := resolveServiceAt(t, "250.250.2.1", 8333, now)
	addr2 := resolveServiceAt(t, "250.251.2.2", 9999, now)
	addr3 := resolveServiceAt(t, "251.252.2.3", 8333, now)
	addr4 := resolveServiceAt(t, "252.253.3.4", 8333, now)
	addr5 := resolveServiceAt(t, "252.254.4.5", 8333, now)

	source1 := resolveIP(t, "250.1.2.1")
	source2 := resolveIP(t, "250.2.3.3")

	// Test 23: Ensure GetAddr works with new addresses.
	addrman.Add(addr1, source1, 0)
	addrman.Add(addr2, source2, 0)
	addrman.Add(addr3, source1, 0)
	addrman.Add(addr4, source2, 0)
	addrman.Add(addr5, source1, 0)

	// GetAddr returns 23% of addresses, 23% of 5 is 1 rounded down.
	require.Len(t, addrman.GetAddr(), 1)

	// Test 24: Ensure GetAddr works with new and tried addresses.
	addrman.Good(addr1, now)
	addrman.Good(addr2, now)
	require.Len(t, addrman.GetAddr(), 1)

	// Test 25: Ensure GetAddr still returns 23% when addrman has many addrs.
	for i := 1; i < 8*256; i++ {
		octet1 := i % 256
		octet2 := (i / 256) % 256
		octet3 := (i / (256 * 2)) % 256

		strAddr := itoa(octet1) + "." + itoa(octet2) + "." + itoa(octet3) + ".23"

		// Ensure that for all addrs in addrman, isTerrible == false.
		addr := resolveServiceAt(t, strAddr, 0, now)
		addrman.Add(addr, resolveIP(t, strAddr), 0)

		if i%8 == 0 {
			addrman.Good(addr, now)
		}
	}

	vAddr := addrman.GetAddr()

	percent23 := (addrman.Size() * 23) / 100
	require.Len(t, vAddr, percent23)
	require.Len(t, vAddr, 461)
	// (Addrman.size() < number of addresses added) due to address collisons.
	require.Equal(t, 2007, addrman.Size())

	// Test 26: IsTerrible() is true for stale address > ADDRMAN_HORIZON_DAYS
	addr6 := resolveServiceAt(t, "252.254.5.6", 8333, now-(addrmanHorizonDays*24*60*60)-1)
	addrman.Add(addr6, source1, 0)

	info6 := addrman.find(newNetAddr(addr6.IP()))
	require.NotNil(t, info6)
	require.True(t, info6.isTerrible(now))
}

// ---------------------------------------------------------------------------
// Persistence — no C++ counterpart. peers.json is a fresh Teranode format
// (OPEN QUESTION 3, closed by the owner: fresh format, same filename, no
// read-compatibility with services/legacy/addrmgr's format).
// ---------------------------------------------------------------------------

// TestAddrManPersistenceRoundTrip proves Save then Load rebuilds identical
// tables: same nKey, same vvNew/vvTried occupancy, same per-entry state.
func TestAddrManPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "peers.json")

	src := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{Path: path})
	src.makeDeterministic()

	source := resolveIP(t, "252.2.2.2")
	now := time.Now().Unix()

	for i := 1; i < 40; i++ {
		addr := resolveServiceAt(t, "250.1.1."+itoa(i), uint16(8333+i), now)
		src.Add(addr, source, 0)

		if i%3 == 0 {
			src.Good(addr, now)
			src.Attempt(addr, true, now)
		}
	}

	for i := 1; i < 12; i++ {
		addr := resolveServiceAt(t, "251.4."+itoa(i)+".9", 8333, now)
		src.Add(addr, resolveIP(t, "251.4.1.1"), 0)
	}

	require.Greater(t, src.Size(), 20)
	require.Greater(t, src.triedCount(), 0)
	require.Greater(t, src.newCount(), 0)

	require.NoError(t, src.Save())

	dst := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{Path: path})
	require.NoError(t, dst.Load())

	require.Equal(t, src.Size(), dst.Size())
	require.Equal(t, src.triedCount(), dst.triedCount())
	require.Equal(t, src.newCount(), dst.newCount())
	require.Equal(t, src.nKey, dst.nKey)
	require.Equal(t, src.tableFingerprint(), dst.tableFingerprint())
}

// TestAddrManPersistenceDisabledIsSilent covers the DEFAULT path:
// legacy_savePeers is false (settings/settings.go:674), so Path is empty. No
// file may be written, no file read, no error returned, and no background
// goroutine started.
func TestAddrManPersistenceDisabledIsSilent(t *testing.T) {
	dir := t.TempDir()

	logger := &countingLogger{}

	a := NewAddrMan(logger, AddrManOptions{})
	a.makeDeterministic()

	require.NoError(t, a.Load())

	a.Add(resolveService(t, "250.1.1.1", 8333), resolveIP(t, "252.2.2.2"), 0)

	a.StartPersistence()
	require.False(t, a.persistenceRunning(), "no goroutine may start when persistence is disabled")

	require.NoError(t, a.Save())
	require.NoError(t, a.Stop())
	// Stop must stay safe when it never started.
	require.NoError(t, a.Stop())

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "nothing may be written when persistence is disabled")

	require.Zero(t, logger.warns, "the disabled path must not log warnings")
	require.Zero(t, logger.errors, "the disabled path must not log errors")
}

// TestAddrManPersistenceTickerStops proves the periodic goroutine is really
// stopped by Stop. It asserts on saveCount — a counter only the ticker's own
// work advances — rather than on a lookup that could not tell a live ticker
// from a dead one.
func TestAddrManPersistenceTickerStops(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")

	a := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{
		Path:         path,
		SaveInterval: 5 * time.Millisecond,
	})
	a.makeDeterministic()
	a.Add(resolveService(t, "250.1.1.1", 8333), resolveIP(t, "252.2.2.2"), 0)

	a.StartPersistence()
	require.True(t, a.persistenceRunning())

	require.Eventually(t, func() bool {
		return a.saveCount() >= 3
	}, 5*time.Second, 2*time.Millisecond, "periodic snapshot never ran")

	require.NoError(t, a.Stop())
	require.False(t, a.persistenceRunning())

	after := a.saveCount()

	time.Sleep(100 * time.Millisecond) // 20 ticker periods

	require.Equal(t, after, a.saveCount(), "ticker still firing after Stop")

	// Stop is idempotent and must not save again.
	require.NoError(t, a.Stop())
	require.Equal(t, after, a.saveCount())

	// The final save on Stop actually landed on disk.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

// TestAddrManStopSavesOnce proves Stop performs the final snapshot exactly
// once even when no ticker ever ran (StartPersistence never called).
func TestAddrManStopSavesWithoutStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")

	a := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{Path: path})
	a.makeDeterministic()
	a.Add(resolveService(t, "250.1.1.1", 8333), resolveIP(t, "252.2.2.2"), 0)

	require.NoError(t, a.Stop())
	require.Equal(t, uint64(1), a.saveCount())

	require.NoError(t, a.Stop())
	require.Equal(t, uint64(1), a.saveCount())

	reloaded := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{Path: path})
	require.NoError(t, reloaded.Load())
	require.Equal(t, 1, reloaded.Size())
}

// TestAddrManStartPersistenceAfterStop proves a StartPersistence that arrives
// after Stop starts nothing. Without the guard it would spawn a goroutine onto
// an already-closed quit channel — the goroutine would return at once, but
// persistenceRunning would then report a live ticker that does not exist.
func TestAddrManStartPersistenceAfterStop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")

	a := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{
		Path:         path,
		SaveInterval: 5 * time.Millisecond,
	})
	a.makeDeterministic()
	a.Add(resolveService(t, "250.1.1.1", 8333), resolveIP(t, "252.2.2.2"), 0)

	require.NoError(t, a.Stop())

	before := a.saveCount()

	a.StartPersistence()
	require.False(t, a.persistenceRunning(), "StartPersistence after Stop must start nothing")

	time.Sleep(50 * time.Millisecond) // 10 ticker periods

	require.Equal(t, before, a.saveCount(), "a ticker was started after Stop")
}

// TestAddrManLoadMissingFile: a cold start on the first svp2p run is accepted
// (OPEN QUESTION 3) — a missing peers.json is not an error.
func TestAddrManLoadMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")

	logger := &countingLogger{}

	a := NewAddrMan(logger, AddrManOptions{Path: path})
	require.NoError(t, a.Load())
	require.Equal(t, 0, a.Size())
	require.Zero(t, logger.errors)
}

// TestAddrManLoadBadAddressIsAtomic covers the input that
// TestAddrManLoadCorruptFile cannot reach: a peers.json that parses as JSON and
// passes every structural check, but whose SECOND new entry carries an `ip`
// string net.ParseIP rejects. The failure therefore happens deep inside the
// populate loop, after earlier entries have already been restored.
//
// Load must be all-or-nothing. The invariant that matters is C++'s
// `vRandom.size() == nTried + nNew` (the first check in CAddrMan::Check_,
// addrman.cpp:415: `if (vRandom.size() != nTried + nNew) return -7;`). If a
// partial load can publish len(vRandom) > 0 with nNew == 0 and nTried == 0,
// Select's new-node walk (addrman.cpp:388-397) can never find an occupied slot
// and spins forever holding cs.
func TestAddrManLoadBadAddressIsAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")

	doc := `{
	  "version": 1,
	  "nKey": "0000000000000000000000000000000000000000000000000000000000000000",
	  "newBucketCount": 1024,
	  "new": [
	    {"ip":"250.1.1.1","port":8333,"services":0,"time":1700000000,"source":"252.2.2.2","lastSuccess":0,"attempts":0},
	    {"ip":"250.1.1.2","port":8333,"services":0,"time":1700000000,"source":"252.2.2.2","lastSuccess":0,"attempts":0},
	    {"ip":"this-is-not-an-ip","port":8333,"services":0,"time":1700000000,"source":"252.2.2.2","lastSuccess":0,"attempts":0}
	  ],
	  "tried": [],
	  "newBuckets": []
	}`

	require.NoError(t, os.WriteFile(path, []byte(doc), 0o600))

	a := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{Path: path})
	a.makeDeterministic()

	require.Error(t, a.Load(), "an unparseable address must be reported")

	// The invariant, asserted before anything that could hang: a partially
	// populated table shows up here as Size() > 0 with both counters at 0.
	require.Equal(t, a.newCount()+a.triedCount(), a.Size(),
		"vRandom must hold exactly nNew+nTried entries after a failed load")

	// Nothing was published at all, so the tables are the empty ones a fresh
	// AddrMan starts with.
	require.Equal(t, 0, a.Size())

	// Only safe to call once the invariant above holds: against a
	// half-populated table this spins forever under cs.
	_, ok := a.Select(false)
	require.False(t, ok)
}

// TestAddrManLoadBadSourceIsAtomic is the same requirement for the `source`
// string, which restoreInfo validates separately.
func TestAddrManLoadBadSourceIsAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")

	doc := `{
	  "version": 1,
	  "nKey": "0000000000000000000000000000000000000000000000000000000000000000",
	  "newBucketCount": 1024,
	  "new": [
	    {"ip":"250.1.1.1","port":8333,"services":0,"time":1700000000,"source":"252.2.2.2","lastSuccess":0,"attempts":0}
	  ],
	  "tried": [
	    {"ip":"250.2.2.2","port":8333,"services":0,"time":1700000000,"source":"nonsense","lastSuccess":1700000000,"attempts":0}
	  ],
	  "newBuckets": [{"bucket":0,"entries":[0]}]
	}`

	require.NoError(t, os.WriteFile(path, []byte(doc), 0o600))

	a := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{Path: path})
	a.makeDeterministic()

	require.Error(t, a.Load())
	require.Equal(t, a.newCount()+a.triedCount(), a.Size(),
		"vRandom must hold exactly nNew+nTried entries after a failed load")
	require.Equal(t, 0, a.Size())
}

// TestAddrManLoadCorruptFile: a damaged snapshot must not stop the node.
func TestAddrManLoadCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	require.NoError(t, os.WriteFile(path, []byte("{not json"), 0o600))

	a := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{Path: path})
	require.Error(t, a.Load())
	require.Equal(t, 0, a.Size())
}

// TestAddrManConcurrentAccess exercises the self-locking contract: AddrMan is
// NOT covered by PeerManager.syncMu, it carries its own mutex (the port of
// CAddrMan's `mutable CCriticalSection cs`, addrman.h:185).
func TestAddrManConcurrentAccess(t *testing.T) {
	a := newTestAddrMan(t)
	source := resolveIP(t, "252.2.2.2")
	now := time.Now().Unix()

	// Addresses are resolved on the test goroutine: require may not be called
	// from a spawned one.
	work := make([][]Address, 4)

	for w := 0; w < 4; w++ {
		for i := 1; i < 60; i++ {
			addr := resolveServiceAt(t, "250."+itoa(w+1)+".1."+itoa(i), uint16(8333+i), now)
			work[w] = append(work[w], addr)
		}
	}

	done := make(chan struct{})

	for w := 0; w < 4; w++ {
		go func(addrs []Address) {
			defer func() { done <- struct{}{} }()

			for _, addr := range addrs {
				a.Add(addr, source, 0)
				a.Good(addr, now)
				a.Attempt(addr, true, now)
				a.Connected(addr, now)
				_, _ = a.Select(false)
				_ = a.GetAddr()
				_ = a.Size()
			}
		}(work[w])
	}

	for w := 0; w < 4; w++ {
		<-done
	}

	require.Greater(t, a.Size(), 0)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// itoa keeps the ported vectors visually close to their C++ std::to_string
// originals.
func itoa(v int) string {
	return strconv.Itoa(v)
}

type countingLogger struct {
	ulogger.TestLogger

	warns  int
	errors int
}

func (l *countingLogger) Warnf(string, ...any) { l.warns++ }
func (l *countingLogger) Errorf(string, ...any) {
	l.errors++
}

// assert the hasher lays bytes out the way CHashWriter does, so a reviewer can
// see the preimage rules the two bucket vectors depend on.
func TestAddrHasherSerialisation(t *testing.T) {
	var h addrHasher

	h.int32(1)
	require.Equal(t, []byte{0x01, 0x00, 0x00, 0x00}, h.buf)

	h = addrHasher{}
	h.uint64(0x0102030405060708)
	require.Equal(t, []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}, h.buf)

	h = addrHasher{}
	h.varBytes([]byte{0xaa, 0xbb})
	require.Equal(t, []byte{0x02, 0xaa, 0xbb}, h.buf)

	h = addrHasher{}
	h.byteVal('N')
	require.Equal(t, []byte{'N'}, h.buf)

	// GetCheapHash reads the first 8 digest bytes little-endian
	// (uint256.h:175: `return ReadLE64(data.data());`).
	h = addrHasher{}
	h.int32(1)
	digest := h.hash()
	require.Equal(t, binary.LittleEndian.Uint64(digest[:8]), h.cheapHash())
}

// ---------------------------------------------------------------------------
// Test-only accessors on AddrMan.
//
// These live here rather than in the production files because nothing in
// services/svp2p calls them: they exist to let a test see state the public
// surface deliberately does not expose. Same package, so they reach the
// unexported fields directly.
// ---------------------------------------------------------------------------

// triedCount and newCount expose nTried and nNew.
func (a *AddrMan) triedCount() int {
	a.cs.Lock()
	defer a.cs.Unlock()

	return a.nTried
}

func (a *AddrMan) newCount() int {
	a.cs.Lock()
	defer a.cs.Unlock()

	return a.nNew
}

// persistenceRunning reports whether the periodic goroutine is live, so a test
// can prove the disabled path starts nothing.
func (a *AddrMan) persistenceRunning() bool {
	return a.running.Load()
}

// saveCount is the number of snapshots actually written. A test can only tell a
// live ticker from a dead one by watching this: a lookup into the tables would
// look identical either way.
func (a *AddrMan) saveCount() uint64 {
	return a.saves.Load()
}

// tableFingerprint digests the full occupancy of both tables plus the stored
// state of every entry they hold. Two AddrMans with the same fingerprint have
// identical tables, which is what the persistence round-trip test asserts.
func (a *AddrMan) tableFingerprint() string {
	a.cs.Lock()
	defer a.cs.Unlock()

	h := sha256.New()

	writeSlot := func(table string, bucket, pos int, nID int) {
		if nID == emptyBucketSlot {
			return
		}

		info, ok := a.mapInfo[nID]
		if !ok {
			return
		}

		_, _ = h.Write([]byte(table))
		_, _ = h.Write(binary.LittleEndian.AppendUint32(nil, uint32(bucket)))
		_, _ = h.Write(binary.LittleEndian.AppendUint32(nil, uint32(pos)))
		_, _ = h.Write(info.addr.ip[:])
		_, _ = h.Write(binary.LittleEndian.AppendUint16(nil, info.port))
		_, _ = h.Write(binary.LittleEndian.AppendUint64(nil, uint64(info.nServices)))
		_, _ = h.Write(binary.LittleEndian.AppendUint64(nil, uint64(info.nTime)))
		_, _ = h.Write(info.source.ip[:])
		_, _ = h.Write(binary.LittleEndian.AppendUint64(nil, uint64(info.nLastSuccess)))
		_, _ = h.Write(binary.LittleEndian.AppendUint32(nil, uint32(info.nAttempts)))
		_, _ = h.Write(binary.LittleEndian.AppendUint32(nil, uint32(info.nRefCount)))
	}

	for bucket := 0; bucket < addrmanNewBucketCount; bucket++ {
		for pos := 0; pos < addrmanBucketSize; pos++ {
			writeSlot("new", bucket, pos, a.vvNew[bucket][pos])
		}
	}

	for bucket := 0; bucket < addrmanTriedBucketCount; bucket++ {
		for pos := 0; pos < addrmanBucketSize; pos++ {
			writeSlot("tried", bucket, pos, a.vvTried[bucket][pos])
		}
	}

	return hex.EncodeToString(h.Sum(nil))
}

// ---------------------------------------------------------------------------
// M3 / M8 coverage
// ---------------------------------------------------------------------------

// TestAddrManFailedLoadNeverOverwritesFile: after Load fails, neither the
// periodic ticker nor Stop's final save may write over the file. Otherwise a
// transient EACCES, or a corrupt snapshot an operator wanted to inspect, is
// destroyed by an empty table on the way out.
func TestAddrManFailedLoadNeverOverwritesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")

	original := []byte(`{"version":1,"nKey":"zz","new":[],"tried":[],"newBucketCount":1024,"newBuckets":[]}`)
	require.NoError(t, os.WriteFile(path, original, 0o600))

	a := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{
		Path:         path,
		SaveInterval: 5 * time.Millisecond,
	})
	a.makeDeterministic()

	require.Error(t, a.Load(), "a malformed nKey must be reported")

	a.Add(resolveService(t, "250.1.1.1", 8333), resolveIP(t, "252.2.2.2"), 0)

	// The periodic writer must not overwrite it either.
	a.StartPersistence()
	time.Sleep(50 * time.Millisecond) // 10 ticker periods

	require.NoError(t, a.Stop())
	require.Zero(t, a.saveCount(), "no snapshot may be written after a failed load")

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, original, after, "the unreadable peers.json was overwritten")
}

// TestAddrManNowSeamGatesIsTerrible exercises the injected `now` seam and pins
// isTerrible at the ADDRMAN_HORIZON_DAYS boundary.
//
// The boundary is `nNow - nTime > ADDRMAN_HORIZON_DAYS * 24 * 60 * 60`
// (addrman.cpp:56), so exactly-horizon is NOT terrible and one second older is.
// The fixed `now` is far in the past on purpose: if GetAddr consulted the wall
// clock instead of the seam, every address here would be past the horizon and
// the result would be empty.
func TestAddrManNowSeamGatesIsTerrible(t *testing.T) {
	const (
		pinned  = int64(1600000000)
		horizon = int64(addrmanHorizonDays * 24 * 60 * 60)
	)

	a := newTestAddrMan(t)
	a.now = func() int64 { return pinned }

	source := resolveIP(t, "252.2.2.2")

	fresh := make(map[string]struct{})

	for i := 1; i <= 5; i++ {
		// Exactly at the horizon: nNow - nTime == horizon, so NOT terrible.
		addr := resolveServiceAt(t, "250.1.1."+itoa(i), uint16(8333+i), pinned-horizon)
		require.True(t, a.Add(addr, source, 0))

		fresh[addr.String()] = struct{}{}
	}

	for i := 1; i <= 5; i++ {
		// One second older: terrible.
		addr := resolveServiceAt(t, "250.2.2."+itoa(i), uint16(8333+i), pinned-horizon-1)
		require.True(t, a.Add(addr, source, 0))
	}

	require.Equal(t, 10, a.Size())

	// Direct boundary assertions on the ported predicate.
	atHorizon := newAddrInfo(resolveServiceAt(t, "250.9.9.9", 8333, pinned-horizon), source)
	require.False(t, atHorizon.isTerrible(pinned), "exactly at the horizon is not terrible")

	pastHorizon := newAddrInfo(resolveServiceAt(t, "250.9.9.9", 8333, pinned-horizon-1), source)
	require.True(t, pastHorizon.isTerrible(pinned), "one second past the horizon is terrible")

	// And the seam actually reaches GetAddr: 23% of 10 is 2, and both must come
	// from the non-terrible half. With the wall clock instead of the seam every
	// entry would be terrible and this would be empty.
	got := a.GetAddr()
	require.Len(t, got, 2)

	for _, addr := range got {
		require.Contains(t, fresh, addr.String(), "GetAddr returned an address past the horizon")
	}
}
