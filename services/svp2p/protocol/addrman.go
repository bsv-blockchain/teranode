package protocol

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"math"
	mrand "math/rand/v2"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// This file is a port of SVNode's stochastic address manager:
// bitcoin-sv@879fc8b42 src/addrman.h (CAddrInfo at :24, CAddrMan at :182) and
// src/addrman.cpp. SVNode field names are kept verbatim — nKey, nNew, nTried,
// mapInfo, mapAddr, vRandom, vvNew, vvTried, nIdCount, nLastGood — and the
// decision order inside every ported function follows the C++ statement for
// statement. Each ported method names its C++ counterpart.
//
// LOCKING — read this before calling from PeerManager.
//
// AddrMan is SELF-SYNCHRONISING. It carries its own mutex, `cs`, which is the
// port of CAddrMan's "critical section to protect the inner data structures"
// (addrman.h:185: `mutable CCriticalSection cs;`). It is NOT covered by
// PeerManager.syncMu and must not be assumed to be.
//
// This is a deliberate exception to this package's convention that sync state
// is caller-locked under syncMu. A caller must therefore NOT hold cs itself
// (there is no exported way to), and must not call AddrMan while holding
// syncMu if it cares about lock ordering — every AddrMan method returns
// without blocking on I/O or on the network, so a short syncMu overlap is
// bounded, but Save() and Stop() write to disk and must never run under
// syncMu. Save() is written so the file write happens after cs is released.

// Bucket geometry. SVNode defines the LOG2 exponents and derives the counts by
// shifting (addrman.h:174-176: `#define ADDRMAN_TRIED_BUCKET_COUNT (1 <<
// ADDRMAN_TRIED_BUCKET_COUNT_LOG2)`), so the derivation is reproduced here
// rather than the derived literals being hardcoded.
const (
	// addrmanTriedBucketCountLog2 is ADDRMAN_TRIED_BUCKET_COUNT_LOG2 (addrman.h:135).
	addrmanTriedBucketCountLog2 = 8

	// addrmanNewBucketCountLog2 is ADDRMAN_NEW_BUCKET_COUNT_LOG2 (addrman.h:138).
	addrmanNewBucketCountLog2 = 10

	// addrmanBucketSizeLog2 is ADDRMAN_BUCKET_SIZE_LOG2 (addrman.h:141).
	addrmanBucketSizeLog2 = 6

	// addrmanTriedBucketCount is ADDRMAN_TRIED_BUCKET_COUNT (addrman.h:174) — 256.
	addrmanTriedBucketCount = 1 << addrmanTriedBucketCountLog2

	// addrmanNewBucketCount is ADDRMAN_NEW_BUCKET_COUNT (addrman.h:175) — 1024.
	addrmanNewBucketCount = 1 << addrmanNewBucketCountLog2

	// addrmanBucketSize is ADDRMAN_BUCKET_SIZE (addrman.h:176) — 64.
	addrmanBucketSize = 1 << addrmanBucketSizeLog2
)

// Group spread limits. These three are what stop one network group from
// filling the tables, which is the eclipse-attack resistance of the structure.
const (
	// addrmanTriedBucketsPerGroup is ADDRMAN_TRIED_BUCKETS_PER_GROUP
	// (addrman.h:145): "over how many buckets entries with tried addresses
	// from a single group (/16 for IPv4) are spread".
	addrmanTriedBucketsPerGroup = 8

	// addrmanNewBucketsPerSourceGroup is ADDRMAN_NEW_BUCKETS_PER_SOURCE_GROUP
	// (addrman.h:149): "over how many buckets entries with new addresses
	// originating from a single group are spread".
	addrmanNewBucketsPerSourceGroup = 64

	// addrmanNewBucketsPerAddress is ADDRMAN_NEW_BUCKETS_PER_ADDRESS
	// (addrman.h:153): "in how many buckets for entries with new addresses a
	// single address may occur".
	addrmanNewBucketsPerAddress = 8
)

// Quality thresholds, all from addrman.h:156-171.
const (
	// addrmanHorizonDays is ADDRMAN_HORIZON_DAYS (addrman.h:156).
	addrmanHorizonDays = 7

	// addrmanRetries is ADDRMAN_RETRIES (addrman.h:159).
	addrmanRetries = 3

	// addrmanMaxFailures is ADDRMAN_MAX_FAILURES (addrman.h:162).
	addrmanMaxFailures = 10

	// addrmanMinFailDays is ADDRMAN_MIN_FAIL_DAYS (addrman.h:165).
	addrmanMinFailDays = 2

	// addrmanGetAddrMaxPct is ADDRMAN_GETADDR_MAX_PCT (addrman.h:168).
	addrmanGetAddrMaxPct = 23

	// addrmanGetAddrMax is ADDRMAN_GETADDR_MAX (addrman.h:171).
	addrmanGetAddrMax = 2500
)

// caddressDefaultNTime is the CAddress member initialiser
// `unsigned int nTime{100000000};` (protocol.h:521). A freshly constructed
// CAddress is therefore already stale by IsTerrible's horizon, and several of
// the ported vectors depend on that, so the default is reproduced exactly
// rather than defaulting to "now".
const caddressDefaultNTime = 100000000

// defaultAddrManSaveInterval mirrors SVNode DUMP_ADDRESSES_INTERVAL
// (net.cpp:55: `#define DUMP_ADDRESSES_INTERVAL 900`), the period at which
// SVNode dumps its address table.
const defaultAddrManSaveInterval = 900 * time.Second

// emptyBucketSlot is the C++ sentinel for an unused bucket position: vvNew and
// vvTried are filled with -1 by Clear() (addrman.h:491, :497).
const emptyBucketSlot = -1

// pchIPv4 is the CNetAddr IPv4-mapped prefix
// (netaddress.cpp:16: `static const std::array<uint8_t, 12> pchIPv4{0, 0, 0, 0,
// 0, 0, 0, 0, 0, 0, 0xff, 0xff};`).
var pchIPv4 = [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff}

// ---------------------------------------------------------------------------
// addrHasher — the CHashWriter(SER_GETHASH, 0) preimage builder
// ---------------------------------------------------------------------------

// addrHasher builds the exact byte sequence a
// `CHashWriter(SER_GETHASH, 0) << ...` chain produces, then double-SHA256s it.
// The bucket vectors in addrman_test.go depend on this layout byte for byte,
// so each writer names the C++ serialisation rule it reproduces.
type addrHasher struct {
	buf []byte
}

// uint256 appends a uint256 as its 32 raw bytes in stored order. base_blob
// serialises its data array as-is (serialize.h's FLATDATA path), with no
// length prefix and no byte reversal.
func (h *addrHasher) uint256(v [32]byte) {
	h.buf = append(h.buf, v[:]...)
}

// rawBytes appends bytes with NO length prefix — the shape of a plain
// `Hash(first, last)` over a byte range, as CNetAddr::GetHash uses over its
// 16 ip bytes (netaddress.cpp:309-315). Unlike varBytes this is not a
// serialisation of a container, so nothing precedes the bytes.
func (h *addrHasher) rawBytes(b []byte) {
	h.buf = append(h.buf, b...)
}

// varBytes appends a std::vector<uint8_t> as CompactSize(len) followed by the
// bytes (serialize.h:718: `WriteCompactSize(os, v.size());`).
func (h *addrHasher) varBytes(b []byte) {
	h.compactSize(uint64(len(b)))
	h.buf = append(h.buf, b...)
}

// compactSize reproduces WriteCompactSize (serialize.h:296).
func (h *addrHasher) compactSize(n uint64) {
	switch {
	case n < 253:
		h.buf = append(h.buf, byte(n))
	case n <= math.MaxUint16:
		h.buf = append(h.buf, 253)
		h.buf = binary.LittleEndian.AppendUint16(h.buf, uint16(n))
	case n <= math.MaxUint32:
		h.buf = append(h.buf, 254)
		h.buf = binary.LittleEndian.AppendUint32(h.buf, uint32(n))
	default:
		h.buf = append(h.buf, 255)
		h.buf = binary.LittleEndian.AppendUint64(h.buf, n)
	}
}

// uint64 appends a uint64_t as 8 bytes little-endian (ser_writedata64).
func (h *addrHasher) uint64(v uint64) {
	h.buf = binary.LittleEndian.AppendUint64(h.buf, v)
}

// int32 appends a C++ `int` as 4 bytes little-endian (ser_writedata32).
func (h *addrHasher) int32(v int32) {
	h.buf = binary.LittleEndian.AppendUint32(h.buf, uint32(v))
}

// byteVal appends a C++ `char` as one byte (serialize.h:196:
// `template <typename Stream> inline void Serialize(Stream &s, char a)`).
func (h *addrHasher) byteVal(v byte) {
	h.buf = append(h.buf, v)
}

// hash is CHashWriter::GetHash — double SHA256 of everything written.
func (h *addrHasher) hash() [32]byte {
	first := sha256.Sum256(h.buf)

	return sha256.Sum256(first[:])
}

// cheapHash is uint256::GetCheapHash (uint256.h:175:
// `uint64_t GetCheapHash() const { return ReadLE64(data.data()); }`).
func (h *addrHasher) cheapHash() uint64 {
	digest := h.hash()

	return binary.LittleEndian.Uint64(digest[:8])
}

// ---------------------------------------------------------------------------
// netAddr — port of CNetAddr (netaddress.h:31)
// ---------------------------------------------------------------------------

// netAddr is a CNetAddr: 16 bytes in network byte order, with IPv4 held in the
// ::FFFF:0:0/96 mapped range.
type netAddr struct {
	ip [16]byte
}

func newNetAddr(ip net.IP) netAddr {
	var a netAddr

	if ip4 := ip.To4(); ip4 != nil {
		// CNetAddr::SetRaw(NET_IPV4, ...) copies pchIPv4 then the 4 octets.
		copy(a.ip[:], pchIPv4[:])
		copy(a.ip[12:], ip4)

		return a
	}

	if ip16 := ip.To16(); ip16 != nil {
		copy(a.ip[:], ip16)
	}

	return a
}

func (a netAddr) netIP() net.IP {
	out := make(net.IP, 16)
	copy(out, a.ip[:])

	return out
}

// getByte is CNetAddr::GetByte (netaddress.cpp:49: `return ip[15 - n];`).
func (a netAddr) getByte(n int) byte {
	return a.ip[15-n]
}

func (a netAddr) isIPv4() bool {
	return a.ip[0] == 0 && a.ip[1] == 0 && a.ip[2] == 0 && a.ip[3] == 0 &&
		a.ip[4] == 0 && a.ip[5] == 0 && a.ip[6] == 0 && a.ip[7] == 0 &&
		a.ip[8] == 0 && a.ip[9] == 0 && a.ip[10] == 0xff && a.ip[11] == 0xff
}

// isRFC1918 is CNetAddr::IsRFC1918 (netaddress.cpp:63).
func (a netAddr) isRFC1918() bool {
	return a.isIPv4() &&
		(a.getByte(3) == 10 || (a.getByte(3) == 192 && a.getByte(2) == 168) ||
			(a.getByte(3) == 172 && (a.getByte(2) >= 16 && a.getByte(2) <= 31)))
}

// isRFC2544 is CNetAddr::IsRFC2544 (netaddress.cpp:69).
func (a netAddr) isRFC2544() bool {
	return a.isIPv4() && a.getByte(3) == 198 &&
		(a.getByte(2) == 18 || a.getByte(2) == 19)
}

// isRFC3927 is CNetAddr::IsRFC3927 (netaddress.cpp:74).
func (a netAddr) isRFC3927() bool {
	return a.isIPv4() && a.getByte(3) == 169 && a.getByte(2) == 254
}

// isRFC6598 is CNetAddr::IsRFC6598 (netaddress.cpp:78).
func (a netAddr) isRFC6598() bool {
	return a.isIPv4() && a.getByte(3) == 100 && a.getByte(2) >= 64 &&
		a.getByte(2) <= 127
}

// isRFC5737 is CNetAddr::IsRFC5737 (netaddress.cpp:83).
func (a netAddr) isRFC5737() bool {
	return a.isIPv4() &&
		((a.getByte(3) == 192 && a.getByte(2) == 0 && a.getByte(1) == 2) ||
			(a.getByte(3) == 198 && a.getByte(2) == 51 && a.getByte(1) == 100) ||
			(a.getByte(3) == 203 && a.getByte(2) == 0 && a.getByte(1) == 113))
}

// isRFC3849 is CNetAddr::IsRFC3849 (netaddress.cpp:90).
func (a netAddr) isRFC3849() bool {
	return a.getByte(15) == 0x20 && a.getByte(14) == 0x01 &&
		a.getByte(13) == 0x0D && a.getByte(12) == 0xB8
}

// isRFC3964 is CNetAddr::IsRFC3964 (netaddress.cpp:95).
func (a netAddr) isRFC3964() bool {
	return a.getByte(15) == 0x20 && a.getByte(14) == 0x02
}

// isRFC6052 is CNetAddr::IsRFC6052 (netaddress.cpp:99).
func (a netAddr) isRFC6052() bool {
	pch := [12]byte{0, 0x64, 0xFF, 0x9B, 0, 0, 0, 0, 0, 0, 0, 0}

	return [12]byte(a.ip[:12]) == pch
}

// isRFC4380 is CNetAddr::IsRFC4380 (netaddress.cpp:106).
func (a netAddr) isRFC4380() bool {
	return a.getByte(15) == 0x20 && a.getByte(14) == 0x01 &&
		a.getByte(13) == 0 && a.getByte(12) == 0
}

// isRFC4862 is CNetAddr::IsRFC4862 (netaddress.cpp:111).
func (a netAddr) isRFC4862() bool {
	pch := [8]byte{0xFE, 0x80, 0, 0, 0, 0, 0, 0}

	return [8]byte(a.ip[:8]) == pch
}

// isRFC4193 is CNetAddr::IsRFC4193 (netaddress.cpp:117).
func (a netAddr) isRFC4193() bool {
	return a.getByte(15)&0xFE == 0xFC
}

// isRFC6145 is CNetAddr::IsRFC6145 (netaddress.cpp:121).
func (a netAddr) isRFC6145() bool {
	pch := [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0, 0}

	return [12]byte(a.ip[:12]) == pch
}

// isRFC4843 is CNetAddr::IsRFC4843 (netaddress.cpp:128).
func (a netAddr) isRFC4843() bool {
	return a.getByte(15) == 0x20 && a.getByte(14) == 0x01 &&
		a.getByte(13) == 0x00 && a.getByte(12)&0xF0 == 0x10
}

// isLocal is CNetAddr::IsLocal (netaddress.cpp:133).
func (a netAddr) isLocal() bool {
	// IPv4 loopback
	if a.isIPv4() && (a.getByte(3) == 127 || a.getByte(3) == 0) {
		return true
	}

	// IPv6 loopback (::1/128)
	local := [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}

	return a.ip == local
}

// isValid is CNetAddr::IsValid (netaddress.cpp:147).
func (a netAddr) isValid() bool {
	// Cleanup 3-byte shifted addresses caused by garbage in size field of addr
	// messages from versions before 0.2.9 checksum.
	if [9]byte(a.ip[:9]) == [9]byte(pchIPv4[3:12]) {
		return false
	}

	var none6 [16]byte
	if a.ip == none6 {
		return false
	}

	// documentation IPv6 address
	if a.isRFC3849() {
		return false
	}

	if a.isIPv4() {
		// INADDR_NONE
		if a.ip[12] == 0xff && a.ip[13] == 0xff && a.ip[14] == 0xff && a.ip[15] == 0xff {
			return false
		}

		// 0
		if a.ip[12] == 0 && a.ip[13] == 0 && a.ip[14] == 0 && a.ip[15] == 0 {
			return false
		}
	}

	return true
}

// isRoutable is CNetAddr::IsRoutable (netaddress.cpp:182).
func (a netAddr) isRoutable() bool {
	return a.isValid() &&
		!(a.isRFC1918() || a.isRFC2544() || a.isRFC3927() || a.isRFC4862() ||
			a.isRFC6598() || a.isRFC5737() || a.isRFC4193() ||
			a.isRFC4843() || a.isLocal())
}

// group is CNetAddr::GetGroup (netaddress.cpp:256): "get canonical identifier
// of an address' group no two connections will be attempted to addresses with
// the same group".
func (a netAddr) group() []byte {
	vchRet := make([]byte, 0, 6)

	nClass := netIPv6
	nStartByte := 0
	nBits := 16

	// all local addresses belong to the same group
	if a.isLocal() {
		nClass = 255
		nBits = 0
	}

	switch {
	case !a.isRoutable():
		// all unroutable addresses belong to the same group
		nClass = netUnroutable
		nBits = 0
	case a.isIPv4() || a.isRFC6145() || a.isRFC6052():
		// for IPv4 addresses, '1' + the 16 higher-order bits of the IP
		nClass = netIPv4
		nStartByte = 12
	case a.isRFC3964():
		// for 6to4 tunnelled addresses, use the encapsulated IPv4 address
		nClass = netIPv4
		nStartByte = 2
	case a.isRFC4380():
		// for Teredo-tunnelled IPv6 addresses, use the encapsulated IPv4 address
		vchRet = append(vchRet, byte(netIPv4), a.getByte(3)^0xFF, a.getByte(2)^0xFF)

		return vchRet
	case a.getByte(15) == 0x20 && a.getByte(14) == 0x01 &&
		a.getByte(13) == 0x04 && a.getByte(12) == 0x70:
		// for he.net, use /36 groups
		nBits = 36
	default:
		// for the rest of the IPv6 network, use /32 groups
		nBits = 32
	}

	vchRet = append(vchRet, byte(nClass))

	for nBits >= 8 {
		vchRet = append(vchRet, a.getByte(15-nStartByte))
		nStartByte++
		nBits -= 8
	}

	if nBits > 0 {
		vchRet = append(vchRet, a.getByte(15-nStartByte)|byte((1<<(8-nBits))-1))
	}

	return vchRet
}

// enum Network (netaddress.h:21).
const (
	netUnroutable = 0
	netIPv4       = 1
	netIPv6       = 2
)

// ---------------------------------------------------------------------------
// Address — port of CAddress (protocol.h:494)
// ---------------------------------------------------------------------------

// Address is a CAddress: a CService (IP + port) plus the advertised service
// bits and the last-seen timestamp. It is the exported surface of this file;
// Tasks 18 and 19 convert to and from wire.NetAddress with NetAddress and
// AddressFromNetAddress.
// EVERY FIELD IS UNEXPORTED ON PURPOSE. nTime carries a non-zero C++ default
// (protocol.h:521), and a zero nTime is not merely "unset": addOne rejects it
// outright for an existing entry (`if (!addr.nTime || ...) return false;`,
// addrman.cpp:279) and stores a new entry as immediately isTerrible, so it
// never appears in GetAddr. An `Address{IP: ip, Port: p}` literal would
// therefore be silently and totally useless. Unexporting nTime alone would not
// help — Go permits a composite literal that names only the exported fields —
// so the whole struct is opaque.
//
// Be precise about what that buys, because Tasks 18 and 19 will read this as
// their contract: Go rejects a literal that NAMES any field of this struct, and
// that is the footgun it closes. It does NOT make a zero Address
// unobtainable — `Address{}`, `var a Address` and `make([]Address, n)` all
// still compile outside this package and all yield nTime == 0. Such a value is
// inert rather than dangerous (addOne rejects it), but do not read the opacity
// as a guarantee that every Address reaching you was built by a constructor.
// Use NewAddress or AddressFromNetAddress.
type Address struct {
	ip        net.IP
	port      uint16
	nServices wire.ServiceFlag
	nTime     int64
}

// NewAddress builds an Address with the CAddress default nTime
// (protocol.h:521), the way `CAddress(addr, NODE_NONE)` does in C++.
func NewAddress(ip net.IP, port uint16, services wire.ServiceFlag) Address {
	return Address{ip: ip, port: port, nServices: services, nTime: caddressDefaultNTime}
}

// NewAddressAtTime builds an Address with an explicit nTime, for a caller that
// knows when the address was last seen.
func NewAddressAtTime(ip net.IP, port uint16, services wire.ServiceFlag, nTime int64) Address {
	return Address{ip: ip, port: port, nServices: services, nTime: nTime}
}

// AddressFromNetAddress converts an inbound wire address (an `addr` message
// entry) into a CAddress. Task 18 owns the message handling that calls this.
//
// A wire timestamp of 0 converts to nTime 0, deliberately: C++ sees the same
// value from the wire and Add_ has explicit handling for it, so normalising it
// away here would change the ported decision order.
func AddressFromNetAddress(na *wire.NetAddress) Address {
	if na == nil {
		return Address{nTime: caddressDefaultNTime}
	}

	return Address{
		ip:        na.IP,
		port:      na.Port,
		nServices: na.Services,
		nTime:     na.Timestamp.Unix(),
	}
}

// IP is the address's CNetAddr part.
func (a Address) IP() net.IP { return a.ip }

// Port is the CService port.
func (a Address) Port() uint16 { return a.port }

// NServices is CAddress::nServices.
func (a Address) NServices() wire.ServiceFlag { return a.nServices }

// NTime is CAddress::nTime, in unix seconds.
func (a Address) NTime() int64 { return a.nTime }

// NetAddress converts back to the wire representation for an outbound `addr`.
func (a Address) NetAddress() *wire.NetAddress {
	return &wire.NetAddress{
		Timestamp: time.Unix(a.nTime, 0),
		Services:  a.nServices,
		IP:        a.ip,
		Port:      a.port,
	}
}

// String is CService::ToStringIPPort (netaddress.cpp:462).
func (a Address) String() string {
	return net.JoinHostPort(newNetAddr(a.ip).netIP().String(), strconv.Itoa(int(a.port)))
}

// ---------------------------------------------------------------------------
// addrInfo — port of CAddrInfo (addrman.h:24)
// ---------------------------------------------------------------------------

// addrInfo is CAddrInfo: "Extended statistics about a CAddress".
type addrInfo struct {
	// CAddress / CService state
	addr      netAddr
	port      uint16
	nServices wire.ServiceFlag
	nTime     int64

	// last try whatsoever by us (memory only)
	nLastTry int64

	// last counted attempt (memory only)
	nLastCountAttempt int64

	// where knowledge about this address first came from
	source netAddr

	// last successful connection by us
	nLastSuccess int64

	// connection attempts since last successful attempt
	nAttempts int

	// reference count in new sets (memory only)
	nRefCount int

	// in tried set? (memory only)
	fInTried bool

	// position in vRandom
	nRandomPos int
}

// newAddrInfo is `CAddrInfo(const CAddress &addrIn, const CNetAddr &addrSource)`
// (addrman.h:65).
func newAddrInfo(addr Address, source net.IP) *addrInfo {
	return &addrInfo{
		addr:       newNetAddr(addr.ip),
		port:       addr.port,
		nServices:  addr.nServices,
		nTime:      addr.nTime,
		source:     newNetAddr(source),
		nRandomPos: -1,
	}
}

func (i *addrInfo) address() Address {
	return Address{
		ip:        i.addr.netIP(),
		port:      i.port,
		nServices: i.nServices,
		nTime:     i.nTime,
	}
}

// getKey is CService::GetKey (netaddress.cpp:450): the 16 IP bytes followed by
// the port big-endian, so two ports on one IP get different keys.
func (i *addrInfo) getKey() []byte {
	vKey := make([]byte, 18)
	copy(vKey, i.addr.ip[:])
	vKey[16] = byte(i.port / 0x100)
	vKey[17] = byte(i.port & 0x0FF)

	return vKey
}

// getTriedBucket is CAddrInfo::GetTriedBucket (addrman.cpp:10).
func (i *addrInfo) getTriedBucket(nKey [32]byte) int {
	var h1 addrHasher

	h1.uint256(nKey)
	h1.varBytes(i.getKey())

	hash1 := h1.cheapHash()

	var h2 addrHasher

	h2.uint256(nKey)
	h2.varBytes(i.addr.group())
	h2.uint64(hash1 % addrmanTriedBucketsPerGroup)

	return int(h2.cheapHash() % addrmanTriedBucketCount)
}

// getNewBucket is CAddrInfo::GetNewBucket(const uint256&, const CNetAddr&)
// (addrman.cpp:23).
func (i *addrInfo) getNewBucket(nKey [32]byte, src netAddr) int {
	vchSourceGroupKey := src.group()

	var h1 addrHasher

	h1.uint256(nKey)
	h1.varBytes(i.addr.group())
	h1.varBytes(vchSourceGroupKey)

	hash1 := h1.cheapHash()

	var h2 addrHasher

	h2.uint256(nKey)
	h2.varBytes(vchSourceGroupKey)
	h2.uint64(hash1 % addrmanNewBucketsPerSourceGroup)

	return int(h2.cheapHash() % addrmanNewBucketCount)
}

// getNewBucketFromSource is the CAddrInfo::GetNewBucket(const uint256&)
// overload that uses the entry's own source (addrman.h:80).
func (i *addrInfo) getNewBucketFromSource(nKey [32]byte) int {
	return i.getNewBucket(nKey, i.source)
}

// getBucketPosition is CAddrInfo::GetBucketPosition (addrman.cpp:38).
func (i *addrInfo) getBucketPosition(nKey [32]byte, fNew bool, nBucket int) int {
	var h addrHasher

	h.uint256(nKey)

	if fNew {
		h.byteVal('N')
	} else {
		h.byteVal('K')
	}

	h.int32(int32(nBucket))
	h.varBytes(i.getKey())

	return int(h.cheapHash() % addrmanBucketSize)
}

// isTerrible is CAddrInfo::IsTerrible (addrman.cpp:48).
func (i *addrInfo) isTerrible(nNow int64) bool {
	// never remove things tried in the last minute
	if i.nLastTry != 0 && i.nLastTry >= nNow-60 {
		return false
	}

	// came in a flying DeLorean
	if i.nTime > nNow+10*60 {
		return true
	}

	// not seen in recent history
	if i.nTime == 0 || nNow-i.nTime > addrmanHorizonDays*24*60*60 {
		return true
	}

	// tried N times and never a success
	if i.nLastSuccess == 0 && i.nAttempts >= addrmanRetries {
		return true
	}

	// N successive failures in the last week
	if nNow-i.nLastSuccess > addrmanMinFailDays*24*60*60 && i.nAttempts >= addrmanMaxFailures {
		return true
	}

	return false
}

// getChance is CAddrInfo::GetChance (addrman.cpp:70).
func (i *addrInfo) getChance(nNow int64) float64 {
	fChance := 1.0

	nSinceLastTry := max(nNow-i.nLastTry, 0)

	// deprioritize very recent attempts away
	if nSinceLastTry < 60*10 {
		fChance *= 0.01
	}

	// deprioritize 66% after each failed attempt, but at most 1/28th to avoid
	// the search taking forever or overly penalizing outages.
	fChance *= math.Pow(0.66, float64(min(i.nAttempts, 8)))

	return fChance
}

// ---------------------------------------------------------------------------
// AddrMan — port of CAddrMan (addrman.h:182)
// ---------------------------------------------------------------------------

// AddrManOptions configures an AddrMan.
type AddrManOptions struct {
	// Path is the peers.json location. An EMPTY Path disables persistence
	// entirely, which is the default deployment: legacy_savePeers defaults to
	// false ("by default we do not save the peers",
	// settings/settings.go:674). On the empty-Path path nothing is read,
	// nothing is written, no goroutine starts, and no warning is logged.
	Path string

	// SaveInterval overrides the periodic snapshot period. Zero selects
	// defaultAddrManSaveInterval.
	SaveInterval time.Duration
}

// AddrMan is CAddrMan, the stochastic address manager. See the LOCKING note at
// the top of this file: it is self-synchronising and is NOT covered by
// PeerManager.syncMu.
type AddrMan struct {
	logger ulogger.Logger

	// cs is CAddrMan::cs (addrman.h:185).
	cs sync.Mutex

	// nIdCount is the last used nId (addrman.h:188).
	nIdCount int

	// mapInfo holds information about all nIds (addrman.h:191).
	mapInfo map[int]*addrInfo

	// mapAddr finds an nId based on its network address (addrman.h:194). The
	// key is a CNetAddr — the IP ONLY, no port. That is why two ports on one
	// IP share a single entry (addrman_tests.cpp:114, "Addr with same IP but
	// diff port does not replace existing addr").
	mapAddr map[netAddr]int

	// vRandom is the randomly-ordered vector of all nIds (addrman.h:197).
	vRandom []int

	// nTried is the number of "tried" entries (addrman.h:200).
	nTried int

	// vvTried is the list of "tried" buckets (addrman.h:204).
	vvTried [][]int

	// nNew is the number of (unique) "new" entries (addrman.h:207).
	nNew int

	// vvNew is the list of "new" buckets (addrman.h:211).
	vvNew [][]int

	// nLastGood is the last time Good was called (addrman.h:214).
	nLastGood int64

	// nKey is the secret key to randomize bucket select with (addrman.h:218).
	nKey [32]byte

	// randomInt is CAddrMan::RandomInt (addrman.cpp:527), virtual in C++ so
	// tests can make it deterministic. Production and tests take the SAME code
	// path; only this function differs.
	randomInt func(nMax int) int

	// insecureRandBits is CAddrMan::insecure_rand.randbits (addrman.h:221),
	// used only by Select_'s occupied-slot walk.
	insecureRandBits func(bits uint) uint64

	// now stands in for GetAdjustedTime(). SVNode adjusts by the peer time
	// offset from timedata.cpp; svp2p has no such offset, so this is plain
	// wall-clock time. Injectable so tests can pin it.
	now func() int64

	// --- persistence (no C++ counterpart; see addrman_persist.go) ---

	path         string
	saveInterval time.Duration
	quit         chan struct{}
	wg           sync.WaitGroup
	stopOnce     sync.Once
	lifecycleMu  sync.Mutex
	running      atomic.Bool
	stopped      atomic.Bool
	loadFailed   atomic.Bool
	saves        atomic.Uint64
}

// NewAddrMan is `CAddrMan() { Clear(); }` (addrman.h:509) plus the Teranode
// persistence configuration.
func NewAddrMan(logger ulogger.Logger, opts AddrManOptions) *AddrMan {
	interval := opts.SaveInterval
	if interval <= 0 {
		interval = defaultAddrManSaveInterval
	}

	a := &AddrMan{
		logger:           logger,
		path:             opts.Path,
		saveInterval:     interval,
		quit:             make(chan struct{}),
		randomInt:        secureRandomInt,
		insecureRandBits: insecureRandBits,
		now:              func() int64 { return time.Now().Unix() },
	}

	a.clear()

	return a
}

// secureRandomInt is GetRandInt (addrman.cpp:528).
func secureRandomInt(nMax int) int {
	if nMax <= 0 {
		return 0
	}

	return mrand.IntN(nMax)
}

// insecureRandBits is FastRandomContext::randbits.
func insecureRandBits(bits uint) uint64 {
	if bits == 0 {
		return 0
	}

	return mrand.Uint64() >> (64 - bits)
}

// makeDeterministic is the test fixture's MakeDeterministic
// (addrman_tests.cpp:19) plus its RandomInt override (:24). It sets the same
// fields production sets, so tests exercise the production code path.
func (a *AddrMan) makeDeterministic() {
	a.cs.Lock()
	defer a.cs.Unlock()

	// nKey.SetNull()
	a.nKey = [32]byte{}

	// int RandomInt(int nMax) override {
	//     state = (CHashWriter(SER_GETHASH, 0) << state).GetHash().GetCheapHash();
	//     return (unsigned int)(state % nMax);
	// }
	state := uint64(1)
	a.randomInt = func(nMax int) int {
		if nMax <= 0 {
			return 0
		}

		var h addrHasher

		h.uint64(state)
		state = h.cheapHash()

		return int(state % uint64(nMax))
	}

	// insecure_rand = FastRandomContext(true) is a ChaCha20 stream keyed with
	// zeros. This is the one C++ construct in the ported vectors that is NOT
	// reproduced exactly; it is replaced by a deterministic hash chain of its
	// own so the Go tests stay reproducible.
	//
	// Be careful about why that is safe, because the obvious reason is wrong.
	// randbits does NOT merely change which empty slots are stepped over: the
	// walk (addrman.cpp:388-397) terminates on the FIRST occupied slot it
	// lands on, so the randbits sequence determines WHICH entry a given Select
	// returns. All that survives in general is the weaker property that every
	// occupied slot stays reachable with positive probability.
	//
	// The divergence is safe for a reason specific to the one vector it could
	// have broken. addrman_select Test 12 (addrman_tests.cpp:187-191) asserts
	// `ports.size() == 3U`, where 3 is the TOTAL number of distinct ports among
	// the 7 entries (8333, 9999, 7777). The assertion therefore means "20 draws
	// reach every port group" — a property of any fair RNG, not a fingerprint
	// of ChaCha20's output. No other ported vector reads randbits at all.
	walk := uint64(0x9E3779B97F4A7C15)
	a.insecureRandBits = func(bits uint) uint64 {
		if bits == 0 {
			return 0
		}

		var h addrHasher

		h.uint64(walk)
		walk = h.cheapHash()

		return walk >> (64 - bits)
	}
}

// clear is CAddrMan::Clear (addrman.h:485).
func (a *AddrMan) clear() {
	a.vRandom = nil
	a.mapInfo = make(map[int]*addrInfo)
	a.mapAddr = make(map[netAddr]int)

	// nKey = GetRandHash()
	var nKey [32]byte
	_, _ = rand.Read(nKey[:]) // crypto/rand.Read never fails on Go 1.24+
	a.nKey = nKey

	a.vvNew = make([][]int, addrmanNewBucketCount)
	for bucket := range a.vvNew {
		a.vvNew[bucket] = newEmptyBucket()
	}

	a.vvTried = make([][]int, addrmanTriedBucketCount)
	for bucket := range a.vvTried {
		a.vvTried[bucket] = newEmptyBucket()
	}

	a.nIdCount = 0
	a.nTried = 0
	a.nNew = 0
	// Initially at 1 so that "never" is strictly worse.
	a.nLastGood = 1
}

// refCountOf reads nRefCount without assuming the entry exists. Its only
// caller is delete's unreachable-guard diagnostic, below.
func refCountOf(info *addrInfo) int {
	if info == nil {
		return 0
	}

	return info.nRefCount
}

func newEmptyBucket() []int {
	b := make([]int, addrmanBucketSize)
	for i := range b {
		b[i] = emptyBucketSlot
	}

	return b
}

// Clear empties both tables and regenerates nKey, like CAddrMan::Clear.
func (a *AddrMan) Clear() {
	a.cs.Lock()
	defer a.cs.Unlock()

	a.clear()
}

// Size is CAddrMan::size (addrman.h:514) — "the number of (unique) addresses
// in all tables".
func (a *AddrMan) Size() int {
	a.cs.Lock()
	defer a.cs.Unlock()

	return len(a.vRandom)
}

// find is CAddrMan::Find (addrman.cpp:84). The lookup is by IP only.
func (a *AddrMan) find(addr netAddr) *addrInfo {
	nID, ok := a.mapAddr[addr]
	if !ok {
		return nil
	}

	return a.mapInfo[nID]
}

// findWithID is Find's `int *pnId` out-parameter form.
func (a *AddrMan) findWithID(addr netAddr) (*addrInfo, int) {
	nID, ok := a.mapAddr[addr]
	if !ok {
		return nil, 0
	}

	return a.mapInfo[nID], nID
}

// create is CAddrMan::Create (addrman.cpp:93).
func (a *AddrMan) create(addr Address, source netAddr) (*addrInfo, int) {
	nID := a.nIdCount
	a.nIdCount++

	info := newAddrInfo(addr, source.netIP())
	a.mapInfo[nID] = info
	a.mapAddr[newNetAddr(addr.ip)] = nID
	info.nRandomPos = len(a.vRandom)
	a.vRandom = append(a.vRandom, nID)

	return info, nID
}

// swapRandom is CAddrMan::SwapRandom (addrman.cpp:104).
func (a *AddrMan) swapRandom(nRndPos1, nRndPos2 int) {
	if nRndPos1 == nRndPos2 {
		return
	}

	nID1 := a.vRandom[nRndPos1]
	nID2 := a.vRandom[nRndPos2]

	a.mapInfo[nID1].nRandomPos = nRndPos2
	a.mapInfo[nID2].nRandomPos = nRndPos1

	a.vRandom[nRndPos1] = nID2
	a.vRandom[nRndPos2] = nID1
}

// delete is CAddrMan::Delete (addrman.cpp:122). "It must not be in tried, and
// have refcount 0."
func (a *AddrMan) delete(nID int) {
	info, ok := a.mapInfo[nID]
	if !ok || info.fInTried || info.nRefCount != 0 {
		// C++ asserts these three preconditions (addrman.cpp:123-126) and so
		// aborts the process. Returning is the better failure mode for a node,
		// but it is NOT harmless: the caller has already emptied the bucket
		// slot and decremented nRefCount, so the entry survives in
		// mapInfo/mapAddr/vRandom with nRefCount == 0 and nNew over-counts by
		// one — exactly the `vRandom.size() != nTried + nNew` breakage
		// Check_ treats as fatal (addrman.cpp:415). Unreachable from every
		// in-tree caller, so this logs rather than staying silent: a future
		// caller that breaks the precondition must produce a support ticket,
		// not slow corruption. The log runs under cs; that is acceptable
		// precisely because this branch cannot be reached in normal operation.
		a.logger.Errorf("[svp2p] addrman delete precondition violated: id=%d present=%t inTried=%t refCount=%d", nID, ok, ok && info.fInTried, refCountOf(info))

		return
	}

	a.swapRandom(info.nRandomPos, len(a.vRandom)-1)
	a.vRandom = a.vRandom[:len(a.vRandom)-1]
	delete(a.mapAddr, info.addr)
	delete(a.mapInfo, nID)
	a.nNew--
}

// clearNew is CAddrMan::ClearNew (addrman.cpp:135). "This is the only place
// where entries are actually deleted."
func (a *AddrMan) clearNew(nUBucket, nUBucketPos int) {
	if a.vvNew[nUBucket][nUBucketPos] == emptyBucketSlot {
		return
	}

	nIDDelete := a.vvNew[nUBucket][nUBucketPos]
	infoDelete := a.mapInfo[nIDDelete]
	infoDelete.nRefCount--
	a.vvNew[nUBucket][nUBucketPos] = emptyBucketSlot

	if infoDelete.nRefCount == 0 {
		a.delete(nIDDelete)
	}
}

// makeTried is CAddrMan::MakeTried (addrman.cpp:152).
func (a *AddrMan) makeTried(info *addrInfo, nID int) {
	// remove the entry from all new buckets
	for bucket := 0; bucket < addrmanNewBucketCount; bucket++ {
		pos := info.getBucketPosition(a.nKey, true, bucket)
		if a.vvNew[bucket][pos] == nID {
			a.vvNew[bucket][pos] = emptyBucketSlot
			info.nRefCount--
		}
	}

	a.nNew--

	// which tried bucket to move the entry to
	nKBucket := info.getTriedBucket(a.nKey)
	nKBucketPos := info.getBucketPosition(a.nKey, false, nKBucket)

	// first make space to add it (the existing tried entry there is moved to
	// new, deleting whatever is there).
	if a.vvTried[nKBucket][nKBucketPos] != emptyBucketSlot {
		// find an item to evict
		nIDEvict := a.vvTried[nKBucket][nKBucketPos]
		infoOld := a.mapInfo[nIDEvict]

		// Remove the to-be-evicted item from the tried set.
		infoOld.fInTried = false
		a.vvTried[nKBucket][nKBucketPos] = emptyBucketSlot
		a.nTried--

		// find which new bucket it belongs to
		nUBucket := infoOld.getNewBucketFromSource(a.nKey)
		nUBucketPos := infoOld.getBucketPosition(a.nKey, true, nUBucket)
		a.clearNew(nUBucket, nUBucketPos)

		// Enter it into the new set again.
		infoOld.nRefCount = 1
		a.vvNew[nUBucket][nUBucketPos] = nIDEvict
		a.nNew++
	}

	a.vvTried[nKBucket][nKBucketPos] = nID
	a.nTried++
	info.fInTried = true
}

// Add is `bool Add(const CAddress&, const CNetAddr&, int64_t)`
// (addrman.h:533).
func (a *AddrMan) Add(addr Address, source net.IP, nTimePenalty int64) bool {
	a.cs.Lock()
	defer a.cs.Unlock()

	return a.addOne(addr, newNetAddr(source), nTimePenalty)
}

// AddMany is the `std::vector<CAddress>` Add overload (addrman.h:547). It
// returns true when at least one address was new.
func (a *AddrMan) AddMany(addrs []Address, source net.IP, nTimePenalty int64) bool {
	a.cs.Lock()
	defer a.cs.Unlock()

	src := newNetAddr(source)
	nAdd := 0

	for _, addr := range addrs {
		if a.addOne(addr, src, nTimePenalty) {
			nAdd++
		}
	}

	return nAdd > 0
}

// addOne is CAddrMan::Add_ (addrman.cpp:253).
func (a *AddrMan) addOne(addr Address, source netAddr, nTimePenalty int64) bool {
	na := newNetAddr(addr.ip)

	if !na.isRoutable() {
		return false
	}

	fNew := false
	nNow := a.now()

	pinfo, nID := a.findWithID(na)

	// Do not set a penalty for a source's self-announcement. The C++
	// comparison is `addr == source` where source is statically a CNetAddr&,
	// so it compares the IP only — the port plays no part.
	if na == source {
		nTimePenalty = 0
	}

	if pinfo != nil {
		// periodically update nTime
		fCurrentlyOnline := nNow-addr.nTime < 24*60*60

		nUpdateInterval := int64(24 * 60 * 60)
		if fCurrentlyOnline {
			nUpdateInterval = 60 * 60
		}

		if addr.nTime != 0 &&
			(pinfo.nTime == 0 || pinfo.nTime < addr.nTime-nUpdateInterval-nTimePenalty) {
			pinfo.nTime = max(int64(0), addr.nTime-nTimePenalty)
		}

		// add services
		pinfo.nServices |= addr.nServices

		// do not update if no new information is present
		if addr.nTime == 0 || (pinfo.nTime != 0 && addr.nTime <= pinfo.nTime) {
			return false
		}

		// do not update if the entry was already in the "tried" table
		if pinfo.fInTried {
			return false
		}

		// do not update if the max reference count is reached
		if pinfo.nRefCount == addrmanNewBucketsPerAddress {
			return false
		}

		// stochastic test: previous nRefCount == N: 2^N times harder to
		// increase it
		nFactor := 1
		for n := 0; n < pinfo.nRefCount; n++ {
			nFactor *= 2
		}

		if nFactor > 1 && a.randomInt(nFactor) != 0 {
			return false
		}
	} else {
		pinfo, nID = a.create(addr, source)
		pinfo.nTime = max(int64(0), pinfo.nTime-nTimePenalty)
		a.nNew++
		fNew = true
	}

	nUBucket := pinfo.getNewBucket(a.nKey, source)
	nUBucketPos := pinfo.getBucketPosition(a.nKey, true, nUBucket)

	if a.vvNew[nUBucket][nUBucketPos] != nID {
		fInsert := a.vvNew[nUBucket][nUBucketPos] == emptyBucketSlot
		if !fInsert {
			infoExisting := a.mapInfo[a.vvNew[nUBucket][nUBucketPos]]
			if infoExisting.isTerrible(nNow) ||
				(infoExisting.nRefCount > 1 && pinfo.nRefCount == 0) {
				// Overwrite the existing new table entry.
				fInsert = true
			}
		}

		if fInsert {
			a.clearNew(nUBucket, nUBucketPos)
			pinfo.nRefCount++
			a.vvNew[nUBucket][nUBucketPos] = nID
		} else {
			if pinfo.nRefCount == 0 {
				a.delete(nID)
			}
		}
	}

	return fNew
}

// Good is `void Good(const CService&, int64_t)` (addrman.h:564) — "Mark an
// entry as accessible."
func (a *AddrMan) Good(addr Address, nTime int64) {
	a.cs.Lock()
	defer a.cs.Unlock()

	a.good(addr, nTime)
}

// good is CAddrMan::Good_ (addrman.cpp:203).
func (a *AddrMan) good(addr Address, nTime int64) {
	a.nLastGood = nTime

	info, nID := a.findWithID(newNetAddr(addr.ip))

	// if not found, bail out
	if info == nil {
		return
	}

	// check whether we are talking about the exact same CService (including
	// same port)
	if info.port != addr.port {
		return
	}

	// update info
	info.nLastSuccess = nTime
	info.nLastTry = nTime
	info.nAttempts = 0
	// nTime is not updated here, to avoid leaking information about
	// currently-connected peers.

	// if it is already in the tried set, don't do anything else
	if info.fInTried {
		return
	}

	// find a bucket it is in now
	nRnd := a.randomInt(addrmanNewBucketCount)
	nUBucket := -1

	for n := 0; n < addrmanNewBucketCount; n++ {
		nB := (n + nRnd) % addrmanNewBucketCount
		nBpos := info.getBucketPosition(a.nKey, true, nB)

		if a.vvNew[nB][nBpos] == nID {
			nUBucket = nB
			break
		}
	}

	// if no bucket is found, something bad happened;
	// TODO: maybe re-add the node, but for now, just bail out
	if nUBucket == -1 {
		return
	}

	// move nId to the tried tables
	a.makeTried(info, nID)
}

// Attempt is `void Attempt(const CService&, bool, int64_t)` (addrman.h:572).
func (a *AddrMan) Attempt(addr Address, fCountFailure bool, nTime int64) {
	a.cs.Lock()
	defer a.cs.Unlock()

	// CAddrMan::Attempt_ (addrman.cpp:329)
	info := a.find(newNetAddr(addr.ip))
	if info == nil {
		return
	}

	if info.port != addr.port {
		return
	}

	info.nLastTry = nTime

	if fCountFailure && info.nLastCountAttempt < a.nLastGood {
		info.nLastCountAttempt = nTime
		info.nAttempts++
	}
}

// NLastTry reads CAddrInfo::nLastTry (addrman.h:33) for one address, returning
// zero when the address is unknown.
//
// C++ needs no such accessor: CAddrMan::Select returns a CAddrInfo, so
// ThreadOpenConnections reads nLastTry straight off the address it just picked
// (net.cpp:1955). This port's Select returns the wire Address alone, so the one
// CAddrInfo field that walk needs is read back by address instead.
func (a *AddrMan) NLastTry(addr Address) int64 {
	a.cs.Lock()
	defer a.cs.Unlock()

	info := a.find(newNetAddr(addr.ip))
	if info == nil {
		return 0
	}

	if info.port != addr.port {
		return 0
	}

	return info.nLastTry
}

// Connected is `void Connected(const CService&, int64_t)` (addrman.h:607).
func (a *AddrMan) Connected(addr Address, nTime int64) {
	a.cs.Lock()
	defer a.cs.Unlock()

	// CAddrMan::Connected_ (addrman.cpp:494)
	info := a.find(newNetAddr(addr.ip))
	if info == nil {
		return
	}

	if info.port != addr.port {
		return
	}

	const nUpdateInterval = int64(20 * 60)

	if nTime-info.nTime > nUpdateInterval {
		info.nTime = nTime
	}
}

// SetServices is `void SetServices(const CService&, ServiceFlags)`
// (addrman.h:614).
func (a *AddrMan) SetServices(addr Address, nServices wire.ServiceFlag) {
	a.cs.Lock()
	defer a.cs.Unlock()

	// CAddrMan::SetServices_ (addrman.cpp:511)
	info := a.find(newNetAddr(addr.ip))
	if info == nil {
		return
	}

	if info.port != addr.port {
		return
	}

	info.nServices = nServices
}

// Select is `CAddrInfo Select(bool newOnly)` (addrman.h:583) — "Choose an
// address to connect to." Where C++ signals "nothing" with a default-
// constructed CAddrInfo whose ToString is "[::]:0", this returns ok=false, so
// a caller cannot mistake the empty answer for a real address.
func (a *AddrMan) Select(newOnly bool) (Address, bool) {
	a.cs.Lock()
	defer a.cs.Unlock()

	// CAddrMan::Select_ (addrman.cpp:350)
	if len(a.vRandom) == 0 {
		return Address{}, false
	}

	if newOnly && a.nNew == 0 {
		return Address{}, false
	}

	nNow := a.now()

	// Use a 50% chance for choosing between tried and new table entries.
	if !newOnly && a.nTried > 0 && (a.nNew == 0 || a.randomInt(2) == 0) {
		// use a tried node
		fChanceFactor := 1.0

		for {
			nKBucket := a.randomInt(addrmanTriedBucketCount)
			nKBucketPos := a.randomInt(addrmanBucketSize)

			for a.vvTried[nKBucket][nKBucketPos] == emptyBucketSlot {
				nKBucket = (nKBucket + int(a.insecureRandBits(addrmanTriedBucketCountLog2))) % addrmanTriedBucketCount
				nKBucketPos = (nKBucketPos + int(a.insecureRandBits(addrmanBucketSizeLog2))) % addrmanBucketSize
			}

			info := a.mapInfo[a.vvTried[nKBucket][nKBucketPos]]

			if float64(a.randomInt(1<<30)) < fChanceFactor*info.getChance(nNow)*float64(1<<30) {
				return info.address(), true
			}

			fChanceFactor *= 1.2
		}
	}

	// use a new node
	fChanceFactor := 1.0

	for {
		nUBucket := a.randomInt(addrmanNewBucketCount)
		nUBucketPos := a.randomInt(addrmanBucketSize)

		for a.vvNew[nUBucket][nUBucketPos] == emptyBucketSlot {
			nUBucket = (nUBucket + int(a.insecureRandBits(addrmanNewBucketCountLog2))) % addrmanNewBucketCount
			nUBucketPos = (nUBucketPos + int(a.insecureRandBits(addrmanBucketSizeLog2))) % addrmanBucketSize
		}

		info := a.mapInfo[a.vvNew[nUBucket][nUBucketPos]]

		if float64(a.randomInt(1<<30)) < fChanceFactor*info.getChance(nNow)*float64(1<<30) {
			return info.address(), true
		}

		fChanceFactor *= 1.2
	}
}

// GetAddr is `std::vector<CAddress> GetAddr()` (addrman.h:595) — "Return a
// bunch of addresses, selected at random." The SVNode name is kept because it
// is also the name of the wire command it answers; Task 18 owns that handler.
func (a *AddrMan) GetAddr() []Address {
	a.cs.Lock()
	defer a.cs.Unlock()

	// CAddrMan::GetAddr_ (addrman.cpp:476)
	nNodes := addrmanGetAddrMaxPct * len(a.vRandom) / 100
	if nNodes > addrmanGetAddrMax {
		nNodes = addrmanGetAddrMax
	}

	nNow := a.now()

	vAddr := make([]Address, 0, nNodes)

	// gather a list of random nodes, skipping those of low quality
	for n := 0; n < len(a.vRandom); n++ {
		if len(vAddr) >= nNodes {
			break
		}

		nRndPos := a.randomInt(len(a.vRandom)-n) + n
		a.swapRandom(n, nRndPos)

		ai := a.mapInfo[a.vRandom[n]]
		if !ai.isTerrible(nNow) {
			vAddr = append(vAddr, ai.address())
		}
	}

	return vAddr
}
