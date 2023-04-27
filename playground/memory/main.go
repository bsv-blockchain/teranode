package main

import (
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"runtime"

	"github.com/dolthub/swiss"
	"github.com/holiman/uint256"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/puzpuzpuz/xsync/v2"
)

type subBytes map[[32]byte]*[32]byte

func main() {
	runtime.GC()
	fmt.Printf("Started with memory: %s\n", printAlloc())

	uint256Map := make(map[uint256.Int]*uint256.Int, 1_000_000)
	for i := uint64(0); i < 1_000_000; i++ {
		ii := uint256.NewInt(i)
		iii := uint256.NewInt(i + 2_000_000)
		uint256Map[*ii] = iii
	}

	fmt.Printf("Mem used for uint256: %s\n", printAlloc())
	uint256Map = nil
	runtime.GC()

	chainhashMap := make(map[chainhash.Hash]*chainhash.Hash, 1_000_000)
	bs := make([]byte, 32)
	for i := uint64(0); i < 1_000_000; i++ {
		binary.LittleEndian.PutUint64(bs, i)
		ii := chainhash.HashH(bs)
		binary.LittleEndian.PutUint64(bs, i+2_000_000)
		iii := chainhash.HashH(bs)
		chainhashMap[ii] = &iii
	}

	fmt.Printf("Mem used for chainhashMap: %s\n", printAlloc())
	chainhashMap = nil
	runtime.GC()

	swissMap := swiss.NewMap[chainhash.Hash, *chainhash.Hash](1_000_000)
	for i := uint64(0); i < 1_000_000; i++ {
		binary.LittleEndian.PutUint64(bs, i)
		ii := chainhash.HashH(bs)
		binary.LittleEndian.PutUint64(bs, i+2_000_000)
		iii := chainhash.HashH(bs)

		swissMap.Put(ii, &iii)
	}

	fmt.Printf("Mem used for swissMap items: %s\n", printAlloc())
	swissMap = nil
	runtime.GC()

	xsyncMap := xsync.NewTypedMapOfPresized[chainhash.Hash, *chainhash.Hash](func(seed maphash.Seed, hash chainhash.Hash) uint64 {
		var h maphash.Hash
		h.SetSeed(seed)
		_ = binary.Write(&h, binary.LittleEndian, hash[:16])
		hh := h.Sum64()
		h.Reset()
		_ = binary.Write(&h, binary.LittleEndian, hash[16:32])
		return 31*hh + h.Sum64()
	}, 1_000_000)
	for i := uint64(0); i < 1_000_000; i++ {
		binary.LittleEndian.PutUint64(bs, i)
		ii := chainhash.HashH(bs)
		binary.LittleEndian.PutUint64(bs, i+2_000_000)
		iii := chainhash.HashH(bs)
		xsyncMap.Store(ii, &iii)
	}

	fmt.Printf("Mem used for xsyncMap items: %s\n", printAlloc())

	// check map
	for i := uint64(0); i < 1_000_000; i++ {
		binary.LittleEndian.PutUint64(bs, i)
		ii := chainhash.HashH(bs)
		binary.LittleEndian.PutUint64(bs, i+2_000_000)
		iii := chainhash.HashH(bs)
		item, ok := xsyncMap.Load(ii)
		if !ok || *item != iii {
			fmt.Printf("item %d not found in xsyncmap\n", i)
		}
	}

	xsyncMap = nil
	runtime.GC()

	key := make([]byte, 32)
	byte32Map := make(map[[1]byte]subBytes)
	for i := uint64(0); i < uint64(256); i++ {
		binary.BigEndian.PutUint64(key, i)
		byte32Map[[1]byte(key[:1])] = make(subBytes)
	}

	for i := uint64(0); i < 1_000_000; i++ {
		binary.BigEndian.PutUint64(key, i)
		bytes := [1]byte(key[:1])
		bytes32 := [32]byte(key[:32])
		byte32Map[bytes][bytes32] = &bytes32
	}

	fmt.Printf("Mem used for byte1+byte32 map: %s\n", printAlloc())
	byte32Map = nil
	runtime.GC()

	keySlice := make([]string, 1_000_000)

	for i := uint64(0); i < 1_000_000; i++ {
		// convert int to byte array
		binary.LittleEndian.PutUint64(key, i)
		keySlice[i] = utils.ReverseAndHexEncodeHash([32]byte(key[:32]))
	}

	fmt.Printf("Mem used for keySlice: %s\n", printAlloc())
	keySlice = nil
	runtime.GC()

	keySliceBytes := make([][32]byte, 1_000_000)

	for i := uint64(0); i < 1_000_000; i++ {
		// convert int to byte array
		binary.LittleEndian.PutUint64(key, i)
		keySliceBytes[i] = [32]byte(key[:32])
	}

	fmt.Printf("Mem used for keySliceBytes: %s\n", printAlloc())
}

func printAlloc() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return fmt.Sprintf("%d MB", m.Alloc/(1024*1024))
}
