package main

import (
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"runtime"

	"github.com/dolthub/swiss"
	"github.com/holiman/uint256"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/puzpuzpuz/xsync/v2"
)

type subBytes map[[32]byte]*[32]byte

var (
	nItems = uint64(1024 * 1024)
)

func main() {
	runtime.GC()
	fmt.Printf("Started with memory: %s\n", printAlloc())

	func() {
		uint256Map := make(map[uint256.Int]*uint256.Int, nItems)
		for i := uint64(0); i < nItems; i++ {
			ii := uint256.NewInt(i)
			iii := uint256.NewInt(i + nItems*2)
			uint256Map[*ii] = iii
		}

		fmt.Printf("Mem used for uint256: %s\n", printAlloc())
		uint256Map = nil
	}()

	runtime.GC()

	bs := make([]byte, 32)
	var ii chainhash.Hash
	var iii chainhash.Hash

	func() {
		chainhashMap := make(map[chainhash.Hash]*chainhash.Hash, nItems)
		for i := uint64(0); i < nItems; i++ {
			binary.LittleEndian.PutUint64(bs, i)
			ii = chainhash.HashH(bs)
			binary.LittleEndian.PutUint64(bs, i+nItems*2)
			iii = chainhash.HashH(bs)
			chainhashMap[ii] = &iii
		}

		fmt.Printf("Mem used for chainhashMap: %s\n", printAlloc())

		// check map
		for i := uint64(0); i < nItems; i++ {
			binary.LittleEndian.PutUint64(bs, i)
			ii = chainhash.HashH(bs)
			binary.LittleEndian.PutUint64(bs, i+nItems*2)
			iii = chainhash.HashH(bs)
			item, ok := chainhashMap[ii]
			if !ok || !item.Equal(iii) {
				fmt.Printf("item %d not found in xsyncmap\n", i)
			}
		}

		chainhashMap = nil
	}()

	runtime.GC()

	func() {
		chainhashMap := make(map[chainhash.Hash]chainhash.Hash, nItems)
		for i := uint64(0); i < nItems; i++ {
			binary.LittleEndian.PutUint64(bs, i)
			ii = chainhash.HashH(bs)
			binary.LittleEndian.PutUint64(bs, i+nItems*2)
			iii = chainhash.HashH(bs)
			chainhashMap[ii] = iii
		}

		fmt.Printf("Mem used for chainhashMap values: %s\n", printAlloc())

		// check map
		for i := uint64(0); i < nItems; i++ {
			binary.LittleEndian.PutUint64(bs, i)
			ii = chainhash.HashH(bs)
			binary.LittleEndian.PutUint64(bs, i+nItems*2)
			iii = chainhash.HashH(bs)
			item, ok := chainhashMap[ii]
			if !ok || !item.Equal(iii) {
				fmt.Printf("item %d not found in xsyncmap\n", i)
			}
		}

		chainhashMap = nil
	}()

	runtime.GC()

	func() {
		swissMap := swiss.NewMap[chainhash.Hash, *chainhash.Hash](uint32(nItems))
		for i := uint64(0); i < nItems; i++ {
			binary.LittleEndian.PutUint64(bs, i)
			ii = chainhash.HashH(bs)
			binary.LittleEndian.PutUint64(bs, i+nItems*2)
			iii = chainhash.HashH(bs)

			swissMap.Put(ii, &iii)
		}

		fmt.Printf("Mem used for swissMap items: %s\n", printAlloc())

		// check map
		for i := uint64(0); i < nItems; i++ {
			binary.LittleEndian.PutUint64(bs, i)
			ii = chainhash.HashH(bs)
			binary.LittleEndian.PutUint64(bs, i+nItems*2)
			iii = chainhash.HashH(bs)
			item, ok := swissMap.Get(ii)
			if !ok || !item.Equal(iii) {
				fmt.Printf("item %d not found in xsyncmap\n", i)
			}
		}

		swissMap = nil
	}()

	runtime.GC()

	func() {
		swissMap := swiss.NewMap[chainhash.Hash, chainhash.Hash](uint32(nItems))
		for i := uint64(0); i < nItems; i++ {
			binary.LittleEndian.PutUint64(bs, i)
			ii = chainhash.HashH(bs)
			binary.LittleEndian.PutUint64(bs, i+nItems*2)
			iii = chainhash.HashH(bs)

			swissMap.Put(ii, iii)
		}

		fmt.Printf("Mem used for swissMap *items: %s\n", printAlloc())

		// check map
		for i := uint64(0); i < nItems; i++ {
			binary.LittleEndian.PutUint64(bs, i)
			ii = chainhash.HashH(bs)
			binary.LittleEndian.PutUint64(bs, i+nItems*2)
			iii = chainhash.HashH(bs)
			item, ok := swissMap.Get(ii)
			if !ok || !item.Equal(iii) {
				fmt.Printf("item %d not found in xsyncmap\n", i)
			}
		}

		swissMap = nil
	}()

	runtime.GC()

	func() {
		xsyncMap := xsync.NewTypedMapOfPresized[chainhash.Hash, *chainhash.Hash](func(seed maphash.Seed, hash chainhash.Hash) uint64 {
			var h maphash.Hash
			h.SetSeed(seed)
			_ = binary.Write(&h, binary.LittleEndian, hash[:16])
			hh := h.Sum64()
			h.Reset()
			_ = binary.Write(&h, binary.LittleEndian, hash[16:32])
			return 31*hh + h.Sum64()
		}, int(nItems))
		for i := uint64(0); i < nItems; i++ {
			binary.LittleEndian.PutUint64(bs, i)
			ii = chainhash.HashH(bs)
			binary.LittleEndian.PutUint64(bs, i+nItems*2)
			iii = chainhash.HashH(bs)
			xsyncMap.Store(ii, &iii)
		}

		fmt.Printf("Mem used for xsyncMap items: %s\n", printAlloc())

		// check map
		for i := uint64(0); i < nItems; i++ {
			binary.LittleEndian.PutUint64(bs, i)
			ii = chainhash.HashH(bs)
			binary.LittleEndian.PutUint64(bs, i+nItems*2)
			iii = chainhash.HashH(bs)
			item, ok := xsyncMap.Load(ii)
			if !ok || *item != iii {
				fmt.Printf("item %d not found in xsyncmap\n", i)
			}
		}

		xsyncMap = nil
	}()

	runtime.GC()

	key := make([]byte, 32)

	func() {
		byte32Map := make(map[[1]byte]subBytes)
		for i := uint64(0); i < uint64(256); i++ {
			binary.BigEndian.PutUint64(key, i)
			byte32Map[[1]byte(key[:1])] = make(subBytes)
		}

		var bytes [1]byte
		var bytes32 [32]byte
		for i := uint64(0); i < nItems; i++ {
			binary.BigEndian.PutUint64(key, i)
			bytes = [1]byte(key[:1])
			bytes32 = [32]byte(key[:32])
			byte32Map[bytes][bytes32] = &bytes32
		}

		fmt.Printf("Mem used for byte1+byte32 map: %s\n", printAlloc())
		byte32Map = nil
	}()

	runtime.GC()

	func() {
		keySlice := make([]string, nItems)

		for i := uint64(0); i < nItems; i++ {
			// convert int to byte array
			binary.LittleEndian.PutUint64(key, i)
			keySlice[i] = utils.ReverseAndHexEncodeHash([32]byte(key[:32]))
		}

		fmt.Printf("Mem used for keySlice: %s\n", printAlloc())
	}()

	runtime.GC()

	keySliceBytes := make([][32]byte, nItems)

	for i := uint64(0); i < nItems; i++ {
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
