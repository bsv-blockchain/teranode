package mmaphash

import (
	"encoding/binary"
	"testing"
)

func BenchmarkUpsert(b *testing.B) {
	tbl, err := New(Options{Dir: b.TempDir(), Prefix: "bench", KeySize: 36, ValueSize: 0, Expected: uint64(b.N)})
	if err != nil {
		b.Fatal(err)
	}
	defer tbl.Close()

	key := make([]byte, 36)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(key[0:8], uint64(i))
		binary.LittleEndian.PutUint64(key[8:16], uint64(i)*0x9e3779b97f4a7c15)
		binary.LittleEndian.PutUint64(key[16:24], uint64(i)*2654435761)
		if _, _, err := tbl.Upsert(key, 0); err != nil {
			b.Fatal(err)
		}
	}
}
