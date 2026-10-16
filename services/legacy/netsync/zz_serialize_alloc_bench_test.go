package netsync

import (
	"bufio"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// BenchmarkSubtreeDataSerializeAllocs measures what the subtree-data write
// costs per transaction, which is the phase a block profile showed is not
// blocked on anything and a CPU profile showed spends 91% of its time in
// runtime.newobject.
//
// The writer is a bufio.Writer over io.Discard, i.e. the same shape the real
// path has (FileStorer wraps a bufio.Writer over a pipe) with the disk and the
// pipe removed, so every allocation counted here is go-bt's, not the store's.
func BenchmarkSubtreeDataSerializeAllocs(b *testing.B) {
	shapes := []struct {
		name              string
		nIn, nOut, sigLen int
		pkLen             int
		extended          bool
	}{
		{"2in_2out_p2pkh", 2, 2, 107, 25, false},
		{"2in_2out_p2pkh_extended", 2, 2, 107, 25, true},
		{"20in_2out_extended", 20, 2, 107, 25, true},
	}

	for _, s := range shapes {
		b.Run(s.name, func(b *testing.B) {
			tx := bt.NewTx()
			tx.Inputs = make([]*bt.Input, s.nIn)

			for i := range tx.Inputs {
				in := &bt.Input{
					UnlockingScript:    bscript.NewFromBytes(make([]byte, s.sigLen)),
					PreviousTxOutIndex: uint32(i),
					SequenceNumber:     0xffffffff,
				}
				h := &chainhash.Hash{}
				h[0] = byte(i)
				_ = in.PreviousTxIDAdd(h)

				if s.extended {
					in.PreviousTxSatoshis = 1000
					in.PreviousTxScript = bscript.NewFromBytes(make([]byte, s.pkLen))
				}

				tx.Inputs[i] = in
			}

			tx.Outputs = make([]*bt.Output, s.nOut)
			for i := range tx.Outputs {
				tx.Outputs[i] = &bt.Output{
					Satoshis:      1000,
					LockingScript: bscript.NewFromBytes(make([]byte, s.pkLen)),
				}
			}

			w := bufio.NewWriterSize(io.Discard, 64*1024)

			b.ReportAllocs()
			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				if _, err := tx.SerializeTo(w); err != nil {
					b.Fatal(err)
				}
			}

			b.StopTimer()
			_ = w.Flush()
		})
	}
}
