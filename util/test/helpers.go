package test

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"

	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
)

func ReadTxMeta(r io.Reader, txMetaStore *txmetacache.TxMetaCache) error {
	// read from the reader and add to txMeta store
	txHash := chainhash.Hash{}
	var fee uint64
	var sizeInBytes uint64

	g := errgroup.Group{}

	batch := make([]FeeAndSize, 0, 1024)

	b := make([]byte, 48)
	for {
		_, err := io.ReadFull(r, b)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		copy(txHash[:], b[:32])
		fee = binary.LittleEndian.Uint64(b[32:40])
		sizeInBytes = binary.LittleEndian.Uint64(b[40:48])

		batch = append(batch, FeeAndSize{
			hash:        txHash,
			fee:         fee,
			sizeInBytes: sizeInBytes,
		})

		if len(batch) == 1024 {
			saveBatch := batch
			g.Go(func() error {
				for _, data := range saveBatch {
					hash := data.hash
					if err = txMetaStore.SetCache(&hash, &meta.Data{
						Fee:            data.fee,
						SizeInBytes:    data.sizeInBytes,
						ParentTxHashes: []chainhash.Hash{},
					}); err != nil {
						return err
					}
				}

				return nil
			})

			batch = make([]FeeAndSize, 0, 1024)
		}
	}

	if err := g.Wait(); err != nil {
		return err
	}

	// remainder batch
	if len(batch) > 0 {
		for _, data := range batch {
			hash := data.hash
			if err := txMetaStore.SetCache(&hash, &meta.Data{
				Fee:            data.fee,
				SizeInBytes:    data.sizeInBytes,
				ParentTxHashes: []chainhash.Hash{},
			}); err != nil {
				return err
			}
		}
	}

	return nil
}

func WriteTxMeta(w io.Writer, txHash chainhash.Hash, fee, sizeInBytes uint64) (int, error) {
	b := make([]byte, 48)
	copy(b[:32], txHash[:])
	binary.LittleEndian.PutUint64(b[32:40], fee)
	binary.LittleEndian.PutUint64(b[40:48], sizeInBytes)

	return w.Write(b)
}

func CalculateMerkleRoot(hashes []*chainhash.Hash) (*chainhash.Hash, error) {
	var calculatedMerkleRootHash *chainhash.Hash
	if len(hashes) == 1 {
		calculatedMerkleRootHash = hashes[0]
	} else if len(hashes) > 0 {
		// Create a new subtree with the hashes of the subtrees
		st, err := util.NewTreeByLeafCount(util.CeilPowerOfTwo(len(hashes)))
		if err != nil {
			return nil, err
		}
		for _, hash := range hashes {
			err := st.AddNode(*hash, 1, 0)
			if err != nil {
				return nil, err
			}
		}

		calculatedMerkleRoot := st.RootHash()
		calculatedMerkleRootHash, err = chainhash.NewHash(calculatedMerkleRoot[:])
		if err != nil {
			return nil, err
		}
	}

	return calculatedMerkleRootHash, nil
}

func LoadTxMetaIntoMemory() error {
	// create a reader from the txmetacache file
	file, err := os.Open(TxMetafileNameTemplate)
	if err != nil {
		return err
	}
	defer file.Close()

	// create a buffered reader for the file
	bufReader := bufio.NewReaderSize(file, 55*1024*1024)

	if err = ReadTxMeta(bufReader, CachedTxMetaStore.(*txmetacache.TxMetaCache)); err != nil {
		return err
	}

	return err
}
