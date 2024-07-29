package model

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
)

// TODO: this test util is here as importing util/test in the model package causes a circular dependency. Fix this

var (
	loadMetaToMemoryOnce sync.Once
	// cachedTxMetaStore is a global variable to cache the txMetaStore in memory, to avoid reading from disk more than once
	cachedTxMetaStore utxo.Store
	// following variables are used to store the file names for the testdata
	fileDir                      string
	fileNameTemplate             string
	fileNameTemplateMerkleHashes string
	fileNameTemplateBlock        string
	txMetafileNameTemplate       string
	subtreeSize                  int
)

func generateTestBlock(transactionIdCount uint64, subtreeStore *localSubtreeStore, generateNewTestData bool) (*Block, error) {
	// create test dir of not exists
	if _, err := os.Stat(fileDir); os.IsNotExist(err) {
		if err = os.Mkdir(fileDir, 0755); err != nil {
			return nil, err
		}
	}

	// read block from file and return if exists
	blockFile, err := os.Open(fileNameTemplateBlock)
	if err == nil && !generateNewTestData {
		blockBytes, err := io.ReadAll(blockFile)
		if err != nil {
			return nil, err
		}
		_ = blockFile.Close()

		block, err := NewBlockFromBytes(blockBytes)
		if err != nil {
			return nil, err
		}

		return block, nil
	}

	txMetastoreFile, err := os.Create(txMetafileNameTemplate)
	if err != nil {
		return nil, err
	}

	txMetastoreWriter := bufio.NewWriter(txMetastoreFile)
	defer func() {
		_ = txMetastoreWriter.Flush() // Ensure all data is written to the underlying writer
		_ = txMetastoreFile.Close()
	}()

	var subtreeBytes []byte
	subtree, err := util.NewTreeByLeafCount(subtreeSize)
	if err != nil {
		return nil, err
	}
	_ = subtree.AddNode(CoinbasePlaceholder, 0, 0)

	var subtreeFile *os.File
	var subtreeFileMerkleHashes *os.File
	subtreeCount := 0

	// create the first files
	subtreeFile, err = os.Create(fmt.Sprintf(fileNameTemplate, subtreeCount))
	if err != nil {
		return nil, err
	}
	subtreeFileMerkleHashes, err = os.Create(fileNameTemplateMerkleHashes)
	if err != nil {
		return nil, err
	}

	subtreeHashes := make([]*chainhash.Hash, 0)

	txId := make([]byte, 32)
	var hash chainhash.Hash
	fees := uint64(0)
	var n int
	for i := 1; i < int(transactionIdCount); i++ {
		binary.LittleEndian.PutUint64(txId, uint64(i))
		hash = chainhash.Hash(txId)

		if err = subtree.AddNode(hash, uint64(i), uint64(i)); err != nil {
			return nil, err
		}

		n, err = WriteTxMeta(txMetastoreWriter, hash, uint64(i), uint64(i))
		if err != nil {
			return nil, err
		}
		if n != 48 {
			return nil, errors.NewProcessingError("expected to write 48 bytes, wrote %d", n)
		}

		fees += uint64(i)

		if subtree.IsComplete() {
			// write subtree bytes to file
			if subtreeBytes, err = subtree.Serialize(); err != nil {
				return nil, err
			}

			if _, err = subtreeFile.Write(subtreeBytes); err != nil {
				return nil, err
			}

			subtreeHashes = append(subtreeHashes, subtree.RootHash())

			if err = subtreeFile.Close(); err != nil {
				return nil, err
			}

			subtreeCount++

			if subtreeFile, err = os.Create(fmt.Sprintf(fileNameTemplate, subtreeCount)); err != nil {
				return nil, err
			}

			// create new tree
			subtree, err = util.NewTreeByLeafCount(subtreeSize)
			if err != nil {
				return nil, err
			}
		}
	}

	// write the last subtree
	if subtree.Length() > 0 {
		// write subtree bytes to file
		subtreeBytes, err = subtree.Serialize()
		if err != nil {
			return nil, err
		}

		if _, err = subtreeFile.Write(subtreeBytes); err != nil {
			return nil, err
		}

		subtreeHashes = append(subtreeHashes, subtree.RootHash())

		if err = subtreeFile.Close(); err != nil {
			return nil, err
		}
	}
	coinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"
	coinbase, err := bt.NewTxFromString(coinbaseHex)
	if err != nil {
		return nil, err
	}
	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+fees)

	nBits := NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

	var merkleRootsubtreeHashes []*chainhash.Hash

	for i := 0; i < subtreeCount; i++ {
		subtreeStore.files[*subtreeHashes[i]] = i

		if i == 0 {
			// read the first subtree into file, replace the coinbase placeholder with the coinbase txid and calculate the merkle root
			replacedCoinbaseSubtree, err := util.NewTreeByLeafCount(subtreeSize)
			if err != nil {
				return nil, err
			}
			subtreeFile, err = os.Open(fmt.Sprintf(fileNameTemplate, i))
			if err != nil {
				return nil, err
			}

			subtreeBytes, err = io.ReadAll(subtreeFile)
			if err != nil {
				return nil, err
			}

			_ = subtreeFile.Close()

			err = replacedCoinbaseSubtree.Deserialize(subtreeBytes)
			if err != nil {
				return nil, err
			}

			replacedCoinbaseSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))

			rootHash := replacedCoinbaseSubtree.RootHash()
			merkleRootsubtreeHashes = append(merkleRootsubtreeHashes, rootHash)
		} else {
			merkleRootsubtreeHashes = append(merkleRootsubtreeHashes, subtreeHashes[i])
		}
	}

	for _, hash := range merkleRootsubtreeHashes {
		if _, err = subtreeFileMerkleHashes.Write(hash[:]); err != nil {
			return nil, err
		}
	}
	if err = subtreeFileMerkleHashes.Close(); err != nil {
		return nil, err
	}

	var calculatedMerkleRootHash *chainhash.Hash
	if calculatedMerkleRootHash, err = calculateMerkleRoot(merkleRootsubtreeHashes); err != nil {
		return nil, err
	}

	blockHeader := &BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           nBits,
		Nonce:          0,
	}

	// mine to the target difficulty
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}
		blockHeader.Nonce++

		if blockHeader.Nonce%1000000 == 0 {
			fmt.Printf("mining Nonce: %d, hash: %s\n", blockHeader.Nonce, blockHeader.Hash().String())
		}
	}

	if subtreeCount != len(subtreeHashes) {
		return nil, errors.NewProcessingError("subtree count %d does not match subtree hash count %d", subtreeCount, len(subtreeHashes))
	}

	block := &Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: transactionIdCount,
		SizeInBytes:      123123,
		Subtrees:         subtreeHashes,
	}

	blockFile, err = os.Create(fileNameTemplateBlock)
	if err != nil {
		return nil, err
	}

	blockBytes, err := block.Bytes()
	if err != nil {
		return nil, err
	}

	_, err = blockFile.Write(blockBytes)
	if err != nil {
		return nil, err
	}
	if err = blockFile.Close(); err != nil {
		return nil, err
	}

	return block, nil
}

func loadTxMetaIntoMemory() error {
	// create a reader from the txmetacache file
	file, err := os.Open(txMetafileNameTemplate)
	if err != nil {
		return err
	}
	defer file.Close()

	// create a buffered reader for the file
	bufReader := bufio.NewReaderSize(file, 55*1024*1024)

	if err = ReadTxMeta(bufReader, cachedTxMetaStore.(*txmetacache.TxMetaCache)); err != nil {
		return err
	}

	return err
}

type feeAndSize struct {
	hash        chainhash.Hash
	fee         uint64
	sizeInBytes uint64
}

func ReadTxMeta(r io.Reader, txMetaStore *txmetacache.TxMetaCache) error {
	// read from the reader and add to txMeta store
	txHash := chainhash.Hash{}
	var fee uint64
	var sizeInBytes uint64

	g := errgroup.Group{}

	batch := make([]feeAndSize, 0, 1024)

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

		batch = append(batch, feeAndSize{
			hash:        txHash,
			fee:         fee,
			sizeInBytes: sizeInBytes,
		})

		if len(batch) == 1024 {
			saveBatch := batch
			g.Go(func() error {
				for _, data := range saveBatch {
					data := data
					if err = txMetaStore.SetCache(&data.hash, &meta.Data{
						Fee:            data.fee,
						SizeInBytes:    data.sizeInBytes,
						ParentTxHashes: []chainhash.Hash{},
					}); err != nil {
						return err
					}
				}

				return nil
			})

			batch = make([]feeAndSize, 0, 1024)
		}
	}

	if err := g.Wait(); err != nil {
		return err
	}

	// remainder batch
	if len(batch) > 0 {
		for _, data := range batch {
			data := data
			if err := txMetaStore.SetCache(&data.hash, &meta.Data{
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

func calculateMerkleRoot(hashes []*chainhash.Hash) (*chainhash.Hash, error) {
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

type localSubtreeStore struct {
	files map[chainhash.Hash]int
}

func newLocalSubtreeStore() *localSubtreeStore {
	return &localSubtreeStore{
		files: make(map[chainhash.Hash]int),
	}
}

func (l localSubtreeStore) Health(_ context.Context) (int, string, error) {
	return 0, "", nil
}

func (l localSubtreeStore) Exists(_ context.Context, key []byte, opts ...options.Options) (bool, error) {
	_, ok := l.files[chainhash.Hash(key)]
	return ok, nil
}

func (l localSubtreeStore) Get(_ context.Context, key []byte, opts ...options.Options) ([]byte, error) {
	file, ok := l.files[chainhash.Hash(key)]
	if !ok {
		return nil, errors.NewProcessingError("file not found")
	}

	subtreeBytes, err := os.ReadFile(fmt.Sprintf(fileNameTemplate, file))
	if err != nil {
		return nil, err
	}

	return subtreeBytes, nil
}

func (l localSubtreeStore) GetIoReader(_ context.Context, key []byte, opts ...options.Options) (io.ReadCloser, error) {
	file, ok := l.files[chainhash.Hash(key)]
	if !ok {
		return nil, errors.NewProcessingError("file not found")
	}

	subtreeFile, err := os.Open(fmt.Sprintf(fileNameTemplate, file))
	if err != nil {
		return nil, err
	}

	return subtreeFile, nil
}

func (l localSubtreeStore) Set(_ context.Context, _ []byte, _ []byte, _ ...options.Options) error {
	panic("not implemented")
}

func (l localSubtreeStore) SetFromReader(_ context.Context, _ []byte, _ io.ReadCloser, _ ...options.Options) error {
	panic("not implemented")
}

func (l localSubtreeStore) SetTTL(_ context.Context, _ []byte, _ time.Duration, opts ...options.Options) error {
	panic("not implemented")
}

func (l localSubtreeStore) Del(_ context.Context, _ []byte, opts ...options.Options) error {
	panic("not implemented")
}

func (l localSubtreeStore) GetHead(_ context.Context, _ []byte, _ int, opts ...options.Options) ([]byte, error) {
	panic("not implemented")
}

func (l localSubtreeStore) Close(_ context.Context) error {
	return nil
}

type BlobStoreStub struct {
	logger ulogger.Logger
}

func New(logger ulogger.Logger) (*BlobStoreStub, error) {
	logger = logger.New("null")

	return &BlobStoreStub{
		logger: logger,
	}, nil
}

func (n *BlobStoreStub) Health(_ context.Context) (int, string, error) {
	return 0, "BlobStoreStub Store", nil
}

func (n *BlobStoreStub) Close(_ context.Context) error {
	return nil
}

func (n *BlobStoreStub) SetFromReader(_ context.Context, _ []byte, _ io.ReadCloser, _ ...options.Options) error {
	return nil
}

func (n *BlobStoreStub) Set(_ context.Context, _ []byte, _ []byte, _ ...options.Options) error {
	return nil
}

func (n *BlobStoreStub) SetTTL(_ context.Context, _ []byte, _ time.Duration, opts ...options.Options) error {
	return nil
}

func (n *BlobStoreStub) GetIoReader(_ context.Context, _ []byte, opts ...options.Options) (io.ReadCloser, error) {
	path := filepath.Join("testdata", "testSubtreeHex.bin")

	// read the file
	subtreeReader, err := os.Open(path)
	if err != nil {
		return nil, errors.NewProcessingError("failed to read file: %s", err)
	}

	return subtreeReader, nil
}

func (n *BlobStoreStub) Get(_ context.Context, hash []byte, opts ...options.Options) ([]byte, error) {
	path := filepath.Join("testdata", "testSubtreeHex.bin")

	// read the file
	subtreeBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.NewProcessingError("failed to read file: %s", err)
	}

	return subtreeBytes, nil
}

func (n *BlobStoreStub) Exists(_ context.Context, _ []byte, opts ...options.Options) (bool, error) {
	return false, nil
}

func (n *BlobStoreStub) Del(_ context.Context, _ []byte, opts ...options.Options) error {
	return nil
}

func (n *BlobStoreStub) GetHead(_ context.Context, _ []byte, _ int, opts ...options.Options) ([]byte, error) {
	return nil, nil
}
