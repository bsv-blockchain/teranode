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

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/txmetacache"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
)

// TODO: this test util is here as importing util/test in the model package causes a circular dependency. Fix this

var (
	TestLoadMetaToMemoryOnce sync.Once
	// TestCachedTxMetaStore is a global variable to cache the txMetaStore in memory, to avoid reading from disk more than once
	TestCachedTxMetaStore utxo.Store
	// following variables are used to store the file names for the testdata
	TestFileDir                      string
	TestFileNameTemplate             string
	TestFileNameTemplateMerkleHashes string
	TestFileNameTemplateBlock        string
	TestTxMetafileNameTemplate       string
	TestSubtreeSize                  int
)

const CoinbaseHex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"
const notImplemented = "not implemented"

func GenerateTestBlock(transactionIDCount uint64, subtreeStore *TestLocalSubtreeStore, generateNewTestData bool) (*Block, error) {
	// create test dir of not exists
	if _, err := os.Stat(TestFileDir); os.IsNotExist(err) {
		if err = os.Mkdir(TestFileDir, 0755); err != nil {
			return nil, err
		}
	}

	// read block from file and return if exists
	blockFile, err := os.Open(TestFileNameTemplateBlock)
	if err == nil && !generateNewTestData {
		blockBytes, err := io.ReadAll(blockFile)
		if err != nil {
			return nil, err
		}

		_ = blockFile.Close()

		block, err := NewBlockFromBytes(blockBytes, nil)
		if err != nil {
			return nil, err
		}

		return block, nil
	}

	txMetastoreFile, err := os.Create(TestTxMetafileNameTemplate)
	if err != nil {
		return nil, err
	}

	txMetastoreWriter := bufio.NewWriter(txMetastoreFile)
	defer func() {
		_ = txMetastoreWriter.Flush() // Ensure all data is written to the underlying writer
		_ = txMetastoreFile.Close()
	}()

	var subtreeBytes []byte

	subtree, err := util.NewTreeByLeafCount(TestSubtreeSize)
	if err != nil {
		return nil, err
	}

	_ = subtree.AddCoinbaseNode()

	var (
		subtreeFile             *os.File
		subtreeFileMerkleHashes *os.File
		subtreeCount            int
	)

	// create the first files
	subtreeFile, err = os.Create(fmt.Sprintf(TestFileNameTemplate, subtreeCount))
	if err != nil {
		return nil, err
	}

	subtreeFileMerkleHashes, err = os.Create(TestFileNameTemplateMerkleHashes)
	if err != nil {
		return nil, err
	}

	subtreeHashes := make([]*chainhash.Hash, 0)

	txID := make([]byte, 32)

	var (
		hash chainhash.Hash
		fees uint64
		n    int
	)

	for i := 1; i < int(transactionIDCount); i++ { // nolint:gosec
		binary.LittleEndian.PutUint64(txID, uint64(i)) // nolint:gosec
		hash = chainhash.Hash(txID)

		if err = subtree.AddNode(hash, uint64(i), uint64(i)); err != nil { // nolint:gosec
			return nil, err
		}

		n, err = WriteTxMeta(txMetastoreWriter, hash, uint64(i), uint64(i)) // nolint:gosec
		if err != nil {
			return nil, err
		}

		if n != 48 {
			return nil, errors.NewProcessingError("expected to write 48 bytes, wrote %d", n)
		}

		fees += uint64(i) // nolint:gosec

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

			if subtreeFile, err = os.Create(fmt.Sprintf(TestFileNameTemplate, subtreeCount)); err != nil {
				return nil, err
			}

			// create new tree
			subtree, err = util.NewTreeByLeafCount(TestSubtreeSize)
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

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	if err != nil {
		return nil, err
	}

	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+fees)

	nBits, _ := NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	var merkleRootsubtreeHashes []*chainhash.Hash

	for i := 0; i < subtreeCount; i++ {
		subtreeStore.Files[*subtreeHashes[i]] = i

		if i == 0 {
			// read the first subtree into file, replace the coinbase placeholder with the coinbase txid and calculate the merkle root
			replacedCoinbaseSubtree, err := util.NewTreeByLeafCount(TestSubtreeSize)
			if err != nil {
				return nil, err
			}

			subtreeFile, err = os.Open(fmt.Sprintf(TestFileNameTemplate, i))
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

			replacedCoinbaseSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) // nolint:gosec

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
		Timestamp:      uint32(time.Now().Unix()), // nolint:gosec
		Bits:           *nBits,
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
		TransactionCount: transactionIDCount,
		SizeInBytes:      123123,
		Subtrees:         subtreeHashes,
		Height:           123,
		settings:         settings.NewSettings(),
	}

	blockFile, err = os.Create(TestFileNameTemplateBlock)
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

func LoadTxMetaIntoMemory() error {
	// create a reader from the txmetacache file
	file, err := os.Open(TestTxMetafileNameTemplate)
	if err != nil {
		return err
	}
	defer file.Close()

	// create a buffered reader for the file
	bufReader := bufio.NewReaderSize(file, 55*1024*1024)

	if err = ReadTxMeta(bufReader, TestCachedTxMetaStore.(*txmetacache.TxMetaCache)); err != nil {
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

	var (
		fee         uint64
		sizeInBytes uint64
	)

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
						Fee:         data.fee,
						SizeInBytes: data.sizeInBytes,
						TxInpoints:  meta.TxInpoints{ParentTxHashes: []chainhash.Hash{}},
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
				Fee:         data.fee,
				SizeInBytes: data.sizeInBytes,
				TxInpoints:  meta.TxInpoints{ParentTxHashes: []chainhash.Hash{}},
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

type TestLocalSubtreeStore struct {
	Files    map[chainhash.Hash]int
	FileData map[string][]byte // For bloom filters and other non-subtree data
}

func NewLocalSubtreeStore() *TestLocalSubtreeStore {
	return &TestLocalSubtreeStore{
		Files:    make(map[chainhash.Hash]int),
		FileData: make(map[string][]byte),
	}
}

func (l TestLocalSubtreeStore) Health(_ context.Context, _ bool) (int, string, error) {
	return 0, "", nil
}

func (l TestLocalSubtreeStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	if len(key) == 0 {
		return false, errors.NewProcessingError("key cannot be empty")
	}

	// Parse options
	var opt options.Options
	for _, o := range opts {
		o(&opt)
	}

	// Check if it exists in the FileData map
	keyString := string(key)
	keyString = keyString + "." + fileType.String()

	if l.FileData != nil {
		if _, ok := l.FileData[keyString]; ok {
			return true, nil
		}
	}

	// Check if it exists in the Files map
	_, ok := l.Files[chainhash.Hash(key)]

	return ok, nil
}

func (l TestLocalSubtreeStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	if len(key) == 0 {
		return nil, errors.NewProcessingError("key cannot be empty")
	}

	// Try to find the data in the FileData map first (for bloom filters and other data)
	keyString := string(key)
	keyString = keyString + "." + fileType.String()

	if l.FileData != nil {
		if data, ok := l.FileData[keyString]; ok {
			return data, nil
		}
	}

	// If not found in FileData, use the original subtree lookup logic
	file, ok := l.Files[chainhash.Hash(key)]
	if !ok {
		return nil, errors.NewProcessingError("file not found")
	}

	subtreeBytes, err := os.ReadFile(fmt.Sprintf(TestFileNameTemplate, file))
	if err != nil {
		return nil, err
	}

	return subtreeBytes, nil
}

func (l TestLocalSubtreeStore) GetIoReader(_ context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	file, ok := l.Files[chainhash.Hash(key)]
	if !ok {
		return nil, errors.NewProcessingError("file not found")
	}

	subtreeFile, err := os.Open(fmt.Sprintf(TestFileNameTemplate, file))
	if err != nil {
		return nil, err
	}

	return subtreeFile, nil
}

func (l *TestLocalSubtreeStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	if len(key) == 0 {
		return errors.NewProcessingError("key cannot be empty")
	}

	// Create a map for storing bloom filters and other data if it doesn't exist
	if l.FileData == nil {
		l.FileData = make(map[string][]byte)
	}

	// Create a storage key based on the hash and extension
	keyString := string(key)
	keyString = keyString + "." + fileType.String()

	// Store the data in memory
	l.FileData[keyString] = make([]byte, len(value))
	copy(l.FileData[keyString], value)

	return nil
}

func (l TestLocalSubtreeStore) SetFromReader(_ context.Context, _ []byte, _ fileformat.FileType, _ io.ReadCloser, _ ...options.FileOption) error {
	panic(notImplemented)
}

func (l TestLocalSubtreeStore) SetDAH(_ context.Context, _ []byte, _ fileformat.FileType, _ uint32, _ ...options.FileOption) error {
	panic(notImplemented)
}

func (l TestLocalSubtreeStore) GetDAH(_ context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) (uint32, error) {
	panic(notImplemented)
}

func (l TestLocalSubtreeStore) Del(_ context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) error {
	panic(notImplemented)
}

func (l TestLocalSubtreeStore) Close(_ context.Context) error {
	panic(notImplemented)
}

func (l TestLocalSubtreeStore) SetCurrentBlockHeight(_ uint32) {
	// noop
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

func (n *BlobStoreStub) Health(_ context.Context, _ bool) (int, string, error) {
	return 0, "BlobStoreStub Store", nil
}

func (n *BlobStoreStub) Close(_ context.Context) error {
	return nil
}

func (n *BlobStoreStub) SetFromReader(_ context.Context, _ []byte, _ fileformat.FileType, _ io.ReadCloser, _ ...options.FileOption) error {
	return nil
}

func (n *BlobStoreStub) Set(_ context.Context, _ []byte, _ fileformat.FileType, _ []byte, _ ...options.FileOption) error {
	return nil
}

func (n *BlobStoreStub) SetDAH(_ context.Context, _ []byte, _ fileformat.FileType, _ uint32, _ ...options.FileOption) error {
	return nil
}

func (n *BlobStoreStub) GetDAH(_ context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) (uint32, error) {
	return 0, nil
}

func (n *BlobStoreStub) GetIoReader(_ context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) (io.ReadCloser, error) {
	path := filepath.Join("testdata", "testSubtreeHex.bin")

	// read the file
	subtreeReader, err := os.Open(path)
	if err != nil {
		return nil, errors.NewProcessingError("failed to read file: %s", err)
	}

	return subtreeReader, nil
}

func (n *BlobStoreStub) Get(_ context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) ([]byte, error) {
	path := filepath.Join("testdata", "testSubtreeHex.bin")

	// read the file
	subtreeBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.NewProcessingError("failed to read file: %s", err)
	}

	return subtreeBytes, nil
}

func (n *BlobStoreStub) Exists(_ context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) (bool, error) {
	return false, nil
}

func (n *BlobStoreStub) Del(_ context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) error {
	return nil
}

func (n *BlobStoreStub) SetCurrentBlockHeight(_ uint32) {
	// noop
}
