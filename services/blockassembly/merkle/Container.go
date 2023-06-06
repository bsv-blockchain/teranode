package merkle

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"

	"github.com/libsv/go-p2p/blockchain"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

var (
	baseFolder = "./merkle_containers"
)

type Container struct {
	chaintip        *chainhash.Hash
	height          uint32
	currentFile     *os.File
	folder          string
	fileCount       int32
	maxItemsPerFile uint32
	count           uint32
	write           bool
	fees            uint64
}

func OpenForWriting(chaintip *chainhash.Hash, height uint32, maxItemsPerFile uint32) (*Container, error) {
	// Always open the last file for this chaintip and hash
	folder := path.Join(baseFolder, fmt.Sprintf("%s-%d", chaintip.String(), height))
	if err := os.MkdirAll(folder, 0777); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(folder)
	if err != nil {
		return nil, err
	}

	var f *os.File
	var fileCount int32
	var count uint32
	var fees uint64

	if len(files) == 0 {
		var err error
		filename := path.Join(folder, fmt.Sprintf("%06d", fileCount)) // fileCount is zero
		f, err = os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			return nil, err
		}
		// When we create the first file, we put a hash in there as a placeholder from the coinbase
		b := make([]byte, 32)
		if _, err := f.Write(b); err != nil {
			return nil, err
		}

		fees := make([]byte, 8)
		if _, err := f.Write(fees); err != nil {
			return nil, err
		}

		count++

	} else {
		//TODO sort the files in order

		filename := path.Join(folder, files[len(files)-1].Name())
		f, err = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			return nil, err
		}

		// Read all the existing records and sum up their fees
		b := make([]byte, 32)
		feeBytes := make([]byte, 8)

		for {
			if _, err := f.Read(b); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, err
			}

			if _, err := f.Read(feeBytes); err != nil {
				return nil, err
			}

			count++
			fees += binary.LittleEndian.Uint64(feeBytes)
		}

		num, err := strconv.ParseInt(files[len(files)-1].Name(), 10, 64)
		if err != nil {
			return nil, err
		}

		fileCount = int32(num)
	}

	return &Container{
		chaintip:        chaintip,
		height:          height,
		fileCount:       fileCount,
		currentFile:     f,
		folder:          folder,
		count:           count,
		maxItemsPerFile: maxItemsPerFile,
		write:           true,
		fees:            fees,
	}, nil
}

func GetContainerCount(chaintip *chainhash.Hash, height uint32) (int, error) {
	folder := path.Join(baseFolder, fmt.Sprintf("%s-%d", chaintip.String(), height))
	dir, err := os.ReadDir(folder)
	if err != nil {
		return 0, err
	}

	return len(dir), nil
}

func OpenForReading(chaintip *chainhash.Hash, height uint32, fileNumber int) (*Container, error) {
	folder := path.Join(baseFolder, fmt.Sprintf("%s-%d", chaintip.String(), height))

	filename := path.Join(folder, fmt.Sprintf("%06d", fileNumber))
	f, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}

	pos, err := f.Seek(0, 2) // Seek to the end of the file
	if err != nil {
		f.Close()
		return nil, err
	}
	count := uint32(pos / int64(32+8))

	if _, err = f.Seek(0, 0); err != nil { // Seek to the beginning of the file
		f.Close()
		return nil, err
	}

	return &Container{
		chaintip:    chaintip,
		height:      height,
		currentFile: f,
		folder:      folder,
		count:       count,
		write:       false,
	}, nil
}

func (c *Container) Close() error {
	return c.currentFile.Close()
}

func (c *Container) AddTxID(txid *chainhash.Hash, fees uint64) error {
	if !c.write {
		return errors.New("file is not in write mode")
	}

	if c.count == c.maxItemsPerFile {
		// Rotate file
		c.currentFile.Close()
		c.fileCount++

		filename := path.Join(c.folder, fmt.Sprintf("%06d", c.fileCount))

		var err error
		c.currentFile, err = os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			return err
		}

		c.count = 0
	}

	if _, err := c.currentFile.Write(txid.CloneBytes()); err != nil {
		return err
	}

	feeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(feeBytes, fees)

	if _, err := c.currentFile.Write(feeBytes); err != nil {
		return err
	}

	c.count++
	c.fees += fees

	return nil
}

func (c *Container) Count() uint32 {
	return c.count
}

func (c *Container) MerkleRoot(coinbase *chainhash.Hash) (*chainhash.Hash, error) {
	if c.write {
		return nil, errors.New("container must be in read mode")
	}

	transactionHashes := make([][]byte, c.count)

	for i := 0; i < int(c.count); i++ {
		txid := make([]byte, 32)
		feeBytes := make([]byte, 8)

		if _, err := c.currentFile.Read(txid); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		if _, err := c.currentFile.Read(feeBytes); err != nil {
			return nil, err
		}

		if coinbase != nil && i == 0 {
			transactionHashes[i] = coinbase.CloneBytes()
		} else {
			transactionHashes[i] = txid[:]
		}

	}

	calculatedMerkleRoot := blockchain.BuildMerkleTreeStore(transactionHashes)

	hash, err := chainhash.NewHash(calculatedMerkleRoot[len(calculatedMerkleRoot)-1])
	if err != nil {
		return nil, err
	}

	return hash, nil
}

func (c *Container) deleteAll() error {
	if err := c.currentFile.Close(); err != nil {
		return err
	}

	return os.RemoveAll(c.folder)
}
