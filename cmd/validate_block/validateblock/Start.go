package validateblock

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bc"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func Start() {
	logger := ulogger.NewGoCoreLogger("main")

	fmt.Println()

	if len(os.Args) < 2 {
		fmt.Printf("Usage: validate_block <filename | hash>\n\n")
		return
	}

	filename := os.Args[1]

	start := time.Now()
	defer func() {
		logger.Infof("Time taken: %s", time.Since(start))
	}()

	var r io.Reader
	var err error

	if hash, err := chainhash.NewHashFromStr(filename); err == nil {
		// This is a valid hash, so we'll assume it's a block hash and read it from the store
		persistURL, err, ok := gocore.Config().GetURL("blockPersister_persistURL")
		if err != nil || !ok {
			logger.Fatalf("Error getting blockpersister_store URL: %v", err)
		}

		store, err := blob.NewStore(logger, persistURL)
		if err != nil {
			logger.Errorf("failed to open store at %s: %s", persistURL, err)
			return
		}

		rc, err := store.GetIoReader(context.Background(), nil, options.WithFileName(hash.String()), options.WithSubDirectory("blocks"))
		if err != nil {
			logger.Errorf("error getting reader from store: %s", err)
			return
		}

		r = rc
		defer rc.Close()

	} else {
		f, err := os.Open(filename)
		if err != nil {
			logger.Errorf("%s", err)
			return
		}

		r = f
		defer f.Close()
	}

	// Wrap the reader with a buffered reader
	r = bufio.NewReaderSize(r, 1024*1024)

	logger.Infof("Validating block %s", filename)

	valid, err := ValidateBlock(r, logger)
	if err != nil {
		logger.Errorf("Error during validation: %v", err)
		return
	}

	if valid {
		logger.Infof("Block is valid")
	} else {
		logger.Errorf("Block is NOT valid")
	}

	fmt.Println()
}

// ValidateBlock validates a Bitcoin block.
func ValidateBlock(r io.Reader, logger ulogger.Logger) (bool, error) {
	buf := make([]byte, 80)
	n, err := r.Read(buf)
	if err != nil {
		return false, err
	}

	if n != 80 {
		return false, errors.New("block header is not 80 bytes")
	}

	blockHeader, err := bc.NewBlockHeaderFromBytes(buf)
	if err != nil {
		return false, err
	}

	txIDs := make([]string, 0)

	// Read the varint of the number of transactions in the block
	var txCount bt.VarInt
	if _, err := txCount.ReadFrom(r); err != nil {
		return false, err
	}

	for {
		// Read the next transaction
		tx := bt.NewTx()
		if _, err := tx.ReadFrom(r); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return false, err
		}

		txIDs = append(txIDs, tx.TxIDChainHash().String())
	}

	root, err := bc.BuildMerkleRoot(txIDs)
	if err != nil {
		logger.Errorf("%s", err)
		return false, err
	}

	if root != blockHeader.HashMerkleRootStr() {
		return false, nil
	}

	return true, nil
}
