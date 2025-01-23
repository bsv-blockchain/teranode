package bitcoin2utxoset

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"github.com/bitcoin-sv/teranode/cmd/bitcoin2utxoset/bitcoin"
	"github.com/bitcoin-sv/teranode/cmd/bitcoin2utxoset/bitcoin/btcleveldb"
	"github.com/bitcoin-sv/teranode/cmd/bitcoin2utxoset/bitcoin/keys"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/utxopersister"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/btcsuite/goleveldb/leveldb"
	"github.com/btcsuite/goleveldb/leveldb/opt"
	"github.com/libsv/go-bt/v2/chainhash"
)

// bitcoin addresses
// segwit bitcoin addresses

// go get github.com/syndtr/goleveldb/leveldb
// set no compression when opening leveldb
// command line arguments

// open file for writing
// execute shell command (check bitcoin isn't running)
// catch interrupt signals CTRL-C to close db connection safely
// catch kill commands too
// bulk writing to file
// convert byte slice to hexadecimal
// parsing flags from command line
// Check OS type for file-handler limitations

type BlockHeader struct {
	Hash              string `json:"hash"`
	Height            int    `json:"height"`
	PreviousBlockHash string `json:"previousblockhash"`
}

func Bitcoin2Utxoset(blockchainDir string, outputDir string, skipHeaders bool, skipUTXOs bool,
	blockHashStr string, previousBlockHashStr string, blockHeightUint uint, dumpRecords int) {
	logger := ulogger.NewGoCoreLogger("b2utxo")

	chainstate := filepath.Join(blockchainDir, "chainstate")

	// Check chainstate LevelDB folder exists
	if _, err := os.Stat(chainstate); os.IsNotExist(err) {
		logger.Errorf("Couldn't find %s: %v", chainstate, err)
		os.Exit(1)
	}

	index := filepath.Join(blockchainDir, "blocks/index")

	// Check index LevelDB folder exists
	if _, err := os.Stat(index); os.IsNotExist(err) {
		logger.Errorf("Couldn't find %s: %v", index, err)
		os.Exit(1)
	}

	// Check if OS type is Mac OS, then increase ulimit -n to 4096 filehandler during runtime and reset to 1024 at the end
	// Mac OS standard is 1024
	// Linux standard is already 4096 which is also "max" for more edit etc/security/limits.conf
	if runtime.GOOS == "darwin" {
		cmd2 := exec.Command("ulimit", "-n", "4096")

		fmt.Println("setting ulimit 4096")

		_, err := cmd2.Output()
		if err != nil {
			fmt.Printf("setting new ulimit failed with %s\n", err)
		}

		defer exec.Command("ulimit", "-n", "1024")
	}

	var (
		blockHash         *chainhash.Hash
		previousBlockHash *chainhash.Hash
		blockHeight       uint32
		err               error
	)

	if skipHeaders {
		if blockHashStr == "" || previousBlockHashStr == "" || blockHeightUint == 0 {
			logger.Errorf("The 'blockHash', 'previousBlockHash', and 'blockHeight' flags are mandatory when skipping headers.")
			return
		}

		blockHash, err = chainhash.NewHashFromStr(blockHashStr)
		if err != nil {
			logger.Errorf("Could not parse block hash: %v", err)
			return
		}

		previousBlockHash, err = chainhash.NewHashFromStr(previousBlockHashStr)
		if err != nil {
			logger.Errorf("Could not parse previous block hash: %v", err)
			return
		}

		// nolint:gosec
		blockHeight = uint32(blockHeightUint)
	} else {
		indexDB, err := bitcoin.NewIndexDB(index)
		if err != nil {
			logger.Errorf("Could not open index: %v", err)
			return
		}

		defer indexDB.Close()

		if dumpRecords > 0 {
			indexDB.DumpRecords(dumpRecords)
			return
		}

		height, err := indexDB.GetLastHeight()
		if err != nil {
			logger.Errorf("Could not get last height: %v", err)
			return
		}

		bestBlock, err := indexDB.WriteHeadersToFile(outputDir, height)
		if err != nil {
			logger.Errorf("Could not write headers: %v", err)
			return
		}

		if int(bestBlock.Height) != height {
			logger.Warnf("Height mismatch: height from blocks indexDB was %d, scanned height was %d", height, bestBlock.Height)
		}

		blockHash = bestBlock.Hash
		previousBlockHash = bestBlock.BlockHeader.HashPrevBlock
		blockHeight = bestBlock.Height
	}

	if !skipUTXOs {
		outFile := filepath.Join(outputDir, blockHash.String()+".utxo-set")

		if _, err := os.Stat(outFile); err == nil {
			logger.Errorf("Output file %s already exists. Please delete and try again", outFile)
			return
		}

		if err := runImport(logger, chainstate, outFile, blockHash, blockHeight, previousBlockHash); err != nil {
			logger.Errorf("%v", err)
			return
		}
	}
}

func runImport(logger ulogger.Logger, chainstate string, outFile string, blockHash *chainhash.Hash, blockHeight uint32, previousBlockHash *chainhash.Hash) error {
	// Select bitcoin chainstate leveldb folder
	// open leveldb without compression to avoid corrupting the database for bitcoin
	opts := &opt.Options{
		Compression: opt.NoCompression,
		ReadOnly:    true,
	}
	// https://bitcoin.stackexchange.com/questions/52257/chainstate-leveldb-corruption-after-reading-from-the-database
	// https://github.com/syndtr/goleveldb/issues/61
	// https://godoc.org/github.com/syndtr/goleveldb/leveldb/opt

	logger.Infof("Opening LevelDB at %s", chainstate)

	db, err := leveldb.OpenFile(chainstate, opts)
	if err != nil {
		return errors.NewProcessingError("Couldn't open LevelDB", err)
	}
	defer db.Close()

	// Declare obfuscateKey (a byte slice)
	var obfuscateKey []byte // obfuscateKey := make([]byte, 0)

	// Open a file for writing
	logger.Infof("Creating output file %s", outFile)

	file, err := os.Create(outFile)
	if err != nil {
		return errors.NewProcessingError("Couldn't create file", err)
	}
	defer file.Close()

	hasher := sha256.New()

	bufferedWriter := bufio.NewWriter(io.MultiWriter(file, hasher))
	defer bufferedWriter.Flush()

	header, err := utxopersister.BuildHeaderBytes("U-S-1.0", blockHash, blockHeight, previousBlockHash)
	if err != nil {
		return errors.NewProcessingError("Couldn't build UTXO set header", err)
	}

	_, err = bufferedWriter.Write(header)
	if err != nil {
		return errors.NewProcessingError("Couldn't write header to file", err)
	}

	// Iterate over LevelDB keys
	iter := db.NewIterator(nil, nil)
	// NOTE: iter.Release() comes after the iteration (not deferred here)
	// err := iter.Error()
	// fmt.Println(err)

	// Catch signals that interrupt the script so that we can close the database safely (hopefully not corrupting it)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() { // goroutine
		<-c // receive from channel

		_ = bufferedWriter.Flush() // flush buffered writer
		_ = file.Close()           // close file
		_ = db.Close()             // close database

		os.Exit(0)
	}()

	var (
		txWritten     uint64
		utxosWritten  uint64
		utxosWritten2 uint64
		utxosSkipped  uint64
		iterCount     uint64
	)

	logger.Infof("Processing UTXOs...")

	var currentUTXOWrapper *utxopersister.UTXOWrapper

	for iter.Next() {
		key := iter.Key()
		value := iter.Value()

		// first byte in key indicates the type of key we've got for leveldb
		prefix := key[0]

		switch prefix {
		// 14 = obfuscateKey (first key)
		case 14:
			obfuscateKey = value
			logger.Infof("Obfuscate key: %x (%d bytes)", obfuscateKey, len(obfuscateKey))

		// 66 = 0x42 = B = "chaintip"
		case 66:
			obfuscateKeyExtended := getObfuscateKeyExtended(obfuscateKey, value)

			// XOR the value with the obfuscateKey to de-obfuscate it
			deobfuscatedValue := make([]byte, len(value))
			for i := range value {
				deobfuscatedValue[i] = value[i] ^ obfuscateKeyExtended[i]
			}

			chainstateTip, err := chainhash.NewHash(deobfuscatedValue)

			if err != nil {
				logger.Errorf("Couldn't create hash from %s: %v", deobfuscatedValue, err)
			}

			if !blockHash.IsEqual(chainstateTip) {
				logger.Errorf("Block hash mismatch between last block and chainstate: %s != %s", blockHash, chainstateTip)
			}

			logger.Infof("Tip in chainstate: %s", chainstateTip)

		// 67 = 0x43 = C = "utxo"
		case 67:
			txid := key[1:33] // little-endian byte order

			// vout
			index := key[33:]

			// convert varint128 index to an integer
			vout := btcleveldb.Varint128Decode(index)

			// -----
			// Value
			// -----
			obfuscateKeyExtended := getObfuscateKeyExtended(obfuscateKey, value)

			// XOR the value with the obfuscateKey (xor each byte) to de-obfuscate the value
			var xor []byte // create a byte slice to hold the xor results

			for i := range value {
				result := value[i] ^ obfuscateKeyExtended[i]
				xor = append(xor, result)
			}

			// -----
			// Value
			// -----

			//   value: 71a9e87d62de25953e189f706bcf59263f15de1bf6c893bda9b045 <- obfuscated
			//          b12dcefd8f872536b12dcefd8f872536b12dcefd8f872536b12dce <- extended obfuscateKey (XOR)
			//          c0842680ed5900a38f35518de4487c108e3810e6794fb68b189d8b <- deobfuscated
			//          <----><----><><-------------------------------------->
			//           /      |    \                   |
			//      varint   varint   varint          script <- P2PKH/P2SH hash160, P2PK public key, or complete script
			//         |        |     nSize
			//         |        |
			//         |     amount (compressesed)
			//         |
			//         |
			//  100000100001010100110
			//  <------------------> \
			//         height         coinbase

			offset := 0

			// First Varint
			// ------------
			// b98276a2ec7700cbc2986ff9aed6825920aece14aa6f5382ca5580
			// <---->
			varint, bytesRead := btcleveldb.Varint128Read(xor, 0) // start reading at 0
			offset += bytesRead
			varintDecoded := btcleveldb.Varint128Decode(varint)

			// Height (first bits)
			height := varintDecoded >> 1 // right-shift to remove last bit

			// Coinbase (last bit)
			coinbase := varintDecoded & 1 // AND to extract right-most bit

			// Second Varint
			// -------------
			// b98276a2ec7700cbc2986ff9aed6825920aece14aa6f5382ca5580
			//       <---->
			varint, bytesRead = btcleveldb.Varint128Read(xor, offset) // start after last varint
			offset += bytesRead
			varintDecoded = btcleveldb.Varint128Decode(varint)

			// Amount
			amount := btcleveldb.DecompressValue(varintDecoded) // int64

			// Third Varint
			// ------------
			// b98276a2ec7700cbc2986ff9aed6825920aece14aa6f5382ca5580
			//             <>
			//
			// nSize - byte to indicate the type or size of script - helps with compression of the script data
			//  - https://github.com/bitcoin/bitcoin/blob/master/src/compressor.cpp

			//  0  = P2PKH <- hash160 public key
			//  1  = P2SH  <- hash160 script
			//  2  = P2PK 02publickey <- nsize makes up part of the public key in the actual script
			//  3  = P2PK 03publickey
			//  4  = P2PK 04publickey (uncompressed - but has been compressed in to leveldb) y=even
			//  5  = P2PK 04publickey (uncompressed - but has been compressed in to leveldb) y=odd
			//  6+ = [size of the upcoming script] (subtract 6 though to get the actual size in bytes, to account for the previous 5 script types already taken)
			varint, bytesRead = btcleveldb.Varint128Read(xor, offset) // start after last varint
			offset += bytesRead
			nsize := btcleveldb.Varint128Decode(varint) //

			// Script (remaining bytes)
			// ------
			// b98276a2ec7700cbc2986ff9aed6825920aece14aa6f5382ca5580
			//               <-------------------------------------->

			// Move offset back a byte if script type is 2, 3, 4, or 5 (because this forms part of the P2PK public key along with the actual script)
			if nsize > 1 && nsize < 6 { // either 2, 3, 4, 5
				offset--
			}

			// Get the remaining bytes
			script := xor[offset:]

			// Decompress the public keys from P2PK scripts that were uncompressed originally. They got compressed just for storage in the database.
			// Only decompress if the public key was uncompressed and
			//   * Script field is selected or
			//   * Address field is selected and p2pk addresses are enabled.
			if nsize == 4 || nsize == 5 {
				script = keys.DecompressPublicKey(script)
			}

			// Addresses - Get address from script (if possible), and set script type (P2PK, P2PKH, P2SH, P2MS, P2WPKH, P2WSH or P2TR)
			// ---------

			var scriptType string // initialize script type

			switch {
			case nsize == 0: // P2PKH
				scriptType = "p2pkh"
				prefix := []byte{0x76, 0xa9, 0x14}
				suffix := []byte{0x88, 0xac}

				newScript := make([]byte, len(prefix)+len(script)+len(suffix))

				copy(newScript, prefix)
				copy(newScript[len(prefix):], script)
				copy(newScript[len(prefix)+len(script):], suffix)

				script = newScript

			case nsize == 1: // P2SH
				scriptType = "p2sh"
				prefix := []byte{0xa9, 0x14}
				suffix := []byte{0x87}

				newScript := make([]byte, len(prefix)+len(script)+len(suffix))

				copy(newScript, prefix)
				copy(newScript[len(prefix):], script)
				copy(newScript[len(prefix)+len(script):], suffix)

				script = newScript

			// P2PK
			case 1 < nsize && nsize < 6: // 2, 3, 4, 5
				//  2 = P2PK 02publickey <- nsize makes up part of the public key in the actual script (e.g. 02publickey)
				//  3 = P2PK 03publickey <- y is odd/even (0x02 = even, 0x03 = odd)
				//  4 = P2PK 04publickey (uncompressed)  y = odd  <- actual script uses an uncompressed public key, but it is compressed when stored in this db
				//  5 = P2PK 04publickey (uncompressed) y = even
				// "The uncompressed pubkeys are compressed when they are added to the db. 0x04 and 0x05 are used to indicate that the key is supposed to be uncompressed and those indicate whether the y value is even or odd so that the full uncompressed key can be retrieved."
				//
				// if nsize is 4 or 5, you will need to uncompress the public key to get it's full form
				// if nsize == 4 || nsize == 5 {
				//     // uncompress (4 = y is even, 5 = y is odd)
				//     script = decompress(script)
				// }
				scriptType = "p2pk"

				// Set the prefix to the length of the script (OP_PUSH len(script))
				prefix := []byte{byte(len(script))} // OP_PUSH len(script)
				suffix := []byte{0xac}              // OP_CHECKSIG

				newScript := make([]byte, len(prefix)+len(script)+len(suffix))

				copy(newScript, prefix)
				copy(newScript[len(prefix):], script)
				copy(newScript[len(prefix)+len(script):], suffix)

				script = newScript

			// P2MS
			case len(script) > 0 && script[len(script)-1] == 174: // if there is a script and if the last opcode is OP_CHECKMULTISIG (174) (0xae)
				scriptType = "p2ms"

			// Non-Standard (if the script type hasn't been identified and set then it remains as an unknown "non-standard" script)
			default:
				scriptType = "non-standard"
			}

			switch scriptType {
			case "p2pkh":
				fallthrough
			case "p2pk":
				fallthrough
			case "p2sh":
				fallthrough
			case "p2ms":
				fallthrough
			case "non-standard":
				hash, err := chainhash.NewHash(txid)
				if err != nil {
					return errors.NewProcessingError("Couldn't create hash from %x:", txid, err)
				}

				if currentUTXOWrapper == nil {
					// nolint:gosec
					currentUTXOWrapper = &utxopersister.UTXOWrapper{
						TxID:     *hash,
						Height:   uint32(height),
						Coinbase: coinbase == 1,
					}
				} else if !currentUTXOWrapper.TxID.IsEqual(hash) {
					// Write the last UTXOWrapper to the file
					_, err = bufferedWriter.Write(currentUTXOWrapper.Bytes())
					if err != nil {
						return errors.NewProcessingError("Couldn't write to file:")
					}

					txWritten++
					utxosWritten += uint64(len(currentUTXOWrapper.UTXOs))

					// nolint:gosec
					currentUTXOWrapper = &utxopersister.UTXOWrapper{
						TxID:     *hash,
						Height:   uint32(height),
						Coinbase: coinbase == 1,
					}
				}

				// nolint:gosec
				currentUTXOWrapper.UTXOs = append(currentUTXOWrapper.UTXOs, &utxopersister.UTXO{
					Index:  uint32(vout),
					Value:  uint64(amount),
					Script: script,
				})
				utxosWritten2++
			default:
				fmt.Printf("ERROR: Unknown script type: %v\n", scriptType)

				utxosSkipped++
			}

			if (txWritten+utxosSkipped)%1_000_000 == 0 {
				logger.Infof("Processed %16s transactions with %16s utxos, skipped %d", formatNumber(txWritten), formatNumber(utxosWritten), utxosSkipped)
			}

		default:
			logger.Errorf("Unhandled LevelDB record: %x : %x", key, value)
		}

		iterCount++
	}

	if currentUTXOWrapper != nil {
		// Write the last UTXOWrapper to the file
		_, err = bufferedWriter.Write(currentUTXOWrapper.Bytes())
		if err != nil {
			return errors.NewProcessingError("Couldn't write to file:")
		}

		txWritten++
		utxosWritten += uint64(len(currentUTXOWrapper.UTXOs))
	}

	// Write the eof marker
	if _, err := bufferedWriter.Write(utxopersister.EOFMarker); err != nil {
		return errors.NewProcessingError("Couldn't write EOF marker", err)
	}

	// Write the number of txs and utxos written
	b := make([]byte, 8)

	binary.LittleEndian.PutUint64(b, txWritten)

	if _, err := bufferedWriter.Write(b); err != nil {
		return errors.NewProcessingError("Couldn't write tx count", err)
	}

	binary.LittleEndian.PutUint64(b, utxosWritten)

	if _, err := bufferedWriter.Write(b); err != nil {
		return errors.NewProcessingError("Couldn't write utxo count", err)
	}

	iter.Release() // Do not defer this, want to release iterator before closing database

	if err := bufferedWriter.Flush(); err != nil {
		return errors.NewProcessingError("Couldn't flush buffer", err)
	}

	hashData := fmt.Sprintf("%x  %s\n", hasher.Sum(nil), blockHash.String()+".utxo-set") // N.B. The 2 spaces is important for the hash to be valid
	//nolint:gosec
	if err := os.WriteFile(outFile+".sha256", []byte(hashData), 0644); err != nil {
		return errors.NewProcessingError("Couldn't write hash file", err)
	}

	logger.Infof("FINISHED  %16s transactions with %16s utxos, skipped %d", formatNumber(txWritten), formatNumber(utxosWritten), utxosSkipped)
	logger.Infof("Processed                                     %16s utxos", formatNumber(utxosWritten2))
	logger.Infof("Processed                                     %16s keys", formatNumber(iterCount))

	return nil
}

func getObfuscateKeyExtended(obfuscateKey []byte, value []byte) []byte {
	// Copy the obfuscateKey ready to extend it
	obfuscateKeyExtended := obfuscateKey[1:] // ignore the first byte, as that just tells you the size of the obfuscateKey

	// Extend the obfuscateKey so it's the same length as the value
	for i, k := len(obfuscateKeyExtended), 0; len(obfuscateKeyExtended) < len(value); i, k = i+1, k+1 {
		// append each byte of obfuscateKey to the end until it's the same length as the value
		// Example
		//   [8 175 184 95 99 240 37 253 115 181 161 4 33 81 167 111 145 131 0 233 37 232 118 180 123 120 78]
		//   [8 177 45 206 253 143 135 37 54]                                                                  <- obfuscate key
		//   [8 177 45 206 253 143 135 37 54 8 177 45 206 253 143 135 37 54 8 177 45 206 253 143 135 37 54]    <- extended
		obfuscateKeyExtended = append(obfuscateKeyExtended, obfuscateKeyExtended[k])
	}

	return obfuscateKeyExtended
}

func formatNumber(n uint64) string {
	in := fmt.Sprintf("%d", n)
	out := make([]string, 0, len(in)+(len(in)-1)/3)

	for i, c := range in {
		if i > 0 && (len(in)-i)%3 == 0 {
			out = append(out, ",")
		}

		out = append(out, string(c))
	}

	return strings.Join(out, "")
}

// func GetBlockHeaderByHeight(height int) (*BlockHeader, error) {
// 	url := fmt.Sprintf("https://api.whatsonchain.com/v1/bsv/main/block/%d/header", height)

// 	// Make the GET request
// 	resp, err := http.Get(url)
// 	if err != nil {
// 		return nil, fmt.Errorf("error making GET request: %v", err)
// 	}
// 	defer resp.Body.Close()

// 	// Check if the request was successful
// 	if resp.StatusCode != http.StatusOK {
// 		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
// 	}

// 	// Decode the JSON response into the BlockInfo struct
// 	var bh BlockHeader
// 	if err := json.NewDecoder(resp.Body).Decode(&bh); err != nil {
// 		return nil, fmt.Errorf("error decoding response: %v", err)
// 	}

// 	return &bh, nil
// }
