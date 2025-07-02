// Package bitcointoutxoset provides tools for extracting and converting Bitcoin UTXO set data.
//
// Usage:
//
//	This package is typically used as a command-line tool to process Bitcoin blockchain data and
//	generate UTXO set files for further analysis or migration.
//
// Functions:
//   - ConvertBitcoinToUtxoSet: Main entry point for processing Bitcoin blockchain data and generating UTXO set files. Handles reading LevelDB, writing headers, and managing UTXO export.
//
// Side effects:
//
//	Functions in this package may read from and write to disk, execute shell commands, and handle
//	system signals to ensure safe shutdown and resource cleanup.
package bitcointoutxoset

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

	"github.com/bitcoin-sv/teranode/cmd/bitcointoutxoset/bitcoin"
	"github.com/bitcoin-sv/teranode/cmd/bitcointoutxoset/bitcoin/btcleveldb"
	"github.com/bitcoin-sv/teranode/cmd/bitcointoutxoset/bitcoin/keys"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/services/utxopersister"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/btcsuite/goleveldb/leveldb"
	"github.com/btcsuite/goleveldb/leveldb/opt"
)

/*
- Bitcoin Addresses
  - SegWit Bitcoin addresses

- Dependencies
  - Install: github.com/syndtr/goleveldb/leveldb
  - Set "no compression" when opening LevelDB

- Command-Line Interaction:
  - Parse flags from command line
  - Handle command line arguments
  - Open file for writing
  - Check OS type for file-handler limitations
  - Catch interrupt signals (e.g., CTRL-C) to safely close DB connection
  - Catch kill commands for graceful shutdown
  - Execute shell command (e.g., check that Bitcoin isn't running)
  - Bulk writing to file
  - Convert byte slice to hexadecimal
*/

// BlockHeader represents a Bitcoin block header structure.
type BlockHeader struct {
	Hash              string `json:"hash"`
	Height            int    `json:"height"`
	PreviousBlockHash string `json:"previousblockhash"`
}

// ConvertBitcoinToUtxoSet is the main entry point for extracting and converting the UTXO set from a Bitcoin node's data directory.
//
// Parameters:
//   - logger: Logger instance for outputting informational and error messages.
//   - _ (unused): Settings pointer, reserved for future configuration.
//   - blockchainDir: Path to the Bitcoin node's data directory (must contain 'chainstate' and 'blocks/index').
//   - outputDir: Directory where the resulting UTXO set file(s) will be written.
//   - skipHeaders: If true, skips header extraction and requires blockHashStr, previousBlockHashStr, and blockHeightUint to be set.
//   - skipUTXOs: If true, skips UTXO extraction and only processes headers.
//   - blockHashStr: Block hash (as a string) to use when skipping headers.
//   - previousBlockHashStr: Previous block hash (as a string) to use when skipping headers.
//   - blockHeightUint: Block height to use when skipping headers.
//   - dumpRecords: If >0, dumps the specified number of records from the block index for debugging.
//
// Behavior:
//   - Validates the presence of required LevelDB directories.
//   - Optionally increases file descriptor limits on macOS for large database access.
//   - If skipHeaders is false, extracts the latest block header and writes it to the output directory.
//   - If skipUTXOs is false, reads the chainstate LevelDB, decodes UTXO records, and writes them to a file named after the block hash.
//   - Handles system signals to ensure safe shutdown and resource cleanup.
//   - Logs progress and errors using the provided logger.
//
// Side Effects:
//   - Reads from and writes to disk.
//   - May execute shell commands to adjust system limits.
//   - May terminate the process on critical errors.
//
//nolint:gocognit // complexity is acceptable for this function
func ConvertBitcoinToUtxoSet(logger ulogger.Logger, _ *settings.Settings, blockchainDir string, outputDir string,
	skipHeaders bool, skipUTXOs bool, blockHashStr string, previousBlockHashStr string, blockHeightUint uint, dumpRecords int) {
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

	// Check if OS type is macOS, then increase ulimit -n to 4096 filehandler during runtime and reset to 1024 at the end
	// macOS standard is 1024
	// Linux standard is already 4096 which is also "max" for more edit etc/security/limits.conf
	if runtime.GOOS == "darwin" {
		cmd2 := exec.Command("ulimit", "-n", "4096")

		fmt.Println("Setting ulimit 4096")

		_, err := cmd2.Output()
		if err != nil {
			fmt.Printf("Setting new ulimit failed with %s\n", err)
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

		var blockHeightUint32 uint32
		blockHeightUint32, err = safeconversion.UintToUint32(blockHeightUint)
		if err != nil {
			logger.Errorf("Could not convert block height to uint32: %v", err)
			return
		}

		blockHeight = blockHeightUint32
	} else {
		var indexDB *bitcoin.IndexDB
		indexDB, err = bitcoin.NewIndexDB(index)
		if err != nil {
			logger.Errorf("Could not open index: %v", err)
			return
		}

		defer func() {
			_ = indexDB.Close()
		}()

		if dumpRecords > 0 {
			indexDB.DumpRecords(dumpRecords)
			return
		}

		var height int
		height, err = indexDB.GetLastHeight()
		if err != nil {
			logger.Errorf("Could not get last height: %v", err)
			return
		}

		var bestBlock *utxopersister.BlockIndex
		bestBlock, err = indexDB.WriteHeadersToFile(outputDir, height)
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

		if _, err = os.Stat(outFile); err == nil {
			logger.Errorf("Output file %s already exists. Please delete and try again", outFile)
			return
		}

		if err = runImport(logger, chainstate, outFile, blockHash, blockHeight, previousBlockHash); err != nil {
			logger.Errorf("%v", err)
			return
		}
	}
}

// runImport processes the Bitcoin chainstate LevelDB and writes the UTXO set to a file.
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
		return errors.NewProcessingError("couldn't open LevelDB", err)
	}

	defer func() {
		_ = db.Close()
	}()

	// Declare obfuscateKey (a byte slice)
	var obfuscateKey []byte // obfuscateKey := make([]byte, 0)

	// Open a file for writing
	logger.Infof("Creating output file %s", outFile)

	var file *os.File
	file, err = os.Create(outFile)
	if err != nil {
		return errors.NewProcessingError("couldn't create file", err)
	}

	defer func() {
		_ = file.Close()
	}()

	// Crate a new SHA256 hasher
	hasher := sha256.New()

	// Create a buffered writer to write to the file and the hasher at the same time
	bufferedWriter := bufio.NewWriter(io.MultiWriter(file, hasher))
	defer func() {
		_ = bufferedWriter.Flush()
	}()

	// Write the header to the file
	header := fileformat.NewHeader(fileformat.FileTypeUtxoSet)

	if err = header.Write(bufferedWriter); err != nil {
		return errors.NewProcessingError("couldn't write header to file", err)
	}

	if _, err = bufferedWriter.Write(blockHash[:]); err != nil {
		return errors.NewProcessingError("error writing header hash", err)
	}

	if err = binary.Write(bufferedWriter, binary.LittleEndian, blockHeight); err != nil {
		return errors.NewProcessingError("error writing header number", err)
	}

	// With UTXOSets, we also write the previous block hash before we start writing the UTXOs
	_, err = bufferedWriter.Write(previousBlockHash[:])
	if err != nil {
		return errors.NewProcessingError("couldn't write previous block hash to file", err)
	}

	// Iterate over LevelDB keys
	iter := db.NewIterator(nil, nil)
	// NOTE: iter.Release() comes after the iteration (not deferred here)
	// err := iter.Error()

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

		// the first byte in a key indicates the type of key we've got for leveldb
		prefixByte := key[0]

		switch prefixByte {
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

			var chainstateTip *chainhash.Hash
			chainstateTip, err = chainhash.NewHash(deobfuscatedValue)

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

			// convert varInt128 index to an integer
			vout := btcleveldb.VarInt128Decode(index)

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
			//      varInt   varInt   varInt          script <- P2PKH/P2SH hash160, P2PK public key, or complete script
			//         |        |     nSize
			//         |        |
			//         |     amount (compressed)
			//         |
			//         |
			//  100000100001010100110
			//  <------------------> \
			//         height         coinbase

			offset := 0

			// First VarInt
			// ------------
			// b98276a2ec7700cbc2986ff9aed6825920aece14aa6f5382ca5580
			// <---->
			varInt, bytesRead := btcleveldb.VarInt128Read(xor, 0) // start reading at 0
			offset += bytesRead
			varIntDecoded := btcleveldb.VarInt128Decode(varInt)

			// Height (first bits)
			height := varIntDecoded >> 1 // right-shift to remove the last bit

			// Coinbase (last bit)
			coinbase := varIntDecoded & 1 // AND to extract right-most bit

			// Second VarInt
			// -------------
			// b98276a2ec7700cbc2986ff9aed6825920aece14aa6f5382ca5580
			//       <---->
			varInt, bytesRead = btcleveldb.VarInt128Read(xor, offset) // start after last varInt
			offset += bytesRead
			varIntDecoded = btcleveldb.VarInt128Decode(varInt)

			// Amount
			amount := btcleveldb.DecompressValue(varIntDecoded) // int64

			// Third VarInt
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
			varInt, bytesRead = btcleveldb.VarInt128Read(xor, offset) // start after last varInt
			offset += bytesRead
			nsize := btcleveldb.VarInt128Decode(varInt) //

			// Script (remaining bytes)
			// ------
			// b98276a2ec7700cbc2986ff9aed6825920aece14aa6f5382ca5580
			//               <-------------------------------------->

			// Move offset back a byte if a script type is 2, 3, 4, or 5 (because this forms part of the P2PK public key along with the actual script)
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

			// Non-Standard (if the script type hasn't been identified and set, then it remains as an unknown "non-standard" script)
			scriptType := "non-standard" // default to non-standard script

			switch {
			// P2PKH
			case nsize == 0:
				scriptType = "p2pkh"
				prefix := []byte{0x76, 0xa9, 0x14}
				suffix := []byte{0x88, 0xac}

				newScript := make([]byte, len(prefix)+len(script)+len(suffix))

				copy(newScript, prefix)
				copy(newScript[len(prefix):], script)
				copy(newScript[len(prefix)+len(script):], suffix)

				script = newScript

			// P2SH
			case nsize == 1:
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
				// "The uncompressed pubkeys are compressed when they are added to the db. 0x04 and 0x05 are used to indicate that the key is supposed to be uncompressed, and those indicate whether the y value is even or odd so that the full uncompressed key can be retrieved."
				//
				// if nsize is 4 or 5, you will need to uncompress the public key to get its full form
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

			}

			// Switch on the script type to handle different UTXO types
			switch scriptType {
			case "p2pkh", "p2pk", "p2sh", "p2ms", "non-standard":
				var hash *chainhash.Hash

				// Create a hash from the txid
				hash, err = chainhash.NewHash(txid)
				if err != nil {
					return errors.NewProcessingError("couldn't create hash from %x:", txid, err)
				}

				if currentUTXOWrapper == nil {
					var heightUint32 uint32

					heightUint32, err = safeconversion.Int64ToUint32(height)
					if err != nil {
						return err
					}

					currentUTXOWrapper = &utxopersister.UTXOWrapper{
						TxID:     *hash,
						Height:   heightUint32,
						Coinbase: coinbase == 1,
					}
				} else if !currentUTXOWrapper.TxID.IsEqual(hash) {
					// Write the last UTXOWrapper to the file
					_, err = bufferedWriter.Write(currentUTXOWrapper.Bytes())
					if err != nil {
						return errors.NewProcessingError("couldn't write to file: " + err.Error())
					}

					txWritten++
					utxosWritten += uint64(len(currentUTXOWrapper.UTXOs))

					var heightUint32 uint32

					heightUint32, err = safeconversion.Int64ToUint32(height)
					if err != nil {
						return err
					}

					currentUTXOWrapper = &utxopersister.UTXOWrapper{
						TxID:     *hash,
						Height:   heightUint32,
						Coinbase: coinbase == 1,
					}
				}

				var voutUint32 uint32

				voutUint32, err = safeconversion.Int64ToUint32(vout)
				if err != nil {
					return err
				}

				var amountUint64 uint64

				amountUint64, err = safeconversion.Int64ToUint64(amount)
				if err != nil {
					return err
				}

				// Add the UTXO to the current UTXOWrapper
				currentUTXOWrapper.UTXOs = append(currentUTXOWrapper.UTXOs, &utxopersister.UTXO{
					Index:  voutUint32,
					Value:  amountUint64,
					Script: script,
				})
				utxosWritten2++
			default:
				fmt.Printf("Error: unknown script type: %v\n", scriptType)

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

	// Check for any errors during iteration
	if currentUTXOWrapper != nil {
		// Write the last UTXOWrapper to the file
		_, err = bufferedWriter.Write(currentUTXOWrapper.Bytes())
		if err != nil {
			return errors.NewProcessingError("couldn't write to file: " + err.Error())
		}

		txWritten++
		utxosWritten += uint64(len(currentUTXOWrapper.UTXOs))
	}

	// Write the number of txs and utxos written
	b := make([]byte, 8)

	binary.LittleEndian.PutUint64(b, txWritten)

	// Write the number of transactions written to the file
	if _, err = bufferedWriter.Write(b); err != nil {
		return errors.NewProcessingError("couldn't write tx count", err)
	}

	binary.LittleEndian.PutUint64(b, utxosWritten)

	if _, err = bufferedWriter.Write(b); err != nil {
		return errors.NewProcessingError("couldn't write utxo count", err)
	}

	iter.Release() // Do not defer this, want to release iterator before closing the database

	if err = bufferedWriter.Flush(); err != nil {
		return errors.NewProcessingError("couldn't flush buffer", err)
	}

	hashData := fmt.Sprintf("%x  %s\n", hasher.Sum(nil), blockHash.String()+".utxo-set") // N.B. The 2 spaces is important for the hash to be valid

	// Write the hash to a file
	if err = os.WriteFile(outFile+".sha256", []byte(hashData), 0600); err != nil {
		return errors.NewProcessingError("couldn't write hash file", err)
	}

	logger.Infof("FINISHED  %16s transactions with %16s utxos, skipped %d", formatNumber(txWritten), formatNumber(utxosWritten), utxosSkipped)
	logger.Infof("Processed                                     %16s utxos", formatNumber(utxosWritten2))
	logger.Infof("Processed                                     %16s keys", formatNumber(iterCount))

	return nil
}

// getObfuscateKeyExtended extends the obfuscateKey to match the length of the value.
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

// formatNumber formats an "uint64" number with commas for readability.
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

/*
func GetBlockHeaderByHeight(height int) (*BlockHeader, error) {
	url := fmt.Sprintf("https://api.whatsonchain.com/v1/bsv/main/block/%d/header", height)

	// Make the GET request
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("error making GET request: %v", err)
	}
	defer resp.Body.Close()

	// Check if the request was successful
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Decode the JSON response into the BlockInfo struct
	var bh BlockHeader
	if err := json.NewDecoder(resp.Body).Decode(&bh); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	return &bh, nil
}
*/
