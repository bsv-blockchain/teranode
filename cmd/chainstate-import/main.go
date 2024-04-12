package main

// local packages
import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"runtime" // chainstate leveldb decoding functions
	"syscall"

	"github.com/bitcoin-sv/ubsv/cmd/chainstate-import/bitcoin/btcleveldb"
	"github.com/bitcoin-sv/ubsv/cmd/chainstate-import/bitcoin/keys"
	"github.com/btcsuite/goleveldb/leveldb"

	"github.com/btcsuite/goleveldb/leveldb/opt"
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

func main() {
	// Command Line Options (Flags)
	chainstate := flag.String("db", "", "Location of bitcoin chainstate db.") // chainstate folder
	flag.Parse()                                                              // execute command line parsing for all declared flags

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

	// Check chainstate LevelDB folder exists
	if _, err := os.Stat(*chainstate); os.IsNotExist(err) {
		fmt.Println("Couldn't find", *chainstate)
		return
	}

	// Select bitcoin chainstate leveldb folder
	// open leveldb without compression to avoid corrupting the database for bitcoin
	opts := &opt.Options{
		Compression: opt.NoCompression,
	}
	// https://bitcoin.stackexchange.com/questions/52257/chainstate-leveldb-corruption-after-reading-from-the-database
	// https://github.com/syndtr/goleveldb/issues/61
	// https://godoc.org/github.com/syndtr/goleveldb/leveldb/opt

	db, err := leveldb.OpenFile(*chainstate, opts) // You have got to dereference the pointer to get the actual value
	if err != nil {
		fmt.Println("Couldn't open LevelDB.")
		fmt.Println(err)
		return
	}
	defer db.Close()

	// txMetaStoreURL, err, found := gocore.Config().GetURL("txmeta_store")
	// if err != nil {
	// 	panic(err)
	// }
	// if !found {
	// 	panic("no txmeta_store setting found")
	// }

	// logger := ulogger.New("chainstate-importer")

	// txMetaStore, err := txmetafactory.New(logger, txMetaStoreURL)
	// if err != nil {
	// 	panic(err)
	// }

	// _ = txMetaStore

	// Declare obfuscateKey (a byte slice)
	var obfuscateKey []byte // obfuscateKey := make([]byte, 0)

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

		// iter.Release() // release database iterator
		db.Close() // close databse
		os.Exit(0) // exit
	}()

	i := 0
	for iter.Next() {

		key := iter.Key()
		value := iter.Value()

		// first byte in key indicates the type of key we've got for leveldb
		prefix := key[0]

		// obfuscateKey (first key)
		if prefix == 14 { // 14 = obfuscateKey
			obfuscateKey = value
		}

		// utxo entry
		if prefix == 67 { // 67 = 0x43 = C = "utxo"

			// ---
			// Key
			// ---

			//      430000155b9869d56c66d9e86e3c01de38e3892a42b99949fe109ac034fff6583900
			//      <><--------------------------------------------------------------><>
			//      /                               |                                  \
			//  type                          txid (little-endian)                      index (varint)

			// txid
			txidLE := key[1:33] // little-endian byte order

			// txid - reverse byte order
			txid := make([]byte, 0)                 // create empty byte slice (dont want to mess with txid directly)
			for i := len(txidLE) - 1; i >= 0; i-- { // run backwards through the txid slice
				txid = append(txid, txidLE[i]) // append each byte to the new byte slice
			}

			// vout
			index := key[33:]

			// convert varint128 index to an integer
			vout := btcleveldb.Varint128Decode(index)

			// -----
			// Value
			// -----

			// Copy the obfuscateKey ready to extend it
			obfuscateKeyExtended := obfuscateKey[1:] // ignore the first byte, as that just tells you the size of the obfuscateKey

			// Extend the obfuscateKey so it's the same length as the value
			for i, k := len(obfuscateKeyExtended), 0; len(obfuscateKeyExtended) < len(value); i, k = i+1, k+1 {
				// append each byte of obfuscateKey to the end until it's the same length as the value
				obfuscateKeyExtended = append(obfuscateKeyExtended, obfuscateKeyExtended[k])
				// Example
				//   [8 175 184 95 99 240 37 253 115 181 161 4 33 81 167 111 145 131 0 233 37 232 118 180 123 120 78]
				//   [8 177 45 206 253 143 135 37 54]                                                                  <- obfuscate key
				//   [8 177 45 206 253 143 135 37 54 8 177 45 206 253 143 135 37 54 8 177 45 206 253 143 135 37 54]    <- extended
			}

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

			// P2MS
			case len(script) > 0 && script[len(script)-1] == 174: // if there is a script and if the last opcode is OP_CHECKMULTISIG (174) (0xae)
				scriptType = "p2ms"

			// Non-Standard (if the script type hasn't been identified and set then it remains as an unknown "non-standard" script)
			default:
				scriptType = "non-standard"

			} // switch

			// txMetaStore.Create(ctx.TODO()) // create txmeta (transaction metadata) from the output results map

			// -------
			// Results
			// -------

			// Increment Count
			i++

			switch scriptType {
			case "p2pkh":
			case "p2sh":
			case "p2ms":
			case "non-standard":
			default:
				fmt.Printf("%x, %v, %v, %v, %v, %x, %v\n",
					txid,
					vout,
					height,
					coinbase,
					amount,
					script,
					scriptType,
				)

				break
			}
		}
	}
	iter.Release() // Do not defer this, want to release iterator before closing database

}
