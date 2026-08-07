package aerospike_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/txparse"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// go test -v -tags test_aerospike ./test/...

func TestStore_GetTxFromExternalStore(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, _, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	t.Run("TestStore_GetTxFromExternalStore", func(t *testing.T) {
		s := &teranode_aerospike.Store{}
		s.SetExternalStore(memory.New())
		s.SetClient(client)
		s.SetNamespace(aerospikeNamespace)
		s.SetName(aerospikeSet)
		s.SetExternalTxCache(util.NewExpiringConcurrentCache[chainhash.Hash, *bt.Tx](1 * time.Minute))

		// read a sample transaction from testdata and store it in the external store
		f, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		require.NoError(t, err)

		txFromFile, err := bt.NewTxFromString(string(f))
		require.NoError(t, err)

		txHash := txFromFile.TxIDChainHash()
		txBytes := txFromFile.Bytes()

		err = s.GetExternalStore().Set(ctx, txHash.CloneBytes(), fileformat.FileTypeTx, txBytes)
		require.NoError(t, err)

		// Test fetching the transaction from the external store
		fetchedTx, err := s.GetTxFromExternalStore(ctx, *txHash)
		require.NoError(t, err)
		require.NotNil(t, fetchedTx)
		require.Equal(t, txFromFile.Version, fetchedTx.Version)
		require.Equal(t, txFromFile.LockTime, fetchedTx.LockTime)
		require.Equal(t, len(txFromFile.Inputs), len(fetchedTx.Inputs))
		require.Equal(t, len(txFromFile.Outputs), len(fetchedTx.Outputs))
		require.Equal(t, txFromFile.Outputs[0].Satoshis, fetchedTx.Outputs[0].Satoshis)
		require.Equal(t, txFromFile.Outputs[0].LockingScript, fetchedTx.Outputs[0].LockingScript)
	})

	t.Run("TestStore_GetTxFromExternalStore concurrent", func(t *testing.T) {
		s := &teranode_aerospike.Store{}
		s.SetSettings(test.CreateBaseTestSettings(t)) // getExternalOutpoints reads ChainCfgParams.GenesisActivationHeight
		s.SetExternalStore(memory.New())
		s.SetClient(client)
		s.SetNamespace(aerospikeNamespace)
		s.SetName(aerospikeSet)
		s.SetExternalTxCache(util.NewExpiringConcurrentCache[chainhash.Hash, *bt.Tx](1 * time.Minute))
		// The outpoint reconstruction has its own cache — it holds a different
		// shape for the same txid, so it must never share with the full-tx cache.
		// New() creates both; a hand-built Store has to wire both too, or this
		// subtest's coalescing assertion has no cache to coalesce through.
		s.SetExternalOutpointsCache(util.NewExpiringConcurrentCache[chainhash.Hash, *bt.Tx](1 * time.Minute))

		// read a sample transaction from testdata and store it in the external store
		f, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		require.NoError(t, err)

		txFromFile, err := bt.NewTxFromString(string(f))
		require.NoError(t, err)

		txHash := txFromFile.TxIDChainHash()
		txBytes := txFromFile.Bytes()

		err = s.GetExternalStore().Set(ctx, txHash.CloneBytes(), fileformat.FileTypeTx, txBytes)
		require.NoError(t, err)

		// Test fetching the transaction from the external store concurrently
		g := errgroup.Group{}
		for i := 0; i < 100; i++ {
			g.Go(func() error {
				// creationHeight 0 (pre-Genesis era) is not "don't care" — it
				// selects an era for the unspendable predicate. It is irrelevant
				// here: this subtest only asserts concurrency safety of the fetch.
				fetchedTx, err := s.GetOutpointsFromExternalStore(ctx, *txHash, 0)
				if err != nil {
					return err
				}

				require.NotNil(t, fetchedTx)

				return nil
			})
		}

		err = g.Wait()
		require.NoError(t, err)

		// check how often the external store was accessed
		memStore, ok := s.GetExternalStore().(*memory.Memory)
		require.True(t, ok)
		assert.Equal(t, memStore.Counters["set"], 1)
		assert.Equal(t, memStore.Counters["get"], 1)
	})
}

func TestParseInputReferencesFromExtendedTx(t *testing.T) {
	t.Run("parses single input correctly", func(t *testing.T) {
		tx := createTestTxWithInputs(t, 1, 100)
		txBytes := tx.ExtendedBytes() // External transactions use Extended Format

		inputs, err := txparse.ParseInputReferencesFromExtendedTx(bytes.NewReader(txBytes))

		require.NoError(t, err)
		require.Len(t, inputs, 1)
		require.NotNil(t, inputs[0].PreviousTxIDChainHash())
		require.Equal(t, uint32(0), inputs[0].PreviousTxOutIndex)
		require.Nil(t, inputs[0].UnlockingScript)
	})

	t.Run("parses multiple inputs", func(t *testing.T) {
		tx := createTestTxWithInputs(t, 10, 50)
		txBytes := tx.ExtendedBytes() // External transactions use Extended Format

		inputs, err := txparse.ParseInputReferencesFromExtendedTx(bytes.NewReader(txBytes))

		require.NoError(t, err)
		require.Len(t, inputs, 10)

		for i, input := range inputs {
			require.NotNil(t, input.PreviousTxIDChainHash())
			require.Equal(t, uint32(i), input.PreviousTxOutIndex)
		}
	})

	t.Run("skips large scripts without allocation", func(t *testing.T) {
		tx := createTestTxWithInputs(t, 2, 1024*100)
		txBytes := tx.ExtendedBytes() // External transactions use Extended Format

		inputs, err := txparse.ParseInputReferencesFromExtendedTx(bytes.NewReader(txBytes))

		require.NoError(t, err)
		require.Len(t, inputs, 2)
	})

	t.Run("handles zero inputs", func(t *testing.T) {
		tx := &bt.Tx{
			Version: 1,
			Inputs:  []*bt.Input{},
			Outputs: []*bt.Output{{Satoshis: 100, LockingScript: &bscript.Script{0x76}}},
		}
		txBytes := tx.ExtendedBytes() // External transactions use Extended Format

		inputs, err := txparse.ParseInputReferencesFromExtendedTx(bytes.NewReader(txBytes))

		require.NoError(t, err)
		require.Len(t, inputs, 0)
	})

	t.Run("error on truncated prevTxID", func(t *testing.T) {
		tx := createTestTxWithInputs(t, 1, 10)
		txBytes := tx.ExtendedBytes() // External transactions use Extended Format

		// Truncate in middle of first input's prevTxID
		truncated := txBytes[:20] // Adjusted for Extended Format marker

		_, err := txparse.ParseInputReferencesFromExtendedTx(bytes.NewReader(truncated))

		require.Error(t, err)
		require.Contains(t, err.Error(), "input")
	})

	t.Run("error on truncated input count", func(t *testing.T) {
		// Just version, no input count
		buf := bytes.NewBuffer(nil)
		err := binary.Write(buf, binary.LittleEndian, uint32(1))
		require.NoError(t, err)

		_, err = txparse.ParseInputReferencesFromExtendedTx(buf)
		require.Error(t, err)
	})

	t.Run("does not read outputs", func(t *testing.T) {
		tx := createTestTxWithInputs(t, 1, 10)

		// Add massive output
		largeScript := make(bscript.Script, 1024*1024*5)
		tx.Outputs = []*bt.Output{{Satoshis: 100, LockingScript: &largeScript}}

		txBytes := tx.ExtendedBytes() // External transactions use Extended Format

		// Should complete without reading the 5MB output
		inputs, err := txparse.ParseInputReferencesFromExtendedTx(bytes.NewReader(txBytes))

		require.NoError(t, err)
		require.Len(t, inputs, 1)
	})
}

func createTestTxWithInputs(t *testing.T, numInputs int, scriptSize int) *bt.Tx {
	tx := &bt.Tx{
		Version: 1,
		Inputs:  make([]*bt.Input, numInputs),
		Outputs: []*bt.Output{{Satoshis: 1000, LockingScript: &bscript.Script{0x76, 0xa9}}},
	}

	for i := 0; i < numInputs; i++ {
		hashBytes := make([]byte, 32)
		binary.BigEndian.PutUint32(hashBytes[28:], uint32(i+1))
		prevHash, err := chainhash.NewHash(hashBytes)
		require.NoError(t, err)

		script := make(bscript.Script, scriptSize)
		for j := range script {
			script[j] = byte(j % 256)
		}

		tx.Inputs[i] = &bt.Input{
			PreviousTxOutIndex: uint32(i),
			UnlockingScript:    &script,
			SequenceNumber:     0xffffffff,
		}
		err = tx.Inputs[i].PreviousTxIDAdd(prevHash)
		require.NoError(t, err)
	}

	return tx
}

// TestParseInputReferencesFromExtendedTxWithExtendedFormat tests that ParseInputReferencesFromExtendedTx correctly
// parses external transactions using the ACTUAL production code path from create.go:869 and get.go:1432.
func TestParseInputReferencesFromExtendedTxWithExtendedFormat(t *testing.T) {
	ctx := context.Background()

	// Create a transaction with multiple inputs and Extended Format metadata
	tx := bt.NewTx()

	// Add 3 inputs with Extended Format fields populated
	for i := 0; i < 3; i++ {
		prevTxIDBytes := make([]byte, 32)
		for j := range prevTxIDBytes {
			prevTxIDBytes[j] = byte(i*10 + j)
		}
		prevTxID, err := chainhash.NewHash(prevTxIDBytes)
		require.NoError(t, err)

		unlockingScript, err := bscript.NewFromASM("OP_1 OP_2")
		require.NoError(t, err)

		previousTxScript, err := bscript.NewFromASM("OP_DUP OP_HASH160 OP_3 OP_EQUALVERIFY OP_CHECKSIG")
		require.NoError(t, err)

		input := &bt.Input{
			PreviousTxOutIndex: uint32(i),
			UnlockingScript:    unlockingScript,
			SequenceNumber:     0xffffffff,
			PreviousTxSatoshis: uint64(1000 * (i + 1)), // Extended Format field
			PreviousTxScript:   previousTxScript,       // Extended Format field
		}
		err = input.PreviousTxIDAdd(prevTxID)
		require.NoError(t, err)

		tx.Inputs = append(tx.Inputs, input)
	}

	// Add a dummy output so the transaction is complete
	script, err := bscript.NewFromASM("OP_FALSE OP_RETURN")
	require.NoError(t, err)
	tx.Outputs = append(tx.Outputs, &bt.Output{
		LockingScript: script,
		Satoshis:      0,
	})

	// Create external blob store
	tempDir := t.TempDir()
	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	externalStore, err := file.New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	txHash := *tx.TxIDChainHash()

	// Store using EXACT production code from create.go:869
	// This is how external transactions are written in production
	extendedBytes := tx.ExtendedBytes()
	err = externalStore.Set(ctx, txHash[:], fileformat.FileTypeTx, extendedBytes, options.WithDeleteAt(0))
	require.NoError(t, err)

	// Create a minimal Store with external store configured
	store := &teranode_aerospike.Store{}
	store.SetExternalStore(externalStore)
	store.SetLogger(ulogger.TestLogger{})

	// Use the EXACT production code path from get.go:1426 (GetTxInpointsFromExternalStore)
	// This is how external transactions are read when loading unmined transactions
	txInpoints, err := store.GetTxInpointsFromExternalStore(ctx, txHash)
	require.NoError(t, err, "GetTxInpointsFromExternalStore should successfully parse Extended Format")

	// Verify the TxInpoints has the correct parent tx hashes
	parentHashes := txInpoints.GetParentTxHashes()
	require.Equal(t, 3, len(parentHashes), "Should have 3 unique parent tx hashes")

	// Verify each parent tx hash matches the original inputs
	for i := 0; i < 3; i++ {
		expectedHash := tx.Inputs[i].PreviousTxID()
		found := false
		for _, hash := range parentHashes {
			if bytes.Equal(expectedHash, hash[:]) {
				found = true
				break
			}
		}
		require.True(t, found, "Input %d prevTxID should be in parent hashes", i)
	}
}

// TestParseInputReferencesFromExtendedTxRejectsNonExtendedFormat verifies that ParseInputReferencesFromExtendedTx
// returns an error when the transaction is not in extended format.
func TestParseInputReferencesFromExtendedTxRejectsNonExtendedFormat(t *testing.T) {
	// Create a simple transaction
	tx := bt.NewTx()

	// Add a single input
	prevTxIDBytes := make([]byte, 32)
	for j := range prevTxIDBytes {
		prevTxIDBytes[j] = byte(j)
	}
	prevTxID, err := chainhash.NewHash(prevTxIDBytes)
	require.NoError(t, err)

	unlockingScript, err := bscript.NewFromASM("OP_1 OP_2")
	require.NoError(t, err)

	input := &bt.Input{
		PreviousTxOutIndex: 0,
		UnlockingScript:    unlockingScript,
		SequenceNumber:     0xffffffff,
	}
	err = input.PreviousTxIDAdd(prevTxID)
	require.NoError(t, err)

	tx.Inputs = append(tx.Inputs, input)

	// Add a dummy output
	script, err := bscript.NewFromASM("OP_FALSE OP_RETURN")
	require.NoError(t, err)
	tx.Outputs = append(tx.Outputs, &bt.Output{
		LockingScript: script,
		Satoshis:      0,
	})

	// Use standard Bytes() instead of ExtendedBytes() - this is NOT extended format
	txBytes := tx.Bytes()

	// Attempt to parse - should fail because it's not in extended format
	reader := bytes.NewReader(txBytes)
	inputs, err := txparse.ParseInputReferencesFromExtendedTx(reader)

	// Verify that we get an error about not being extended format
	require.Error(t, err, "ParseInputReferencesFromExtendedTx should reject non-extended format transactions")
	require.Nil(t, inputs, "Should return nil inputs on error")
	require.Contains(t, err.Error(), "transaction is not in extended format", "Error message should indicate the transaction is not in extended format")
}
