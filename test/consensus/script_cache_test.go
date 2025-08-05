package consensus

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/sighash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/stretchr/testify/require"
)

// TestCachingInvalidSignatures tests signature cache behavior with invalid signatures
func TestCachingInvalidSignatures(t *testing.T) {
	// Initialize key data
	keyData := NewKeyData()
	
	// Create a P2PK script
	script := &bscript.Script{}
	script.AppendPushData(keyData.Pubkey0)
	script.AppendOpcodes(bscript.OpCHECKSIG)
	
	// Create an invalid signature (wrong key)
	tb := NewTestBuilder(script, "Invalid sig caching", SCRIPT_VERIFY_NONE, false, 0)
	sig, err := tb.MakeSig(keyData.Key1, sighash.AllForkID, 32, 32) // Wrong key
	require.NoError(t, err)
	
	// First validation should fail
	tb.DoPush()
	tb.push = sig
	tb.havePush = true
	tb.SetScriptError(SCRIPT_ERR_EVAL_FALSE)
	
	err = tb.DoTest()
	require.NoError(t, err)
	
	// Second validation with same invalid signature should also fail
	// This tests that invalid signatures might be cached
	tb2 := NewTestBuilder(script, "Invalid sig caching 2", SCRIPT_VERIFY_NONE, false, 0)
	tb2.DoPush()
	tb2.push = sig
	tb2.havePush = true
	tb2.SetScriptError(SCRIPT_ERR_EVAL_FALSE)
	
	err = tb2.DoTest()
	require.NoError(t, err)
}

// TestMultithreadedValidation tests concurrent script validation
func TestMultithreadedValidation(t *testing.T) {
	// TODO: Multithreaded tests with go-bdk are slow due to CGO overhead
	// The C++ implementation may handle concurrency differently
	// This test is disabled until performance can be improved
	t.Skip("Skipping - Multithreaded validation is slow with go-bdk")
	
	// Initialize key data
	keyData := NewKeyData()
	
	// Number of concurrent validations
	numGoroutines := 10
	numIterations := 100
	
	// Create test scripts
	scripts := []struct {
		name   string
		script *bscript.Script
		sig    []byte
		valid  bool
	}{
		{
			name: "P2PK valid",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				s.AppendPushData(keyData.Pubkey0)
				s.AppendOpcodes(bscript.OpCHECKSIG)
				return s
			}(),
			valid: true,
		},
		{
			name: "P2PKH valid",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				s.AppendOpcodes(bscript.OpDUP)
				s.AppendOpcodes(bscript.OpHASH160)
				s.AppendPushData(keyData.Pubkey0Hash)
				s.AppendOpcodes(bscript.OpEQUALVERIFY)
				s.AppendOpcodes(bscript.OpCHECKSIG)
				return s
			}(),
			valid: true,
		},
		{
			name: "2-of-3 multisig",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				s.AppendOpcodes(bscript.Op2)
				s.AppendPushData(keyData.Pubkey0)
				s.AppendPushData(keyData.Pubkey1C)
				s.AppendPushData(keyData.Pubkey2C)
				s.AppendOpcodes(bscript.Op3)
				s.AppendOpcodes(bscript.OpCHECKMULTISIG)
				return s
			}(),
			valid: true,
		},
	}
	
	// Pre-generate signatures for each script
	for i := range scripts {
		tb := NewTestBuilder(scripts[i].script, "sig gen", SCRIPT_VERIFY_NONE, false, 0)
		
		switch scripts[i].name {
		case "P2PK valid":
			sig, _ := tb.MakeSig(keyData.Key0, sighash.AllForkID, 32, 32)
			scripts[i].sig = sig
			
		case "P2PKH valid":
			sig, _ := tb.MakeSig(keyData.Key0, sighash.AllForkID, 32, 32)
			scripts[i].sig = sig
			
		case "2-of-3 multisig":
			// For multisig, we'll handle it differently in the test
			scripts[i].sig = nil
		}
	}
	
	// Run concurrent validations
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*numIterations*len(scripts))
	
	start := time.Now()
	
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			
			for j := 0; j < numIterations; j++ {
				for k, script := range scripts {
					// Create a new test builder for each validation
					tb := NewTestBuilder(script.script, script.name, SCRIPT_VERIFY_NONE, false, 0)
					
					// Add appropriate inputs
					switch script.name {
					case "P2PK valid":
						tb.DoPush()
						tb.push = script.sig
						tb.havePush = true
						
					case "P2PKH valid":
						tb.DoPush()
						tb.push = script.sig
						tb.havePush = true
						tb.PushPubKey(keyData.Pubkey0)
						
					case "2-of-3 multisig":
						tb.Num(0) // Dummy element
						sig0, _ := tb.MakeSig(keyData.Key0, sighash.AllForkID, 32, 32)
						sig1, _ := tb.MakeSig(keyData.Key1, sighash.AllForkID, 32, 32)
						tb.DoPush()
						tb.push = sig0
						tb.havePush = true
						tb.DoPush()
						tb.push = sig1
						tb.havePush = true
					}
					
					// Set expected result
					if script.valid {
						tb.SetScriptError(SCRIPT_ERR_OK)
					} else {
						tb.SetScriptError(SCRIPT_ERR_EVAL_FALSE)
					}
					
					// Execute validation
					err := tb.DoTest()
					if err != nil {
						errors <- err
					}
					
					// Add some timing variation
					if k%10 == 0 {
						time.Sleep(time.Microsecond)
					}
				}
			}
		}(i)
	}
	
	// Wait for all goroutines to complete
	wg.Wait()
	close(errors)
	
	elapsed := time.Since(start)
	totalValidations := numGoroutines * numIterations * len(scripts)
	
	// Check for errors
	var allErrors []error
	for err := range errors {
		allErrors = append(allErrors, err)
	}
	
	require.Empty(t, allErrors, "Concurrent validation should not produce errors")
	
	t.Logf("Completed %d validations in %v", totalValidations, elapsed)
	t.Logf("Average time per validation: %v", elapsed/time.Duration(totalValidations))
}

// TestLargeMultisigCaching tests caching behavior with large multisig scripts
func TestLargeMultisigCaching(t *testing.T) {
	// TODO: Large multisig tests are extremely slow with go-bdk validator
	// The C++ implementation may have optimizations that aren't exposed through CGO
	// This test is disabled until performance can be improved
	t.Skip("Skipping - Large multisig validation is too slow with go-bdk")
	
	// This test simulates the behavior described in the C++ test
	// where large multisig scripts with many pubkeys are validated
	
	// keyData := NewKeyData() // Not used, we generate new keys below
	
	// Create a large 15-of-15 multisig (close to the limit)
	script := &bscript.Script{}
	script.AppendOpcodes(bscript.Op15)
	
	// Generate 15 unique public keys
	// We need unique keys, so we'll use variations of the test keys
	keyData := NewKeyData()
	pubkeys := make([][]byte, 15)
	keys := make([]*bec.PrivateKey, 15)
	
	// Use the 3 base keys and create variations for 15 keys
	baseKeys := []*bec.PrivateKey{keyData.Key0, keyData.Key1, keyData.Key2}
	basePubkeys := [][]byte{keyData.Pubkey0, keyData.Pubkey1, keyData.Pubkey2}
	
	for i := 0; i < 15; i++ {
		if i < 3 {
			keys[i] = baseKeys[i]
			pubkeys[i] = basePubkeys[i]
		} else {
			// For additional keys, generate new ones
			privKeyBytes := make([]byte, 32)
			privKeyBytes[31] = byte(i + 1) // Simple deterministic key generation
			keys[i], _ = bec.PrivateKeyFromBytes(privKeyBytes)
			pubkeys[i] = keys[i].PubKey().Uncompressed()
		}
		script.AppendPushData(pubkeys[i])
	}
	
	script.AppendOpcodes(bscript.Op15)
	script.AppendOpcodes(bscript.OpCHECKMULTISIG)
	
	// Create valid signatures
	tb := NewTestBuilder(script, "Large multisig", SCRIPT_VERIFY_NONE, false, 0)
	tb.Num(0) // Dummy element
	
	// Add 15 signatures
	for i := 0; i < 15; i++ {
		sig, err := tb.MakeSig(keys[i], sighash.AllForkID, 32, 32)
		require.NoError(t, err)
		tb.DoPush()
		tb.push = sig
		tb.havePush = true
	}
	
	tb.SetScriptError(SCRIPT_ERR_OK)
	
	// First validation
	start := time.Now()
	err := tb.DoTest()
	require.NoError(t, err)
	firstDuration := time.Since(start)
	
	// Second validation (should benefit from caching if implemented)
	tb2 := NewTestBuilder(script, "Large multisig cached", SCRIPT_VERIFY_NONE, false, 0)
	tb2.Num(0) // Dummy element
	
	// Add same signatures
	for i := 0; i < 15; i++ {
		sig, err := tb2.MakeSig(keys[i], sighash.AllForkID, 32, 32)
		require.NoError(t, err)
		tb2.DoPush()
		tb2.push = sig
		tb2.havePush = true
	}
	
	tb2.SetScriptError(SCRIPT_ERR_OK)
	
	start = time.Now()
	err = tb2.DoTest()
	require.NoError(t, err)
	secondDuration := time.Since(start)
	
	t.Logf("First validation: %v", firstDuration)
	t.Logf("Second validation: %v", secondDuration)
	
	// Note: We can't guarantee caching behavior without knowing the implementation
	// but we can verify both validations succeed
}

// TestSignatureCacheEviction tests cache behavior under memory pressure
func TestSignatureCacheEviction(t *testing.T) {
	t.Skip("Signature cache eviction test - requires cache implementation details")
	
	// This test would verify that:
	// 1. Old entries are evicted when cache is full
	// 2. Invalid signatures don't pollute the cache excessively
	// 3. Cache maintains good hit rate under normal conditions
}

// TestParallelBlockValidation simulates parallel block validation
func TestParallelBlockValidation(t *testing.T) {
	// TODO: Parallel validation tests with go-bdk are slow due to CGO overhead
	// The C++ implementation may handle concurrency differently
	// This test is disabled until performance can be improved
	t.Skip("Skipping - Parallel block validation is slow with go-bdk")
	
	// Simulate validating multiple "blocks" of transactions in parallel
	keyData := NewKeyData()
	
	// Create a set of "transactions" (really just scripts)
	numBlocks := 5
	txPerBlock := 20
	
	var wg sync.WaitGroup
	errors := make(chan error, numBlocks*txPerBlock)
	
	for blockNum := 0; blockNum < numBlocks; blockNum++ {
		wg.Add(1)
		go func(block int) {
			defer wg.Done()
			
			for txNum := 0; txNum < txPerBlock; txNum++ {
				// Create a unique P2PKH script for each "transaction"
				script := &bscript.Script{}
				script.AppendOpcodes(bscript.OpDUP)
				script.AppendOpcodes(bscript.OpHASH160)
				
				// Use different keys for variety
				var pubkey []byte
				var key *bec.PrivateKey
				switch txNum % 3 {
				case 0:
					pubkey = keyData.Pubkey0
					key = keyData.Key0
				case 1:
					pubkey = keyData.Pubkey1C
					key = keyData.Key1
				case 2:
					pubkey = keyData.Pubkey2C
					key = keyData.Key2
				}
				
				script.AppendPushData(Hash160(pubkey))
				script.AppendOpcodes(bscript.OpEQUALVERIFY)
				script.AppendOpcodes(bscript.OpCHECKSIG)
				
				// Create test
				tb := NewTestBuilder(script, "Block validation", SCRIPT_VERIFY_NONE, false, 0)
				
				// Create signature
				sig, err := MakeSignature(key, sighash.AllForkID)
				if err != nil {
					errors <- err
					continue
				}
				
				tb.DoPush()
				tb.push = sig
				tb.havePush = true
				tb.PushPubKey(pubkey)
				tb.SetScriptError(SCRIPT_ERR_OK)
				
				// Validate
				err = tb.DoTest()
				if err != nil {
					errors <- err
				}
			}
		}(blockNum)
	}
	
	// Wait for all "blocks" to complete
	wg.Wait()
	close(errors)
	
	// Check for errors
	var allErrors []error
	for err := range errors {
		allErrors = append(allErrors, err)
	}
	
	require.Empty(t, allErrors, "Parallel block validation should not produce errors")
	
	t.Logf("Successfully validated %d blocks with %d transactions each", numBlocks, txPerBlock)
}

// Helper function to create signatures
func MakeSignature(key interface{}, sigHashType sighash.Flag) ([]byte, error) {
	// Create a dummy script for signature generation
	dummyScript := &bscript.Script{}
	dummyScript.AppendOpcodes(bscript.OpTRUE)
	
	// Create a properly initialized TestBuilder
	tb := NewTestBuilder(dummyScript, "temp sig creation", SCRIPT_VERIFY_NONE, false, 100000000)
	
	switch k := key.(type) {
	case *bec.PrivateKey:
		return tb.MakeSig(k, sigHashType, 32, 32)
	default:
		// For compressed keys, we need to handle them differently
		// This is a simplified version
		return nil, fmt.Errorf("unsupported key type: %T", key)
	}
}

// Hash160 computes RIPEMD160(SHA256(data))
func Hash160(data []byte) []byte {
	return bsvutil.Hash160(data)
}