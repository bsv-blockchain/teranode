// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/legacy/testdata"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/stores/blob"
	blobmemory "github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/kafka" //nolint:gci
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// tx, _  = hex.DecodeString("0100000001ae73759b118b9c8d54a13ad4ccd5de662bbaa2175ee7f4b413f402affb831ed3000000006b483045022100a6af846212b0c611056a9a30c22f0eed3adc29f8c688e804509b113f322459220220708fb79c66d235d937e8348ac022b3cfa6b64fec8d35a749ea0b2293ad95da014121039f271b930111fd7c818100ee1603d5c5094c68b3d15ad0a58f712e7d766225edffffffff0550c30000000000001976a91448bea2d45f4f6175e47ccb717e4f5d19d8f68f3b88ac204e0000000000001976a91442859b9bada6461d08a0aab8a18105ef30457a8b88ac10270000000000001976a914d0e2122bdeed7b2235f670cdc832f518fb63db9f88ac0c040000000000001976a914d56f84ae869e4a743e929e31218b198f02ce67fe88ac8d0c0100000000001976a91444a8e7fb1a426e4c60597d9d3f534c677d4f858388ac00000000")
	tx1, _ = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	tx2    = newTx(2)
	tx3    = newTx(3)
	tx4    = newTx(4)

	hash1 = tx1.TxIDChainHash()
	hash2 = tx2.TxIDChainHash()
	hash3 = tx3.TxIDChainHash()
	hash4 = tx4.TxIDChainHash()
)

func newTx(random uint32) *bt.Tx {
	tx := bt.NewTx()
	tx.LockTime = random

	return tx
}

func TestBlockValidationValidateSubtree(t *testing.T) {
	t.Run("validateSubtree - smoke test", func(t *testing.T) {
		InitPrometheusMetrics()

		txMetaStore, validatorClient, txStore, subtreeStore, blockchainClient, deferFunc := setup()
		defer deferFunc()

		subtree, err := util.NewTreeByLeafCount(4)
		require.NoError(t, err)
		require.NoError(t, subtree.AddNode(*hash1, 121, 0))
		require.NoError(t, subtree.AddNode(*hash2, 122, 0))
		require.NoError(t, subtree.AddNode(*hash3, 123, 0))
		require.NoError(t, subtree.AddNode(*hash4, 123, 0))

		_, err = txMetaStore.Create(context.Background(), tx1, 0)
		require.NoError(t, err)

		_, err = txMetaStore.Create(context.Background(), tx2, 0)
		require.NoError(t, err)

		_, err = txMetaStore.Create(context.Background(), tx3, 0)
		require.NoError(t, err)

		_, err = txMetaStore.Create(context.Background(), tx4, 0)
		require.NoError(t, err)

		nodeBytes, err := subtree.SerializeNodes()
		require.NoError(t, err)

		httpmock.RegisterResponder(
			"GET",
			`=~^/subtree/[a-z0-9]+\z`,
			httpmock.NewBytesResponder(200, nodeBytes),
		)

		nilConsumer := &kafka.KafkaConsumerGroup{}
		tSettings := test.CreateBaseTestSettings()

		subtreeValidation, err := New(context.Background(), ulogger.TestLogger{}, tSettings, subtreeStore, txStore, txMetaStore, validatorClient, blockchainClient, nilConsumer, nilConsumer)
		require.NoError(t, err)

		v := ValidateSubtree{
			SubtreeHash:   *subtree.RootHash(),
			BaseURL:       "http://localhost:8000",
			TxHashes:      nil,
			AllowFailFast: false,
		}
		err = subtreeValidation.ValidateSubtreeInternal(context.Background(), v, chaincfg.GenesisActivationHeight)
		require.NoError(t, err)
	})
}

func setup() (utxo.Store, *validator.MockValidatorClient, blob.Store, blob.Store, blockchain.ClientI, func()) {
	// we only need the httpClient, utxoStore and validatorClient when blessing a transaction
	httpmock.Activate()
	httpmock.RegisterResponder(
		"GET",
		`=~^/tx/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, tx1.ExtendedBytes()),
	)

	httpmock.RegisterResponder(
		"POST",
		`=~^/txs`,
		httpmock.NewBytesResponder(200, tx1.ExtendedBytes()),
	)

	utxoStore := memory.New(ulogger.TestLogger{})
	txStore := blobmemory.New()
	subtreeStore := blobmemory.New()

	validatorClient := &validator.MockValidatorClient{TxMetaStore: utxoStore}

	blockchainClient := &blockchain.LocalClient{}

	return utxoStore, validatorClient, txStore, subtreeStore, blockchainClient, func() {
		httpmock.DeactivateAndReset()
	}
}

func TestBlockValidationValidateSubtreeInternalWithMissingTx(t *testing.T) {
	InitPrometheusMetrics()

	utxoStore, validatorClient, txStore, subtreeStore, blockchainClient, deferFunc := setup()
	defer deferFunc()

	subtree, err := util.NewTreeByLeafCount(1)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(*hash1, 121, 0))

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	httpmock.RegisterResponder(
		"GET",
		`=~^/subtree/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, nodeBytes),
	)

	nilConsumer := &kafka.KafkaConsumerGroup{}

	tSettings := test.CreateBaseTestSettings()

	subtreeValidation, err := New(context.Background(), ulogger.TestLogger{}, tSettings, subtreeStore, txStore, utxoStore, validatorClient, blockchainClient, nilConsumer, nilConsumer)
	require.NoError(t, err)

	// Create a mock context
	ctx := context.Background()

	// Create a mock ValidateSubtree struct
	v := ValidateSubtree{
		SubtreeHash:   *hash1,
		BaseURL:       "http://localhost:8000",
		TxHashes:      nil,
		AllowFailFast: false,
	}

	// Call the ValidateSubtreeInternal method
	err = subtreeValidation.ValidateSubtreeInternal(ctx, v, chaincfg.GenesisActivationHeight)
	require.NoError(t, err)
}

func TestServer_prepareTxsPerLevel(t *testing.T) {
	testCases := []struct {
		name             string
		blockFilePath    string
		expectedLevels   uint32
		expectedTxMapLen int
	}{
		{
			name:             "Block1",
			blockFilePath:    "../legacy/testdata/00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386.bin",
			expectedLevels:   15,
			expectedTxMapLen: 563,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			block, err := testdata.ReadBlockFromFile(tc.blockFilePath)
			require.NoError(t, err)

			s := &Server{}

			transactions := make([]missingTx, 0)

			for _, wireTx := range block.Transactions() {
				// Serialize the tx
				var txBytes bytes.Buffer
				err = wireTx.MsgTx().Serialize(&txBytes)
				require.NoError(t, err)

				tx, err := bt.NewTxFromBytes(txBytes.Bytes())
				require.NoError(t, err)

				transactions = append(transactions, missingTx{
					tx: tx,
				})
			}

			maxLevel, blockTXsPerLevel := s.prepareTxsPerLevel(context.Background(), transactions)
			assert.Equal(t, tc.expectedLevels, maxLevel)

			allParents := 0

			for i := range blockTXsPerLevel {
				allParents += len(blockTXsPerLevel[i])
			}

			assert.Equal(t, tc.expectedTxMapLen, allParents)
		})
	}
}

func createSpendingTx(t *testing.T, prevTx *bt.Tx, vout uint32, amount uint64, address *bscript.Address, privateKey *bec.PrivateKey) *bt.Tx {
	tx := bt.NewTx()
	err := tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      prevTx.TxIDChainHash(),
		Vout:          vout,
		LockingScript: prevTx.Outputs[vout].LockingScript,
		Satoshis:      prevTx.Outputs[vout].Satoshis,
	})
	require.NoError(t, err)

	err = tx.AddP2PKHOutputFromAddress(address.AddressString, amount)
	require.NoError(t, err)

	err = tx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})
	require.NoError(t, err)

	return tx
}

func createTestTransactionChainWithCount(t *testing.T, count int) []*bt.Tx {
	privateKey, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err)

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	// Create coinbase transaction
	coinbaseTx := bt.NewTx()
	err = coinbaseTx.From(
		"0000000000000000000000000000000000000000000000000000000000000000",
		0xffffffff,
		"",
		0,
	)
	require.NoError(t, err)

	blockHeight := uint32(100)
	blockHeightBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockHeightBytes, blockHeight)

	arbitraryData := []byte{}
	arbitraryData = append(arbitraryData, 0x03)
	arbitraryData = append(arbitraryData, blockHeightBytes[:3]...)
	arbitraryData = append(arbitraryData, []byte("/Test miner/")...)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)

	err = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)
	require.NoError(t, err)

	// Create tx1 with multiple outputs for the chain
	tx1 := bt.NewTx()
	err = tx1.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	// Create enough outputs for the chain
	outputAmount := uint64(20 * 100000000)
	remainingAmount := coinbaseTx.Outputs[0].Satoshis

	for i := 0; i < count-1; i++ {
		if i == count-2 {
			// Last output gets remaining amount minus fees
			outputAmount = remainingAmount - 100000 // Leave some for fees
		}

		err = tx1.AddP2PKHOutputFromAddress(address.AddressString, outputAmount)
		require.NoError(t, err)

		remainingAmount -= outputAmount
	}

	err = tx1.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})
	require.NoError(t, err)

	// Create the chain of transactions
	result := []*bt.Tx{coinbaseTx, tx1}

	for i := 0; i < count-2; i++ {
		//nolint:gosec
		tx := createSpendingTx(t, tx1, uint32(i), 15*100000000, address, privateKey)
		result = append(result, tx)
	}

	return result
}

// TNE-1.1: If Teranode has already validated the same transaction it is not required to perform
// the same validation again. The transaction can already be considered valid.
func TestSubtreeValidationWhenBlessMissingTransactions(t *testing.T) {
	t.Run("test get subtree tx hashes", func(t *testing.T) {
		InitPrometheusMetrics()
		// Setup test
		txMetaStore, validatorClient, txStore, subtreeStore, blockchainClient, deferFunc := setup()
		defer deferFunc()

		// Create test transactions
		txs := createTestTransactionChainWithCount(t, 7)
		coinbaseTx, tx1, tx2, tx3, tx4, tx5, tx6 := txs[0], txs[1], txs[2], txs[3], txs[4], txs[5], txs[6]

		// Store initial transactions in txMetaStore
		_, _ = txMetaStore.Create(context.Background(), coinbaseTx, 1)
		_, _ = txMetaStore.Create(context.Background(), tx1, 1)
		_, _ = txMetaStore.Create(context.Background(), tx2, 1)
		_, _ = txMetaStore.Create(context.Background(), tx3, 1)
		_, _ = txMetaStore.Create(context.Background(), tx4, 1)

		// Create subtrees
		subtree1, err := util.NewTreeByLeafCount(4)
		require.NoError(t, err)
		subtree2, err := util.NewTreeByLeafCount(4)
		require.NoError(t, err)

		// Add transactions to subtrees
		hash1 := tx1.TxIDChainHash()
		hash2 := tx2.TxIDChainHash()
		hash3 := tx3.TxIDChainHash()
		hash4 := tx4.TxIDChainHash()
		hash5 := tx5.TxIDChainHash()
		hash6 := tx6.TxIDChainHash()

		// Create subtree1 with tx1, tx2, tx3, tx4
		require.NoError(t, subtree1.AddNode(*hash1, 121, 0))
		require.NoError(t, subtree1.AddNode(*hash2, 122, 0))
		require.NoError(t, subtree1.AddNode(*hash3, 123, 0))
		require.NoError(t, subtree1.AddNode(*hash4, 124, 0))

		// Create subtree2 with tx1, tx2, tx5, tx6
		require.NoError(t, subtree2.AddNode(*hash1, 121, 0))
		require.NoError(t, subtree2.AddNode(*hash2, 122, 0))
		require.NoError(t, subtree2.AddNode(*hash5, 123, 0))
		require.NoError(t, subtree2.AddNode(*hash6, 124, 0))

		// Setup HTTP mocks
		nodes1Bytes, err := subtree1.SerializeNodes()
		require.NoError(t, err)
		nodes2Bytes, err := subtree2.SerializeNodes()
		require.NoError(t, err)

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("/subtree/%s", subtree1.RootHash().String()),
			httpmock.NewBytesResponder(200, nodes1Bytes),
		)
		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("/subtree/%s", subtree2.RootHash().String()),
			httpmock.NewBytesResponder(200, nodes2Bytes),
		)

		// Setup batch transaction responder
		httpmock.RegisterResponder(
			"POST",
			"/txs",
			func(req *http.Request) (*http.Response, error) {
				body, err := io.ReadAll(req.Body)
				if err != nil {
					return nil, err
				}

				numHashes := len(body) / 32

				var responseBytes []byte

				for i := 0; i < numHashes; i++ {
					hashBytes := body[i*32 : (i+1)*32]

					hash, err := chainhash.NewHash(hashBytes)
					require.NoError(t, err)

					var tx *bt.Tx

					switch hash.String() {
					case tx5.TxIDChainHash().String():
						tx = tx5
					case tx6.TxIDChainHash().String():
						tx = tx6
					default:
						return nil, errors.NewError("unexpected hash in request: " + hash.String())
					}

					responseBytes = append(responseBytes, tx.ExtendedBytes()...)
				}

				return httpmock.NewBytesResponse(200, responseBytes), nil
			},
		)

		// Setup and run validation
		nilConsumer := &kafka.KafkaConsumerGroup{}
		tSettings := test.CreateBaseTestSettings()
		subtreeValidation, err := New(context.Background(), ulogger.TestLogger{}, tSettings, subtreeStore, txStore, txMetaStore, validatorClient, blockchainClient, nilConsumer, nilConsumer)
		require.NoError(t, err)

		// Validate subtree1
		v1 := ValidateSubtree{
			SubtreeHash:   *subtree1.RootHash(),
			BaseURL:       "http://localhost:8000",
			TxHashes:      nil,
			AllowFailFast: false,
		}
		err = subtreeValidation.ValidateSubtreeInternal(context.Background(), v1, 100)
		require.NoError(t, err)

		// Verify initial cache state
		_, err = txMetaStore.Get(context.Background(), hash1)
		require.NoError(t, err, "tx1 should be in cache")
		_, err = txMetaStore.Get(context.Background(), hash2)
		require.NoError(t, err, "tx2 should be in cache")
		_, err = txMetaStore.Get(context.Background(), hash3)
		require.NoError(t, err, "tx3 should be in cache")
		_, err = txMetaStore.Get(context.Background(), hash4)
		require.NoError(t, err, "tx4 should be in cache")
		_, err = txMetaStore.Get(context.Background(), hash5)
		require.Error(t, err, "tx5 should NOT be in cache before processing subtree2")
		_, err = txMetaStore.Get(context.Background(), hash6)
		require.Error(t, err, "tx6 should NOT be in cache before processing subtree2")

		// Validate subtree2
		v2 := ValidateSubtree{
			SubtreeHash:   *subtree2.RootHash(),
			BaseURL:       "http://localhost:8000",
			TxHashes:      nil,
			AllowFailFast: false,
		}
		err = subtreeValidation.ValidateSubtreeInternal(context.Background(), v2, 100)
		require.NoError(t, err)

		// Verify final cache state
		_, err = txMetaStore.Get(context.Background(), hash1)
		require.NoError(t, err, "tx1 should still be in cache")
		_, err = txMetaStore.Get(context.Background(), hash2)
		require.NoError(t, err, "tx2 should still be in cache")
		_, err = txMetaStore.Get(context.Background(), hash3)
		require.NoError(t, err, "tx3 should still be in cache")
		_, err = txMetaStore.Get(context.Background(), hash4)
		require.NoError(t, err, "tx4 should still be in cache")
		_, err = txMetaStore.Get(context.Background(), hash5)
		require.NoError(t, err, "tx5 should now be in cache")
		_, err = txMetaStore.Get(context.Background(), hash6)
		require.NoError(t, err, "tx6 should now be in cache")
	})
}
