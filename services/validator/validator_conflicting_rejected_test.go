package validator

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestConflictingRejectionIsPublished pins the source of the subtree validators'
// conflict knowledge: a transaction the validator rejects because one of its inputs
// is already spent by a different transaction is published, raw, on the
// policy-rejected topic with Conflicting set. Subtree validation uses it to keep such
// a transaction aside (no UTXO-store write) when a peer subtree contains it.
func TestConflictingRejectionIsPublished(t *testing.T) {
	newFixture := func(t *testing.T, name string) (*Validator, *kafka.KafkaAsyncProducerMock, *bt.Tx, *bt.Tx) {
		ctx := t.Context()
		logger := ulogger.NewErrorTestLogger(t)
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockAssembly.Disabled = true

		utxoStoreURL, err := url.Parse("sqlitememory:///" + name)
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
		require.NoError(t, err)
		utxoStore.RawDB().SetMaxOpenConns(1)
		require.NoError(t, utxoStore.SetBlockHeight(100))
		require.NoError(t, utxoStore.SetMedianBlockTime(uint32(time.Now().Unix()))) //nolint:gosec

		privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("CONFLICTING_PUBLISH_TEST_KEY!!!!"))
		parentTx := transactions.Create(t,
			transactions.WithCoinbaseData(1, "/conflicting publish test/"),
			transactions.WithP2PKHOutputs(1, 100_000, publicKey),
		)
		_, err = utxoStore.Create(ctx, parentTx, 1, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 1, BlockHeight: 1, SubtreeIdx: 0}))
		require.NoError(t, err)

		winner := transactions.Create(t,
			transactions.WithPrivateKey(privateKey),
			transactions.WithInput(parentTx, 0, privateKey),
			transactions.WithP2PKHOutputs(1, 90_000, publicKey),
		)
		loser := transactions.Create(t,
			transactions.WithPrivateKey(privateKey),
			transactions.WithInput(parentTx, 0, privateKey),
			transactions.WithP2PKHOutputs(1, 80_000, publicKey),
		)

		producer := kafka.NewKafkaAsyncProducerMock()

		validator, err := New(ctx, logger, tSettings, utxoStore, nil, nil, producer, nil, nil)
		require.NoError(t, err)

		v := validator.(*Validator)

		_, err = v.Validate(ctx, winner, 100)
		require.NoError(t, err)

		return v, producer, winner, loser
	}

	t.Run("rejected conflicting tx is published with Conflicting set", func(t *testing.T) {
		v, producer, _, loser := newFixture(t, "conflicting_rejected_published")

		txMeta, err := v.Validate(t.Context(), loser, 100)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrSpent), "the rejection must be a spend conflict: %v", err)
		require.Nil(t, txMeta, "a rejected conflicting tx is not stored")

		select {
		case msg := <-producer.PublishChannel():
			var m kafkamessage.KafkaTxPolicyRejectedTopicMessage
			require.NoError(t, proto.Unmarshal(msg.Value, &m))
			require.True(t, m.Conflicting)
			require.Equal(t, loser.TxIDChainHash().CloneBytes(), m.TxHash)
			require.Equal(t, loser.SerializeBytes(), m.RawTx)
		default:
			t.Fatal("expected the rejected conflicting tx to be published")
		}
	})

	t.Run("tx created as conflicting is not published", func(t *testing.T) {
		v, producer, _, loser := newFixture(t, "conflicting_rejected_created")

		txMeta, err := v.Validate(t.Context(), loser, 100, WithCreateConflicting(true))
		require.True(t, errors.Is(err, errors.ErrTxConflicting), "block-path create must return conflicting: %v", err)
		require.NotNil(t, txMeta)
		require.True(t, txMeta.Conflicting)

		select {
		case msg := <-producer.PublishChannel():
			t.Fatalf("a tx stored as conflicting must not be published, got %d bytes", len(msg.Value))
		default:
		}
	})
}

// TestConflictingRejectionNeedsEveryFailedSpendToConflict pins that a rejection is only
// marked conflicting when every input that failed to spend is spent by a different tx.
// A tx that also spends a frozen output is invalid, not merely a double spend: it is not
// published as conflicting, so subtree validation does not keep it aside.
func TestConflictingRejectionNeedsEveryFailedSpendToConflict(t *testing.T) {
	newFixture := func(t *testing.T, name string) (*Validator, *kafka.KafkaAsyncProducerMock, *bt.Tx, *bec.PrivateKey, *bec.PublicKey) {
		ctx := t.Context()
		logger := ulogger.NewErrorTestLogger(t)
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockAssembly.Disabled = true

		utxoStoreURL, err := url.Parse("sqlitememory:///" + name)
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
		require.NoError(t, err)
		utxoStore.RawDB().SetMaxOpenConns(1)
		require.NoError(t, utxoStore.SetBlockHeight(100))
		require.NoError(t, utxoStore.SetMedianBlockTime(uint32(time.Now().Unix()))) //nolint:gosec

		privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("CONFLICTING_FROZEN_TEST_KEY!!!!!"))
		parentTx := transactions.Create(t,
			transactions.WithCoinbaseData(1, "/conflicting frozen test/"),
			transactions.WithP2PKHOutputs(2, 100_000, publicKey),
		)
		_, err = utxoStore.Create(ctx, parentTx, 1, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 1, BlockHeight: 1, SubtreeIdx: 0}))
		require.NoError(t, err)

		utxoHash, err := util.UTXOHashFromOutput(parentTx.TxIDChainHash(), parentTx.Outputs[1], 1)
		require.NoError(t, err)
		require.NoError(t, utxoStore.FreezeUTXOs(ctx, []*utxostore.Spend{{TxID: parentTx.TxIDChainHash(), Vout: 1, UTXOHash: utxoHash}}, tSettings))

		producer := kafka.NewKafkaAsyncProducerMock()

		validator, err := New(ctx, logger, tSettings, utxoStore, nil, nil, producer, nil, nil)
		require.NoError(t, err)

		v := validator.(*Validator)

		winner := transactions.Create(t,
			transactions.WithPrivateKey(privateKey),
			transactions.WithInput(parentTx, 0, privateKey),
			transactions.WithP2PKHOutputs(1, 90_000, publicKey),
		)
		_, err = v.Validate(ctx, winner, 100)
		require.NoError(t, err)

		return v, producer, parentTx, privateKey, publicKey
	}

	requireNotPublished := func(t *testing.T, producer *kafka.KafkaAsyncProducerMock) {
		select {
		case msg := <-producer.PublishChannel():
			t.Fatalf("expected nothing published, got %d bytes", len(msg.Value))
		default:
		}
	}

	t.Run("only conflicting spends", func(t *testing.T) {
		v, producer, parentTx, privateKey, publicKey := newFixture(t, "conflicting_only")

		loser := transactions.Create(t,
			transactions.WithPrivateKey(privateKey),
			transactions.WithInput(parentTx, 0, privateKey),
			transactions.WithP2PKHOutputs(1, 80_000, publicKey),
		)

		_, err := v.Validate(t.Context(), loser, 100)
		require.True(t, errors.Is(err, errors.ErrTxRejectedConflicting), "every failed spend conflicts: %v", err)

		select {
		case msg := <-producer.PublishChannel():
			var m kafkamessage.KafkaTxPolicyRejectedTopicMessage
			require.NoError(t, proto.Unmarshal(msg.Value, &m))
			require.True(t, m.Conflicting)
		default:
			t.Fatal("expected the rejected conflicting tx to be published")
		}
	})

	t.Run("an output of a conflicting tx and a frozen spend", func(t *testing.T) {
		v, producer, parentTx, privateKey, publicKey := newFixture(t, "conflicting_parent_and_frozen")

		// stored as conflicting, the way the block path stores a loser
		loser := transactions.Create(t,
			transactions.WithPrivateKey(privateKey),
			transactions.WithInput(parentTx, 0, privateKey),
			transactions.WithP2PKHOutputs(1, 80_000, publicKey),
		)
		_, err := v.Validate(t.Context(), loser, 100, WithCreateConflicting(true))
		require.True(t, errors.Is(err, errors.ErrTxConflicting), "%v", err)

		select {
		case <-producer.PublishChannel():
			t.Fatal("a tx stored as conflicting is not published")
		default:
		}

		tx := transactions.Create(t,
			transactions.WithPrivateKey(privateKey),
			transactions.WithInput(loser, 0, privateKey),
			transactions.WithInput(parentTx, 1, privateKey),
			transactions.WithP2PKHOutputs(1, 170_000, publicKey),
		)

		_, err = v.Validate(t.Context(), tx, 100)
		require.True(t, errors.Is(err, errors.ErrFrozen), "the frozen input fails: %v", err)
		require.True(t, errors.Is(err, errors.ErrTxConflicting), "the store reports the conflicting parent's output: %v", err)
		require.False(t, errors.Is(err, errors.ErrTxRejectedConflicting), "a frozen input makes the tx invalid, not merely conflicting: %v", err)

		requireNotPublished(t, producer)
	})

	// the stores join the per-input errors in place, so whichever input fails first carries
	// the others in its chain: both orders must be classified by each input's own error
	for _, frozenFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("a conflicting and a frozen spend, frozen first %v", frozenFirst), func(t *testing.T) {
			v, producer, parentTx, privateKey, publicKey := newFixture(t, fmt.Sprintf("conflicting_and_frozen_%v", frozenFirst))

			first, second := uint32(0), uint32(1)
			if frozenFirst {
				first, second = 1, 0
			}

			loser := transactions.Create(t,
				transactions.WithPrivateKey(privateKey),
				transactions.WithInput(parentTx, first, privateKey),
				transactions.WithInput(parentTx, second, privateKey),
				transactions.WithP2PKHOutputs(1, 180_000, publicKey),
			)

			_, err := v.Validate(t.Context(), loser, 100)
			require.True(t, errors.Is(err, errors.ErrFrozen), "the frozen input fails: %v", err)
			require.True(t, errors.Is(err, errors.ErrSpent), "the spent input fails: %v", err)
			require.False(t, errors.Is(err, errors.ErrTxRejectedConflicting), "a frozen input makes the tx invalid, not merely conflicting: %v", err)

			requireNotPublished(t, producer)
		})
	}
}

// TestBlockPathCreatesConflictingOnlyWhenEveryFailedSpendConflicts pins the block path:
// a tx in a block whose only failed spends are double spends is created as conflicting,
// but one that also spends an immature coinbase is invalid and must not be created at all,
// whatever the order of its inputs. Creating it as conflicting dropped the maturity failure
// and let the block through.
func TestBlockPathCreatesConflictingOnlyWhenEveryFailedSpendConflicts(t *testing.T) {
	for _, immatureFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("immature first %v", immatureFirst), func(t *testing.T) {
			ctx := t.Context()
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockAssembly.Disabled = true

			utxoStoreURL, err := url.Parse(fmt.Sprintf("sqlitememory:///block_path_immature_%v", immatureFirst))
			require.NoError(t, err)

			utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
			require.NoError(t, err)
			utxoStore.RawDB().SetMaxOpenConns(1)
			require.NoError(t, utxoStore.SetBlockHeight(100))
			require.NoError(t, utxoStore.SetMedianBlockTime(uint32(time.Now().Unix()))) //nolint:gosec

			privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("BLOCK_PATH_IMMATURE_TEST_KEY!!!!"))

			parentTx := transactions.Create(t,
				transactions.WithCoinbaseData(1, "/block path immature parent/"),
				transactions.WithP2PKHOutputs(1, 100_000, publicKey),
			)
			_, err = utxoStore.Create(ctx, parentTx, 1, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 1, BlockHeight: 1, SubtreeIdx: 0}))
			require.NoError(t, err)

			// mined at the current height, so it matures only after the next block
			immatureCoinbase := transactions.Create(t,
				transactions.WithCoinbaseData(100, "/block path immature coinbase/"),
				transactions.WithP2PKHOutputs(1, 100_000, publicKey),
			)
			_, err = utxoStore.Create(ctx, immatureCoinbase, 100, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 100, BlockHeight: 100, SubtreeIdx: 0}))
			require.NoError(t, err)

			validator, err := New(ctx, logger, tSettings, utxoStore, nil, nil, nil, nil, nil)
			require.NoError(t, err)

			winner := transactions.Create(t,
				transactions.WithPrivateKey(privateKey),
				transactions.WithInput(parentTx, 0, privateKey),
				transactions.WithP2PKHOutputs(1, 90_000, publicKey),
			)
			_, err = validator.Validate(ctx, winner, 100)
			require.NoError(t, err)

			first := transactions.WithInput(parentTx, 0, privateKey)
			second := transactions.WithInput(immatureCoinbase, 0, privateKey)

			if immatureFirst {
				first, second = second, first
			}

			tx := transactions.Create(t,
				transactions.WithPrivateKey(privateKey),
				first,
				second,
				transactions.WithP2PKHOutputs(1, 180_000, publicKey),
			)

			txMeta, err := validator.Validate(ctx, tx, 100, WithCreateConflicting(true))
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrTxCoinbaseImmature), "the immature input fails: %v", err)
			require.False(t, errors.Is(err, errors.ErrTxConflicting), "an immature coinbase spend is not merely conflicting: %v", err)
			require.Nil(t, txMeta)

			err = utxoStore.GetMeta(ctx, tx.TxIDChainHash(), &meta.Data{})
			require.True(t, errors.Is(err, errors.ErrTxNotFound), "the tx is not created, as conflicting or otherwise: %v", err)
		})
	}
}
