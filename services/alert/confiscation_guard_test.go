// Package alert — the ownership-changing confiscation refusal (reconciliation
// notes 63, open question 42; upstream issue 1725).
package alert

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-alert-system/app/config"
	"github.com/bsv-blockchain/go-bn/models"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// confiscationTxForGuard builds a confiscation transaction spending vout 0 of
// the package test tx. Its input carries a two-push scriptSig (dummy signature,
// then the new owner's public key) which is all extractPublicKey parses.
func confiscationTxForGuard(t *testing.T, pubKey []byte) *bt.Tx {
	t.Helper()

	locking := tx.Outputs[0].LockingScript

	c := &bt.Tx{}
	err := c.FromUTXOs([]*bt.UTXO{{
		TxIDHash:       tx.TxIDChainHash(),
		Vout:           0,
		LockingScript:  locking,
		Satoshis:       tx.Outputs[0].Satoshis,
		SequenceNumber: bt.DefaultSequenceNumber,
	}}...)
	require.NoError(t, err)

	// 0x48 + 72-byte dummy signature, 0x21 + 33-byte public key.
	scriptSig := make([]byte, 0, 1+72+1+len(pubKey))
	scriptSig = append(scriptSig, 0x48)
	scriptSig = append(scriptSig, make([]byte, 72)...)
	require.Len(t, pubKey, 33)
	scriptSig = append(scriptSig, 0x21)
	scriptSig = append(scriptSig, pubKey...)

	c.Inputs[0].UnlockingScript = bscript.NewFromBytes(scriptSig)

	return c
}

func newGuardNode(t *testing.T, allowUnpersisted bool) config.NodeInterface {
	t.Helper()

	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Alert.ConfiscationAllowReassignWithoutScript = allowUnpersisted

	logger := ulogger.NewErrorTestLogger(t)
	utxoStoreURL, err := url.Parse("sqlitememory:///confiscation-guard")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	require.NoError(t, utxoStore.SetBlockHeight(101))
	_, err = utxoStore.Create(ctx, tx, 101)
	require.NoError(t, err)

	return NewNodeConfig(ulogger.TestLogger{}, nil, utxoStore, nil, nil, nil, tSettings)
}

func TestConfiscation_OwnershipChangeRefusedWithoutOptIn(t *testing.T) {
	ctx := context.Background()
	node := newGuardNode(t, false)

	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	c := confiscationTxForGuard(t, priv.PubKey().Compressed())

	response, err := node.AddToConfiscationTransactionWhitelist(ctx, []models.ConfiscationTransactionDetails{{
		ConfiscationTransaction: models.ConfiscationTransaction{
			EnforceAtHeight: 102,
			Hex:             c.String(),
		},
	}})
	require.NoError(t, err)
	require.Equal(t, 1, len(response.NotProcessed))
	require.Contains(t, response.NotProcessed[0].Reason, "issue 1725")
	require.Contains(t, response.NotProcessed[0].Reason, "alert_confiscationAllowReassignWithoutScript=true")
}

func TestConfiscation_OwnershipChangeProceedsWithOptIn(t *testing.T) {
	ctx := context.Background()
	node := newGuardNode(t, true)

	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	c := confiscationTxForGuard(t, priv.PubKey().Compressed())

	response, err := node.AddToConfiscationTransactionWhitelist(ctx, []models.ConfiscationTransactionDetails{{
		ConfiscationTransaction: models.ConfiscationTransaction{
			EnforceAtHeight: 102,
			Hex:             c.String(),
		},
	}})
	require.NoError(t, err)

	// Past the guard: the store's own precondition is what fails next - the
	// output was never frozen - which proves the refusal is no longer what
	// stopped the reassignment.
	require.Equal(t, 1, len(response.NotProcessed))
	require.Contains(t, response.NotProcessed[0].Reason, "not frozen")
	require.NotContains(t, response.NotProcessed[0].Reason, "issue 1725")
}

// The key sanity check for the hand-built scriptSig above: the public key the
// guard's extraction must see is exactly the pushed one.
func TestConfiscation_ScriptSigCarvesPublicKey(t *testing.T) {
	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	pub := priv.PubKey().Compressed()
	c := confiscationTxForGuard(t, pub)

	got, err := extractPublicKey(c.Inputs[0].UnlockingScript.Bytes())
	require.NoError(t, err)
	require.Equal(t, pub, got)
}
