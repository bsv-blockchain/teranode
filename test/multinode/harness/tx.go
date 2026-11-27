//go:build network_chaos

package harness

import (
	"context"
	"encoding/hex"
	"net/http"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
)

// minerWalletWIFs are the miner_wallet_private_keys.docker keys from
// settings.conf (PK1 | PK2 | PK3). Every coinbase mined by the multinode
// stack pays one P2PKH output to each of their addresses.
var minerWalletWIFs = []string{
	"L56TgyTpDdvL3W24SMoALYotibToSCySQeo4pThLKxw6EFR6f93Q",
	"KyAwSjuXZNgj78w3W7mR1fVMbPFu2heaCJJkWK5Yy58NZ4xafV6k",
	"L3NVjmwg3nC7ZPrwMVF6FXiG1a1RZ89nhizmJVctGztRKLYrhtFL",
}

// coinbaseMaturity is the regtest coinbase maturity used by the stack.
const coinbaseMaturity = 100

// spendFee is the flat fee paid by BuildCoinbaseSpend, comfortably above the
// default relay policy for a one-in/one-out P2PKH tx.
const spendFee = 1000

// SpendableOutput is a mature, unspent coinbase output the harness can sign for.
type SpendableOutput struct {
	TxID string
	Vout uint32
	utxo *bt.UTXO
	key  *bec.PrivateKey
}

// maturityBatch is how many blocks MatureCoinbaseOutput mines before waiting
// for the stack to converge. Peers fetch new blocks from the miner's asset
// service, whose heavy-endpoint limit (asset_httpHeavyRateLimit, 10 req/s per
// IP) a large generate burst trips; the resulting 429s mark the miner
// low_reputation and the peers never catch up.
const maturityBatch = 5

// MatureCoinbaseOutput mines on node c, in small batches that every node
// follows, until a mature coinbase exists. It then returns the most recent
// mature coinbase output (paying a miner wallet key) that c still reports as
// unspent. Scanning down from the newest mature height means repeated calls in
// one package run pick different coins as the chain grows.
func (s *Stack) MatureCoinbaseOutput(ctx context.Context, t *testing.T, c *RPCClient) *SpendableOutput {
	t.Helper()

	info, err := c.GetBlockchainInfo(ctx)
	if err != nil {
		t.Fatalf("getblockchaininfo on teranode%d: %v", c.NodeIndex, err)
	}
	if need := int64(coinbaseMaturity+1) - info.Blocks; need > 0 {
		t.Logf("mining %d block(s) on teranode%d for coinbase maturity", need, c.NodeIndex)
		for need > 0 {
			batch := min(need, maturityBatch)
			if _, err := c.Generate(ctx, int(batch)); err != nil {
				t.Fatalf("generate on teranode%d: %v", c.NodeIndex, err)
			}
			need -= batch
			WaitForHeightOn(t, s.Nodes(), coinbaseMaturity+1-need, 2*time.Minute)
		}
		info, err = c.GetBlockchainInfo(ctx)
		if err != nil {
			t.Fatalf("getblockchaininfo on teranode%d: %v", c.NodeIndex, err)
		}
	}

	keys := make(map[string]*bec.PrivateKey, len(minerWalletWIFs))
	for _, wif := range minerWalletWIFs {
		key, err := bec.PrivateKeyFromWif(wif)
		if err != nil {
			t.Fatalf("decode miner key: %v", err)
		}
		script, err := bscript.NewP2PKHFromPubKeyBytes(key.PubKey().Compressed())
		if err != nil {
			t.Fatalf("p2pkh script: %v", err)
		}
		keys[script.String()] = key
	}

	for h := info.Blocks - coinbaseMaturity; h >= 1; h-- {
		block, err := GetBlockByHeight(ctx, c.NodeIndex, uint32(h)) //nolint:gosec // h is a positive block height
		if err != nil {
			t.Fatalf("get block at height %d from teranode%d: %v", h, c.NodeIndex, err)
		}
		cbTx, err := bt.NewTxFromString(block.CoinbaseTx.Hex)
		if err != nil {
			t.Fatalf("decode coinbase at height %d: %v", h, err)
		}

		items, status, err := UTXOs(ctx, c.NodeIndex, cbTx.TxID())
		if err != nil || status != http.StatusOK {
			continue
		}

		for vout, out := range cbTx.Outputs {
			key, ok := keys[out.LockingScript.String()]
			if !ok || out.Satoshis <= spendFee || vout >= len(items) || items[vout].Status != "OK" {
				continue
			}

			t.Logf("using coinbase output %s:%d (height %d, %d sats)", cbTx.TxID(), vout, h, out.Satoshis)

			return &SpendableOutput{
				TxID: cbTx.TxID(),
				Vout: uint32(vout), //nolint:gosec // output index
				utxo: &bt.UTXO{
					TxIDHash:      cbTx.TxIDChainHash(),
					Vout:          uint32(vout), //nolint:gosec // output index
					LockingScript: out.LockingScript,
					Satoshis:      out.Satoshis,
				},
				key: key,
			}
		}
	}

	t.Fatalf("no mature unspent coinbase output found on teranode%d", c.NodeIndex)
	return nil
}

// BuildSpend returns a signed one-in/one-out transaction spending out back to
// the same key, minus spendFee.
func BuildSpend(ctx context.Context, out *SpendableOutput) (*bt.Tx, error) {
	tx := bt.NewTx()
	if err := tx.FromUTXOs(out.utxo); err != nil {
		return nil, errors.NewProcessingError("add input", err)
	}

	script, err := bscript.NewP2PKHFromPubKeyBytes(out.key.PubKey().Compressed())
	if err != nil {
		return nil, errors.NewProcessingError("p2pkh script", err)
	}
	tx.AddOutput(&bt.Output{LockingScript: script, Satoshis: out.utxo.Satoshis - spendFee})

	if err := tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: out.key}); err != nil {
		return nil, errors.NewProcessingError("sign", err)
	}
	return tx, nil
}

// TxHex returns the standard (non-extended) hex serialisation of tx.
func TxHex(tx *bt.Tx) string {
	return hex.EncodeToString(tx.Bytes())
}
