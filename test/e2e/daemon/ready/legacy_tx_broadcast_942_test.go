package smoke

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/bsv-blockchain/teranode/test/utils/svnode"
	"github.com/stretchr/testify/require"
)

// TestLegacyTxBroadcast942_TeranodeRPCToSVMempool is the regression cover for
// issue #942 — txs accepted by Teranode via `sendrawtransaction` failing to
// reach connected SV-Node mempools. It mirrors the harness reproducer in
// node-validation: stand up a real SV-Node container, peer Teranode to it
// over legacy P2P, submit a spending tx via Teranode's RPC, and assert the
// tx appears in SV's mempool within the broadcast timeout.
//
// Steps:
//
//  1. Start an SV-Node container (regtest, mined 101 blocks to mature coinbase).
//  2. Start Teranode in legacy mode, ConnectPeers=[sv].
//  3. Wait for Teranode to sync to height 101.
//  4. Have SV mine a funding tx to Teranode's address.
//  5. Submit a *spending* tx via Teranode's RPC `sendrawtransaction`.
//  6. Poll SV's `getrawmempool` for the tx hash.
//
// Requires Docker. Lives under the `legacy-sync` Makefile target — see the
// Makefile `-run` list and the smoketest `-skip` list. Run directly with:
//
//	go test -v -count=1 -timeout 5m \
//	    -run TestLegacyTxBroadcast942_TeranodeRPCToSVMempool \
//	    ./test/e2e/daemon/ready
func TestLegacyTxBroadcast942_TeranodeRPCToSVMempool(t *testing.T) {
	legacySyncTestLock.Lock()
	defer legacySyncTestLock.Unlock()

	const (
		initialHeight           = 101
		fundingAmountBSV        = 10.0
		spendingFeeSat          = 10_000
		timeoutSyncInitial      = 60 * time.Second
		timeoutSyncFundingBlock = 60 * time.Second
		timeoutMempoolBroadcast = 30 * time.Second
		pollIntervalMempool     = 500 * time.Millisecond
	)

	ctx := t.Context()

	// SV's RPC/P2P use non-default ports so this test runs alongside the
	// node-validation harness stack (which holds 18332/18333/18444).
	// Teranode's legacy listener binds an ephemeral port (":0", OS-assigned):
	// 942 only dials OUT (ConnectPeers=[sv]) and SV never connects back, so a
	// fixed inbound port is unnecessary. A fixed port (was 48444) intermittently
	// failed to bind under CI load (TIME_WAIT/teardown of preceding containers),
	// which used to crash the whole package — see issue #1032.
	const (
		svRPCPort            = 48332
		svP2PPort            = 48333
		teranodeLegacyListen = "0.0.0.0:0"
	)

	opts := svnode.DefaultOptions()
	opts.RPCPort = svRPCPort
	opts.P2PPort = svP2PPort
	sv := svnode.New(opts)
	require.NoError(t, sv.Start(ctx), errStartSVNode)
	// Use a fresh context for the cleanup Stop: t.Context() is cancelled
	// at end-of-test BEFORE deferred functions run, which would force the
	// SV container shutdown rather than letting it stop gracefully.
	defer func() { _ = sv.Stop(context.Background()) }()

	_, err := sv.Generate(initialHeight)
	require.NoError(t, err, "SV Node generate initial blocks")
	t.Logf("[#942] SV Node generated %d blocks", initialHeight)

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableLegacy:    true,
		EnableValidator: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.Legacy.ConnectPeers = []string{sv.P2PHost()}
				s.Legacy.ListenAddresses = []string{teranodeLegacyListen}
				s.P2P.StaticPeers = []string{}
				s.ChainCfgParams.CoinbaseMaturity = 2
				// No P2P.ListenMode override: settings.conf resolves to
				// listen_only in the .dev context, but this PR decoupled
				// the legacy announce path from P2P.ListenMode, so the
				// test exercises the documented post-PR behaviour
				// (legacy announces regardless of modern-P2P listen_mode).
				// If this test ever fails on a listen_mode gate, the
				// decoupling has been reverted — investigate before
				// forcing it here.
			},
		),
		FSMState: blockchain.FSMStateRUNNING,
	})

	defer td.Stop(t)

	require.NoError(t,
		helper.WaitForNodeBlockHeight(ctx, td.BlockchainClient, uint32(initialHeight), timeoutSyncInitial),
		"Teranode should sync SV's initial chain")
	t.Logf("[#942] Both nodes synced to height %d", initialHeight)

	// Create a funding UTXO addressed to Teranode's coinbase key, mined by SV.
	// After this block syncs into Teranode, Teranode's UTXO store contains the
	// output we need to build a spending tx.
	privKey := td.GetPrivateKey(t)
	txCreator, err := svnode.NewTxCreator(sv, privKey)
	require.NoError(t, err)
	t.Logf("[#942] TxCreator address: %s", txCreator.Address())

	fundingUTXO, err := txCreator.CreateConfirmedFunding(fundingAmountBSV)
	require.NoError(t, err, "fund Teranode address via SV")
	fundingBlockHeight := uint32(initialHeight + 1)
	t.Logf("[#942] Funding UTXO: %s:%d amount=%d sat", fundingUTXO.TxID, fundingUTXO.Vout, fundingUTXO.Amount)

	require.NoError(t,
		helper.WaitForNodeBlockHeight(ctx, td.BlockchainClient, fundingBlockHeight, timeoutSyncFundingBlock),
		"Teranode should sync funding block")

	// Build a spending transaction. Submit it through Teranode's RPC — this is
	// the path that the issue claims silently drops on the legacy P2P relay.
	tx, err := txCreator.CreateSpendingTransaction(
		[]*svnode.FundingUTXO{fundingUTXO},
		txCreator.SelfPaymentBuilder(spendingFeeSat),
	)
	require.NoError(t, err, "build spending tx")
	txID := tx.TxID()
	t.Logf("[#942] Built spending tx %s; submitting to Teranode via sendrawtransaction", txID)

	resp, err := td.CallRPC(td.Ctx, "sendrawtransaction", []any{tx.String()})
	require.NoError(t, err, "Teranode sendrawtransaction must accept the tx")
	t.Logf("[#942] Teranode accepted tx — RPC response: %s", resp)

	// Now the bug-window: did Teranode actually INV the tx to SV?
	if waitErr := waitForSVMempool(t, sv, txID, timeoutMempoolBroadcast, pollIntervalMempool); waitErr != nil {
		t.Fatalf("[#942] REPRODUCED — Teranode→SV legacy P2P tx broadcast failed: %v", waitErr)
	}

	t.Logf("[#942] PASS — tx %s appeared in SV mempool", txID)
}

// waitForSVMempool polls SV's getrawmempool until the txid is present or the
// timeout elapses. SV-Node returns a JSON array of txid strings for the
// default (non-verbose) form.
func waitForSVMempool(t *testing.T, sv svnode.SVNodeI, txID string, timeout, poll time.Duration) error {
	t.Helper()

	deadline := time.Now().Add(timeout)

	var (
		lastRaw string
		lastErr error
	)

	for {
		raw, err := helper.CallRPC(sv.RPCURL(), "getrawmempool", []interface{}{})
		lastRaw, lastErr = raw, err
		if err == nil {
			var resp struct {
				Result []string `json:"result"`
				Error  any      `json:"error"`
			}
			if jsonErr := json.Unmarshal([]byte(raw), &resp); jsonErr == nil {
				for _, h := range resp.Result {
					if h == txID {
						return nil
					}
				}
			}
		}

		if time.Now().After(deadline) {
			return errors.NewProcessingError(
				"tx %s not present in SV mempool after %s — lastRPCErr: %v; lastRPCBody: %s",
				txID, timeout, lastErr, lastRaw)
		}
		time.Sleep(poll)
	}
}
