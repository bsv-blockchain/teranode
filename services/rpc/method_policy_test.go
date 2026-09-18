package rpc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/test/mocklogger"
	"github.com/stretchr/testify/require"
)

// Every registered handler must be classified, and every classified method must
// have a handler. An unclassified handler silently becomes admin-only at runtime,
// which is safe, but the table should still be the complete, reviewable contract.
func TestRPCMethodPolicyIsExhaustive(t *testing.T) {
	for method := range rpcHandlersBeforeInit {
		_, ok := rpcMethodPolicy[method]
		require.True(t, ok, "registered RPC handler %q is not classified in rpcMethodPolicy", method)
	}

	for method := range rpcMethodPolicy {
		_, ok := rpcHandlersBeforeInit[method]
		require.True(t, ok, "rpcMethodPolicy classifies %q but no handler is registered for it", method)
	}
}

func TestRPCMethodPolicyAlertMethodsAreAdminOnly(t *testing.T) {
	for _, method := range []string{"freeze", "unfreeze", "reassign"} {
		require.Equal(t, rpcAccessAdmin, methodAccess(method), "%s must be admin-only", method)
		require.False(t, methodAccess(method).limitedMayCall(), "%s must not be callable by the limited role", method)
	}
}

func TestRPCMethodPolicyUnknownMethodIsAdminOnly(t *testing.T) {
	require.Equal(t, rpcAccessAdmin, methodAccess("no-such-method"))
	require.False(t, methodAccess("no-such-method").limitedMayCall())
}

func TestRPCMethodPolicyLimitedTiers(t *testing.T) {
	require.True(t, rpcAccessLimitedRead.limitedMayCall())
	require.True(t, rpcAccessLimitedWrite.limitedMayCall())
	require.False(t, rpcAccessAdmin.limitedMayCall())

	// The writable grants to the limited role are an explicit, short list.
	var writable []string

	for method, access := range rpcMethodPolicy {
		if access == rpcAccessLimitedWrite {
			writable = append(writable, method)
		}
	}

	require.ElementsMatch(t, []string{"getminingcandidate", "sendrawtransaction", "submitblock", "submitminingsolution"}, writable)
}

// recordingUTXOStore wraps a real store and records every alert-system mutation
// so a test can prove the handler was, or was not, reached.
type recordingUTXOStore struct {
	utxo.Store

	mu        sync.Mutex
	freezes   []*utxo.Spend
	unfreezes []*utxo.Spend
	reassigns int
}

func (r *recordingUTXOStore) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	r.mu.Lock()
	r.freezes = append(r.freezes, spends...)
	r.mu.Unlock()

	return r.Store.FreezeUTXOs(ctx, spends, tSettings)
}

func (r *recordingUTXOStore) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	r.mu.Lock()
	r.unfreezes = append(r.unfreezes, spends...)
	r.mu.Unlock()

	return r.Store.UnFreezeUTXOs(ctx, spends, tSettings)
}

func (r *recordingUTXOStore) ReAssignUTXO(ctx context.Context, oldUtxo, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	r.mu.Lock()
	r.reassigns++
	r.mu.Unlock()

	return r.Store.ReAssignUTXO(ctx, oldUtxo, newUtxo, tSettings)
}

func (r *recordingUTXOStore) mutationCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.freezes) + len(r.unfreezes) + r.reassigns
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func basicAuth(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

// startTestRPCServer binds a real listener and serves the full HTTP path
// (checkAuth -> jsonRPCRead -> policy -> handler) exactly as a client hits it.
func startTestRPCServer(t *testing.T, store utxo.Store, tSettings *settings.Settings) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	tSettings.RPC.RPCTimeout = 5 * time.Second

	s := &RPCServer{
		logger:                 mocklogger.NewTestLogger(),
		settings:               tSettings,
		rpcMaxClients:          10,
		listeners:              []net.Listener{listener},
		statusLines:            make(map[int]string),
		requestProcessShutdown: make(chan struct{}),
		utxoStore:              store,
		authsha:                sha256.Sum256([]byte(basicAuth("admin", "adminpass"))),
		limitauthsha:           sha256.Sum256([]byte(basicAuth("limited", "limitpass"))),
	}
	require.NoError(t, s.Init(context.Background()))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	readyCh := make(chan struct{})

	go func() { _ = s.Start(ctx, readyCh) }()

	select {
	case <-readyCh:
	case <-time.After(5 * time.Second):
		t.Fatal("rpc server did not become ready")
	}

	return "http://" + listener.Addr().String()
}

func callRPC(t *testing.T, serverURL, auth, method string, params ...interface{}) rpcResponse {
	t.Helper()

	if params == nil {
		params = []interface{}{}
	}

	body, err := json.Marshal(map[string]interface{}{"jsonrpc": "1.0", "id": method, "method": method, "params": params})
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, serverURL, strings.NewReader(string(body)))
	require.NoError(t, err)
	req.Header.Set("Authorization", auth)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", raw)

	var out rpcResponse
	require.NoError(t, json.Unmarshal(raw, &out), "body: %s", raw)

	return out
}

// End to end over HTTP: a valid rpc_limit_user credential must receive an
// authorization error for freeze, unfreeze and reassign, and the UTXO store
// must never see a mutation call. The same credential can still call a
// read-only method, and the admin credential can freeze and unfreeze the
// output with the caller-supplied commitment reaching the store unchanged.
func TestLimitedUserCannotAdministerUTXOs(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	logger := ulogger.NewErrorTestLogger(t)

	storeURL, err := url.Parse("sqlitememory:///rpc_limited_role_utxo")
	require.NoError(t, err)

	sqlStore, err := sql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close(ctx)) })

	store := &recordingUTXOStore{Store: sqlStore}

	parent := bt.NewTx()
	require.NoError(t, parent.From("a000000000000000000000000000000000000000000000000000000000000001", 0, "51", 100_000_000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 100_000_000))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})
	_, err = sqlStore.Create(ctx, parent, 0, utxo.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	txID := parent.TxIDChainHash()
	utxoHash, err := util.UTXOHashFromOutput(txID, parent.Outputs[0], 0)
	require.NoError(t, err)
	require.NotEqual(t, txID.String(), utxoHash.String(), "test setup: commitment must differ from txid")

	serverURL := startTestRPCServer(t, store, tSettings)

	limited := basicAuth("limited", "limitpass")
	admin := basicAuth("admin", "adminpass")

	t.Run("limited credential is valid for a read-only method", func(t *testing.T) {
		resp := callRPC(t, serverURL, limited, "version")
		require.Nil(t, resp.Error, "version should succeed for the limited role")
	})

	for _, tc := range []struct {
		method string
		params []interface{}
	}{
		{"freeze", []interface{}{txID.String(), 0, utxoHash.String()}},
		{"unfreeze", []interface{}{txID.String(), 0, utxoHash.String()}},
		{"reassign", []interface{}{txID.String(), 0, utxoHash.String(), utxoHash.String()}},
	} {
		t.Run(fmt.Sprintf("limited credential is rejected for %s", tc.method), func(t *testing.T) {
			before := store.mutationCount()

			resp := callRPC(t, serverURL, limited, tc.method, tc.params...)
			require.NotNil(t, resp.Error, "%s must fail for the limited role", tc.method)
			require.Equal(t, "limited user not authorized for this method", resp.Error.Message)
			require.Equal(t, before, store.mutationCount(), "store mutation reached for %s", tc.method)
		})
	}

	// The wrong-credential path must also fail closed before any handler runs.
	t.Run("invalid credential never reaches the store", func(t *testing.T) {
		before := store.mutationCount()

		body := `{"jsonrpc":"1.0","id":1,"method":"freeze","params":["` + txID.String() + `",0,"` + utxoHash.String() + `"]}`
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, serverURL, strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Authorization", basicAuth("limited", "wrong"))

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		require.Equal(t, before, store.mutationCount())
	})

	t.Run("admin freezes and unfreezes with the supplied commitment", func(t *testing.T) {
		resp := callRPC(t, serverURL, admin, "freeze", txID.String(), 0, utxoHash.String())
		require.Nil(t, resp.Error, "admin freeze failed: %+v", resp.Error)

		require.Len(t, store.freezes, 1)
		require.Equal(t, utxoHash.String(), store.freezes[0].UTXOHash.String(), "handler must pass the caller-supplied commitment, not the txid")
		require.Equal(t, txID.String(), store.freezes[0].TxID.String())

		spendResp, err := sqlStore.GetSpend(ctx, &utxo.Spend{TxID: txID, Vout: 0, UTXOHash: utxoHash})
		require.NoError(t, err)
		require.Equal(t, int(utxo.Status_FROZEN), spendResp.Status)

		resp = callRPC(t, serverURL, admin, "unfreeze", txID.String(), 0, utxoHash.String())
		require.Nil(t, resp.Error, "admin unfreeze failed: %+v", resp.Error)
		require.Len(t, store.unfreezes, 1)
		require.Equal(t, utxoHash.String(), store.unfreezes[0].UTXOHash.String())

		spendResp, err = sqlStore.GetSpend(ctx, &utxo.Spend{TxID: txID, Vout: 0, UTXOHash: utxoHash})
		require.NoError(t, err)
		require.Equal(t, int(utxo.Status_OK), spendResp.Status)
	})

	t.Run("freeze without utxohash is a parameter error", func(t *testing.T) {
		resp := callRPC(t, serverURL, admin, "freeze", txID.String(), 0)
		require.NotNil(t, resp.Error)
		require.Contains(t, resp.Error.Message, "wrong number of params")
	})
}

func TestAlertSpendFromArgs(t *testing.T) {
	txID := "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	hash := "101112131415161718191a1b1c1d1e1f000102030405060708090a0b0c0d0e0f"

	t.Run("uses the supplied utxo hash", func(t *testing.T) {
		spend, err := alertSpendFromArgs(txID, 3, hash)
		require.NoError(t, err)
		require.Equal(t, txID, spend.TxID.String())
		require.Equal(t, uint32(3), spend.Vout)
		require.Equal(t, hash, spend.UTXOHash.String())
	})

	t.Run("rejects an invalid utxo hash", func(t *testing.T) {
		_, err := alertSpendFromArgs(txID, 0, "not-a-hash")
		require.Error(t, err)
	})

	t.Run("rejects a negative vout", func(t *testing.T) {
		_, err := alertSpendFromArgs(txID, -1, hash)
		require.Error(t, err)
	})

}
