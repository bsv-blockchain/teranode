// Package rpc implements the Bitcoin JSON-RPC API service for Teranode.
package rpc

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/util/test/mocklogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestHandleGetMiningInfo(t *testing.T) {
	difficulty := 97415240192.16336
	expectedHashRate := 6.973254512622107e+17
	networkHashPS := calculateHashRate(difficulty, chaincfg.MainNetParams.TargetTimePerBlock.Seconds())
	assert.Equal(t, networkHashPS, expectedHashRate)
}

func TestIPOrSubnetValidation(t *testing.T) {
	ip := "172.19.0.8"

	assert.True(t, isIPOrSubnet(ip))

	// test subnet
	subnet := "172.19.0.0/24"
	assert.True(t, isIPOrSubnet(subnet))

	// test invalid ip
	invalidIP := "172.19.0.8.8"
	assert.False(t, isIPOrSubnet(invalidIP))

	// test invalid subnet
	invalidSubnet := "172.19.0.0/33"
	assert.False(t, isIPOrSubnet(invalidSubnet))

	// test ip with port
	invalidSubnet = "172.19.0.0:8080"
	assert.False(t, isIPOrSubnet(invalidSubnet))

	// test subnet with port (not valid CIDR)
	invalidSubnet = "172.19.0.0/24:8080"
	assert.False(t, isIPOrSubnet(invalidSubnet))
}

func TestSanityCheckGetRawTransaction(t *testing.T) {
	txHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a01000000434104e70a02f5af48a1989bf630d92523c9d14c45c75f7d1b998e962bff6ff9995fc5bdb44f1793b37495d80324acba7c8f537caaf8432b8d47987313060cc82d8a93ac00000000"

	bytes, err := hex.DecodeString(txHex)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := bt.NewTxFromBytes(bytes)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "bfc04d8db515101ccbaf674cae7700ad413b9b334984c377e71755f2361cb766", tx.TxID())
}

func TestHandleGetRawTransaction(t *testing.T) {
	// Sample transaction hex data - this is a complete, valid transaction hex
	txHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a01000000434104e70a02f5af48a1989bf630d92523c9d14c45c75f7d1b998e962bff6ff9995fc5bdb44f1793b37495d80324acba7c8f537caaf8432b8d47987313060cc82d8a93ac00000000"
	txid := "bfc04d8db515101ccbaf674cae7700ad413b9b334984c377e71755f2361cb766"

	// Create a test server that will respond with our sample transaction
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/api/v1/tx/") && strings.Contains(r.URL.Path, "/hex") {
			// Extract the transaction ID from the URL path
			pathParts := strings.Split(r.URL.Path, "/")
			requestTxid := ""

			for i, part := range pathParts {
				if part == "tx" && i+1 < len(pathParts) {
					requestTxid = pathParts[i+1]
					break
				}
			}

			// Only return the transaction if the ID matches our test transaction
			if requestTxid == txid {
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, txHex)

				return
			}
		}

		// Return 404 for any other request or non-matching transaction ID
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	// Create a mock RPCServer with the test server URL
	assetURL, _ := url.Parse(server.URL)
	s := &RPCServer{
		assetHTTPURL: assetURL,
		settings: &settings.Settings{
			ChainCfgParams: &chaincfg.Params{},
		},
	}

	// Test case 1: Non-verbose mode (return raw hex)
	verboseLevel := 0
	cmd := &bsvjson.GetRawTransactionCmd{
		Txid:    txid,
		Verbose: &verboseLevel,
	}

	result, err := handleGetRawTransaction(context.Background(), s, cmd, nil)
	require.NoError(t, err)
	assert.Equal(t, txHex, result)

	// Test case 2: Verbose mode (return transaction details)
	verboseLevel = 1
	cmd = &bsvjson.GetRawTransactionCmd{
		Txid:    txid,
		Verbose: &verboseLevel,
	}

	result, err = handleGetRawTransaction(context.Background(), s, cmd, nil)
	require.NoError(t, err)

	txResult, ok := result.(bsvjson.TxRawResult)
	require.True(t, ok)
	assert.Equal(t, txid, txResult.Txid)
	assert.Equal(t, txHex, txResult.Hex)
	assert.Equal(t, txid, txResult.Hash)
	assert.Equal(t, int32(134), txResult.Size)
	assert.Equal(t, int32(1), txResult.Version)
	assert.Equal(t, uint32(0x0), txResult.LockTime)
	assert.Equal(t, 1, len(txResult.Vin))
	assert.Equal(t, 1, len(txResult.Vout))
	assert.Equal(t, bsvjson.BTCValue(50), txResult.Vout[0].Value)

	// Test case 3: Error when asset URL is not set
	s.assetHTTPURL = nil
	_, err = handleGetRawTransaction(context.Background(), s, cmd, nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errors.ErrConfiguration))

	// Test case 4: Error when transaction not found
	s.assetHTTPURL = assetURL
	cmd.Txid = "nonexistenttxid"
	_, err = handleGetRawTransaction(context.Background(), s, cmd, nil)
	require.Error(t, err)
}

// TestHandleSendRawTransaction_RefusedWhileSyncing pins issue 2003 on RPC: in
// CATCHINGBLOCKS sendrawtransaction answers -10, the code bitcoind uses during
// initial block download, and neither the blob store nor the validator, the
// only writer of the UTXO store, sees the transaction. RUNNING and a cached
// IDLE both accept.
func TestHandleSendRawTransaction_RefusedWhileSyncing(t *testing.T) {
	for _, tt := range []struct {
		state   blockchain.FSMStateType
		refuses bool
	}{
		{blockchain.FSMStateCATCHINGBLOCKS, true},
		{blockchain.FSMStateRUNNING, false},
		{blockchain.FSMStateIDLE, false},
	} {
		t.Run(tt.state.String(), func(t *testing.T) {
			state := tt.state
			blockchainClient := &blockchain.Mock{}
			blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)

			validated := 0
			txStore := blobmemory.New()

			s := &RPCServer{
				logger:   mocklogger.NewTestLogger(),
				settings: &settings.Settings{ChainCfgParams: &chaincfg.MainNetParams, Policy: &settings.PolicySettings{}},
				txStore:  txStore,
				validatorClient: &validator.MockValidator{
					ValidateFunc: func(context.Context, *bt.Tx) (*meta.Data, error) {
						validated++
						return &meta.Data{}, nil
					},
				},
				unminedTxGate: blockchain.NewUnminedTxGate(mocklogger.NewTestLogger(), blockchainClient, nil, blockchain.UnminedTxRouteRPCSendRawTx),
			}

			tx := transactions.CreateTestTransactionChainWithCount(t, 3)[1]
			refusedBefore := s.unminedTxGate.RefusedTotal()

			_, err := handleSendRawTransaction(context.Background(), s, &bsvjson.SendRawTransactionCmd{HexTx: hex.EncodeToString(tx.ExtendedBytes())}, nil)

			stored, existsErr := txStore.Exists(context.Background(), tx.TxIDChainHash().CloneBytes(), fileformat.FileTypeTx)
			require.NoError(t, existsErr)

			if tt.refuses {
				var rpcErr *bsvjson.RPCError
				require.ErrorAs(t, err, &rpcErr)
				require.Equal(t, bsvjson.ErrRPCClientInInitialDownload, rpcErr.Code)
				require.Equal(t, 0, validated, "a refused tx must not reach the validator or the UTXO store")
				require.False(t, stored, "a refused tx must not reach the blob store")
				require.InDelta(t, refusedBefore+1, s.unminedTxGate.RefusedTotal(), 0)

				return
			}

			require.NoError(t, err)
			require.Equal(t, 1, validated)
			require.True(t, stored)
			require.InDelta(t, refusedBefore, s.unminedTxGate.RefusedTotal(), 0)
		})
	}
}

// voutRecordingUtxoStore records the spends the alert handlers hand to the store.
type voutRecordingUtxoStore struct {
	utxo.Store
	freezeSpends   []*utxo.Spend
	unfreezeSpends []*utxo.Spend
	reassignSpends []*utxo.Spend
	reassignNew    []*utxo.Spend
}

func (r *voutRecordingUtxoStore) FreezeUTXOs(_ context.Context, spends []*utxo.Spend, _ *settings.Settings) error {
	r.freezeSpends = append(r.freezeSpends, spends...)

	return nil
}

func (r *voutRecordingUtxoStore) UnFreezeUTXOs(_ context.Context, spends []*utxo.Spend, _ *settings.Settings) error {
	r.unfreezeSpends = append(r.unfreezeSpends, spends...)

	return nil
}

func (r *voutRecordingUtxoStore) ReAssignUTXO(_ context.Context, old *utxo.Spend, newUtxo *utxo.Spend, _ *settings.Settings) error {
	r.reassignSpends = append(r.reassignSpends, old)
	r.reassignNew = append(r.reassignNew, newUtxo)

	return nil
}

func (r *voutRecordingUtxoStore) calls() int {
	return len(r.freezeSpends) + len(r.unfreezeSpends) + len(r.reassignSpends)
}

// TestUTXOAlertHandlers_VoutRange checks that freeze, unfreeze and reassign refuse a
// vout outside uint32 before the store sees it, and pass an in-range vout through exactly.
func TestUTXOAlertHandlers_VoutRange(t *testing.T) {
	hash := strings.Repeat("ab", 32)

	handlers := []struct {
		name   string
		call   func(s *RPCServer, vout int) error
		spends func(r *voutRecordingUtxoStore) []*utxo.Spend
	}{
		{
			name: "freeze",
			call: func(s *RPCServer, vout int) error {
				_, err := handleFreeze(context.Background(), s, &bsvjson.FreezeCmd{TxID: hash, Vout: vout, UTXOHash: hash}, nil)

				return err
			},
			spends: func(r *voutRecordingUtxoStore) []*utxo.Spend { return r.freezeSpends },
		},
		{
			name: "unfreeze",
			call: func(s *RPCServer, vout int) error {
				_, err := handleUnfreeze(context.Background(), s, &bsvjson.UnfreezeCmd{TxID: hash, Vout: vout, UTXOHash: hash}, nil)

				return err
			},
			spends: func(r *voutRecordingUtxoStore) []*utxo.Spend { return r.unfreezeSpends },
		},
		{
			name: "reassign",
			call: func(s *RPCServer, vout int) error {
				_, err := handleReassign(context.Background(), s, &bsvjson.ReassignCmd{
					OldTxID: hash, OldVout: vout, OldUTXOHash: hash, NewUTXOHash: hash,
				}, nil)

				return err
			},
			spends: func(r *voutRecordingUtxoStore) []*utxo.Spend { return r.reassignSpends },
		},
	}

	rejected := []int{-1, 4294967296, 4294967297}
	accepted := []int{0, 1, 4294967295}

	newServer := func() (*RPCServer, *voutRecordingUtxoStore) {
		store := &voutRecordingUtxoStore{}

		return &RPCServer{
			logger:    newCapturingLogger(),
			settings:  &settings.Settings{ChainCfgParams: &chaincfg.MainNetParams},
			utxoStore: store,
		}, store
	}

	for _, h := range handlers {
		for _, vout := range rejected {
			t.Run(fmt.Sprintf("%s rejects vout %d", h.name, vout), func(t *testing.T) {
				s, store := newServer()

				err := h.call(s, vout)
				require.Error(t, err)

				var rpcErr *bsvjson.RPCError
				require.ErrorAs(t, err, &rpcErr)
				require.Equal(t, bsvjson.ErrRPCInvalidParameter, rpcErr.Code)
				require.Zero(t, store.calls())
			})
		}

		for _, vout := range accepted {
			t.Run(fmt.Sprintf("%s passes vout %d to the store", h.name, vout), func(t *testing.T) {
				s, store := newServer()

				require.NoError(t, h.call(s, vout))
				require.Equal(t, 1, store.calls())

				spends := h.spends(store)
				require.Len(t, spends, 1)
				require.Equal(t, uint32(vout), spends[0].Vout) // nolint:gosec
			})
		}
	}
}

// TestUTXOAlertHandlers_HashArguments checks the txid and UTXO hash arguments of
// freeze, unfreeze and reassign. A short hex string parses as a left-padded hash, so
// it names a record or commitment the caller did not mean; it must be refused as a
// parameter error before the store sees it. A full-length commitment must reach the
// store unchanged, not be replaced by the txid.
func TestUTXOAlertHandlers_HashArguments(t *testing.T) {
	txid := strings.Repeat("ab", 32)
	commitment := strings.Repeat("cd", 32)
	newCommitment := strings.Repeat("ef", 32)

	type args struct{ txid, utxoHash, newUTXOHash string }

	handlers := []struct {
		name   string
		call   func(s *RPCServer, a args) error
		spends func(r *voutRecordingUtxoStore) []*utxo.Spend
		// fields lists which arguments the handler takes, in the order the tests vary them.
		fields []string
	}{
		{
			name: "freeze",
			call: func(s *RPCServer, a args) error {
				_, err := handleFreeze(context.Background(), s, &bsvjson.FreezeCmd{TxID: a.txid, Vout: 1, UTXOHash: a.utxoHash}, nil)

				return err
			},
			spends: func(r *voutRecordingUtxoStore) []*utxo.Spend { return r.freezeSpends },
			fields: []string{"txid", "utxohash"},
		},
		{
			name: "unfreeze",
			call: func(s *RPCServer, a args) error {
				_, err := handleUnfreeze(context.Background(), s, &bsvjson.UnfreezeCmd{TxID: a.txid, Vout: 1, UTXOHash: a.utxoHash}, nil)

				return err
			},
			spends: func(r *voutRecordingUtxoStore) []*utxo.Spend { return r.unfreezeSpends },
			fields: []string{"txid", "utxohash"},
		},
		{
			name: "reassign",
			call: func(s *RPCServer, a args) error {
				_, err := handleReassign(context.Background(), s, &bsvjson.ReassignCmd{
					OldTxID: a.txid, OldVout: 1, OldUTXOHash: a.utxoHash, NewUTXOHash: a.newUTXOHash,
				}, nil)

				return err
			},
			spends: func(r *voutRecordingUtxoStore) []*utxo.Spend { return r.reassignSpends },
			fields: []string{"txid", "utxohash", "newutxohash"},
		},
	}

	valid := args{txid: txid, utxoHash: commitment, newUTXOHash: newCommitment}

	with := func(field, value string) args {
		a := valid

		switch field {
		case "txid":
			a.txid = value
		case "utxohash":
			a.utxoHash = value
		case "newutxohash":
			a.newUTXOHash = value
		}

		return a
	}

	newServer := func() (*RPCServer, *voutRecordingUtxoStore) {
		store := &voutRecordingUtxoStore{}

		return &RPCServer{
			logger:    newCapturingLogger(),
			settings:  &settings.Settings{ChainCfgParams: &chaincfg.MainNetParams},
			utxoStore: store,
		}, store
	}

	t.Run("reassign passes the new commitment to the store", func(t *testing.T) {
		s, store := newServer()

		_, err := handleReassign(context.Background(), s, &bsvjson.ReassignCmd{
			OldTxID: txid, OldVout: 1, OldUTXOHash: commitment, NewUTXOHash: newCommitment,
		}, nil)
		require.NoError(t, err)

		require.Len(t, store.reassignNew, 1)
		require.Equal(t, newCommitment, store.reassignNew[0].UTXOHash.String())
	})

	for _, h := range handlers {
		t.Run(h.name+" passes the supplied commitment to the store", func(t *testing.T) {
			s, store := newServer()

			require.NoError(t, h.call(s, valid))

			spends := h.spends(store)
			require.Len(t, spends, 1)
			require.Equal(t, txid, spends[0].TxID.String())
			require.Equal(t, commitment, spends[0].UTXOHash.String(), "the commitment must be the caller's utxohash, not the txid")
		})

		for _, field := range h.fields {
			for _, short := range []string{"", "0", "deadbeef", strings.Repeat("ab", 31)} {
				t.Run(fmt.Sprintf("%s rejects a %d-character %s", h.name, len(short), field), func(t *testing.T) {
					s, store := newServer()

					err := h.call(s, with(field, short))
					require.Error(t, err)

					var rpcErr *bsvjson.RPCError
					require.ErrorAs(t, err, &rpcErr)
					require.Equal(t, bsvjson.ErrRPCInvalidParameter, rpcErr.Code)
					require.Contains(t, rpcErr.Message, field)
					require.Zero(t, store.calls())
				})
			}

			t.Run(fmt.Sprintf("%s rejects a non-hex %s", h.name, field), func(t *testing.T) {
				s, store := newServer()

				err := h.call(s, with(field, strings.Repeat("zz", 32)))
				require.Error(t, err)

				var rpcErr *bsvjson.RPCError
				require.ErrorAs(t, err, &rpcErr)
				require.Equal(t, bsvjson.ErrRPCDecodeHexString, rpcErr.Code)
				require.Zero(t, store.calls())
			})
		}
	}
}
