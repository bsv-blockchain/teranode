package db

import (
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"strconv"
	"testing"

	"github.com/TAAL-GmbH/ubsv/db/model"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func equalBlocks(a, b model.Block) bool {
	return a.Height == b.Height && a.BlockHash == b.BlockHash && a.PrevBlockHash == b.PrevBlockHash
}

func equalUtxos(a, b model.UTXO) bool {
	return a.Txid == b.Txid &&
		a.Vout == b.Vout &&
		a.Script == b.Script &&
		a.Amount == b.Amount &&
		a.Address == b.Address &&
		a.Spent == b.Spent
}

func TestConnect(t *testing.T) {
	store, ok := gocore.Config().Get("coinbasetracker_store")
	if !ok {
		fmt.Println("coinbasetracker_store is not set. Using sqlite.")
		store = "sqlite"
	}

	store_config, ok := gocore.Config().Get("coinbasetracker_store_config")
	if !ok {
		fmt.Println("coinbasetracker_store_config is not set. Using sqlite in-mem.")
		store_config = "file::memory:?cache=shared"
	}

	mgr := Create(store, store_config)
	if mgr == nil {
		t.Fatalf("Cannot create sqlite database instance with %s | %s", store, store_config)
	}

	err := mgr.Disconnect()
	if err != nil {
		t.Fatalf("Cannot disconnect sqlite database instance %s | %s: %s", store, store_config, err)
	}
}

func TestAddBlock(t *testing.T) {
	store, ok := gocore.Config().Get("coinbasetracker_store")
	if !ok {
		fmt.Println("coinbasetracker_store is not set. Using sqlite.")
		store = "sqlite"
	}

	store_config, ok := gocore.Config().Get("coinbasetracker_store_config")
	if !ok {
		fmt.Println("coinbasetracker_store_config is not set. Using sqlite in-mem.")
		store_config = "file::memory:?cache=shared"
	}

	mgr := Create(store, store_config)
	if mgr == nil {
		t.Fatalf("Cannot create sqlite database instance with %s | %s", store, store_config)
	}

	tn1bin := make([]byte, 32)
	_, _ = rand.Read(tn1bin)
	hash1, _ := chainhash.NewHash(tn1bin)
	tn2bin := make([]byte, 32)
	_, _ = rand.Read(tn2bin)
	hash2, _ := chainhash.NewHash(tn2bin)

	m := &model.Block{
		Height:        uint64(mrand.Uint32()) + 1,
		BlockHash:     hash2.String(),
		PrevBlockHash: hash1.String(),
	}

	err := mgr.Create(m)
	if err != nil {
		t.Fatalf("Create failed: %s", err.Error())
	}

	err = mgr.Disconnect()
	if err != nil {
		t.Fatalf("Cannot disconnect sqlite database instance %s | %s: %s", store, store_config, err)
	}
}

func TestAddReadBlock(t *testing.T) {
	store, ok := gocore.Config().Get("coinbasetracker_store")
	if !ok {
		fmt.Println("coinbasetracker_store is not set. Using sqlite.")
		store = "sqlite"
	}

	store_config, ok := gocore.Config().Get("coinbasetracker_store_config")
	if !ok {
		fmt.Println("coinbasetracker_store_config is not set. Using sqlite in-mem.")
		store_config = "file::memory:?cache=shared"
	}

	mgr := Create(store, store_config)
	if mgr == nil {
		t.Fatalf("Cannot create sqlite database instance with %s | %s", store, store_config)
	}

	tn1bin := make([]byte, 32)
	_, _ = rand.Read(tn1bin)
	hash1, _ := chainhash.NewHash(tn1bin)
	tn2bin := make([]byte, 32)
	_, _ = rand.Read(tn2bin)
	hash2, _ := chainhash.NewHash(tn2bin)

	m := &model.Block{
		Height:        uint64(mrand.Uint32()) + 1,
		BlockHash:     hash2.String(),
		PrevBlockHash: hash1.String(),
	}

	err := mgr.Create(m)
	if err != nil {
		t.Fatalf("Create failed: %s", err.Error())
	}

	mr := &model.Block{}
	err = mgr.Read(mr)
	if err != nil {
		t.Fatalf("Read failed: %s", err.Error())
	}
	if !equalBlocks(*m, *mr) {
		t.Fatal("Blocks mismatch")
	}
	err = mgr.Disconnect()
	if err != nil {
		t.Fatalf("Cannot disconnect sqlite database instance %s | %s: %s", store, store_config, err)
	}
}

func TestAddUtxo(t *testing.T) {
	store, ok := gocore.Config().Get("coinbasetracker_store")
	if !ok {
		fmt.Println("coinbasetracker_store is not set. Using sqlite.")
		store = "sqlite"
	}

	store_config, ok := gocore.Config().Get("coinbasetracker_store_config")
	if !ok {
		fmt.Println("coinbasetracker_store_config is not set. Using sqlite in-mem.")
		store_config = "file::memory:?cache=shared"
	}

	mgr := Create(store, store_config)
	if mgr == nil {
		t.Fatalf("Cannot create sqlite database instance with %s | %s", store, store_config)
	}

	tx1bin := make([]byte, 32)
	_, _ = rand.Read(tx1bin)
	hash1, _ := chainhash.NewHash(tx1bin)

	pk, _ := bec.NewPrivateKey(bec.S256())
	pubkey := pk.PubKey()
	script, err := bscript.NewP2PKHFromPubKeyBytes(pubkey.SerialiseCompressed())
	if err != nil {
		t.Fatalf("Failed to generate script: %+v", err)
	}

	addr, err := bscript.NewAddressFromPublicKey(pubkey, false)
	if err != nil {
		t.Fatalf("Failed to generate address: %+v", err)
	}

	m := &model.UTXO{
		Txid:    hash1.String(),
		Vout:    mrand.Uint32(),
		Script:  script.String(),
		Amount:  uint64(mrand.Uint32()) + 1,
		Address: addr.AddressString,
		Spent:   mrand.Int31n(2) > 0,
	}

	err = mgr.Create(m)
	if err != nil {
		t.Fatalf("Create failed: %s", err.Error())
	}

	err = mgr.Disconnect()
	if err != nil {
		t.Fatalf("Cannot disconnect sqlite database instance %s | %s: %s", store, store_config, err)
	}
}

func TestAddReadUtxo(t *testing.T) {
	store, ok := gocore.Config().Get("coinbasetracker_store")
	if !ok {
		fmt.Println("coinbasetracker_store is not set. Using sqlite.")
		store = "sqlite"
	}

	store_config, ok := gocore.Config().Get("coinbasetracker_store_config")
	if !ok {
		fmt.Println("coinbasetracker_store_config is not set. Using sqlite in-mem.")
		store_config = "file::memory:?cache=shared"
	}

	mgr := Create(store, store_config)
	if mgr == nil {
		t.Fatalf("Cannot create sqlite database instance with %s | %s", store, store_config)
	}

	tx1bin := make([]byte, 32)
	_, _ = rand.Read(tx1bin)
	hash1, _ := chainhash.NewHash(tx1bin)

	pk, _ := bec.NewPrivateKey(bec.S256())
	pubkey := pk.PubKey()
	script, err := bscript.NewP2PKHFromPubKeyBytes(pubkey.SerialiseCompressed())
	if err != nil {
		t.Fatalf("Failed to generate script: %+v", err)
	}

	addr, err := bscript.NewAddressFromPublicKey(pubkey, false)
	if err != nil {
		t.Fatalf("Failed to generate address: %+v", err)
	}

	m1 := &model.UTXO{
		Txid:    hash1.String(),
		Vout:    mrand.Uint32(),
		Script:  script.String(),
		Amount:  uint64(mrand.Uint32()) + 1,
		Address: addr.AddressString,
		Spent:   mrand.Int31n(2) > 0,
	}

	err = mgr.Create(m1)
	if err != nil {
		t.Fatalf("Create failed: %s", err.Error())
	}

	m2 := &model.UTXO{}
	err = mgr.Read(m2)
	if err != nil {
		t.Fatalf("Read failed: %s", err.Error())
	}

	if !equalUtxos(*m1, *m2) {
		t.Fatal("UTXO(Write) and UTXO(Read) should be equal")
	}

	err = mgr.Disconnect()
	if err != nil {
		t.Fatalf("Cannot disconnect sqlite database instance %s | %s: %s", store, store_config, err)
	}
}

func TestAddReadCondUtxo(t *testing.T) {
	store, ok := gocore.Config().Get("coinbasetracker_store")
	if !ok {
		fmt.Println("coinbasetracker_store is not set. Using sqlite.")
		store = "sqlite"
	}

	store_config, ok := gocore.Config().Get("coinbasetracker_store_config")
	if !ok {
		fmt.Println("coinbasetracker_store_config is not set. Using sqlite in-mem.")
		store_config = "file::memory:?cache=shared"
	}

	mgr := Create(store, store_config)
	if mgr == nil {
		t.Fatalf("Cannot create sqlite database instance with %s | %s", store, store_config)
	}

	tx1bin := make([]byte, 32)
	_, _ = rand.Read(tx1bin)
	hash1, _ := chainhash.NewHash(tx1bin)

	pk, _ := bec.NewPrivateKey(bec.S256())
	pubkey := pk.PubKey()
	script, err := bscript.NewP2PKHFromPubKeyBytes(pubkey.SerialiseCompressed())
	if err != nil {
		t.Fatalf("Failed to generate script: %+v", err)
	}

	addr, err := bscript.NewAddressFromPublicKey(pubkey, false)
	if err != nil {
		t.Fatalf("Failed to generate address: %+v", err)
	}

	m1 := &model.UTXO{
		Txid:    hash1.String(),
		Vout:    mrand.Uint32(),
		Script:  script.String(),
		Amount:  uint64(mrand.Uint32()) + 1,
		Address: addr.AddressString,
		Spent:   mrand.Int31n(2) > 0,
	}

	err = mgr.Create(m1)
	if err != nil {
		t.Fatalf("Create failed: %s", err.Error())
	}

	m2 := &model.UTXO{}
	cond := []interface{}{"address = ? AND amount = ?", m1.Address, strconv.FormatInt(int64(m1.Amount), 10)}

	payload, err := mgr.Read_All_Cond(m2, cond)
	if err != nil {
		t.Fatalf("Read failed: %s", err.Error())
	}
	if len(payload) == 0 {
		t.Fatal("Read failed - payload is empty")
	}
	x, ok := payload[0].(*model.UTXO)
	if !ok {
		t.Fatal("Result is not a model.UTXO")
	}
	if !equalUtxos(*m1, *x) {
		t.Fatal("UTXO(Write) and UTXO(Read) should be equal")
	}

	err = mgr.Disconnect()
	if err != nil {
		t.Fatalf("Cannot disconnect sqlite database instance %s | %s: %s", store, store_config, err)
	}
}

func TestAddReadCond1Utxo(t *testing.T) {
	store, ok := gocore.Config().Get("coinbasetracker_store")
	if !ok {
		fmt.Println("coinbasetracker_store is not set. Using sqlite.")
		store = "sqlite"
	}

	store_config, ok := gocore.Config().Get("coinbasetracker_store_config")
	if !ok {
		fmt.Println("coinbasetracker_store_config is not set. Using sqlite in-mem.")
		store_config = "file::memory:?cache=shared"
	}

	mgr := Create(store, store_config)
	if mgr == nil {
		t.Fatalf("Cannot create sqlite database instance with %s | %s", store, store_config)
	}

	tx1bin := make([]byte, 32)
	_, _ = rand.Read(tx1bin)
	hash1, _ := chainhash.NewHash(tx1bin)

	pk, _ := bec.NewPrivateKey(bec.S256())
	pubkey := pk.PubKey()
	script, err := bscript.NewP2PKHFromPubKeyBytes(pubkey.SerialiseCompressed())
	if err != nil {
		t.Fatalf("Failed to generate script: %+v", err)
	}

	addr, err := bscript.NewAddressFromPublicKey(pubkey, false)
	if err != nil {
		t.Fatalf("Failed to generate address: %+v", err)
	}

	m1 := &model.UTXO{
		Txid:    hash1.String(),
		Vout:    mrand.Uint32(),
		Script:  script.String(),
		Amount:  uint64(mrand.Uint32()) + 1,
		Address: addr.AddressString,
		Spent:   mrand.Int31n(2) > 0,
	}

	err = mgr.Create(m1)
	if err != nil {
		t.Fatalf("Create failed: %s", err.Error())
	}

	m2 := &model.UTXO{}
	cond := []interface{}{"address = ? AND amount = ?", m1.Address, strconv.FormatInt(int64(m1.Amount), 10)}

	payload, err := mgr.Read_Cond(m2, cond)
	if err != nil {
		t.Fatalf("Read failed: %s", err.Error())
	}
	x, ok := payload.(*model.UTXO)
	if !ok {
		t.Fatal("Result is not a model.UTXO")
	}
	if !equalUtxos(*m1, *x) {
		t.Fatal("UTXO(Write) and UTXO(Read) should be equal")
	}

	err = mgr.Disconnect()
	if err != nil {
		t.Fatalf("Cannot disconnect sqlite database instance %s | %s: %s", store, store_config, err)
	}
}
