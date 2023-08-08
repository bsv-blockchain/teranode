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
		a.LockingScript == b.LockingScript &&
		a.Satoshis == b.Satoshis &&
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

func TestBlockModelEq(t *testing.T) {

	tn1bin := make([]byte, 32)
	_, _ = rand.Read(tn1bin)
	hash1, _ := chainhash.NewHash(tn1bin)
	tn2bin := make([]byte, 32)
	_, _ = rand.Read(tn2bin)
	hash2, _ := chainhash.NewHash(tn2bin)

	m1 := &model.Block{
		Height:        uint64(mrand.Uint32()) + 1,
		BlockHash:     hash2.String(),
		PrevBlockHash: hash1.String(),
	}

	if !m1.Equal(m1) {
		t.Fatal("Blocks mismatch")
	}

	m2 := &model.Block{}
	if m1.Equal(m2) {
		t.Fatal("Blocks are different")
	}

	if m1.Equal(nil) {
		t.Fatal("Blocks are not equal")
	}

	var m0 *model.Block = nil
	if m0.Equal(nil) {
		t.Fatal("Nil block must return false")
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
		Txid:          hash1.String(),
		Vout:          mrand.Uint32(),
		LockingScript: script.String(),
		Satoshis:      uint64(mrand.Uint32()) + 1,
		Address:       addr.AddressString,
		Spent:         mrand.Int31n(2) > 0,
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
		Txid:          hash1.String(),
		Vout:          mrand.Uint32(),
		LockingScript: script.String(),
		Satoshis:      uint64(mrand.Uint32()) + 1,
		Address:       addr.AddressString,
		Spent:         mrand.Int31n(2) > 0,
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

func TestModelUtxo(t *testing.T) {

	tx1bin := make([]byte, 32)
	_, _ = rand.Read(tx1bin)
	hash1, _ := chainhash.NewHash(tx1bin)

	tx2bin := make([]byte, 32)
	_, _ = rand.Read(tx2bin)
	hash2, _ := chainhash.NewHash(tx2bin)

	pk1, _ := bec.NewPrivateKey(bec.S256())
	pubkey1 := pk1.PubKey()
	script1, err := bscript.NewP2PKHFromPubKeyBytes(pubkey1.SerialiseCompressed())
	if err != nil {
		t.Fatalf("Failed to generate script: %+v", err)
	}

	addr1, err := bscript.NewAddressFromPublicKey(pubkey1, false)
	if err != nil {
		t.Fatalf("Failed to generate address: %+v", err)
	}

	m1 := &model.UTXO{
		Txid:          hash1.String(),
		Vout:          mrand.Uint32(),
		LockingScript: script1.String(),
		Satoshis:      uint64(mrand.Uint32()) + 1,
		Address:       addr1.AddressString,
		Spent:         mrand.Int31n(2) > 0,
	}

	pk2, _ := bec.NewPrivateKey(bec.S256())
	pubkey2 := pk2.PubKey()
	script2, err := bscript.NewP2PKHFromPubKeyBytes(pubkey2.SerialiseCompressed())
	if err != nil {
		t.Fatalf("Failed to generate script: %+v", err)
	}

	addr2, err := bscript.NewAddressFromPublicKey(pubkey2, false)
	if err != nil {
		t.Fatalf("Failed to generate address: %+v", err)
	}

	m2 := &model.UTXO{
		Txid:          hash2.String(),
		Vout:          mrand.Uint32(),
		LockingScript: script2.String(),
		Satoshis:      uint64(mrand.Uint32()) + 1,
		Address:       addr2.AddressString,
		Spent:         mrand.Int31n(2) > 0,
	}

	if m1.Equal(m2) {
		t.Fatal("Blocks are not equal")
	}

	mE := &model.UTXO{}

	if m1.Equal(mE) {
		t.Fatal("Blocks are not equal")
	}

	var m0 *model.UTXO = nil

	if m1.Equal(m0) {
		t.Fatal("Blocks are not equal")
	}

	if m0.Equal(m1) {
		t.Fatal("Blocks are not equal")
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
		Txid:          hash1.String(),
		Vout:          mrand.Uint32(),
		LockingScript: script.String(),
		Satoshis:      uint64(mrand.Uint32()) + 1,
		Address:       addr.AddressString,
		Spent:         mrand.Int31n(2) > 0,
	}

	err = mgr.Create(m1)
	if err != nil {
		t.Fatalf("Create failed: %s", err.Error())
	}

	m2 := &model.UTXO{}
	cond := []interface{}{"address = ? AND satoshis = ?", m1.Address, strconv.FormatInt(int64(m1.Satoshis), 10)}

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
		Txid:          hash1.String(),
		Vout:          mrand.Uint32(),
		LockingScript: script.String(),
		Satoshis:      uint64(mrand.Uint32()) + 1,
		Address:       addr.AddressString,
		Spent:         mrand.Int31n(2) > 0,
	}

	err = mgr.Create(m1)
	if err != nil {
		t.Fatalf("Create failed: %s", err.Error())
	}

	m2 := &model.UTXO{}
	cond := []interface{}{"address = ? AND satoshis = ?", m1.Address, strconv.FormatInt(int64(m1.Satoshis), 10)}

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
