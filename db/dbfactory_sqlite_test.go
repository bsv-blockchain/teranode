package db

import (
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"testing"

	"github.com/TAAL-GmbH/ubsv/db/model"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func equalBlocks(a, b model.Block) bool {
	return a.Height == b.Height && a.BlockHash == b.BlockHash && a.PrevBlockHash == b.PrevBlockHash
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
	rand.Read(tn1bin)
	hash1, _ := chainhash.NewHash(tn1bin)
	tn2bin := make([]byte, 32)
	rand.Read(tn2bin)
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
	rand.Read(tn1bin)
	hash1, _ := chainhash.NewHash(tn1bin)
	tn2bin := make([]byte, 32)
	rand.Read(tn2bin)
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
