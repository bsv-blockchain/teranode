package tna

import (
	"context"
	"testing"

	"github.com/libsv/go-bt/v2/chainhash"
)

func TestAcceptanceNextBlock(t *testing.T) {
	ctx := context.Background()
	ba := framework.Nodes[0].BlockassemblyClient
	bc := framework.Nodes[0].BlockchainClient
	miningCandidate, err := ba.GetMiningCandidate(ctx)

	if err != nil {
		t.Errorf("error getting mining candidate: %v", err)
	}

	bestBlockheader, _, errBbh := bc.GetBestBlockHeader(ctx)
	if errBbh != nil {
		t.Errorf("error getting best block header: %v", errBbh)
	}

	prevHash, errHash := chainhash.NewHash(miningCandidate.PreviousHash)

	if errHash != nil {
		t.Errorf("error getting previous hash: %v", errHash)
	}

	if bestBlockheader.Hash().String() != prevHash.String() {
		t.Errorf("Error comparing hashes")
	}
}
