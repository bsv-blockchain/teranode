package svp2ptest

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestFixtureChain_PublishHeaderIsSafeAgainstServingReads is the -race proof for
// the hazard a scenario that announces a block mid-run creates: the test
// goroutine appends to the chain while a peer's serve goroutine is answering a
// getheaders or a getblocks from it. Before the chain carried a lock, the append
// and the reads shared a slice header with no happens-before edge between them.
//
// Run under -race, this fails on the unlocked chain and passes on the locked one.
func TestFixtureChain_PublishHeaderIsSafeAgainstServingReads(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	chain := BuildFixtureChain(t, tSettings, 3)

	peer := &ScriptedPeer{Chain: chain}

	locator := chain.Headers[0].BlockHash()

	getHeaders := wire.NewMsgGetHeaders()
	require.NoError(t, getHeaders.AddBlockLocatorHash(&locator))

	getBlocks := wire.NewMsgGetBlocks(&locator)

	var (
		wg   sync.WaitGroup
		stop = make(chan struct{})
	)

	for i := 0; i < 4; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-stop:
					return
				default:
				}

				_ = peer.HeadersFor(getHeaders)
				_ = peer.InvFor(getBlocks)
				_ = peer.Chain.Tip()
			}
		}()
	}

	// Every append is a candidate reallocation of the slice the readers walk.
	for i := 0; i < 16; i++ {
		block := chain.BuildNextBlock(t, tSettings, []*bt.Tx{})
		chain.PublishHeader(t, block)
	}

	close(stop)
	wg.Wait()

	require.Len(t, chain.Headers, 19)
}
