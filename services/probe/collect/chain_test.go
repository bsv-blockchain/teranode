package collect

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

type fakePeers struct {
	p2p.ClientI
	peers []*p2p.PeerInfo
}

func (f *fakePeers) GetPeers(context.Context) ([]*p2p.PeerInfo, error) {
	return f.peers, nil
}

type chainRig struct {
	bc    blockchain.ClientI
	tip   *chainhash.Hash
	chain *Chain
}

func newChainRig(t *testing.T, peers []*p2p.PeerInfo) *chainRig {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	store, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	bc, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	rig := &chainRig{bc: bc, tip: tSettings.ChainCfgParams.GenesisHash}
	rig.chain = NewChain(bc, &fakePeers{peers: peers})

	return rig
}

func (r *chainRig) addBlock(t *testing.T, height uint32) {
	t.Helper()

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))

	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, byte(height & 0xff), byte((height >> 8) & 0xff), byte((height >> 16) & 0xff)})
	coinbaseTx.Inputs[0].SequenceNumber = 0xffffffff
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000))

	merkleRoot := chainhash.HashH(coinbaseTx.Bytes())
	subtreeHash := chainhash.HashH([]byte(fmt.Sprintf("subtree-%d", height)))

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  r.tip,
			HashMerkleRoot: &merkleRoot,
			Timestamp:      1231006505 + height,
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
			Nonce:          height,
		},
		CoinbaseTx:       coinbaseTx,
		Subtrees:         []*chainhash.Hash{&subtreeHash},
		TransactionCount: 1,
		SizeInBytes:      1000,
	}

	require.NoError(t, r.bc.AddBlock(context.Background(), block, ""))

	r.tip = block.Header.Hash()
}

func collectSnapshot(t *testing.T, c *Chain) snapshot.Snapshot {
	t.Helper()

	patch, err := c.Collect(context.Background())
	require.NoError(t, err)
	require.NotNil(t, patch)

	var s snapshot.Snapshot

	patch(&s)

	return s
}

func TestCollectFSMAndTip(t *testing.T) {
	rig := newChainRig(t, nil)
	rig.addBlock(t, 1)
	rig.addBlock(t, 2)

	s := collectSnapshot(t, rig.chain)

	require.Equal(t, uint32(2), s.Tip)
	require.Equal(t, "RUNNING", s.FSM)
	require.Equal(t, "chain", rig.chain.Name())
}

func TestCollectBlocksWaiting(t *testing.T) {
	rig := newChainRig(t, nil)
	rig.addBlock(t, 1)
	rig.addBlock(t, 2)
	rig.addBlock(t, 3)

	s := collectSnapshot(t, rig.chain)

	require.Subset(t, s.Chain.BlocksWaiting, []uint32{1, 2, 3})
	require.IsIncreasing(t, s.Chain.BlocksWaiting)
	require.GreaterOrEqual(t, s.Chain.PersisterBehind.Value, 3.0)
	require.Equal(t, snapshot.StatusWarn, s.Chain.PersisterBehind.Status)
}

func TestCollectBlocksWaitingCapsAtTwentyAndIsNeverNil(t *testing.T) {
	rig := newChainRig(t, nil)

	empty, err := rig.chain.Collect(context.Background())
	require.NoError(t, err)

	var es snapshot.Snapshot

	empty(&es)
	require.NotNil(t, es.Chain.BlocksWaiting)

	for h := uint32(1); h <= 25; h++ {
		rig.addBlock(t, h)
	}

	s := collectSnapshot(t, rig.chain)
	require.Len(t, s.Chain.BlocksWaiting, 20)
	require.IsIncreasing(t, s.Chain.BlocksWaiting)
}

func TestCollectOwnsOnlyItsFields(t *testing.T) {
	rig := newChainRig(t, nil)
	rig.addBlock(t, 1)

	patch, err := rig.chain.Collect(context.Background())
	require.NoError(t, err)

	s := snapshot.Snapshot{}
	s.Chain.ForkDepth = snapshot.Reading{Value: 7, Status: snapshot.StatusWarn}
	s.Chain.SubscribersDropped = snapshot.Reading{Value: 3, Status: snapshot.StatusCrit}
	s.Assembly.TipLagBlocks = snapshot.Reading{Value: 9, Status: snapshot.StatusOK}

	patch(&s)

	require.Equal(t, 7.0, s.Chain.ForkDepth.Value)
	require.Equal(t, 3.0, s.Chain.SubscribersDropped.Value)
	require.Equal(t, 9.0, s.Assembly.TipLagBlocks.Value)
}

func TestNoPeersLeavesPeerTipZero(t *testing.T) {
	rig := newChainRig(t, nil)
	rig.addBlock(t, 1)

	s := collectSnapshot(t, rig.chain)

	require.Equal(t, uint32(0), s.PeerTip)
}

func TestPeerTipIsMaxPeerHeight(t *testing.T) {
	rig := newChainRig(t, []*p2p.PeerInfo{
		{Height: 10, AdvertisedHeight: 50},
		{Height: 70},
		{Height: 20, AdvertisedHeight: 30},
	})

	s := collectSnapshot(t, rig.chain)

	require.Equal(t, uint32(70), s.PeerTip)
}

func TestEventsSignalsOnBlock(t *testing.T) {
	rig := newChainRig(t, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := rig.chain.Events(ctx)

	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("no initial signal")
	}

	rig.addBlock(t, 1)

	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("no signal after block")
	}

	cancel()

	require.Eventually(t, func() bool {
		select {
		case _, ok := <-ch:
			return !ok
		default:
			return false
		}
	}, 3*time.Second, 10*time.Millisecond)
}

func TestCollectFailureReturnsErrorAndNilPatch(t *testing.T) {
	rig := newChainRig(t, nil)
	rig.addBlock(t, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	patch, err := rig.chain.Collect(ctx)

	require.Error(t, err)
	require.Nil(t, patch)
}
