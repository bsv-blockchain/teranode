package model

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// fieldsRecordingStore answers Get with data and records the fields each Get asked for.
type fieldsRecordingStore struct {
	utxo.Store
	data  *meta.Data
	asked [][]fields.FieldName
}

func (s *fieldsRecordingStore) Get(_ context.Context, _ *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	s.asked = append(s.asked, f)

	d := *s.data

	return &d, nil
}

type observedParent struct {
	hash     chainhash.Hash
	pos      uint64
	coinbase bool
	blockIDs []uint32
}

type recordingObserver struct {
	mu   sync.Mutex
	seen []observedParent
}

func (o *recordingObserver) ObserveParent(h chainhash.Hash, pos uint64, coinbase bool, blockIDs []uint32) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.seen = append(o.seen, observedParent{h, pos, coinbase, append([]uint32(nil), blockIDs...)})
}

// Without an observer, the state at off, the parent check reads only the block IDs, as before
// lanes; with one it also reads the lane position and coinbase flag in the same Get and tells the
// observer.
func TestParentCheckReadsTheLanePositionOnlyForTheLanesShadow(t *testing.T) {
	parent := missingParentTx{parentTxHash: chainhash.Hash{7}, txHash: chainhash.Hash{8}}

	store := &fieldsRecordingStore{data: &meta.Data{BlockIDs: []uint32{5}, LanePosition: 42, IsCoinbase: true}}

	_, err := getParentTxMetaBlockIDs(context.Background(), store, parent, nil)
	require.NoError(t, err)
	require.Equal(t, [][]fields.FieldName{{fields.BlockIDs}}, store.asked)

	observer := &recordingObserver{}
	store.asked = nil

	_, err = getParentTxMetaBlockIDs(context.Background(), store, parent, observer)
	require.NoError(t, err)
	require.Equal(t, [][]fields.FieldName{{fields.BlockIDs, fields.LanePosition, fields.IsCoinbase}}, store.asked)
	require.Equal(t, []observedParent{{parent.parentTxHash, 42, true, []uint32{5}}}, observer.seen)
}

// A parent with no mined block is still told to the observer before the check reports it
// incomplete: today's answer, not mined, is compared too.
func TestParentCheckTellsTheObserverOfAnUnminedParent(t *testing.T) {
	parent := missingParentTx{parentTxHash: chainhash.Hash{7}, txHash: chainhash.Hash{8}}
	store := &fieldsRecordingStore{data: &meta.Data{LanePosition: 42}}
	observer := &recordingObserver{}

	_, err := getParentTxMetaBlockIDs(context.Background(), store, parent, observer)
	require.ErrorIs(t, err, errors.ErrBlockIncomplete)
	require.Equal(t, []observedParent{{parent.parentTxHash, 42, false, nil}}, observer.seen)
}

// checkParentExistsOnChain takes the observer from its context.
func TestCheckParentExistsOnChainTakesTheObserverFromItsContext(t *testing.T) {
	parent := missingParentTx{parentTxHash: chainhash.Hash{7}, txHash: chainhash.Hash{8}}
	store := &fieldsRecordingStore{data: &meta.Data{BlockIDs: []uint32{5}, LanePosition: 42}}
	observer := &recordingObserver{}

	ctx := ContextWithLaneParentObserver(context.Background(), observer)
	require.Equal(t, context.Background(), ContextWithLaneParentObserver(context.Background(), nil))

	b := &Block{Header: &BlockHeader{}}

	_, err := b.checkParentExistsOnChain(ctx, ulogger.TestLogger{}, store, parent, map[uint32]struct{}{5: {}})
	require.NoError(t, err)
	require.Len(t, observer.seen, 1)
}

// failingGetStore fails every Get.
type failingGetStore struct{ utxo.Store }

func (failingGetStore) Get(context.Context, *chainhash.Hash, ...fields.FieldName) (*meta.Data, error) {
	return nil, errors.NewStorageError("store down")
}

// A failed parent read is not told to the observer, and the check's error is the same with it.
func TestParentCheckDoesNotTellTheObserverOfAFailedRead(t *testing.T) {
	parent := missingParentTx{parentTxHash: chainhash.Hash{7}, txHash: chainhash.Hash{8}}
	observer := &recordingObserver{}

	_, errWith := getParentTxMetaBlockIDs(context.Background(), failingGetStore{}, parent, observer)
	_, errWithout := getParentTxMetaBlockIDs(context.Background(), failingGetStore{}, parent, nil)

	require.Error(t, errWith)
	require.Equal(t, errWithout.Error(), errWith.Error())
	require.Empty(t, observer.seen)
}

// The observer never changes the check's verdict: a parent mined in a block outside the recent
// ancestors is handed back for the deeper check, the same with or without it.
func TestParentCheckVerdictIsTheSameWithTheObserver(t *testing.T) {
	parent := missingParentTx{parentTxHash: chainhash.Hash{7}, txHash: chainhash.Hash{8}}
	store := &fieldsRecordingStore{data: &meta.Data{BlockIDs: []uint32{40}, LanePosition: 42}}
	b := &Block{Header: newTestBlockHeader(t)}
	recent := map[uint32]struct{}{90: {}, 91: {}}

	without, errWithout := b.checkParentExistsOnChainObserved(context.Background(), ulogger.TestLogger{}, store, parent, recent, nil)
	with, errWith := b.checkParentExistsOnChainObserved(context.Background(), ulogger.TestLogger{}, store, parent, recent, &recordingObserver{})

	require.NoError(t, errWithout)
	require.NoError(t, errWith)
	require.Equal(t, []uint32{40}, without)
	require.Equal(t, without, with)
}
