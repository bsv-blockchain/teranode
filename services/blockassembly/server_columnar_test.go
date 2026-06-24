package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/stretchr/testify/require"
)

// inpointsForParents builds wire-format TxInpoints bytes for tests with
// one or more parents and N vouts per parent (matching old patterns).
func inpointsForParents(t *testing.T, parents []chainhash.Hash, vouts [][]uint32) []byte {
	t.Helper()

	inputs := make([]*bt.Input, 0)

	for i := range parents {
		for _, v := range vouts[i] {
			in := &bt.Input{PreviousTxOutIndex: v}
			require.NoError(t, in.PreviousTxIDAdd(&parents[i]))
			inputs = append(inputs, in)
		}
	}

	ti, err := subtreepkg.NewTxInpointsFromInputs(inputs)
	require.NoError(t, err)

	b, err := ti.Serialize()
	require.NoError(t, err)

	return b
}

// TestAddTxBatchColumnar_Success verifies that the columnar batch format processes transactions correctly.
func TestAddTxBatchColumnar_Success(t *testing.T) {
	ba, _ := setupServer(t)

	// Create test transactions with varying TxInpoints sizes
	txCount := 5
	txidsPacked := make([]byte, txCount*32)
	fees := make([]uint64, txCount)
	sizes := make([]uint64, txCount)
	parentTxOffsets := make([]uint32, txCount+1)
	parentTxHashesPacked := make([]byte, 0)
	voutIdxsPacked := make([]uint32, 0)
	voutIdxsTxOffsets := make([]uint32, txCount+1)

	currentParentHashCount := uint32(0)
	parentTxOffsets[0] = 0
	voutIdxsTxOffsets[0] = 0

	// Generate test data
	for i := 0; i < txCount; i++ {
		// Create a unique TXID
		txid := chainhash.Hash{}
		txid[0] = byte(i)
		copy(txidsPacked[i*32:(i+1)*32], txid[:])

		// Set fee and size
		fees[i] = uint64(1000 * (i + 1))
		sizes[i] = uint64(250 + i*10)

		// Create TxInpoints with i+1 inputs, one vout each, count-prefixed
		// packed layout (matches TxInpoints.voutIdxs).
		numParentHashes := i + 1
		for j := 0; j < numParentHashes; j++ {
			prevTxid := chainhash.Hash{}
			prevTxid[0] = byte(j)
			parentTxHashesPacked = append(parentTxHashesPacked, prevTxid[:]...)
			currentParentHashCount++

			// One vout per parent: [count=1, vout=j]
			voutIdxsPacked = append(voutIdxsPacked, 1, uint32(j))
		}

		parentTxOffsets[i+1] = currentParentHashCount
		voutIdxsTxOffsets[i+1] = uint32(len(voutIdxsPacked))
	}

	// Create columnar request
	req := &blockassembly_api.AddTxBatchColumnarRequest{
		TxidsPacked:          txidsPacked,
		Fees:                 fees,
		Sizes:                sizes,
		ParentTxHashesPacked: parentTxHashesPacked,
		ParentTxOffsets:      parentTxOffsets,
		VoutIdxsPacked:       voutIdxsPacked,
		VoutIdxsTxOffsets:    voutIdxsTxOffsets,
	}

	// Call AddTxBatchColumnar
	resp, err := ba.AddTxBatchColumnar(context.Background(), req)

	// Verify
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.Ok)
}

// TestAddTxBatchColumnar_ValidatesTxidsLength verifies TXID length validation.
func TestAddTxBatchColumnar_ValidatesTxidsLength(t *testing.T) {
	ba, _ := setupServer(t)

	// Create request with invalid txids length (not divisible by 32)
	req := &blockassembly_api.AddTxBatchColumnarRequest{
		TxidsPacked:          make([]byte, 33), // Invalid length
		Fees:                 []uint64{1000},
		Sizes:                []uint64{250},
		ParentTxHashesPacked: []byte{},
		ParentTxOffsets:      []uint32{0, 0},
		VoutIdxsPacked:       []uint32{},
		VoutIdxsTxOffsets:    []uint32{0, 0},
	}

	_, err := ba.AddTxBatchColumnar(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "txids_packed length must be divisible by 32")
}

// TestAddTxBatchColumnar_ValidatesEmptyBatch checks empty batch handling.
func TestAddTxBatchColumnar_ValidatesEmptyBatch(t *testing.T) {
	ba, _ := setupServer(t)

	req := &blockassembly_api.AddTxBatchColumnarRequest{
		TxidsPacked:          []byte{}, // Empty
		Fees:                 []uint64{},
		Sizes:                []uint64{},
		ParentTxHashesPacked: []byte{},
		ParentTxOffsets:      []uint32{0},
		VoutIdxsPacked:       []uint32{},
		VoutIdxsTxOffsets:    []uint32{0},
	}

	_, err := ba.AddTxBatchColumnar(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no transactions in batch")
}

// TestAddTxBatchColumnar_ValidatesArrayLengths verifies array length consistency.
func TestAddTxBatchColumnar_ValidatesArrayLengths(t *testing.T) {
	ba, _ := setupServer(t)

	txid := chainhash.Hash{}
	req := &blockassembly_api.AddTxBatchColumnarRequest{
		TxidsPacked:          txid[:],              // 1 transaction
		Fees:                 []uint64{1000, 2000}, // 2 fees (mismatch!)
		Sizes:                []uint64{250},
		ParentTxHashesPacked: []byte{},
		ParentTxOffsets:      []uint32{0, 0},
		VoutIdxsPacked:       []uint32{},
		VoutIdxsTxOffsets:    []uint32{0, 0},
	}

	_, err := ba.AddTxBatchColumnar(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mismatched array lengths")
}

// TestAddTxBatchColumnar_ValidatesParentTxOffsets checks parent tx offset array validation.
func TestAddTxBatchColumnar_ValidatesParentTxOffsets(t *testing.T) {
	ba, _ := setupServer(t)

	txid := chainhash.Hash{}
	req := &blockassembly_api.AddTxBatchColumnarRequest{
		TxidsPacked:          txid[:], // 1 transaction
		Fees:                 []uint64{1000},
		Sizes:                []uint64{250},
		ParentTxHashesPacked: []byte{},
		ParentTxOffsets:      []uint32{0}, // Should be 2 elements (txCount+1), not 1
		VoutIdxsPacked:       []uint32{},
		VoutIdxsTxOffsets:    []uint32{0, 0},
	}

	_, err := ba.AddTxBatchColumnar(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "parent_tx_offsets must have exactly txCount+1 elements")
}

// TestAddTxBatchColumnar_ValidatesParentHashesLength checks parent hashes length validation.
func TestAddTxBatchColumnar_ValidatesParentHashesLength(t *testing.T) {
	ba, _ := setupServer(t)

	txid := chainhash.Hash{}
	req := &blockassembly_api.AddTxBatchColumnarRequest{
		TxidsPacked:          txid[:],
		Fees:                 []uint64{1000},
		Sizes:                []uint64{250},
		ParentTxHashesPacked: make([]byte, 33), // Not divisible by 32
		ParentTxOffsets:      []uint32{0, 1},
		VoutIdxsPacked:       []uint32{1, 0}, // [count=1, vout=0]
		VoutIdxsTxOffsets:    []uint32{0, 2},
	}

	_, err := ba.AddTxBatchColumnar(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "parent_tx_hashes_packed length must be divisible by 32")
}

// TestAddTxBatchColumnar_ValidatesVoutIdxsTxOffsets checks the per-tx vout
// offset array validation.
func TestAddTxBatchColumnar_ValidatesVoutIdxsTxOffsets(t *testing.T) {
	ba, _ := setupServer(t)

	txid := chainhash.Hash{}
	parentHash := chainhash.Hash{}
	req := &blockassembly_api.AddTxBatchColumnarRequest{
		TxidsPacked:          txid[:],
		Fees:                 []uint64{1000},
		Sizes:                []uint64{250},
		ParentTxHashesPacked: parentHash[:], // 1 parent hash
		ParentTxOffsets:      []uint32{0, 1},
		VoutIdxsPacked:       []uint32{1, 0},
		VoutIdxsTxOffsets:    []uint32{0}, // Should be 2 elements (txCount+1), not 1
	}

	_, err := ba.AddTxBatchColumnar(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "vout_idxs_tx_offsets must have exactly txCount+1 elements")
}

// TestAddTxBatchColumnar_RejectsOOBParentTxOffsets verifies the handler
// rejects a malformed ParentTxOffsets pointing past the parent hash buffer
// rather than panicking inside the per-tx loop. Regression test for the
// security audit (see PR #889): grpc-go does not recover handler panics,
// so a single bad request would otherwise crash block-assembly.
func TestAddTxBatchColumnar_RejectsOOBParentTxOffsets(t *testing.T) {
	ba, _ := setupServer(t)

	enableStoreTxInpoints(t, ba)

	txid := chainhash.Hash{}
	parentHash := chainhash.Hash{}
	req := &blockassembly_api.AddTxBatchColumnarRequest{
		TxidsPacked:          txid[:],
		Fees:                 []uint64{1000},
		Sizes:                []uint64{250},
		ParentTxHashesPacked: parentHash[:],   // 1 parent hash → totalParents=1
		ParentTxOffsets:      []uint32{0, 99}, // claims 99 parents — OOB
		VoutIdxsPacked:       []uint32{1, 0},
		VoutIdxsTxOffsets:    []uint32{0, 2},
	}

	require.NotPanics(t, func() {
		_, err := ba.AddTxBatchColumnar(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "parent_tx_offsets[txCount]")
	})
}

// TestAddTxBatchColumnar_RejectsNonMonotonicOffsets ensures non-monotonic
// offset arrays produce a clean error rather than a slice-bounds panic.
func TestAddTxBatchColumnar_RejectsNonMonotonicOffsets(t *testing.T) {
	ba, _ := setupServer(t)

	enableStoreTxInpoints(t, ba)

	// Two parents but offsets dip in the middle. Endpoints still satisfy the
	// totalParents/voutIdxsLen checks, so the monotonicity loop is what
	// must catch this.
	parents := make([]byte, 64) // 2 parent hashes → totalParents=2
	txids := make([]byte, 64)   // 2 txs
	req := &blockassembly_api.AddTxBatchColumnarRequest{
		TxidsPacked:          txids,
		Fees:                 []uint64{1000, 2000},
		Sizes:                []uint64{250, 260},
		ParentTxHashesPacked: parents,
		ParentTxOffsets:      []uint32{0, 3, 2}, // 3 → 2: monotonicity violation
		VoutIdxsPacked:       []uint32{1, 0, 1, 1},
		VoutIdxsTxOffsets:    []uint32{0, 2, 4},
	}

	require.NotPanics(t, func() {
		_, err := ba.AddTxBatchColumnar(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "parent_tx_offsets must be monotonic")
	})
}

// TestAddTxBatchColumnar_RejectsOOBVoutIdxsTxOffsets covers the vout side of
// the same exploit class.
func TestAddTxBatchColumnar_RejectsOOBVoutIdxsTxOffsets(t *testing.T) {
	ba, _ := setupServer(t)

	enableStoreTxInpoints(t, ba)

	txid := chainhash.Hash{}
	parentHash := chainhash.Hash{}
	req := &blockassembly_api.AddTxBatchColumnarRequest{
		TxidsPacked:          txid[:],
		Fees:                 []uint64{1000},
		Sizes:                []uint64{250},
		ParentTxHashesPacked: parentHash[:],
		ParentTxOffsets:      []uint32{0, 1},
		VoutIdxsPacked:       []uint32{1, 0},
		VoutIdxsTxOffsets:    []uint32{0, 999}, // OOB
	}

	require.NotPanics(t, func() {
		_, err := ba.AddTxBatchColumnar(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "vout_idxs_tx_offsets[txCount]")
	})
}

// enableStoreTxInpoints flips the StoreTxInpointsForSubtreeMeta setting so
// the per-tx loop actually exercises the unsafe.Slice + offset arithmetic
// that the regression tests target.
func enableStoreTxInpoints(t *testing.T, ba *BlockAssembly) {
	t.Helper()
	ba.settings.BlockAssembly.StoreTxInpointsForSubtreeMeta = true
}

// TestConvertToColumnarFormat_Success verifies columnar conversion.
func TestConvertToColumnarFormat_Success(t *testing.T) {
	ba, _ := setupServer(t)
	client := &Client{
		client:   nil,
		logger:   ba.logger,
		settings: ba.settings,
	}

	// Create batch items
	batch := make([]*batchItem, 3)
	for i := 0; i < 3; i++ {
		txid := chainhash.Hash{}
		txid[0] = byte(i)

		// Build i+1 parents, one vout each, into wire-format TxInpoints bytes.
		parents := make([]chainhash.Hash, i+1)
		vouts := make([][]uint32, i+1)
		for j := 0; j < i+1; j++ {
			prevTxid := chainhash.Hash{}
			prevTxid[0] = byte(j)
			parents[j] = prevTxid
			vouts[j] = []uint32{uint32(j)}
		}

		batch[i] = &batchItem{
			req: &blockassembly_api.AddTxRequest{
				Txid:       txid[:],
				Fee:        uint64(1000 * (i + 1)),
				Size:       uint64(250 + i*10),
				TxInpoints: inpointsForParents(t, parents, vouts),
			},
			done: make(chan error, 1),
		}
	}

	// Convert to columnar format
	columnar, err := client.convertToColumnarFormat(batch)

	// Verify
	require.NoError(t, err)
	require.NotNil(t, columnar)
	require.Equal(t, 3*32, len(columnar.TxidsPacked)) // 3 transactions × 32 bytes
	require.Equal(t, 3, len(columnar.Fees))
	require.Equal(t, 3, len(columnar.Sizes))
	require.Equal(t, 4, len(columnar.ParentTxOffsets))   // txCount + 1
	require.Equal(t, 4, len(columnar.VoutIdxsTxOffsets)) // txCount + 1

	// Verify TXIDs are packed correctly
	for i := 0; i < 3; i++ {
		expectedTxid := chainhash.Hash{}
		expectedTxid[0] = byte(i)
		actualTxid := columnar.TxidsPacked[i*32 : (i+1)*32]
		require.Equal(t, expectedTxid[:], actualTxid)
	}

	// Verify fees and sizes
	require.Equal(t, uint64(1000), columnar.Fees[0])
	require.Equal(t, uint64(2000), columnar.Fees[1])
	require.Equal(t, uint64(3000), columnar.Fees[2])
	require.Equal(t, uint64(250), columnar.Sizes[0])
	require.Equal(t, uint64(260), columnar.Sizes[1])
	require.Equal(t, uint64(270), columnar.Sizes[2])

	// Verify parent tx offsets are monotonically increasing
	for i := 0; i < len(columnar.ParentTxOffsets)-1; i++ {
		require.LessOrEqual(t, columnar.ParentTxOffsets[i], columnar.ParentTxOffsets[i+1])
	}

	// Verify total parent hashes count
	totalParentHashes := columnar.ParentTxOffsets[len(columnar.ParentTxOffsets)-1]
	require.Equal(t, int(totalParentHashes), len(columnar.ParentTxHashesPacked)/32)

	// Verify VoutIdxsPacked has the count-prefix layout. With i+1 parents
	// (1 vout each) for tx i, each tx contributes (count=1 + value) = 2
	// entries per parent, so total = 2 * (1+2+3) = 12 entries.
	require.Equal(t, 12, len(columnar.VoutIdxsPacked))
}

// TestConvertToColumnarFormat_InvalidTxidLength verifies TXID length validation in conversion.
func TestConvertToColumnarFormat_InvalidTxidLength(t *testing.T) {
	ba, _ := setupServer(t)
	client := &Client{
		client:   nil,
		logger:   ba.logger,
		settings: ba.settings,
	}

	batch := []*batchItem{
		{
			req: &blockassembly_api.AddTxRequest{
				Txid:       []byte{1, 2, 3}, // Invalid length (not 32)
				Fee:        1000,
				Size:       250,
				TxInpoints: []byte{},
			},
			done: make(chan error, 1),
		},
	}

	_, err := client.convertToColumnarFormat(batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid txid length")
}

// TestConvertToColumnarFormat_EmptyBatch verifies empty batch handling.
func TestConvertToColumnarFormat_EmptyBatch(t *testing.T) {
	ba, _ := setupServer(t)
	client := &Client{
		client:   nil,
		logger:   ba.logger,
		settings: ba.settings,
	}

	batch := []*batchItem{}

	_, err := client.convertToColumnarFormat(batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty batch")
}

// TestAddTxBatchColumnar_RoundTrip verifies end-to-end data integrity.
func TestAddTxBatchColumnar_RoundTrip(t *testing.T) {
	ba, _ := setupServer(t)
	client := &Client{
		client:   nil,
		logger:   ba.logger,
		settings: ba.settings,
	}

	// Create batch with complex TxInpoints — two parents, three vouts each.
	batch := make([]*batchItem, 2)
	for i := 0; i < 2; i++ {
		txid := chainhash.Hash{}
		txid[0] = byte(i + 10)

		parents := make([]chainhash.Hash, 2)
		vouts := make([][]uint32, 2)
		for j := 0; j < 2; j++ {
			prevTxid := chainhash.Hash{}
			prevTxid[0] = byte(j + 20)
			parents[j] = prevTxid
			vouts[j] = []uint32{uint32(j), uint32(j + 10), uint32(j + 20)}
		}

		batch[i] = &batchItem{
			req: &blockassembly_api.AddTxRequest{
				Txid:       txid[:],
				Fee:        uint64(5000 * (i + 1)),
				Size:       uint64(500 + i*50),
				TxInpoints: inpointsForParents(t, parents, vouts),
			},
			done: make(chan error, 1),
		}
	}

	// Convert to columnar
	columnar, err := client.convertToColumnarFormat(batch)
	require.NoError(t, err)

	// Process with server
	resp, err := ba.AddTxBatchColumnar(context.Background(), columnar)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.Ok)
}
