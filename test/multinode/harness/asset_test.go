//go:build network_chaos

package harness

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMinedBlockHash(t *testing.T) {
	t.Run("metadata arrives after a later block", func(t *testing.T) {
		meta := &TxMeta{
			BlockIDs:       []uint32{42},
			BlockHashes:    []string{"block-A"},
			MainChainIndex: 0,
		}

		require.Equal(t, "block-A", MinedBlockHash(meta, []string{"block-A", "block-B"}))
	})

	t.Run("does not select an orphan", func(t *testing.T) {
		meta := &TxMeta{
			BlockIDs:       []uint32{42},
			BlockHashes:    []string{"block-A"},
			MainChainIndex: -1,
		}

		require.Empty(t, MinedBlockHash(meta, []string{"block-A", "block-B"}))
	})

	t.Run("selects the indexed main-chain entry", func(t *testing.T) {
		var meta TxMeta
		require.NoError(t, json.Unmarshal([]byte(`{"blockIDs":[41,42],"blockHashes":["orphan","block-A"],"mainChainIndex":1}`), &meta))

		require.Equal(t, "block-A", MinedBlockHash(&meta, []string{"block-A", "block-B"}))
	})

	t.Run("waits for a block hash", func(t *testing.T) {
		meta := &TxMeta{BlockIDs: []uint32{42}, MainChainIndex: 0}

		require.Empty(t, MinedBlockHash(meta, []string{"block-A"}))
	})

	t.Run("ignores a block outside this mining attempt", func(t *testing.T) {
		meta := &TxMeta{
			BlockIDs:       []uint32{42},
			BlockHashes:    []string{"block-C"},
			MainChainIndex: 0,
		}

		require.Empty(t, MinedBlockHash(meta, []string{"block-A", "block-B"}))
	})
}
