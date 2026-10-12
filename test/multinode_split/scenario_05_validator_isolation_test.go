//go:build network_chaos

package multinodesplit

import (
	"testing"
)

// TestValidatorIsolation is intended to assert that killing teranode3-validator
// stalls node 3's block sync, because the receive path
//
//	blockvalidation.ProcessBlock
//	  -> subtreeValidationClient.CheckBlockSubtrees   (BlockValidation.go)
//	    -> validatorClient.ValidateWithOptions         (SubtreeValidation.go)
//
// would fail when validator is gone.
//
// Currently SKIPPED: the receive path has a fast-path at
// services/blockvalidation/BlockValidation.go:2023-2027 that returns nil
// before subtreevalidation (and thus validator) is ever called when the
// inbound block has no subtrees:
//
//	if len(block.Subtrees) == 0 {
//	    return nil
//	}
//
// `Generate`-mined blocks are empty (coinbase-only, txCount=1, 0 subtrees),
// confirmed empirically by inspecting node 3 logs after a 2-block Generate:
//
//	[ValidateBlock][...] not caching block - subtrees not loaded (0 slices, 0 hashes)
//	[Block ... (height: 2, id: 2, txCount: 1, size: 261)] fetching and validating subtrees DONE in 67.344µs
//	[UpdateTxMinedStatus] [...] blockID 2 for 0 subtrees
//
// So with the current harness mining only empty blocks, killing the
// standalone validator container is a no-op on the receive path and node 3
// catches up regardless.
//
// To meaningfully exercise this coupling the test needs txs inside the
// mined blocks, which requires either:
//   - injecting via the harness's StartBlaster (the coinbase blaster handles
//     UTXO sourcing but needs CoinbaseMaturity blocks of warmup), or
//   - sendrawtransaction RPC against a fresh stack with CoinbaseMaturity
//     overridden low (regtest already, but the test would need to construct
//     and sign a valid spending tx from a mature coinbase).
//
// Both add real test-infrastructure work beyond the scope of the original
// scenario file. Leaving SKIPPED rather than deleted so the intent and the
// path forward stay visible.
//
// Tracking: follow-up to add tx-injection plumbing before unskipping.
func TestValidatorIsolation(t *testing.T) {
	t.Skip("validator coupling on the receive path requires non-empty blocks; see file-level docstring for the path forward")
}
