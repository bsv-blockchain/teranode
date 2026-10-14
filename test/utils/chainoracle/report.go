package chainoracle

import (
	"fmt"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
)

// Kind classifies a single disagreement between the best chain and a store.
type Kind string

const (
	// KindChainBroken: the best chain read from the blockchain store does not link
	// (a header's previous hash or height does not follow from its child).
	KindChainBroken Kind = "chain-broken"
	// KindChainDuplicateTx: the same txid appears twice on the best chain.
	KindChainDuplicateTx Kind = "chain-duplicate-tx"
	// KindChainDoubleSpend: two transactions on the best chain spend the same outpoint.
	KindChainDoubleSpend Kind = "chain-double-spend"
	// KindReplayIncomplete: a block's transactions could not be recovered, so the
	// expected coin state after it is unknown. Spend checks that depend on a
	// complete replay are skipped when any block is incomplete.
	KindReplayIncomplete Kind = "replay-incomplete"

	// KindUTXOTxMissing: a transaction on the best chain has no UTXO record while
	// at least one of its outputs should still be unspent (so it cannot have been pruned).
	KindUTXOTxMissing Kind = "utxo-tx-missing"
	// KindUTXOTxNotMined: a transaction on the best chain is recorded as unmined, or
	// its record does not name the best-chain block that mined it.
	KindUTXOTxNotMined Kind = "utxo-tx-not-mined"
	// KindUTXOOutputMissing: a transaction record exists but one of its outputs does not.
	KindUTXOOutputMissing Kind = "utxo-output-missing"
	// KindUTXOUnspentShouldBeSpent: the chain spends the output, the store has it unspent.
	KindUTXOUnspentShouldBeSpent Kind = "utxo-unspent-should-be-spent"
	// KindUTXOSpentShouldBeUnspent: the chain leaves the output unspent, but the store
	// has it spent by a transaction that is mined, or whose record does not exist.
	KindUTXOSpentShouldBeUnspent Kind = "utxo-spent-should-be-unspent"
	// KindUTXOWrongSpender: the chain and the store both have the output spent, by
	// different transactions (or different input indexes).
	KindUTXOWrongSpender Kind = "utxo-wrong-spender"

	// KindBlobMissing: a file the best chain implies is absent from the blob store.
	KindBlobMissing Kind = "blob-missing"
	// KindBlobMismatch: a file exists but its content does not match the best chain.
	KindBlobMismatch Kind = "blob-mismatch"
)

// Discrepancy is one disagreement between the best chain and a store. Fields that
// do not apply to a Kind are left at their zero value (Vout is -1 when unused).
type Discrepancy struct {
	Kind     Kind
	Height   uint32
	Block    chainhash.Hash
	Subtree  chainhash.Hash
	TxID     chainhash.Hash
	Vout     int
	FileType fileformat.FileType
	Detail   string
}

func (d Discrepancy) String() string {
	var b strings.Builder

	fmt.Fprintf(&b, "%-30s height=%d block=%s", d.Kind, d.Height, d.Block.String())

	if d.Subtree != (chainhash.Hash{}) {
		fmt.Fprintf(&b, " subtree=%s", d.Subtree.String())
	}

	if d.TxID != (chainhash.Hash{}) {
		fmt.Fprintf(&b, " tx=%s", d.TxID.String())
	}

	if d.Vout >= 0 {
		fmt.Fprintf(&b, " vout=%d", d.Vout)
	}

	if d.FileType != "" {
		fmt.Fprintf(&b, " file=%s", d.FileType)
	}

	if d.Detail != "" {
		fmt.Fprintf(&b, ": %s", d.Detail)
	}

	return b.String()
}

// Report is the oracle's structured result: what it checked and every
// discrepancy found. An empty Discrepancies slice means the stores agree with the
// best chain over [FromHeight, TipHeight].
type Report struct {
	Tip        chainhash.Hash
	TipHeight  uint32
	FromHeight uint32

	Blocks  int
	Txs     int
	Outputs int

	// PrunedTxs counts best-chain transactions with no UTXO record whose outputs
	// are all spent on the chain, which the pruner is allowed to have deleted.
	PrunedTxs int
	// MempoolSpends counts outputs the chain leaves unspent that the store has
	// spent by an unmined transaction, which is legitimate mempool state.
	MempoolSpends int
	// ExternalInputs counts inputs spending outputs created below FromHeight,
	// which the oracle has no expectation for and does not check.
	ExternalInputs int

	// BlockFilesChecked is false when no block store was supplied.
	BlockFilesChecked bool
	// UTXODeltasChecked is false when no block store was supplied or delta
	// checking was disabled.
	UTXODeltasChecked bool
	// ReplayComplete is false when at least one block's transactions could not be
	// recovered; see KindReplayIncomplete.
	ReplayComplete bool

	Discrepancies []Discrepancy
}

// OK reports whether the stores agree with the best chain.
func (r *Report) OK() bool {
	return len(r.Discrepancies) == 0
}

// Count returns the number of discrepancies of the given kind.
func (r *Report) Count(kind Kind) int {
	n := 0

	for _, d := range r.Discrepancies {
		if d.Kind == kind {
			n++
		}
	}

	return n
}

// String renders the summary followed by every discrepancy, one per line, so a
// failing test prints a diff that can be acted on without re-running.
func (r *Report) String() string {
	var b strings.Builder

	fmt.Fprintf(&b, "chain oracle: tip=%s height=%d from=%d blocks=%d txs=%d outputs=%d pruned=%d mempoolSpends=%d externalInputs=%d blockFiles=%v utxoDeltas=%v replayComplete=%v discrepancies=%d",
		r.Tip.String(), r.TipHeight, r.FromHeight, r.Blocks, r.Txs, r.Outputs, r.PrunedTxs, r.MempoolSpends, r.ExternalInputs,
		r.BlockFilesChecked, r.UTXODeltasChecked, r.ReplayComplete, len(r.Discrepancies))

	for _, d := range r.Discrepancies {
		b.WriteString("\n  ")
		b.WriteString(d.String())
	}

	return b.String()
}

func (r *Report) add(d Discrepancy) {
	r.Discrepancies = append(r.Discrepancies, d)
}
