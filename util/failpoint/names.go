// Package failpoint provides named crash-injection points for crash-consistency
// testing.
//
// Production code marks a seam with Inject(name). In normal builds Inject is an
// empty function the compiler inlines away. In builds with the "failpoints" tag
// Inject checks whether the named point is armed and, if so, terminates the
// process immediately with os.Exit(1): no deferred functions run and nothing is
// flushed, which mimics a SIGKILL landing exactly at the seam.
//
// Points are armed by listing their names, comma separated, in the
// TERANODE_FAILPOINTS environment variable, or in-process via Enable.
package failpoint

// EnvVar is the environment variable that arms failpoints in "failpoints" builds.
const EnvVar = "TERANODE_FAILPOINTS"

// HitMarker prefixes the line written to stderr when an armed failpoint fires.
const HitMarker = "FAILPOINT HIT: "

// Seam names. Keep these in sync with the Inject call sites.
const (
	// UTXOSpendBeforeCreate fires in utxo.SequentialSpendAndCreate after the
	// parent outputs have been spent and before the child tx record is created.
	UTXOSpendBeforeCreate = "utxo/spend-before-create"

	// BlockAssemblyPersistBeforeAddBlock fires in block assembly's
	// submitMiningSolution after the block's subtrees and coinbase have been
	// persisted and before the block is added to the blockchain (and announced).
	BlockAssemblyPersistBeforeAddBlock = "blockassembly/persist-before-addblock"
)
