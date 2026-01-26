package debugflags

import "sync/atomic"

// Flags captures the debug switches that can be toggled via settings.conf/env.
// Each flag targets a subsystem; All enables every subsystem.
type Flags struct {
	All       bool
	File      bool
	Blobstore bool
	UTXOStore bool
}

var current atomic.Value

func init() {
	current.Store(Flags{})
}

// Init sets the global flags snapshot. Should be called once during startup.
func Init(flags Flags) {
	current.Store(flags)
}

func load() Flags {
	flags, _ := current.Load().(Flags)
	return flags
}

// FileEnabled reports whether file-level debug logging is enabled.
func FileEnabled() bool {
	flags := load()
	return flags.All || flags.File
}

// BlobstoreEnabled reports whether blob store operations should emit debug logs.
// Blob store logs are also enabled when file-level logging is active.
func BlobstoreEnabled() bool {
	flags := load()
	return flags.All || flags.File || flags.Blobstore
}

// UTXOStoreEnabled reports whether UTXO store debug logging is enabled.
// This follows the same pattern so file-level flags still enable file operations
// invoked by UTXO store components.
func UTXOStoreEnabled() bool {
	flags := load()
	return flags.All || flags.File || flags.UTXOStore
}
