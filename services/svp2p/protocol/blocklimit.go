package protocol

import (
	"github.com/bsv-blockchain/teranode/settings"
)

// defaultExcessiveBlockSize mirrors the `excessiveblocksize` default in
// settings/policy_settings.go (4 GiB). It is the ceiling used when the setting
// is absent or non-positive: go-wire derives its whole message sizing from
// this number, so it can never be zero or negative here.
const defaultExcessiveBlockSize uint64 = 4294967296

// BlockPayloadLimit is the largest block payload this node accepts over the
// legacy wire, taken from the excessiveblocksize policy setting.
//
// Two consumers need the same number. Server.Init hands it to wire.SetLimits,
// which bounds every block and tx message go-wire reads or writes on the
// buffered path. PeerManager hands it to transport.Config.MaxBlockPayload,
// which bounds the streaming block path (transport/conn.go readBlock). A
// literal in either place caps the node below its configured limit.
func BlockPayloadLimit(tSettings *settings.Settings) uint64 {
	if tSettings == nil || tSettings.Policy == nil || tSettings.Policy.ExcessiveBlockSize <= 0 {
		return defaultExcessiveBlockSize
	}

	return uint64(tSettings.Policy.ExcessiveBlockSize)
}
