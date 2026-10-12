package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestProcessBlocksFinalMessage checks that a valid blocks_final message
// announces the block to peers and signals the rebroadcast queue, and that
// malformed messages do neither and are not retried.
func TestProcessBlocksFinalMessage(t *testing.T) {
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{0x01},
		HashMerkleRoot: &chainhash.Hash{0x02},
		Timestamp:      1700000000,
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
		Nonce:          42,
	}
	hash := header.Hash()

	validValue, err := proto.Marshal(&kafkamessage.KafkaBlocksFinalTopicMessage{Header: header.Bytes(), Height: 100})
	require.NoError(t, err)

	badHeaderValue, err := proto.Marshal(&kafkamessage.KafkaBlocksFinalTopicMessage{Header: []byte{0x01, 0x02}})
	require.NoError(t, err)

	tests := []struct {
		name      string
		msg       *kafka.KafkaMessage
		wantRelay bool
	}{
		{"valid", &kafka.KafkaMessage{Key: []byte(hash.String()), Value: validValue}, true},
		{"missing key", &kafka.KafkaMessage{Value: validValue}, false},
		{"bad key", &kafka.KafkaMessage{Key: []byte("not-a-hash"), Value: validValue}, false},
		{"bad value", &kafka.KafkaMessage{Key: []byte(hash.String()), Value: []byte{0xff, 0xff}}, false},
		{"bad header", &kafka.KafkaMessage{Key: []byte(hash.String()), Value: badHeaderValue}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			notifier := NewMockPeerNotifier()
			sm := &SyncManager{logger: ulogger.TestLogger{}, peerNotifier: notifier}

			require.NoError(t, sm.processBlocksFinalMessage(tc.msg))

			if !tc.wantRelay {
				require.Empty(t, notifier.relayInventoryChan)
				require.Zero(t, notifier.blockConnectedCalls.Load())

				return
			}

			require.Len(t, notifier.relayInventoryChan, 1)

			call := <-notifier.relayInventoryChan
			require.Equal(t, wire.InvTypeBlock, call.invVect.Type)
			require.Equal(t, *hash, call.invVect.Hash)

			wireHeader, ok := call.data.(*wire.BlockHeader)
			require.True(t, ok, "relay data must be the wire block header, got %T", call.data)
			require.Equal(t, header.Nonce, wireHeader.Nonce)

			require.Equal(t, int32(1), notifier.blockConnectedCalls.Load())
		})
	}
}
