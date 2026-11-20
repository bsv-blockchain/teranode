package blockvalidation

import (
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// capturingInvalidBlockProducer retains every published message so a test can unmarshal the
// invalid-block payload the p2p consumer would receive.
type capturingInvalidBlockProducer struct {
	kafka.KafkaAsyncProducerI

	mu        sync.Mutex
	published [][]byte
}

func (p *capturingInvalidBlockProducer) Publish(msg *kafka.Message) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.published = append(p.published, msg.Value)
}

func (p *capturingInvalidBlockProducer) messages() [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([][]byte, len(p.published))
	copy(out, p.published)

	return out
}

// newInvalidBlockNotifyBlock builds a minimal well-formed block; kafkaNotifyBlockInvalid only
// reads its Hash().
func newInvalidBlockNotifyBlock(t *testing.T) *model.Block {
	t.Helper()

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x01, 0x00, 0x00})

	nBits, err := model.NewNBitFromString("2000ffff")
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: coinbaseTx.TxIDChainHash(),
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
	}

	block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), 100, 0) //nolint:gosec
	require.NoError(t, err)

	return block
}

// TestKafkaNotifyBlockInvalid_SanitisesLegacyPeerID pins that a legacy-namespaced peerID never
// leaves this producer (bitcoin-sv/teranode#4692). The p2p consumer takes the message peerID as its
// strongest attribution source and ban-scores it without validating the value, so a legacy TCP
// address would create a phantom registry entry and — through the consumer's hash-keyed dedupe —
// stop the real p2p announcer of the same hash from being scored at all. A genuine libp2p peerID
// must still pass through, so the sanitiser is specific to the legacy namespace.
func TestKafkaNotifyBlockInvalid_SanitisesLegacyPeerID(t *testing.T) {
	block := newInvalidBlockNotifyBlock(t)

	notify := func(t *testing.T, peerID, peerURL string) *kafkamessage.KafkaInvalidBlockTopicMessage {
		t.Helper()

		producer := &capturingInvalidBlockProducer{}
		bv := &BlockValidation{
			logger:                    ulogger.TestLogger{},
			invalidBlockKafkaProducer: producer,
		}

		bv.kafkaNotifyBlockInvalid(block, "corrupt_block_body", peerID, peerURL)

		msgs := producer.messages()
		require.Len(t, msgs, 1, "exactly one invalid-block message must be published")

		var msg kafkamessage.KafkaInvalidBlockTopicMessage
		require.NoError(t, proto.Unmarshal(msgs[0], &msg))

		return &msg
	}

	t.Run("legacy peerID and legacy sentinel URL are both cleared", func(t *testing.T) {
		msg := notify(t, LegacyPeerIDPrefix+"1.2.3.4:8333", "legacy")

		require.Empty(t, msg.GetPeerId(),
			"a legacy TCP address must never reach p2p ban scoring as a peer ID")
		require.Empty(t, msg.GetPeerUrl(),
			"the legacy routing sentinel is not a fetchable URL")
		require.Equal(t, block.Hash().String(), msg.GetBlockHash())
	})

	t.Run("real peerID and URL pass through unchanged", func(t *testing.T) {
		const (
			realPeerID = "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"
			realURL    = "http://peer:8000"
		)

		msg := notify(t, realPeerID, realURL)

		require.Equal(t, realPeerID, msg.GetPeerId(),
			"a genuine libp2p peer ID must still carry provenance to the consumer")
		require.Equal(t, realURL, msg.GetPeerUrl())
	})

	t.Run("empty provenance stays empty", func(t *testing.T) {
		msg := notify(t, "", "")

		require.Empty(t, msg.GetPeerId())
		require.Empty(t, msg.GetPeerUrl())
	})
}
