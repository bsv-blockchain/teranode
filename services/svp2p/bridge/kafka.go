package bridge

import (
	"context"
	"encoding/binary"
	"net/url"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"google.golang.org/protobuf/proto"
)

// BlockFinalHandler is called once per successfully decoded blocks-final
// message: a block our own chain has already finalized, for the caller to
// relay to peers (protocol.PeerManager.RelayBlock). It is declared here
// rather than satisfied by a protocol type so that the composition happens
// in the svp2p service package, which imports both. (Spec §4.4 governs the
// other direction — protocol never imports bridge — and says nothing
// against bridge reading protocol, which it does for the compact-block
// short-ID helpers.)
type BlockFinalHandler func(hash chainhash.Hash, header *wire.BlockHeader)

// StartBlocksFinalConsumer wires the blocks-final Kafka topic
// (settings.Kafka.BlocksFinalConfig) to onFinal, decoding each message the
// way legacy netsync's kafkaBlocksFinalListener does
// (services/legacy/netsync/manager.go:3443-3475): the hash from the message
// key, the header from kafkamessage.KafkaBlocksFinalTopicMessage.Header.
//
// A nil BlocksFinalConfig means the topic is not configured, matching legacy
// (netsync/manager.go:3362-3366, "if blocksFinalConfigURL != nil"): the
// listener is not started at all, and no block is ever relayed by this path.
// It returns (nil, nil) rather than an error — an unconfigured topic is not a
// failure the caller should abort startup over.
//
// It is a PLAIN listener, not legacy's controlled one
// (kafka.StartKafkaControlledListener, netsync/manager.go:3366): legacy's own
// control channel for this specific listener is fed `blockEnabled = true`
// unconditionally by a poller goroutine (netsync/manager.go:3308, "Block
// listeners are always enabled. The only FSM state that previously disabled
// them ... was removed") — dead machinery that never actually pauses this
// topic today. Wiring an unused control channel here to replicate that would
// be exactly the premature complexity bridge.Bridge's own doctrine warns
// against (bridge.go: "not built early as unreachable no-ops"). The one real
// pause/resume story in this area, Task 16's LegacyInvConfig round trip
// (spec §7/§11), is a different topic entirely and owns its own control
// channel; nothing in the plan asks blocks-final to share it. If a future
// task needs to pause this listener, converting a plain
// kafka.NewKafkaConsumerGroupFromURL + Start into a controlled one is a
// small, local change — not a reason to build the control path now.
//
// The returned consumer's lifecycle is the caller's: Start already begins
// consuming (kafka.KafkaConsumerGroup.Start is non-blocking, spawning its own
// goroutine), tied to ctx, and the caller must call Close on it during Stop
// — ahead of tearing down the peer registry, not only cancelling ctx — so no
// new RelayBlock call can start against a registry that is already being
// torn down. (Fix round 1, review finding I3: this is NOT a claim that
// Close proves the underlying consumer goroutine has exited; against
// util/kafka/in_memory_kafka's own fake it provably does not — see
// Server.go's wiring for the full reasoning and kafka_test.go for what the
// lifecycle tests actually establish.)
func StartBlocksFinalConsumer(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, onFinal BlockFinalHandler) (kafka.KafkaConsumerGroupI, error) {
	configURL := tSettings.Kafka.BlocksFinalConfig
	if configURL == nil {
		logger.Infof("[svp2p] block announcement relay disabled: kafka_blocksFinalConfig is not set")
		return nil, nil
	}

	groupID := "blocksfinal.legacy." + tSettings.ClientName

	consumer, err := kafka.NewKafkaConsumerGroupFromURL(logger, configURL, groupID, true, &tSettings.Kafka)
	if err != nil {
		return nil, err
	}

	consumer.Start(ctx, func(msg *kafka.KafkaMessage) error {
		return handleBlocksFinalMessage(logger, msg, onFinal)
	})

	return consumer, nil
}

// handleBlocksFinalMessage decodes one blocks-final message and calls
// onFinal. Every parse failure logs and returns nil rather than an error:
// legacy's own discipline here (netsync/manager.go:3443-3475, "not going to
// retry, if we cannot parse the message") — a message this node cannot parse
// will never become parseable, so returning an error and letting the
// consumer retry it would only spin.
func handleBlocksFinalMessage(logger ulogger.Logger, msg *kafka.KafkaMessage, onFinal BlockFinalHandler) error {
	if msg.Key == nil {
		logger.Errorf("[svp2p] blocks-final message has no Kafka key, skipping")
		return nil
	}

	hash, err := chainhash.NewHashFromStr(string(msg.Key))
	if err != nil {
		logger.Errorf("[svp2p] blocks-final message key %q is not a block hash, skipping: %v", string(msg.Key), err)
		return nil
	}

	var blockMsg kafkamessage.KafkaBlocksFinalTopicMessage
	if err := proto.Unmarshal(msg.Value, &blockMsg); err != nil {
		logger.Errorf("[svp2p][%s] failed to unmarshal blocks-final message, skipping: %v", hash, err)
		return nil
	}

	header, err := model.NewBlockHeaderFromBytes(blockMsg.Header)
	if err != nil {
		logger.Errorf("[svp2p][%s] failed to decode block header from blocks-final message, skipping: %v", hash, err)
		return nil
	}

	if *header.Hash() != *hash {
		// The producer always keys the message with block.Header.Hash()
		// (services/blockchain/Server.go sendKafkaBlockFinalNotification), so
		// a mismatch means the message is corrupt in a way proto.Unmarshal did
		// not catch. Trusting the decoded header instead of the key would
		// relay the wrong hash to every peer; skip it instead, for the same
		// never-parseable reason as the errors above. Compared by value
		// (chainhash.Hash is a fixed byte array) rather than by formatting
		// both sides to hex first — fix round 1, review Minor 10.
		logger.Errorf("[svp2p][%s] blocks-final message key does not match its decoded header hash %s, skipping", hash, header.Hash())
		return nil
	}

	// legacy's own log line at the equivalent point (netsync/manager.go:3475,
	// "received block final message from Kafka: %s, %s"), kept so an operator
	// can still see "we announced block X" — fix round 1, review Minor 7.
	logger.Infof("[svp2p] relaying blocks-final message from Kafka: %s", hash)

	onFinal(*hash, header.ToWireBlockHeader())

	return nil
}

// decodeTxMetaBatch's own truncation errors, one per failure site in legacy's
// processTXmetaBatchMessage: too short to even hold the leading entry count,
// too short for the next entry's fixed-size header, or too short for the
// content length that entry's header declared.
var (
	errShortTxMetaBatch       = errors.NewProcessingError("svp2p: txmeta batch message shorter than the entry count header")
	errTruncatedTxMetaEntry   = errors.NewProcessingError("svp2p: txmeta batch message truncated mid-entry")
	errTruncatedTxMetaContent = errors.NewProcessingError("svp2p: txmeta batch message truncated mid-content")
)

// TxMetaHandler is called once per non-coinbase, not-yet-relayed transaction
// decoded off the txmeta topic's ADD entries — hash, fee (satoshis), and
// serialized size (bytes), exactly the three fields legacy's own
// TxHashAndFee carries (netsync/manager.go:361-365) — for the caller to
// batch and relay to peers. Declared with primitive parameters rather than
// a protocol type for the same reason as BlockFinalHandler: the composition
// belongs in the service package that imports both.
type TxMetaHandler func(hash chainhash.Hash, fee uint64, size uint64)

// txRunningPollInterval mirrors legacy netsync's own control-goroutine tick
// (netsync/manager.go:3301, startKafkaListeners: `case <-time.After(1 *
// time.Second)`): how often the tx listener's RUNNING gate is re-evaluated
// against the FSM.
const txRunningPollInterval = 1 * time.Second

// StartTxMetaConsumer wires the txmeta Kafka topic
// (settings.Kafka.TxMetaConfig) to onTx, decoding each message the way
// legacy netsync's kafkaTXmetaListener / processTXmetaBatchMessage does
// (netsync/manager.go:3479-3629): a binary batch of ADD/DELETE entries in
// either the v1 or the v2 (partition-aware) wire format, skipping coinbase
// and already-in-a-block entries before ever calling onTx.
//
// Unlike StartBlocksFinalConsumer this IS a controlled listener
// (kafka.StartKafkaControlledListener), because — unlike blocks-related
// listeners, whose control channel legacy feeds `true` unconditionally,
// dead machinery today (see StartBlocksFinalConsumer's doc comment) — the
// tx control channel is genuinely load-bearing: legacy's own poller
// (netsync/manager.go:3316-3321) gates it on
// `IsFSMCurrentState(FSMStateRUNNING)`, the "relay txs only in RUNNING"
// rule (spec §7). That poll is carried here as its own goroutine, tied to
// ctx, polling every txRunningPollInterval and sending into a buffered(1)
// control channel with a NON-BLOCKING send — matching legacy's own
// AGGREGATOR channel shape (manager.go:3295, buffered(1), "Non-blocking
// send to avoid deadlock if no one is reading"), NOT its per-listener
// channel: legacy's own per-listener control channel is unbuffered with a
// BLOCKING forward (manager.go:3364 `make(chan bool)`, :3413-3421), so
// legacy never drops a listener-directed control value — review round 1,
// Minor 1. Because this port collapses the two-stage fan-out into one hop
// (see the DEVIATION note below), the non-blocking send now sits directly
// on the listener-facing channel, so a control update CAN be dropped here
// where legacy's never is. The effect is bounded and self-correcting: at
// most one txRunningPollInterval tick of staleness, since the poller
// re-sends every tick regardless of whether the previous one was read.
//
// DEVIATION from legacy's own wiring, disclosed rather than silent (E1):
// legacy's poller writes into a package-level aggregator channel
// (txControlChan) that a SEPARATE forwarder goroutine then fans out, by a
// blocking send, to every tx-related listener's own control channel
// (manager.go:3299-3300, :3413-3421) — because legacy can have more than
// one tx listener sharing that gate. This port has exactly one
// (txmeta), so the intermediate fan-out stage is pure indirection with
// nothing to fan out to; the poller here sends directly into the one
// control channel kafka.StartKafkaControlledListener reads. The poll
// cadence, the FSM check, the buffered(1)-with-non-blocking-send shape, and
// the gate's effect on the listener are otherwise unchanged.
//
// replay=0 (E2): the URL's query is rewritten before the listener starts,
// mirroring legacy exactly (netsync/manager.go:3374-3380, "disable replay
// for txmeta in the legacy service, we do not have to replay anything,
// ever"). Deliberately NOT done for blocks-final (see Task 12's review):
// the two consumers differ on purpose. blockchainClient's default
// (util.GetQueryParamInt(url, "replay", 1)) is "replay everything" if this
// rewrite is ever skipped, so leaving it off here would be a silent
// behaviour change, not a neutral one.
//
// A nil TxMetaConfig means the topic is not configured, matching legacy's
// own `if txmetaKafkaURL != nil` gate (netsync/manager.go:3363): the tx
// announcement relay is simply off, and nothing is relayed by this path.
//
// The consumer's lifecycle is entirely ctx-owned, like legacy's own
// kafka.StartKafkaControlledListener call (manager.go:3382): both the
// poller and the controlled listener return on ctx.Done(), with no
// separate Close() to call from Stop — StartKafkaControlledListener itself
// has no return value to close (it manages its own internal
// consumer/cancel pair). Server.Stop relies on the Start ctx already having
// been cancelled by the daemon before Stop runs, exactly as it already
// documents for the header index subscription goroutine.
func StartTxMetaConsumer(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, blockchainClient blockchain.ClientI, index *RecentTxIndex, onTx TxMetaHandler) {
	configURL := tSettings.Kafka.TxMetaConfig
	if configURL == nil {
		logger.Infof("[svp2p] tx announcement relay disabled: kafka_txmetaConfig is not set")
		return
	}

	values := configURL.Query()
	values.Set("replay", "0")
	configURL.RawQuery = values.Encode()

	groupID := "txmeta.legacy." + tSettings.ClientName

	// Buffered(1), matching legacy's own aggregator channel shape
	// (manager.go:3295, "buffered to prevent blocking"); see the DEVIATION
	// note above for why the poller writes directly into this one instead
	// of through legacy's fan-out stage.
	controlCh := make(chan bool, 1)

	go pollTxRunningGate(ctx, blockchainClient, controlCh)

	go kafka.StartKafkaControlledListener(ctx, logger, groupID, controlCh, configURL,
		func(lctx context.Context, kafkaURL *url.URL, lGroupID string) {
			kafka.StartKafkaListener(lctx, logger, kafkaURL, lGroupID, true, func(msg *kafka.KafkaMessage) error {
				return handleTxMetaMessage(logger, msg, index, onTx)
			}, &tSettings.Kafka)
		})
}

// pollTxRunningGate is legacy's control-channel poller
// (netsync/manager.go:3294-3329), reduced to the tx half: every
// txRunningPollInterval, ask the FSM whether it is RUNNING, and push the
// answer into controlCh without blocking if nobody is currently reading it.
// Returns when ctx is done, the same shutdown signal the controlled
// listener itself watches.
//
// Uses ctx (this function's own parameter, StartTxMetaConsumer's caller's
// ctx), not legacy's sm.ctx — E1 flagged this as a decision to make
// deliberately rather than copy: legacy's poller reads sm.blockchainClient's
// FSM state on sm.ctx while startKafkaListeners itself was handed a
// DIFFERENT ctx parameter, an inconsistency this port has no reason to
// carry forward, since pollTxRunningGate has only the one ctx its caller
// gave it and no separate longer-lived one to reach for (review round 1,
// Minor 2).
func pollTxRunningGate(ctx context.Context, blockchainClient blockchain.ClientI, controlCh chan bool) {
	ticker := time.NewTicker(txRunningPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			running, err := blockchainClient.IsFSMCurrentState(ctx, blockchain.FSMStateRUNNING)
			if err != nil {
				// canRelayTx's own fail-closed rule (services/legacy/peer_server.go
				// canRelayTx, "Fails closed: any error reading the state
				// suppresses relay"): an error reading FSM state is treated as
				// "not running", never as "keep the previous answer".
				running = false
			}

			select {
			case controlCh <- running:
			default:
			}
		}
	}
}

// handleTxMetaMessage decodes one txmeta batch message and calls onTx for
// every entry that should be relayed. Every such entry also enters the
// recent-transaction index (recenttx.go), which is what compact-block
// reconstruction matches short IDs against: the entries that pass the
// skips below are this node's nearest equivalent of mempool membership,
// which is what SVNode's own reconstruction walks
// (blockencodings.cpp:171-199). index is nil when compact blocks are off,
// and Add is a no-op on a nil index. Every parse failure at the top level
// (truncated buffer) logs and returns nil rather than an error, the same
// never-going-to-become-parseable discipline as handleBlocksFinalMessage
// and legacy's own processTXmetaBatchMessage (netsync/manager.go:3519-3520,
// "truncated message", ":3562, "truncated content").
func handleTxMetaMessage(logger ulogger.Logger, msg *kafka.KafkaMessage, index *RecentTxIndex, onTx TxMetaHandler) error {
	entries, err := decodeTxMetaBatch(msg.Value)
	if err != nil {
		logger.Errorf("[svp2p] failed to decode txmeta batch message, skipping: %v", err)
		return nil
	}

	for _, e := range entries {
		if e.action != txmetacache.WireActionADD {
			continue
		}

		var txMeta meta.Data
		if err := meta.NewMetaDataFromBytes(e.content, &txMeta); err != nil {
			logger.Errorf("[svp2p][%s] failed to decode tx meta data from txmeta message, skipping entry: %v", e.hash, err)
			continue
		}

		// Coinbase transactions are never announced: relaying one to a peer
		// as a fresh mempool tx earns an instant ban (net_processing.cpp's
		// own coinbase-is-not-a-standalone-tx rule), mirrored from legacy's
		// own skip (netsync/manager.go:3592-3594, "Never announce coinbase").
		if txMeta.IsCoinbase {
			continue
		}

		// Never announce transactions that arrived as part of a block: the
		// txmeta topic also carries those (block validation, subtree
		// validation, legacy sync pre-warm) to populate the subtree-
		// validation cache, and relaying them as fresh mempool txs floods
		// peers with getdata for transactions that are long mined — and
		// often already pruned. Mirrors legacy's own InBlock skip
		// (netsync/manager.go:3596-3601) and PR 1073's "never announce
		// block-originated txs".
		if txMeta.InBlock {
			continue
		}

		index.Add(e.hash)

		onTx(e.hash, txMeta.Fee, txMeta.SizeInBytes)
	}

	return nil
}

// InvHandler is called once per decoded legacy-inv Kafka message that
// carries at least one tx entry AND passed the RUNNING gate: the peer
// address the original inv came from, and every tx hash it announced.
// Declared with primitive parameters rather than a protocol type, for the
// same spec §4.4 reason as BlockFinalHandler/TxMetaHandler above.
type InvHandler func(peerAddr string, hashes []chainhash.Hash)

// StartLegacyInvConsumer wires the legacy-inv Kafka topic
// (settings.Kafka.LegacyInvConfig) to onInv — the CONSUME half of Task 16's
// tx-inv round trip; StartLegacyInvProducer below is the PRODUCE half.
// legacy's own kafkaINVListener (netsync/manager.go:3417-3441) decodes the
// identical KafkaInvTopicMessage and calls handleInvMsg directly;
// handleLegacyInvMessage below is this port's counterpart.
//
// This is a PLAIN listener (kafka.NewKafkaConsumerGroupFromURL, like
// StartBlocksFinalConsumer), NOT a controlled one — read this paragraph
// before changing that, because an earlier version of this function got it
// wrong. legacy's own INV listener IS built through
// kafka.StartKafkaControlledListener (manager.go:3350-3352), but its
// control channel is `blockListenersCh` (manager.go:3355-3358), fed
// `blockEnabled := true` UNCONDITIONALLY by the poller — the identical
// "always true" gate StartBlocksFinalConsumer's own doc comment already
// describes as dead machinery. legacy's tx RUNNING gate
// (`pollTxRunningGate`'s equivalent poll, manager.go:3316-3321) feeds a
// DIFFERENT channel, `txListenersCh`, which only the TXMETA listener is
// registered on. So legacy's INV listener never pauses; the RUNNING check
// for tx invs is applied per-message, inside handleInvMsg/processInvMsg
// (manager.go:2270-2280, :2365-2370), by handleLegacyInvMessage below.
// Gating the LISTENER instead (an earlier version of this port did, citing
// "pause without dropping announcements") behaves differently on resume: a
// paused listener replays a backlog of now-stale invs for peers that may
// already be gone, where legacy's always-on-listener-plus-per-message-drop
// resumes with a clean slate — the better behavior as well as the faithful
// one, since a stale inv is worthless and the departed-peer drop
// (PeerManager.InvFromKafka) already does that job one layer down anyway.
// "The ingestion-pause mechanism, carried unchanged" (spec §7) describes
// WHY this round trip exists — so tx ingestion can pause without
// announcements being lost — not a directive to gate this consumer.
//
// Headers-first suppression is a SEPARATE gate, checked separately, inside
// BlockDownloader.RequestTxs on the other side of onInv (protocol
// package's own state) — see that method's doc comment for the ordering
// relative to peer.AddKnownInventory that legacy's source makes load-
// bearing, which handleLegacyInvMessage's RUNNING check below is the other
// half of (RUNNING is tested BEFORE AddKnownInventory in legacy;
// headers-first is tested AFTER it).
//
// A nil LegacyInvConfig means the topic is not configured, matching
// legacy's own `if legacyInvConfigURL != nil` gate (netsync/manager.go:
// 3330): no consumer is started, and no getdata is ever built by this
// path. Returns (nil, nil) in that case, like StartBlocksFinalConsumer.
//
// The returned consumer's lifecycle is the caller's, exactly like
// StartBlocksFinalConsumer's own doc comment describes: Start already
// begins consuming, tied to ctx, and the caller must Close it during Stop
// ahead of tearing down the peer registry.
func StartLegacyInvConsumer(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, blockchainClient blockchain.ClientI, onInv InvHandler) (kafka.KafkaConsumerGroupI, error) {
	configURL := tSettings.Kafka.LegacyInvConfig
	if configURL == nil {
		logger.Infof("[svp2p] legacy inv round trip disabled: kafka_legacyInvConfig is not set")
		return nil, nil
	}

	groupID := "inv.legacy." + tSettings.ClientName

	consumer, err := kafka.NewKafkaConsumerGroupFromURL(logger, configURL, groupID, true, &tSettings.Kafka)
	if err != nil {
		return nil, err
	}

	consumer.Start(ctx, func(msg *kafka.KafkaMessage) error {
		return handleLegacyInvMessage(ctx, logger, blockchainClient, msg, onInv)
	})

	return consumer, nil
}

// handleLegacyInvMessage decodes one legacy-inv message, applies the
// RUNNING gate, and calls onInv with every InvType_Tx entry's hash that
// survived — mirroring legacy's own newInvFromKafkaMessage
// (netsync/inv_msg.go:104-137) for the decode, and handleInvMsg's own
// processInvs computation (manager.go:2270-2280, done once per inv
// message, reused for every entry in it) for the gate. The peer lookup
// legacy does at this same site is NOT done here: it happens on the
// protocol-package side of onInv (PeerManager.InvFromKafka), since this
// package has no peer registry to look one up in (spec §4.4).
//
// Order matters and mirrors legacy's own (manager.go processInvMsg,
// :2365-2376): decode and filter first (a message that carries nothing
// answerable is never worth an FSM call), THEN the RUNNING gate — checked
// BEFORE onInv is called at all, so a not-RUNNING message never reaches
// peer.AddKnownInventory's equivalent (RequestTxs' own knownTxs marking,
// protocol package) either, exactly as legacy's own
// "if !processInvs { return }" returns before AddKnownInventory ever runs
// for a tx type. Fails closed on an FSM read error, matching legacy's own
// `processInvs` default (initialized false, only set true on a successful
// RUNNING read) and canRelayTx's documented "fails closed" discipline
// elsewhere in this port.
//
// Non-tx entries (block invs never travel this topic in this port — block
// invs keep their in-process path, task scope fence) are filtered out
// rather than erroring, the same defensive discipline as every other
// decode in this file. A hash that fails to parse is logged and skipped;
// the rest of the message still reaches onInv, matching
// handleTxMetaMessage's own per-entry (not whole-message) failure
// handling. onInv is not called at all when no tx entry survives, or when
// the RUNNING gate is closed.
func handleLegacyInvMessage(ctx context.Context, logger ulogger.Logger, blockchainClient blockchain.ClientI, msg *kafka.KafkaMessage, onInv InvHandler) error {
	var invMsg kafkamessage.KafkaInvTopicMessage
	if err := proto.Unmarshal(msg.Value, &invMsg); err != nil {
		logger.Errorf("[svp2p] failed to unmarshal legacy inv message, skipping: %v", err)
		return nil
	}

	var hashes []chainhash.Hash

	for _, inv := range invMsg.Inv {
		if inv.Type != kafkamessage.InvType_Tx {
			continue
		}

		hash, err := chainhash.NewHashFromStr(inv.Hash)
		if err != nil {
			logger.Errorf("[svp2p] legacy inv message from peer %s carries an unparseable tx hash %q, skipping entry: %v", invMsg.PeerAddress, inv.Hash, err)
			continue
		}

		hashes = append(hashes, *hash)
	}

	if len(hashes) == 0 {
		return nil
	}

	running := false

	if blockchainClient != nil {
		r, err := blockchainClient.IsFSMCurrentState(ctx, blockchain.FSMStateRUNNING)
		if err == nil {
			running = r
		}
	}

	if !running {
		logger.Debugf("[svp2p] ignoring legacy inv message from peer %s: not in running state", invMsg.PeerAddress)
		return nil
	}

	onInv(invMsg.PeerAddress, hashes)

	return nil
}

// legacyInvProducerChannelSize matches legacy's own legacyKafkaInvCh buffer
// (netsync/manager.go:3337, "make(chan *kafka.Message, 10_000)").
const legacyInvProducerChannelSize = 10_000

// LegacyInvProducer is the PRODUCE half of Task 16's tx-inv round trip:
// legacy QueueInv's Kafka write (netsync/manager.go:2958-3011), relocated
// behind a Produce method protocol.TxInvProducer is satisfied by directly
// (services/svp2p/Server.go composes it into protocol.SyncConfig with no
// adapter needed, the same way BlocksFinalConsumer's onFinal is
// s.manager.RelayBlock).
type LegacyInvProducer struct {
	logger   ulogger.Logger
	ch       chan *kafka.Message
	producer kafka.KafkaAsyncProducerI
}

// StartLegacyInvProducer builds the legacy-inv Kafka producer, matching
// legacy's own construction (netsync/manager.go:3335-3350): an async
// producer over a buffered channel, started on its own goroutine tied to
// ctx. A nil LegacyInvConfig returns (nil, nil), matching every other
// unconfigured-topic case in this file — no producer is built, and
// LegacyInvProducer.Produce/Stop are both nil-receiver safe so a caller
// that always calls them unconditionally (Server.go) needs no extra nil
// check of its own.
func StartLegacyInvProducer(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (*LegacyInvProducer, error) {
	configURL := tSettings.Kafka.LegacyInvConfig
	if configURL == nil {
		logger.Infof("[svp2p] legacy inv round trip disabled: kafka_legacyInvConfig is not set")
		return nil, nil
	}

	ch := make(chan *kafka.Message, legacyInvProducerChannelSize)

	producer, err := kafka.NewKafkaAsyncProducerFromURL(ctx, logger, configURL, &tSettings.Kafka)
	if err != nil {
		return nil, err
	}

	go producer.Start(ctx, ch)

	return &LegacyInvProducer{logger: logger, ch: ch, producer: producer}, nil
}

// Produce marshals hashes as a KafkaInvTopicMessage tagged with peerAddr and
// sends it to the legacy-inv topic, mirroring legacy's own QueueInv
// (netsync/manager.go:2989-3004): a plain, blocking channel send — no
// select/default drop — because the channel's own backpressure IS the
// pause mechanism (this method's own doc note on StartLegacyInvConsumer):
// a stalled Kafka producer slows the peer's own read loop rather than
// silently losing an announcement. Safe to call on a nil *LegacyInvProducer
// (an unconfigured topic): a no-op, matching StartLegacyInvConsumer's own
// unconfigured-topic silence.
//
// Called by PeerManager.Inv only AFTER releasing syncMu (protocol.
// TxInvProducer's own doc comment) — never while any lock in this package's
// caller is held, satisfying spec §4.3's no-blocking-call-under-a-lock rule
// for the blocking send this method performs.
//
// Recovers "send on closed channel" (fix round 2, Important 1): Stop's own
// doc comment used to claim this could never race a live Produce call,
// reasoning Server.Stop always ran Stop() after PeerManager.Stop() had
// joined every peer goroutine. Fixing Important 1 — the DC11 flush being
// skipped on an early-return shutdown path — moved Stop() into a defer
// that CAN now fire before manager.Stop() has joined every peer, reopening
// exactly the race legacy's own sendDuringShutdown protects against at
// this identical seam (netsync/manager.go:2937-2949, "Inv delivery runs on
// peer read-loop goroutines... but the channels they target are torn down
// by a different goroutine during shutdown"). Recovering here, rather than
// re-establishing the ordering guarantee, is the same trade Server.Stop's
// own defer already makes for admission/stoppableBridge: releasing early
// on an abnormal, something-already-failed path beats leaking or crashing.
func (p *LegacyInvProducer) Produce(peerAddr string, hashes []chainhash.Hash) {
	if p == nil || len(hashes) == 0 {
		return
	}

	msg := &kafkamessage.KafkaInvTopicMessage{PeerAddress: peerAddr}
	for _, h := range hashes {
		msg.Inv = append(msg.Inv, &kafkamessage.Inv{Type: kafkamessage.InvType_Tx, Hash: h.String()})
	}

	value, err := proto.Marshal(msg)
	if err != nil {
		p.logger.Errorf("[svp2p] failed to marshal legacy inv message for peer %s, skipping: %v", peerAddr, err)
		return
	}

	sendToLegacyInvChannel(p.ch, &kafka.Message{Value: value})
}

// sendToLegacyInvChannel is legacy's own sendDuringShutdown
// (netsync/manager.go:2937-2949), generalized to nothing legacy-specific:
// a plain blocking send that recovers "send on closed channel" rather than
// crashing the process, because a flag check and a channel send are not
// atomic against a concurrent close. Dropping one produce during shutdown
// is safe — an inv is advisory, and this port's whole reason for this
// round trip's existence is that a dropped announcement gets re-sent by
// the peer (or a later connection) on its own.
func sendToLegacyInvChannel(ch chan *kafka.Message, msg *kafka.Message) {
	defer func() {
		_ = recover()
	}()

	ch <- msg
}

// Stop flushes the producer synchronously — the DC11 note (netsync/
// manager.go:3080-3091, "stop the legacy INV async producer so its final
// flush runs during shutdown. Safe here ... Stop() has no caller ctx to
// honour ..., so it is raced against an internal timeout: a wedged broker
// flush can't block shutdown"). Carried here identically, via the same
// kafka.StopProducerCtx helper and util.DefaultBatcherDrainTimeout bound
// legacy's own DC11 call uses. Safe on a nil *LegacyInvProducer.
//
// Server.Stop calls this from a defer (fix round 2, Important 1: an
// earlier version called it inline at the very end, which an early return
// from any of blocksFinalConsumer.Close/legacyInvConsumer.Close/
// manager.Stop skipped entirely — silently dropping the flush on exactly
// the failure path where dropping announcements matters most). On the
// ordinary success path the defer still runs after PeerManager.Stop() has
// already joined every peer goroutine, preserving the original ordering
// exactly. On an abnormal path it can now fire BEFORE that join completes,
// which reopens a real send-after-close race against Produce — see
// Produce's own doc comment for why that is now recovered rather than
// re-establishing the ordering guarantee (the same early-release-over-
// leaking trade Server.Stop's defer already makes for admission/
// stoppableBridge).
func (p *LegacyInvProducer) Stop() error {
	if p == nil {
		return nil
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), util.DefaultBatcherDrainTimeout)
	defer cancel()

	kafka.StopProducerCtx(stopCtx, p.logger, "legacy INV", p.producer)

	return nil
}

// txMetaEntry is one decoded entry off the txmeta wire batch, before its
// content (if any) has been deserialized into a meta.Data. Kept as its own
// type so decodeTxMetaBatch — the pure, directly-unit-testable half of this
// file's decode logic — has no knowledge of meta.Data or the ADD/DELETE
// dispatch; handleTxMetaMessage does that.
type txMetaEntry struct {
	hash    chainhash.Hash
	action  byte
	content []byte
}

// decodeTxMetaBatch parses a txmeta binary batch message in either the v1
// or v2 wire format, mirroring legacy's processTXmetaBatchMessage
// (netsync/manager.go:3479-3629) byte for byte. Two wire formats are
// accepted, distinguished by a multi-byte signature at the start of the
// message (mirrors services/subtreevalidation/txmetaHandler.go, cited by
// legacy's own doc comment at the same site):
//
//	v1 (legacy)
//	  [4 bytes] entry count (uint32 LE)
//	  per entry: [32 hash][1 action][4 contentLen][N content]
//
//	v2 (partition-aware)
//	  [1 byte magic=0xFF][1 byte version=0x02][2 reserved=0][4 entry count LE]
//	  per entry: [8 xxhash][32 hash][1 action][4 contentLen][N content]
//
// v2 detection requires the full 4-byte header signature AND a plausible
// entry count for the buffer length, otherwise the message is parsed as
// v1 — this avoids misclassifying a v1 message whose entry count happens to
// begin with byte 0xFF (counts 255, 511, 767, ...). The xxhash prefix in v2
// is read and discarded, exactly as legacy does — this port only needs the
// 32-byte tx hash to announce.
//
// A truncated buffer (not enough bytes left for the next entry's header, or
// for its declared content) returns an error rather than a partial result,
// and the caller logs and drops the WHOLE message — matching legacy's own
// "not going to retry" discipline (netsync/manager.go:3562, ":3585, both
// `return nil` rather than an error the consumer would retry), but NOT its
// partial-announce behaviour: legacy announces entries as it walks them
// (`sm.announceTx` inside the loop), so a truncation at entry n still keeps
// whatever it already announced for entries before n
// (netsync/manager.go:3565, :3588). This port collects every entry before
// returning any of them, so a truncated message here yields NONE of its
// entries, even well-formed leading ones — strictly the safer direction
// (fewer announcements, never more), but a real divergence from legacy's
// own behaviour at this site, not a match to it (review round 1, Minor 3).
//
// A second, smaller divergence at the same site: legacy does not
// bounds-check a DELETE entry's contentLen before advancing past it
// (netsync/manager.go:3618, a bare `offset += int(contentLen)`), so a final
// DELETE entry with an over-long contentLen succeeds in legacy and errors
// here. Also safer, also unmentioned there until now.
func decodeTxMetaBatch(data []byte) ([]txMetaEntry, error) {
	if len(data) < 4 {
		return nil, errShortTxMetaBatch
	}

	var (
		offset     int
		entryCount uint32
		isV2       bool
	)

	if len(data) >= txmetacache.WireV2HeaderLen &&
		data[0] == txmetacache.WireV2Magic &&
		data[1] == txmetacache.WireV2Version &&
		data[2] == 0 && data[3] == 0 {
		candidateCount := binary.LittleEndian.Uint32(data[4:])
		remaining := uint64(len(data) - txmetacache.WireV2HeaderLen)

		if uint64(candidateCount)*uint64(txmetacache.WireV2MinEntrySize) <= remaining {
			entryCount = candidateCount
			offset = txmetacache.WireV2HeaderLen
			isV2 = true
		}
	}

	if !isV2 {
		entryCount = binary.LittleEndian.Uint32(data[:4])
		offset = 4
	}

	entryHeaderSize := txmetacache.WireV1MinEntrySize
	if isV2 {
		entryHeaderSize = txmetacache.WireV2MinEntrySize
	}

	// Capacity hint bounded by what the buffer could actually hold, not by
	// entryCount directly: legacy's own loop (netsync/manager.go:3562-3568)
	// never allocates a slice sized by entryCount up front, so a short
	// message with an enormous claimed count just fails the truncation
	// check on the FIRST iteration below. Sizing this slice's capacity by
	// entryCount alone, unbounded, would let a single small message with
	// entryCount near uint32's max try to reserve gigabytes before that
	// check ever runs — an allocation DoS the v2 path's own
	// candidateCount*WireV2MinEntrySize<=remaining check already guards
	// against, but nothing bounded the v1 path here until this line.
	capacityHint := entryCount
	if maxPossible := uint32(len(data) / entryHeaderSize); maxPossible < capacityHint { //nolint:gosec // len(data) is bounded by the Kafka message size
		capacityHint = maxPossible
	}

	entries := make([]txMetaEntry, 0, capacityHint)

	for i := uint32(0); i < entryCount; i++ {
		if offset+entryHeaderSize > len(data) {
			return nil, errTruncatedTxMetaEntry
		}

		if isV2 {
			// The xxhash prefix; netsync doesn't use it either.
			offset += 8
		}

		var hash chainhash.Hash
		copy(hash[:], data[offset:offset+32])
		offset += 32

		action := data[offset]
		offset++

		contentLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		if offset+int(contentLen) > len(data) {
			return nil, errTruncatedTxMetaContent
		}

		content := data[offset : offset+int(contentLen)]
		offset += int(contentLen)

		entries = append(entries, txMetaEntry{hash: hash, action: action, content: content})
	}

	return entries, nil
}
