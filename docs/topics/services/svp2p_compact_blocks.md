# svp2p Compact Blocks (BIP152, receive-only)

**Related Settings**: [Legacy Service Settings](../../references/settings/services/legacy_settings.md)

SVNode line references are against `bitcoin-sv` `src/net/net_processing.cpp`,
`src/blockencodings.cpp` and `src/blockencodings.h` at commit `12a9f5b6c`.

## 1. What this is

svp2p receives compact blocks. It does not announce them and it does not serve
them. A peer that announces a block as `cmpctblock` saves svp2p the bytes of
every transaction svp2p already holds. svp2p asks for the rest with one
`getblocktxn`.

The feature is off by default. `legacy_compactBlocks` turns it on.
`legacy_compactBlocksRecentTxs` sizes the index it reconstructs from.

Three things are deliberately absent:

- **svp2p never announces a compact block.** It sends
  `sendcmpct(announce=false, version=1)`, so a peer keeps announcing by
  `headers` or `inv`. `sendcmpct` carries the announce bool FIRST and the
  version second (`net_processing.cpp:2390-2394`; go-wire's `MsgSendcmpct` is
  `{SendCmpct bool, Version uint64}`). That announce bool IS BIP152
  high-bandwidth mode. There is no separate flag for it.
- **svp2p never serves a compact block, and it answers nothing when asked for
  one.** A `getdata` for `MSG_CMPCT_BLOCK` is not recognised: `OnGetData` maps
  only `InvTypeTx`, `InvTypeBlock` and `InvTypeFilteredBlock`
  (`services/svp2p/protocol/serving.go:495-505`), and everything else becomes
  `getDataUnsupported` (`serving.go:336-339`). That entry draws a warning log
  and no reply at all — not even a `notfound` (`getdata.go:246-248`) — and it
  does not end the serving pass, because `blockType()` is false for it
  (`serving.go:342-345`). SVNode instead falls back to the full block
  (`net_processing.cpp:1310-1312`). This port does not.
- **svp2p records what it never acts on.** An inbound `sendcmpct` sets
  `fPreferHeaderAndIDs` from the peer's announce bool
  (`services/svp2p/protocol/syncstate.go:538-552`). Nothing reads it, because
  this port never announces. Phase 5b is where it starts to matter.

Every failure of the compact path falls back to an ordinary `getdata` for the
whole block, so the flag costs bandwidth rather than correctness. It is not free
of consequence for the announcing peer: a `readFailed` costs that peer nothing
and any known holder may then serve the block, but a `readInvalid` scores it 100
and disconnects it (`compactdispatch.go:111-118`, `manager.go:1858-1867`). When
that peer was the only known holder, the block waits for another announcement.
Parity row 15b shows exactly that shape: the fixture chain stays at height 5.

## 2. Why a recent-transaction index

SVNode matches a compact block's short IDs against its mempool
(`blockencodings.cpp:164-192`). Teranode has no mempool. `bridge.RecentTxIndex`
stands in for one. It is a fixed-capacity ring of transaction hashes, oldest
evicted first.

One site feeds the ring: the txmeta topic's ADD entries, filtered to those that
are neither coinbase nor block-originated (`services/svp2p/bridge/kafka.go:337-340`
skips `txMeta.IsCoinbase`, `:344-351` skips `txMeta.InBlock`). The filter is what
keeps mined transactions out of the ring. What is left is the closest thing this
node has to "entered the mempool".

The index holds hashes only. It reads the transaction bytes from the store at
assembly time, through the same fetch seam the `getdata tx` answerer uses.

The orphan pool deliberately does NOT feed it. An orphan failed validation, so it
lives only in the pool's memory and never reaches the store, and a hash the index
names but cannot serve is worse than one it never named: reconstruction matches
it, marks the slot held, leaves it out of the `getblocktxn`, and then fails at
that slot mid-assembly and refetches the whole block. Left unnamed, the same slot
is a gap the `getblocktxn` fills. SVNode's `vExtraTxnForCompact`
(`blockencodings.cpp:194-215`) can take the opposite side because it stores the
transaction bytes, not just the hash; carrying those bytes here is a possible
follow-up.

## 3. Message flow

1. **Handshake.** After verack, svp2p sends `sendcmpct(0, 1)` once per peer.
   SVNode does the same, at `net_processing.cpp:1942-1953` inside
   `ProcessVerAckMessage` (`:1899-1955`). svp2p sends it only when
   `legacy_compactBlocks` is on and a transaction index is wired.
2. **Record.** An inbound `sendcmpct` sets three flags on the peer's sync
   state, the port of `ProcessSendCompactMessage`
   (`net_processing.cpp:2390-2411`). A version other than 1 is ignored, with no
   score.
3. **Announce.** The peer sends `cmpctblock`: the 80-byte header, a nonce, the
   prefilled transactions, and one 48-bit short ID per remaining transaction.
4. **Accept the header.** svp2p accepts the header into the index AFTER the
   parent check and BEFORE every guard in section 4
   (`services/svp2p/protocol/compactdispatch.go:166-192`). A `cmpctblock` whose
   parent is unknown returns earlier than this, with no header accepted, exactly
   as SVNode returns at `net_processing.cpp:3721-3733` before
   `ProcessNewBlockHeaders` at `:3740`.
5. **Decide.** Five guards decide whether the block is worth reconstructing.
   Section 4 lists them.
6. **Match.** svp2p derives the SipHash key from the header and the nonce
   (`blockencodings.cpp:65-76`), then asks the index for a hash per short ID
   (`blockencodings.cpp:78-81`).
7. **Ask for the gaps.** svp2p sends one `getblocktxn` naming the slots the
   index could not fill (`net_processing.cpp:3864-3881`). With no gaps, the
   block assembles at once.
8. **Fill.** The peer answers `blocktxn`. svp2p latches the stream into the
   partial block. It does not read the transactions yet.
9. **Assemble and ingest.** The ingest goroutine reads one assembled stream. The
   stream yields, in block order, the prefilled transactions, the held
   transactions from the store, and the gap transactions from the `blocktxn`
   stream. Nothing is materialised.

## 4. The guards, in SVNode's order

`BlockDownloader.wantCompact` carries the run of guards
`ProcessCompactBlockMessage` applies between the header accept and
`MarkBlockAsInFlight`:

| Order | Guard | SVNode |
|---|---|---|
| 1. | We already hold the block | `net_processing.cpp:3795-3799` |
| 2. | The block's chain work does not beat the active tip | `net_processing.cpp:3801-3802` |
| 3. | The block is not in flight and `CanDirectFetch` fails | `net_processing.cpp:3818-3820` |
| 4. | The height is above tip + 2 | `net_processing.cpp:3825` |
| 5. | The claim rule: not in flight and under 16 blocks per peer, or already in flight from this peer | `net_processing.cpp:3827-3829` |

Two more rules sit outside that table:

- A `cmpctblock` whose parent svp2p does not hold draws a `getheaders` and no
  score (`net_processing.cpp:3721-3733`). SVNode gates that push on
  `!IsInitialBlockDownload()` (`:3725`). svp2p uses
  `HeaderSync.tipIsNearAdjustedTime` as the proxy for that predicate. Without
  the gate, one small message from a catching-up peer draws one full locator.
- One partial block per peer. A second `cmpctblock` from a peer that is still
  reconstructing, or still ingesting, is ignored with no score. SVNode refuses
  the same case at `net_processing.cpp:3839-3844`.

`MAX_CMPCTBLOCK_DEPTH` (5, `validation.h:133`) is NOT one of these guards. It
bounds how deep a block SVNode is willing to SERVE as a compact block
(`net_processing.cpp:1310-1312`). A receive-only port never reaches it. The
receive-side conservatism rule is the height ceiling at `:3825`, and it is
`tip + 2`.

## 5. READ_STATUS to outcome

`ReadStatus` is SVNode's three-way verdict on a reconstruction
(`blockencodings.h:141-150`). It decides what happens to the block AND what
happens to the peer.

| Status | What produced it | Peer score | Block |
|---|---|---|---|
| `readInvalid` | `cmpctblock` with a null header, or with no short IDs and no prefilled transactions, or over the transaction-count cap (`blockencodings.cpp:87-94`) | 100, disconnect (`net_processing.cpp:3849-3855`) | Claim released |
| `readInvalid` | A prefilled index that is not strictly increasing, or out of range (`blockencodings.cpp:101-122`) | 100, disconnect (`net_processing.cpp:3849-3855`) | Claim released |
| `readInvalid` | Two equal short IDs in one message (divergence, see section 7) | 100, disconnect | Claim released |
| `readInvalid` | `blocktxn` carrying a different count from the one requested (`blockencodings.cpp:254-256`, `:267-269`) | 100, disconnect (`net_processing.cpp:3610-3616`) | Claim released |
| `readInvalid` | A gap transaction that does not decode | 100, disconnect | Claim released |
| `readFailed` | The index answered a different number of short IDs from the number asked | None | Released, re-offered by `getdata` (`net_processing.cpp:3856-3861`) |
| `readFailed` | A gap transaction whose short ID does not match its slot | None | Released, re-offered by `getdata` (`net_processing.cpp:3618-3623`) |
| `readFailed` | A held transaction the store can no longer open, or one whose bytes under-run or over-run the length the store declared | None | Released, re-offered by `getdata` |
| `readOK` | Every slot filled | None | Ingested |

The line between the two failure statuses is the line between malice and bad
luck. A short ID is 48 bits. An honest peer's transaction can hash onto the slot
svp2p asked about. SVNode reads that as a possible collision and not as an
attack: `net_processing.cpp:3618-3623` says "Might have collided, fall back to
getdata now" and calls no `Misbehaving`. `readFailed` therefore scores nobody.

An unexpected `blocktxn` — one that answers no outstanding request — is a third
case, and it is neither. SVNode logs it and returns
(`net_processing.cpp:3602-3606`). There is no `Misbehaving` call and no
`MarkBlockAsFailed` on that path. svp2p drops it the same way. A `blocktxn` that
races a claim this node released is a timing artefact, not evidence of malice.

## 6. Fallback semantics

SVNode pushes a `getdata` to the announcing peer at the moment a
reconstruction fails (`net_processing.cpp:3856-3861` and `:3618-3623`). svp2p
does not. The download scheduler owns `getdata` here. svp2p releases the claim,
the block goes back on offer, and the next `syncPass` walk requests it.

Two consequences follow, and both are improvements:

- The request goes out one `TickInterval` later, not immediately.
- ANY peer known to hold the block may serve it, not only the peer whose reply
  failed.

Three further points of the fallback:

- **The verdict on the peer comes from the partial block, not from the
  pipeline.** A compact block's assembly can only fail while the assembled
  stream is read, so the ingestor reports it as a stream error. `BlockDone`
  reads the partial block's own status to tell the two cases apart. The status
  REPLACES the ingest outcome's `PeerFault` verdict. `PeerFault` is a verdict on
  the bytes, and on a compact ingest this node assembled those bytes itself,
  from its own index and against its own short IDs.
- **The partial block is detached at the fill handoff.** From that moment a
  replayed `blocktxn` finds nothing and takes the unsolicited path. This is
  SVNode's `MarkBlockAsReceived` at `net_processing.cpp:3646`, which destroys
  the queued block before the block is processed.
- **A compact ingest is bounded by time, not by bytes.** No payload declares a
  compact block's size, so `SizeBytes` is zero. The stall meter therefore treats
  the ingest as fully streamed from the start and bounds it by
  `MaxBlockDownloadTime`. That is the right bound: most of the bytes come from
  this node's own index, so silence on the socket says nothing about the peer.

## 7. Divergences from SVNode

1. **A duplicate short ID inside one message is `readInvalid` here and
   `READ_STATUS_FAILED` in SVNode** (`blockencodings.cpp:159-162`). svp2p scores
   the peer 100 for it. A peer cannot produce two equal short IDs for two
   distinct transactions by accident at the rate this would need.
2. **The bucket-size guard is not ported.** SVNode fails a reconstruction when
   any hash bucket holds more than 12 short IDs
   (`blockencodings.cpp:150-153`). That guard defends `std::unordered_map`
   against a crafted distribution. The Go map svp2p builds has no equivalent
   exposure.
3. **`READ_STATUS_CHECKBLOCK_FAILED` is not carried**
   (`blockencodings.h:148-149`). SVNode uses it to separate a merkle-root mismatch
   from other `CheckBlock` failures. svp2p checks each gap transaction's short
   ID against its slot directly, so it reaches the collision verdict without
   running `CheckBlock`.
4. **Optimistic reconstruction is not ported**
   (`net_processing.cpp:3883-3900`). SVNode reconstructs a block already in
   flight from ANOTHER peer, on the chance it needs no round trip. That needs a
   second partial block per hash and buys one saved round trip.
5. **The two above-the-ceiling branches are reached by other machinery**
   (`net_processing.cpp:3902-3923`). SVNode either pushes a `getdata` for a
   block already in flight (`:3904-3911`), or reprocesses the announcement as a
   plain `headers` message (`:3913-3921`). svp2p's scheduler re-offers the block
   on its own tick, and svp2p's header accept runs ahead of every guard. The end
   state is the same.
6. **svp2p never serves a compact block, and a peer that asks gets silence.**
   `MAX_CMPCTBLOCK_DEPTH` and `MAX_BLOCKTXN_DEPTH` have no counterpart here.
   Section 1 gives the mechanism. Note the consequence of turning the flag on:
   SVNode reads our `sendcmpct` as "we are willing to provide version 1 or 2
   cmpctblocks ... they may wish to request compact blocks from us"
   (`net_processing.cpp:1942-1946`), so a peer MAY now send a `getdata` for
   `MSG_CMPCT_BLOCK`. svp2p answers that entry with nothing. An inbound
   `getblocktxn` is refused with nothing for the same reason
   (`services/svp2p/protocol/peer.go:843-849`).
7. **svp2p never requests a compact block either.** In low-bandwidth mode SVNode
   asks for one by rewriting a single-block direct fetch to `getdata
   MSG_CMPCT_BLOCK` (`net_processing.cpp:3553-3560`). BIP152 makes that request
   optional, and the decision for this port is not to support `MSG_CMPCT_BLOCK`
   in either direction. So a stock SVNode peer, which announces in low-bandwidth
   mode by default, sends this node full blocks. The receive path in this page
   is exercised only by a peer that pushes `cmpctblock` unasked.

## 8. Test harness note

`services/svp2p/svp2ptest` carries a SECOND implementation of the BIP152 short
transaction ID, apart from the one in `services/svp2p/protocol`. A scripted peer
must not share the code under test: one that announced compact blocks with the
node's own derivation could not tell a correct derivation from a consistently
wrong one. It could not import that package in any case, because `protocol`'s
own tests import `svp2ptest`, so the reverse import is a cycle. The two copies
are held to each other by
`TestScriptedPeer_AnnounceCompactCarriesShortIDsAndPrefilled`.

Parity row 15, in `services/svp2p/parity-watchlist.md`, records the compact
receive path beside the legacy service on the same announcement.
