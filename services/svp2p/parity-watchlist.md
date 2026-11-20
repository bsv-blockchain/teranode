# svp2p parity watch-list

Scenarios the svp2p unit tests cannot adjudicate. Each needs a scripted or
recorded SVNode peer, so each waits on the parity harness (design spec §9, a
scripted-peer replay rig on `cmd/peercli`).

The harness does not exist yet. Phase 3 landed without it, so this file is the
backlog the harness plan must consume. Nothing here is a known defect. Each entry
is a place where this port makes a choice a unit test cannot judge, with the
SVNode or legacy source that defines the correct answer.

SVNode line references are against `bitcoin-sv` @ `12a9f5b6c`. Rows 1 to 14 were
written against `879fc8b42`, which no longer resolves in the local checkout;
only row 15 has been re-checked line by line against `12a9f5b6c` (2026-08-30).

## How to use an entry

Each entry gives the scenario to script, what to observe, and the pass criterion.
A divergence is filed against the task named in the entry, not against the
harness.

---

## 1. Headers batch with no progress

**Task 5, residual N1.**

An SVNode peer that re-sends its previous headers batch, rather than an empty
batch, trips `ErrHeadersNoProgress` in this port. Whether a real SVNode peer ever
does that is unknown — it is a question about SVNode's send path, not about our
receive path, so no unit test can settle it.

- **Script:** a recorded peer that answers a `getheaders` with the batch it
  already sent, at least twice in a row.
- **Observe:** whether `ErrHeadersNoProgress` fires, and whether the peer is
  scored or dropped.
- **Pass:** no honest SVNode peer reaches the error. If one does, the policy is
  wrong and Task 5 owns the fix.


**Verdict 2026-08-26 (parity harness, `services/svp2p/parity`, `TestParity_HeadersReplayIsDropped`):**
both implementations drop the replaying peer — legacy for "block header that does
not properly connect to the chain", svp2p through `ErrHeadersNoProgress` — after
asking twice. The honest case (an inv-drawn duplicate reply mid-round) is pinned
by `TestParity_HonestDuplicateReplyMidRoundStaysConnected` after fix 79f0870ba.
Whether a real SVNode ever replays a batch remains a question for a recorded peer.

## 2. Sync-peer election order

**headersync.go `PeerEstablished` note.**

This port elects the first eligible peer. Legacy ranks candidates by height. The
two agree whenever the first eligible peer is also the best one, and a unit test
cannot show how often that holds against real peers.

- **Script:** three peers announcing distinctly different heights, connecting in
  an order that makes the first eligible peer the worst choice.
- **Observe:** which peer is elected, and how long the node stays with a poor
  pick before rotation moves it.
- **Pass:** election quality is no worse than legacy's ranking over the corpus.
  Upgrade to a ranking only if the harness shows poor picks; do not pre-empt it.


**Verdict 2026-08-26 (`TestParity_SyncPeerElectionOrder`, peers claiming 10/200/50
over a 60-block chain):** legacy ranks by claimed height only among the
candidates present when `startSync` runs — often just the first peer to finish
its handshake — so across three runs it elected peer1, peer2 and peer1, and each
time downloaded all 60 blocks from that one peer and nothing from the others.
svp2p asks whichever candidate the sweep meets first and then spreads the window
over every useful peer (16/26/18). Both reach the tip in ~12 s. Election quality
is moot for svp2p's download because download does not follow election; no
ranking needed. (This assertion was the one intermittent parity failure seen
under -race; it assumed legacy always picks the tallest.)

## 3. Unrequested-headers policy score

**Task 11 (9be584555).**

Task 11 landed the unsolicited bulk-header scoring on unit tests alone, because
the harness it names as its verifier does not exist. The score VALUE is the risk:
too high fragments connectivity against honest peers that batch differently.

- **Script:** honest SVNode peers that send header batches we did not request,
  in the shapes SVNode's own send path produces.
- **Observe:** accumulated misbehavior score per honest peer over a full IBD.
- **Pass:** no honest peer reaches `banScoreThreshold` (100). Any honest peer
  that does means the value is wrong.


**Verdict 2026-08-26 (`TestParity_UnsolicitedHeadersScore`):** the non-elected
peer pushes one unsolicited 2000-header batch and five announcement-sized
batches while the round runs. svp2p keeps the peer with a score ≤ 20 (on this
rig the push landed after the 2040-header round had already closed, so the
observed score was 0; the 20 for a bulk batch mid-round is pinned by the unit
tests); legacy disconnects the peer for "unrequested headers". No honest shape
approaches the threshold.

## 4. Ban and misbehavior score parity across every handler

**Spec §11 risk. Touches Tasks 11, 18, 20 and the handshake.**

Every scoring site in this port carries the SVNode `Misbehaving` value it was
ported from. The values are individually cited but have never been compared to a
real peer's accumulated total across a session.

Sites and their ported values:

| Site | Value | SVNode source |
|---|---|---|
| `scoreInvalidBlock` | 100 | `block_download_tracker.cpp:113-127`, `validation.cpp` DoS(100) sites |
| `scoreTooManyHeaders` | 20 | `Misbehaving(pfrom, 20, "too-many-headers")` |
| `scoreNonContinuousHeaders` | 20 | `Misbehaving(pfrom, 20, "disconnected headers")` |
| `scoreTooManyUnconnectedHeaders` | 20 | `Misbehaving(pfrom, 20, "too-many-unconnected-headers")` |
| `scoreOversizedAddr` | 20 | `Misbehaving(pfrom, 20, "oversized-addr")` |
| `scoreMultipleVersion` | 1 | `Misbehaving(pfrom, 1, "multiple-version")` |
| `scoreMissingVersion` | 1 | `Misbehaving(pfrom, 1, "missing-version")` |
| threshold | 100 | `DEFAULT_BANSCORE_THRESHOLD`, `validation.h:191` |

- **Script:** a full session against honest peers, then against peers that
  misbehave in each scored way exactly once.
- **Observe:** the running total per peer, compared against SVNode's own
  `nMisbehavior` for the identical input.
- **Pass:** totals match SVNode's, and no honest peer accumulates anything.

Note two known structural differences, neither a defect:

- **No score decay.** SVNode's `nMisbehavior` is per-connection and dies with the
  connection; it never decays while the connection lives. This port matches that.
  The unrelated `p2p` service documents a -1/minute decay for its own scorer
  (`settings/p2p_settings.go:33`), which does not apply here.
- **`punish` is not ported.** The `Misbehaving` call in
  `block_download_tracker.cpp:124` is gated on `it->second.punish`, and both
  `punish=false` sites are BIP 152 compact-block paths
  (`net_processing.cpp:3683`, `:3984`). Compact blocks are out of Phase 3, so
  `punish=true` is the only reachable case today. **Whoever implements compact
  blocks must add the flag in the same change, or this port will start banning
  peers for innocently relayed compact blocks.**


**Verdict 2026-08-26 (`TestParity_MisbehaviourScores`, one peer per row, both
services):**

| Row | svp2p | legacy | SVNode |
|---|---|---|---|
| multiple-version | 1, keeps | 0, keeps (ignores) | 1, keeps |
| missing-version | 1, keeps | disconnects | 1, keeps |
| unconnected-headers ×10 | 20, keeps | disconnects ("unrequested headers") | 20, keeps |
| non-continuous headers | 20, keeps | disconnects ("unrequested headers") | 20, keeps |
| invalid block BODY (2nd coinbase) | **0, keeps — GAP** | disconnects | 100, bans |
| headers > 2000 | connection fails (go-wire decode) | keeps, swallows | 20, keeps |
| addr > 1000 | connection fails (go-wire decode) | keeps, swallows | 20, keeps |

svp2p matches SVNode on the four scored rows; legacy is harsher. The two decoder
rows cannot reach either scorer: go-wire refuses the payload (svp2p fails the
connection at `transport/conn.go`, legacy's read loop drops it silently). The
invalid-body row is a gap: the error reaches the bridge as TX_ERROR, outside
PeerFault's block-describing allow-list (Task 20), so the peer is neither scored
nor dropped — ledger carried residual 15. Also found while building the rig:
legacy rejects and BANS any peer whose user agent lacks "Bitcoin SV"/"BSV"
(`peer_server.go:617`); svp2p has no such fence — scenario 12 below.


**Update 2026-08-26 (later the same day):** the invalid-body gap is CLOSED —
`createTxMap` now judges a missing or second coinbase as a block failure before
any transaction reaches the validator (91673dd66), and the row asserts svp2p's
DoS(100) and disconnect.

## 5. Multi-peer download distribution

**Tasks 3-7, 6b.**

The lead item of Phase 3. Unit tests pin the rules — rotation release, all-peer
scheduling, the per-block timeout, parallel fetch — but not the distribution they
produce against real peers with real bandwidth.

- **Script:** identical scripted peers, then a deliberately mixed set (one fast,
  one slow, one silent), driving a multi-block download.
- **Observe:** blocks per peer, duplicate fetches, wall-clock to tip, and
  disconnects. Compare against SVNode on the same corpus.
- **Pass:** distribution and time-to-tip no worse than SVNode's; duplicate
  fetches bounded by `legacy_blockDownloadMaxParallelFetch`.

Watch specifically for the download-timeout livelock the ledger carries: at the
default steady-state window a 4 GB block needs 6.7 MB/s from ONE peer, and the
per-peer grant is zero exactly then. Racing covers most of it, but not with a
single useful peer, and not when every holder stalls at the cap.


**Verdict 2026-08-26 (`TestParity_MultiPeerDistribution`, 200 × 200 KB blocks,
fast/slow/silent peers, per-block budget 1 % = 6 s):** svp2p reaches the tip,
drops the silent peer on the carried per-block clock (79f0870ba) and is redialed
by `legacy_connect_peers`; legacy downloads from its single sync peer only.
Distribution is real (both serving peers carry blocks) but so is the cost: svp2p
served **1,316 blocks for a 200-block chain** (~5.6×) because a block arriving
before its parent is refused pre-admission, discarded and fetched again — the
ledger's carried residual 1, measured at "twice" on 36 blocks in Task 21 and at
five times here under a slow in-process ingest. Wall clock: legacy 40 s–2m40s
(depends on which peer its height-ranked election picks first; the silent one
costs a 180 s rotation), svp2p 51–67 s. The row pins the multiplier as a KNOWN
GAP; the rule-derived bound a fix must meet is in the test.

**Update 2026-08-26 (later the same day): CLOSED.** Orphan-block retention
(648f05a8a) spools a block that arrives before its parent into the TempStore and
ingests it when the parent lands; duplicate fetches on this scenario fell from
1,116 to **0**, svp2p 43 s beside legacy's 40 s. The row now asserts the
rule-derived bound.

---

## Phase 3 divergences worth observing at the same time

Not from the plan's watch-list, but the harness is the only thing that can see
them. Each is a deliberate, documented choice.

### 6. getheaders flood limit absent

SVNode disconnects a non-whitelisted peer whose queued getheaders replies pass a
limit (`net_processing.cpp:2974-2988`, `CNode::MonitoredPendingResponses`). It
measures send-queue depth per request type, which this port's transport does not
expose, and there is no whitelist notion either. One peer can ask for 2000
headers per message as fast as we serve them.

- **Script:** a peer issuing continuous `getheaders` at maximum rate.
- **Observe:** our send-queue depth, memory, and whether serving starves other
  peers.
- **Pass:** no unbounded growth. If it grows, the guard must be built — Task 25
  or later. Note Task 10 added a raw send lane to `transport.Conn`; check whether
  queue depth is now cheap to read.


**Verdict 2026-08-26 (`TestParity_GetHeadersFlood`, 300 getheaders in a burst):**
both answer all 300, keep the connection, and show no heap growth (GC-adjusted
delta negative on both). No flood guard needed at this rate; the send-queue depth
SVNode measures is still not exposed.

### 7. No unsolicited self-advertisement

SVNode advertises its local address to outbound peers unprompted
(`net_processing.cpp:1847-1864`, gated on `fListen && !IsInitialBlockDownload()`).
This port advertises only inside a `getaddr` reply, which is legacy's shape. A
node that never self-advertises is less discoverable.

- **Observe:** how many peers learn our address over a session, versus SVNode.


**Verdict 2026-08-26 (`TestParity_NoUnsolicitedSelfAdvertisement`):** over 8 s
after handshake with no getaddr, legacy sent 0 addr, svp2p sent 0 addr; SVNode
would send its own address once. Documented divergence, both sides alike.

### 8. Cold-start bootstrap

Neither DNS seeds (`net.cpp:1622`) nor the fixed-seed fallback
(`net.cpp:1842-1855`) is carried, and feelers (`net.cpp:1897-1918`) are absent so
the tried table is never warmed by probes. A node with an empty address table
cannot bootstrap outbound at all; it needs `legacy_connect_peers` or an inbound
peer.

- **Script:** a node started with no `peers.json` and no configured peers.
- **Observe:** whether it ever reaches `legacy_targetOutboundPeers`.
- **Pass:** this is expected to FAIL today. It is an owner decision, not a bug —
  record the behavior so the decision is made on evidence.


**Verdict 2026-08-26 (`TestParity_ColdStartBootstrap`):** with no configured
peers both sides hold 0 connections after 10 s (regtest legacy has no seeds
either). Expected FAIL, recorded; the owner decision stands.

**Owner decision 2026-08-27: add DNS seeds.** svp2p now carries
ThreadDNSAddressSeed (query the chain's DNS seeds when the table is empty or
fewer than two full-node peers are up, after SVNode's 11 s wait) and the
fixed-seed fallback (SVNode's `pnSeed6_*` tables, added once the table has
stayed empty for 60 s) in `protocol/seeds.go`; `legacy_disableDNSSeed` is the
--nodnsseed switch. Regtest has neither kind of seed, so this in-process row
still reads 0/0 and now asserts exactly that; the bootstrap itself is pinned in
`protocol/seeds_test.go` with an injected resolver. Scenario CLOSED.

### 9. Blocks at and over the 4 GiB envelope

A block a basic message header cannot declare is answered `notfound`
(`getdata.go` OPEN QUESTION 5 site) until Phase 4 brings the extended header.
SVNode frames such a payload with an extmsg header (`protocol.cpp:220-237`) and
so never has this branch.

- **Script:** a peer requesting a block above the envelope.
- **Observe:** `notfound` plus the log line, and that the connection survives.
- **Pass:** interop gap confirmed and bounded, not a crash or a stall.


**Verdict 2026-08-26: NOT ADJUDICABLE IN-PROCESS.** The `notfound` branch fires
on a block whose declared size exceeds the basic envelope, which needs a real
block above 4 GiB in the node's store. Needs a fat-block rig; carried.

**Verdict 2026-08-28 (`TestParity_ExtendedBlockServing`, Phase 4): svp2p frames
4 GiB + 1 with the extended header and the peer receives every byte.** A peer
that negotiated 70016 asks for a block the node declares at 4,294,967,297 bytes.
svp2p writes SVNode's extmsg header (`protocol.cpp:220-237`, command "extmsg",
length `0xffffffff`, zero checksum, then command "block" and a 64 bit length) and
streams the payload: the peer counted 4,294,967,297 bytes against a declared
4,294,967,297, sent no `notfound`, and the connection survived. Scenario wall
clock 4.6 s, peak resident set 1.6 GB — the body is never materialised on either
side.

**Legacy, observed the same day on the same scenario (`PARITY_FAT_BLOCK_LEGACY=1`,
race detector off): it fetches the whole body, sends the peer nothing, and drops
the peer.** The transcript shows the `getdata` arriving and no reply of any kind
— no `block`, no `notfound` — and the node then closing the connection
(`Disconnected[peer0]=node`, `still-connected: 0`) after 2 m 1 s. The asset stub
recorded one completed body write, so legacy did read all 4 GiB + 1.

That legacy observation is OPT-IN: it runs only with `PARITY_FAT_BLOCK_LEGACY=1`
set, and CI never sets it, so nothing re-checks this comparison on a later
change to legacy.

Two INDEPENDENT facts produce that silence. They are not a chain: the second
happens after the first has already decided the answer.

1. **No `notfound` is ever considered.** `pushBlockMsg` ends at
   `sp.QueueMessageWithEncoding` (`services/legacy/peer_server.go:2323`) and
   returns nil — success means QUEUED, not sent. The getdata loop adds a
   `notfound` entry only for a non-nil return
   (`services/legacy/peer_server.go:1663-1680`), so the decision is made and
   closed before any writer runs.
2. **The frame is then dropped by go-wire's writer.**
   `WriteMessageWithEncodingN` returns `(0, nil)` for a payload above
   `math.MaxUint32` (`message.go:385`) — no bytes, no error. That happens later,
   on the out handler goroutine, where nothing inspects the result.

The cost of that path is why the legacy leg is opt-in. legacy materialises the
payload twice over — `NewRawBlockMessage` reads the asset body into one
`[]byte` (`services/legacy/raw_block_message.go:28`, `io.ReadAll`) and go-wire
encodes that into a `bytes.Buffer` (`message.go:351-353`) — measured at **94.8 GB
maximum resident set size** without the race detector. With `-race` and both
legs the test binary was killed by the OS at 62.8 GB, 762 s in, still inside the
legacy leg. The default leg set is therefore svp2p alone, which keeps the
package inside `make test`; the row's assertions are on svp2p, as a verdict on
this port should be. **Scenario CLOSED.**

### 10. Inv-driven getheaders amplification

**Task 25 (59dc6310c), the opposite direction to scenario 6.**

A peer that announces N distinct unknown block hashes draws N getheaders in reply,
one per hash — SVNode's own behavior (`ProcessInvMessage` answers inside the
per-entry loop, `net_processing.cpp:2461-2462`, pushing at `:2489-2493` with that
entry's own hash as hashStop at `:2492`). The rule is not collapsible: hashStop
TRUNCATES the peer's reply, so one getheaders can bound only one announced branch.
The cost is 36 bytes in, a full locator out, per fabricated hash, up to
`wire.MaxInvPerMsg`.

SVNode carries this identically at the identical site, so bounding it here would be
a divergence rather than a port — which is exactly why the harness, not a unit
test, is the right place to judge it.

- **Script:** a peer sending a maximal inv of distinct hashes that no chain
  contains.
- **Observe:** our outbound bytes versus the peer's inbound bytes, and whether
  serving other peers degrades.
- **Pass:** amplification no worse than SVNode's on the identical input. Read this
  together with scenario 6 — that one guards what a peer can make us SERVE, this
  one guards what a peer can make us ASK.


**Verdict 2026-08-26 (`TestParity_InvGetHeadersAmplification`, 500 fabricated
hashes):** svp2p drew exactly 500 getheaders (SVNode-identical); legacy drew 0
getheaders and instead sent a block GETDATA per hash (520 block requests) — the
bsvd shape. Neither side dropped the peer. Amplification is bounded by
`MaxInvPerMsg` on both; recorded, no change.

### 11. Addr forwarding widths

Task 18 ported `RelayAddress` (`net_processing.cpp:998-1041`) including the
daily-hash target pick (`:1010`) and both relay widths (`:1000-1001`). Legacy
dropped forwarding entirely, so there is no Teranode-side behavior to compare
against — only SVNode.

- **Observe:** how many peers each received addr is forwarded to, against SVNode
  on the same input.


**Verdict 2026-08-26 (`TestParity_AddrForwardingWidths`):** an outbound peer
announces one fresh routable address; svp2p forwards it to **2** of the inbound
peers (the reachable width, net_processing.cpp:1000), legacy to 0. Note the
gate: svp2p accepts an unrequested addr from an INBOUND peer only when it is
that peer's own address (addrrelay.go processAddrEntries), so the sender must be
outbound to exercise forwarding.

### 12. User-agent fence

Legacy rejects and bans (24 h) any peer whose user agent contains neither
"Bitcoin SV" nor "BSV" (`services/legacy/peer_server.go:617`, "Only BSV
Blockchain clients are supported"). svp2p accepts any agent; SVNode has no such
fence either. Found 2026-08-26 when the harness's scripted peer, announcing
`/svp2p-scripted-peer:1.0/`, was banned by the legacy leg and accepted by svp2p.

- **Observe:** which agents connect to each over a testnet session.
- **Pass:** an owner decision — carry legacy's fence into svp2p, or drop it at
  cutover as a Teranode-only policy with no SVNode counterpart.

**Verdict 2026-08-26 (`TestParity_UserAgentFence`):** legacy rejects and bans a
peer announcing `/scriptpeer:0.1/`; svp2p accepts it and syncs from it. Pinned;
owner decision at cutover.

**Owner decision 2026-08-27: DROP the fence.** svp2p follows SVNode and accepts
any user agent; legacy's ban is a Teranode-only policy with no SVNode
counterpart and ends with the legacy service at cutover. Recorded as a
deliberate behaviour change in the Phase 3 PR. Scenario CLOSED.

### 13. BlockPriority stream negotiation

**Tasks 8, 9, 10 (Phase 4).** Both implementations now negotiate the
BlockPriority stream policy and open a second connection for the DATA1 stream,
in both directions: each accepts an inbound `createstream` and moves that socket
into an existing association (`net.cpp:3188-3245` `CConnman::MoveStream`), and
each dials a second connection out and sends `createstream` once the handshake
has agreed BlockPriority — the peer's advertised policy list is intersected with
ours in `net.cpp:904-923` `CNode::SetSupportedStreamPolicies`, and the first of
our prioritised list that is in common is picked in `net.cpp:945-963`
`CNode::GetPreferredStreamPolicyName`. Blocks then travel on DATA1
(`stream_policy.cpp:25-31`, `IsHighPriorityMsg`).

Nothing here is left for the harness to judge — both directions are pinned by
in-process tests over real sockets, so this row records the state rather than
asking a question of it.

- **svp2p, inbound:** `TestInbound_CreateStreamAttachesAndAcksOnNewStream`,
  `TestInbound_CreateStreamFromOtherIPIsBanned`,
  `TestInbound_CreateStreamRefusedWhenBlockPriorityOff`,
  `TestInbound_UnsolicitedStreamAckIsRejected`,
  `TestInbound_AssociationUnregistersOnDisconnect`
  (`services/svp2p/protocol/streams_test.go`).
- **svp2p, outbound:** `TestOutbound_OpensData1AfterBlockPriorityHandshake`,
  `TestOutbound_NoData1WithoutBlockPriority`,
  `TestOutbound_NoStreamsWhenBlockPriorityOff`,
  `TestOutbound_Data1DialFailureIsNotFatal`,
  `TestOutbound_SilentStreamAckDoesNotStallTheQueue`
  (same file).
- **Both stacks together:** `TestIntegrationTwoServersOpenADataStream`
  (`services/svp2p/multistream_integration_test.go`) and
  `TestIntegrationBlocksTravelOnData1`
  (`services/svp2p/sync_integration_test.go`).
- **legacy:** the same two directions ship in `services/legacy/peer`, and the
  cross-stack e2e set (`TestMultistream*` in `test/e2e/daemon/ready`) covers
  legacy against SVNode.

**Recorded 2026-08-28:** svp2p initiates and accepts; legacy initiates and
accepts. Both directions pinned; no divergence to watch.

### 14. `headers` routing under BlockPriority

**Task 9 (Phase 4), corrected and FIXED 2026-08-28.** This row first recorded
that svp2p matched SVNode by leaving `headers` on GENERAL. Reading the SVNode
source showed the opposite, and the port has been changed.

**SVNode's send router** is `BlockPriorityStreamPolicy::PushMessage`
(`stream_policy.cpp:161-184`): when the caller has not named a stream, DATA1 is
taken whenever `IsHighPriorityMsg` holds (`stream_policy.cpp:25-31`), GENERAL
otherwise. `IsHighPriorityMsg` is `ping`, `pong`, or `IsBlockMsg`, and
`IsBlockMsg` (`stream_policy.cpp:11-22`) is a WIDE set: `block`, `cmpctblock`,
`blocktxn`, `getblocktxn`, **`headers`**, **`getheaders`**, `hdrsen`,
`gethdrsen`, plus any message whose payload type is BLOCK.

`BlockPriorityStreamPolicy::GetStreamTypeForMessage` (`stream_policy.cpp:187-195`),
the three-value `BLOCK`/`PING`/`OTHER` function svp2p's `transport/policy.go`
was first written against, is NOT the router. Its only caller is
`Association::GetAverageBandwidth` (`association.cpp:222`), which asks which
stream carries a CLASS of message so it can read that stream's bandwidth meter —
the statistic behind `net_processing.cpp:107` and `:5697`.

**svp2p now matches SVNode.** `blockPriorityStreamPolicy.StreamFor`
(`services/svp2p/transport/policy.go`) routes exactly the `IsHighPriorityMsg`
set to DATA1 and everything else to GENERAL, pinned command by command in
`TestStreamPolicy_Routing`. The payload-type arm of `IsBlockMsg` has no
counterpart: it exists for SVNode's sender-side tag on a pre-serialised body,
and this transport frames a block through `SendBlock`.

**Legacy is now the divergence, and it is narrower than it looks.** legacy
routes `block`, `headers`, `ping` and `pong` to DATA1
(`services/legacy/peer/stream_policy.go:27`) and leaves `getheaders`,
`cmpctblock`, `blocktxn`, `getblocktxn`, `hdrsen` and `gethdrsen` on GENERAL,
where SVNode puts all six on DATA1. Of those, `getheaders` is the only one
legacy actually sends.

**Recorded 2026-08-28:** not peer-visible in any way a parity scenario can
score — both sockets belong to one association, a peer reads both, and neither
the message nor its order changes, only which socket carries it. It is visible
to a peer that measures per-stream bandwidth, which is what SVNode's own stall
detection does on its side of the link. No action for legacy: its narrower set
ends with the legacy service at cutover. Scenario CLOSED.

### 15. Compact block receive

**Task 10 (Phase 5), recorded 2026-08-29, closed 2026-08-30.** Five sub-rows,
15a to 15e, in `services/svp2p/parity/scenario_compact_test.go`. Every SVNode
citation in this row was re-checked against `bitcoin-sv` @ `12a9f5b6c` on
2026-08-30.

**Setup.** Both legs sync a five block fixture chain. One scripted peer relays a
transaction that the node validates and, through the txmeta topic, enters in the
bridge's recent-transaction index. A second peer then announces a SIXTH block —
coinbase, that transaction, and one the node has never seen — as `cmpctblock` to
svp2p and as `inv` to legacy. The `inv` is not a convenience: SVNode sends
`cmpctblock` only to a peer that sent `sendcmpct`, and legacy sends none.

**Observe.** `getblocktxn` count and the slots it asks for, `getdata` count for
the announced block, the ingested height, the announcing peer's score, and
whether it is dropped.

#### Sub-rows

| Row | Test | Expected | Observed | Verdict |
|---|---|---|---|---|
| 15a | `TestParity_CompactBlockReceive`, svp2p leg | 1 `sendcmpct`; 1 `getblocktxn` for slot 2; 0 `getdata` for the block; height 6 | `sendcmpct`=1, `getblocktxn`=1, slots `[[2]]`, `getdata`=0, height 6 | PASS |
| 15a | the same test, legacy leg | 0 `sendcmpct`; 0 `getblocktxn`; 1 `getdata`; height 6 | as expected | PASS |
| 15a | the same test, comparison | both legs end on the same height; `Requests` and `Served` diverge, and that IS the row | 0 unaccepted diffs, 2 accepted | PASS |
| 15b | `TestParity_CompactBlockShortBlockTxnIsBanned` | a `blocktxn` shorter than the request: score 100, peer dropped, chain stays at 5 | `getblocktxn`=1, score 100, `Disconnected[peer1]`="node", height 5 | PASS |
| 15c | `TestParity_UnsolicitedBlockTxnIsDroppedUnscored` | a `blocktxn` nobody asked for: branch entered, score 0, both peers connected, height 5 | `unsolicited-branch`=true, score 0, 2 connected, height 5 | PASS |
| 15d | `TestParity_CompactBlockWrongGapFallsBackToGetData` | the wrong gap transaction (`readFailed`): score 0, peer kept, block by `getdata`, height 6 on both legs | BEFORE the port fix: the fallback ran, THEN score 100 and a disconnect, height 5. AFTER: `getblocktxn`=1, `getdata`=1, score 0, connected, height 6 both legs | **DEFECT FOUND AND FIXED** — PASS |
| 15e | `TestParity_CompactBlockAboveHeightCeilingIsDeclined` | above the ceiling: no `getblocktxn`, header accepted, peer unscored, block later by `getdata` | declined at tip 5, `getblocktxn`=0, header in the index at height 8, score 0, connected, `getdata`=2, height 8 | PASS |

#### Rules these sub-rows pin

- The height ceiling is `tip + 2` (`net_processing.cpp:3825`), and it is the
  RECEIVE-side conservatism rule. `MAX_CMPCTBLOCK_DEPTH` (5, `validation.h:133`)
  is the SERVE-side depth (`net_processing.cpp:1310-1312`) and does not apply to
  a receive-only port.
- `CanDirectFetch` (`net_processing.cpp:3818-3820`) and the claim rule
  (`:3827-3829`) both run before a compact claim is taken.
- A `cmpctblock` whose parent this node does not hold draws a `getheaders` and
  no score (`net_processing.cpp:3721-3733`), gated on `!IsInitialBlockDownload()`
  (`:3725`).
- One partial block per peer (`net_processing.cpp:3839-3844`), counting both a
  reconstruction waiting on a `blocktxn` and one being ingested.
- The partial block is detached at the fill handoff, so a replayed `blocktxn`
  is unsolicited. That is `MarkBlockAsReceived` at `net_processing.cpp:3646`.
- A compact ingest declares no payload size, so `SizeBytes` is zero and the
  ingest is bounded by `MaxBlockDownloadTime` rather than by byte progress.

#### 15a, recorded 2026-08-29

svp2p asked 1 `getblocktxn` for slots `[[2]]` and 0 `getdata`; legacy asked 1
`getdata`. Both legs ended at height 6. The `Requests` and `Served` divergence is
accepted and IS the row: one block arrives as a gap request on svp2p and as a
full download on legacy. Sub-row CLOSED.

Observed disconnect line for 15b, from the node's own log, is in the Task 10
report.

#### 15d — the `READ_STATUS_FAILED` path, and a port defect

The peer answers the gap request with the right count and the wrong
transaction. The arity check passes, and the assembler's short-ID check fails
the slot (`compactblock.go` `readGap`). A short ID is 48 bits, so an honest
peer's transaction can hash onto the slot we asked about. SVNode treats that as
a possible collision and not as malice (`net_processing.cpp:3618-3623`, "Might
have collided, fall back to getdata now"), with no `Misbehaving` call.

**PORT DEFECT FOUND BY THIS SUB-ROW, and FIXED.** `manager.go` `BlockDone` read
its compact override as additive when it is a replacement. The default branch had
already set the score and the disconnect from `outcome.PeerFault`, and the
`readFailed` case only logged. A collision therefore cost an honest peer the same
100 and the same disconnect as a malicious fill — `readInvalid` and `readFailed`
were indistinguishable to a peer — and it stranded the block, because the peer
that announced it is normally the only one known to hold it. `PeerFault` is the
pipeline's verdict on the BYTES, and on a compact ingest this node assembled those
bytes itself, from its own index and against its own short IDs, so it cannot stand
as a verdict on the peer. The branch now clears both. Unit test:
`protocol.TestBlockDoneCompactStatusDecidesThePeersFate`.

**Recorded 2026-08-30, after the fix:** svp2p asked 1 `getblocktxn`, fell back to
1 `getdata`, scored the peer 0, kept the connection, and reached height 6; legacy
reached height 6. 15b is the control that `readInvalid` still scores 100 and
still drops. Sub-row CLOSED.

#### 15e — the height ceiling

The peer publishes headers 6 and 7 and then withholds every block body, so the
node's header index reaches 7 while its active tip stays at 5. Block 8 is then
announced by `cmpctblock`. Of `wantCompact`'s five guards only the ceiling can
refuse: the node never held block 8, block 8 outweighs the tip, and
`CanDirectFetch` passes (the fixture tip is about 30 minutes old against a
3 h 20 m regtest window).

**Recorded 2026-08-30: svp2p matches.** Declined at tip 5 with no `getblocktxn`,
the header accepted into the index at height 8 — the accept runs ahead of every
guard, which is what the `:3913-3921` "same treatment as a header message" branch
achieves — the peer unscored and still connected, and the block ingested by
ordinary `getdata` once serving resumed. Sub-row CLOSED.

**Scenario CLOSED.** See
[docs/topics/services/svp2p_compact_blocks.md](../../docs/topics/services/svp2p_compact_blocks.md)
for the message flow and the full divergence list.
