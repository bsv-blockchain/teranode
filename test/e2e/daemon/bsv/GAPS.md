# bitcoin-sv porting: gap register

<!-- Generated from registry.yaml by TestRegistryGapsDoc. Do not edit by hand. -->
<!-- Regenerate with: make bsvportinggapsdoc -->

Every obstacle standing between Teranode's current bitcoin-sv port coverage and a
larger one. Each entry here is generated from the `gaps:` block at the top of
[`registry.yaml`](registry.yaml), which is the source of truth; editing this file
by hand will fail `make test`. For how the register fits into the wider exercise,
see [`PORTING.md`](PORTING.md).

Gaps are ordered by how many upstream scripts they hold up, worst first, because
that is the order in which fixing them buys the most coverage. A gap that holds up
nothing is not therefore unimportant - it is a defect that happens to cost no
coverage, recorded because porting found it.

**`kind` decides who acts:**

- **`defect`** - a confirmed Teranode bug; belongs in the issue tracker
- **`test-config`** - ours to fix, in this repository
- **`product-decision`** - not ours to make; needs a deliberate decision elsewhere
- **`unknown`** - needs diagnosis before anything else can be decided

## Summary

| Gap | Kind | Status | Holds up |
|-----|------|--------|----------|
| [`submitblock-rpc`](#submitblock-rpc) | product-decision | deferred | 30 upstream scripts |
| [`headers-announcement-disconnects`](#headers-announcement-disconnects) | defect | open | 4 upstream scripts |
| [`legacy-block-announcement`](#legacy-block-announcement) | unknown | open | 3 upstream scripts |
| [`net-rpcs-unimplemented`](#net-rpcs-unimplemented) | product-decision | open | 3 upstream scripts |
| [`no-pending-response-limit`](#no-pending-response-limit) | defect | open | 2 upstream scripts |
| [`legacy-wire-message-subset`](#legacy-wire-message-subset) | product-decision | open | 2 upstream scripts |
| [`no-cpfp-ancestor-fee-accounting`](#no-cpfp-ancestor-fee-accounting) | product-decision | open | 2 upstream scripts |
| [`log-assertions-unreachable`](#log-assertions-unreachable) | test-config | open | 2 upstream scripts |
| [`tx-validation-timeouts-inert`](#tx-validation-timeouts-inert) | defect | open | 1 upstream script |
| [`peer-id-empty-under-test-context`](#peer-id-empty-under-test-context) | defect | open | 1 upstream script |
| [`setban-address-format`](#setban-address-format) | defect | open | 1 upstream script |
| [`listbanned-duplicate-entries`](#listbanned-duplicate-entries) | defect | open | 1 upstream script |
| [`getpeerinfo-omits-txninvsize`](#getpeerinfo-omits-txninvsize) | product-decision | open | 1 upstream script |
| [`reconsiderblock-ignores-ancestors`](#reconsiderblock-ignores-ancestors) | defect | open | no tests - found while porting `bsv-command-line-invalid-block.py` |
| [`reconsiderblock-error-contract`](#reconsiderblock-error-contract) | defect | open | no tests - found while porting `bsv-command-line-invalid-block.py` |
| [`invalidblockrequest-port-red`](#invalidblockrequest-port-red) | test-config | resolved | no tests - found while porting `invalidblockrequest.py` |
| [`opaque-block-reject-reason`](#opaque-block-reject-reason) | defect | open | no tests - found while porting `invalidblockrequest.py` |
| [`getpeerinfo-stalls-without-p2p-service`](#getpeerinfo-stalls-without-p2p-service) | defect | open | no tests - found while porting `net.py` |
| [`short-payload-read-as-peer-eof`](#short-payload-read-as-peer-eof) | defect | open | no tests - found while porting `bsv-empty-payload.py` |
| [`unknown-command-disconnects-off-regtest`](#unknown-command-disconnects-off-regtest) | defect | open | no tests - found while porting `bsv-empty-msg-cmd.py` |
| [`validated-tx-not-rechecked-across-activation`](#validated-tx-not-rechecked-across-activation) | defect | open | no tests - found while porting `bsv-genesis-pushonly.py` |
| [`legacy-whitelist-inert`](#legacy-whitelist-inert) | defect | open | no tests - found while porting `bsv-p2p-max-connections-from-addr.py` |
| [`per-ip-connection-count-leaks-on-duplicate-peer`](#per-ip-connection-count-leaks-on-duplicate-peer) | defect | open | no tests - found while porting `bsv-p2p-max-connections-from-addr.py` |
| [`max-peers-no-reservation-no-eviction`](#max-peers-no-reservation-no-eviction) | product-decision | open | no tests - found while porting `bsv-p2p-max-connections.py` |
| [`no-reject-for-undecodable-version`](#no-reject-for-undecodable-version) | defect | open | no tests - found while porting `bsv-p2p-version_msg.py` |
| [`unsupported-inv-type-ignored-not-disconnected`](#unsupported-inv-type-ignored-not-disconnected) | defect | open | no tests - found while porting `bsv-p2p-invalid-inv.py` |
| [`tx-inv-dropped-during-startup-window`](#tx-inv-dropped-during-startup-window) | unknown | investigating | no tests - found while porting `bsv-p2p-invalid-inv.py` |
| [`no-duplicate-protoconf-guard`](#no-duplicate-protoconf-guard) | defect | open | no tests - found while porting `bsv-protoconf-violation.py` |
| [`protoconf-payload-limit-not-honoured`](#protoconf-payload-limit-not-honoured) | defect | open | no tests - found while porting `bsv-protoconf.py` |
| [`protoconf-not-validated`](#protoconf-not-validated) | defect | open | no tests - found while porting `bsv-protoconf-versions-compatibility.py` |
| [`unsolicited-addr-accepted`](#unsolicited-addr-accepted) | defect | open | no tests - found while porting `p2p-unsolicited_addr.py` |
| [`pre-handshake-message-leak`](#pre-handshake-message-leak) | defect | open | no tests - found while porting `p2p-leaktests.py` |
| [`block-txcount-preallocation`](#block-txcount-preallocation) | defect | open | no tests - found while porting `bsv-block-bad-count.py` |
| [`porttest-suite-intermittent-failure`](#porttest-suite-intermittent-failure) | test-config | resolved | no tests - found while porting `bsv-peer-flood.py` |
| [`opaque-tx-reject-reason`](#opaque-tx-reject-reason) | defect | open | no tests - found while porting `invalidtxrequest.py` |

35 open gaps: 24 `defect`, 6 `product-decision`, 3 `test-config`, 2 `unknown`.

## submitblock-rpc

**submitblock and getblocktemplate are handleUnimplemented in services/rpc**

- **Kind:** `product-decision` - not ours to make; needs a deliberate decision elsewhere
- **Status:** `deferred`
- **Holds up:** 30 upstream scripts
- **Blocks:** every script needing `submitblock`, every script needing `getblocktemplate`

### Impact

The single largest cluster in bucket B. Both RPCs are largely adapters over the mining path Teranode already has (getminingcandidate / submitminingsolution), so most of bucket B is ordinary RPC work rather than an architectural limit.

### Plan

Deliberately NOT bundled into the porting exercise. Implementing them widens Teranode's public RPC surface — submitblock in particular accepts a fully-formed block from an untrusted caller, a different trust posture from the mining-candidate flow — so it belongs in its own proposal with its own review. Porting tests must not become a vehicle for expanding the node's API. Revisit after bucket A is exhausted.

## headers-announcement-disconnects

**A peer that announces a block by header is disconnected, which is how BSV peers announce blocks**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** 4 upstream scripts
- **Blocks:** `bsv-log-time-diff.py`, `minchainwork.py`, `bsv-toobusyrejectmessage.py`, `bsv-check-ttor-violation.py`
- **Found while porting:** `bsv-accept-header-with-ancestor.py`

### Impact

Found while porting bsv-accept-header-with-ancestor.py, whose first step is to announce a header and expect a getdata. Teranode disconnects instead. SyncManager.handleHeadersMsg (services/legacy/netsync/manager.go:2054) rejects any headers message that arrives while headersFirstMode is not set: if !sm.headersFirstMode.Load() { reason := fmt.Sprintf("Got %d unrequested headers from %s", ...) peer.DisconnectWithWarning(reason) return } headersFirstMode is an initial-sync state, so on a node that has caught up any headers message at all is treated as misbehaviour. The rule is inherited from btcd, where headers are only ever expected as the answer to a getheaders the node itself sent. That is not how BSV peers announce blocks. Since BIP130 a peer that has sent sendheaders announces new blocks by pushing a headers message, and bitcoin-sv answers one with a getdata for the block - which is precisely what the upstream script asserts. A peer following the norm is disconnected by Teranode. MEASURED: a peer announcing one valid header on a caught-up node is dropped within seconds and the node's peer list empties. The disconnect is specific to the announcement direction, not to headers generally - the same peer sending getheaders is answered with headers and keeps its connection, which the port asserts as a control. This compounds with legacy-block-announcement rather than duplicating it. That gap is the node not volunteering its own blocks to legacy peers; this is the node refusing to be told about theirs, and punishing the attempt. Together they leave legacy peering unable to participate in block announcement in either direction: Teranode learns of new blocks only by asking, and tells no one. NOT ESTABLISHED: what happens on a real network in practice. A peer that is disconnected will reconnect, and Teranode may still discover the block by polling with getheaders on its own schedule, so the operational cost may be churn and latency rather than missed blocks. Establishing that needs a two-node test against a real bitcoin-sv peer, which this exercise cannot set up - see peer-id-empty-under-test-context for why multi-node pairing is unavailable here.

### Plan

Raised as https://github.com/bitcoin-sv/teranode/issues/4785, and worth reading together with legacy-block-announcement since they are two halves of one picture and a fix for either alone leaves the other. The fix is to distinguish an announcement from an unsolicited flood rather than treating all headers outside initial sync as misbehaviour. bitcoin-sv's rule is the model: accept a headers message, and if it extends a known chain, request the blocks. Bounding it is reasonable - a cap on how many unrequested headers a peer may send before it is charged for them - but the current behaviour has no middle setting between initial sync and disconnect. Two things to settle first. Whether Teranode's legacy service is intended to participate in block announcement at all, or is meant purely as a pull-based sync client with libp2p doing real peering - that decision covers both gaps and should be made once. And what a real bitcoin-sv peer does after being disconnected this way, which decides whether this is a latency problem or a connectivity one.

## legacy-block-announcement

**A locally mined block is not announced to a connected legacy wire peer**

- **Kind:** `unknown` - needs diagnosis before anything else can be decided
- **Status:** `open`
- **Holds up:** 3 upstream scripts
- **Blocks:** `sendheaders.py`, `p2p-acceptblock.py`, `sendheadersban.py`

### Impact

Ports that need the node to volunteer a block cannot be written. Ports that request data (getheaders/getdata) are unaffected and work today. If the cause turns out to be production rather than test configuration, this is a defect in its own right: a real Teranode would fail to tell its BSV-wire peers about blocks it mined, leaving them to discover the tip by asking.

### Plan

Diagnose before implementing. Legacy announcement is driven by the blocks-final Kafka topic (services/legacy/netsync/manager.go, kafkaBlocksFinalListener); the listener is always enabled, so establish whether the in-process TestDaemon publishes to blocks-final at all. If it does not, the fix is in the test harness. If it does, the consumer path is at fault and the finding is escalated as a product defect outside this exercise. Set kind once known.

## net-rpcs-unimplemented

**Teranode implements only one of the eight net RPCs upstream net.py exercises**

- **Kind:** `product-decision` - not ours to make; needs a deliberate decision elsewhere
- **Status:** `open`
- **Holds up:** 3 upstream scripts
- **Blocks:** `net.py`, `bsv-addnode.py`, `p2p-txn_propagation2.py`

### Impact

Found while porting net.py. getpeerinfo is the only one that answers. getconnectioncount, getnettotals, getnetworkinfo, addnode, getaddednodeinfo and ping are all wired to handleUnimplemented in services/rpc/Server.go and return -1 "Command unimplemented"; setnetworkactive and getauthconninfo are not in the dispatch table at all and return -32601 "Method not found". Two of these are genuinely not-applicable — getauthconninfo and getpeerinfo's authconn field describe bitcoin-sv authenticated connections, which Teranode has no counterpart for. The other six are ordinary unimplemented RPCs, and services/rpc/Server.go's own rpcUnimplemented map says of getnetworkinfo that it "should ultimately be" implemented. Operationally the missing ones are the peer-administration and traffic-accounting surface an operator reaches for first.

### Plan

Not ours to decide, and deliberately not implemented here: adding them widens Teranode's public RPC surface, and addnode in particular lets an RPC caller direct the node's outbound connections. Ports assert what getpeerinfo supports — per-peer byte counters, connection arrival and departure — and TestBSVNet carries a tripwire subtest that fails as soon as any of these eight starts answering, so the waivers cannot quietly go stale.

## no-pending-response-limit

**A peer can queue unbounded response data by asking and never reading, measured at 349x for getheaders and 8857x for getdata**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** 2 upstream scripts
- **Blocks:** `bsv-p2p-max-pending-responses.py`, `bsv-factorMaxSendQueuesBytes.py`
- **Found while porting:** `bsv-p2p-max-pending-responses.py`

### Impact

Found while porting bsv-p2p-max-pending-responses.py, which exists because bitcoin-sv added a limit for exactly this. It supersedes an inconclusive measurement recorded against bsv-peer-flood.py, and the difference between the two attempts is the point. peer.queueHandler holds output for a peer in an unbounded list.List. When the peer stops reading, the socket write blocks, sendDoneQueue stops firing, and every further message is appended to that list with nothing to bound it. Teranode has no maxpendingresponses setting - the only pendingResponses in the tree is stallHandler tracking replies the node is waiting FOR, which is the other direction. The earlier attempt flooded pings and found no growth, and concluded nothing was demonstrable. That was a measurement problem: a ping response is a pong, so the amplification is about 1x and 30 MB of pings buys 30 MB of pongs. getheaders is the right lever, and it is the one upstream reaches for. MEASURED, on a 400-block chain: 200 getheaders requests sent from a connection that never read, then reading resumed and the socket drained. 6.19 MB came back containing ~200 headers messages - so every response was produced and held. Each request is 93 bytes on the wire and buys about 32.5 KB of queued response, which is 349x. The peer was never disconnected and the node stayed healthy. EXTRAPOLATED, not executed: a getheaders response is capped at 2000 headers, about 162 KB, so on any chain longer than 2000 blocks - which is every real one - the factor is about 1742x. A peer sending 1 MB of requests and reading nothing would have the node hold on the order of 1.7 GB for it. Nothing was driven to exhaustion here; 6.19 MB is what was actually observed, and the rest is arithmetic from the measured per-request figure and the documented cap. GETDATA IS WORSE, and this was the question left open above. A getdata naming one block is 61 bytes on the wire and buys the whole block. MEASURED: 20 such requests for the same ~900 KB block, from a connection that never read, queued 10.30 MB - 1,220 bytes of request against 10,805,414 bytes of response, or 8,857x. The peer was not disconnected and the node stayed healthy. Unlike getheaders there is no cap on the response: the factor scales with block size, so on a network with large blocks it has no ceiling worth quoting. bitcoin-sv bounds this separately from the pending-response count, with -factormaxsendqueuesbytes limiting total bytes queued across peers and answering REJECT_TOOBUSY once it is exceeded - see bsv-factorMaxSendQueuesBytes.py, which is blocked for want of it. So upstream has TWO defences here, a count and a byte budget, and Teranode has neither. Live heap growth during the flood read as NEGATIVE, which is not evidence of absence: the baseline was taken straight after mining 400 blocks, so collection of that garbage dominated the 6 MB under test. The drained-socket count is the reliable evidence here, not the heap figure.

### Plan

Raised as https://github.com/bitcoin-sv/teranode/issues/4780. bitcoin-sv treats it as a defect and shipped a limit with a settable maximum per request type. Distinguished from #4700 in the issue, and the two should not be merged: that one covers a peer that READS, repeating identical getheaders to burn bandwidth and CPU, with the CPU half mitigated by the SQL responseCache. This is a peer that never reads, so responses accumulate in the output queue - memory retained on the node, which no response cache helps with - and it also covers getdata, which #4700 does not. The measurements do serve as partial verification for #4700's checklist. The fix has an established shape: count responses queued but not yet written per peer, and disconnect when the count exceeds a configured maximum. bitcoin-sv splits it per request type (-maxpendingresponses_getheaders, -maxpendingresponses_gethdrsen) because response sizes differ wildly by type, which is worth copying rather than using one global number. Bounding total queued BYTES rather than message count would be closer to the actual resource and is the obvious alternative. A byte budget matters more than a message count here, and the getdata numbers are why: bounding pending RESPONSES by count would still let a peer queue arbitrary megabytes with a handful of block requests. bitcoin-sv carries both, and if only one is implemented it should be the byte budget. Still unmeasured: whether the same holds for other large responses - getblocks and the extended-message paths - and whether the queue is per-peer or shared, which decides whether one peer can degrade service for the rest. Neither was established.

## legacy-wire-message-subset

**The legacy wire implements a subset of the BSV protocol, missing four message families**

- **Kind:** `product-decision` - not ours to make; needs a deliberate decision elsewhere
- **Status:** `open`
- **Holds up:** 2 upstream scripts
- **Blocks:** `bsv-merkle-pruned-block.py`, `bsv-getdata-datareftx.py`
- **Found while porting:** `bsv-merkle-pruned-block.py`

### Impact

Established while porting bsv-merkle-pruned-block.py and bsv-getdata-datareftx.py, both of which turned out to need messages that do not exist. Recorded as reference rather than as a complaint: it bounds what this exercise can ever cover, and it is the kind of fact that is expensive to rediscover one script at a time. go-wire's command set, read from message.go, is: addr, authch, authresp, block, cfcheckpt, cfheaders, cfilter, createstrm, extmsg, exttx, feefilter, filteradd, filterclear, filterload, getaddr, getblocks, getcfcheckpt, getcfheaders, getcfilters, getdata, getheaders, headers, inv, mempool, merkleblock, notfound, ping, pong, protoconf, reject, sendcmpct, sendheaders, streamack, tx, verack, version. Four families bitcoin-sv speaks are absent: - gethdrsen / hdrsen, enriched headers carrying a coinbase merkle proof - datareftx, the miner-ID dataref transaction reply - blocktxn / getblocktxn / cmpctblock, compact blocks - note sendcmpct IS present, so a peer can negotiate compact blocks that cannot then be used - dsdetected, the double-spend notification, and revokemid SIZED HONESTLY, because the raw count misleads. Thirteen upstream scripts use one of these messages, but only TWO are bucket-A entries this exercise would otherwise have ported - bsv-merkle-pruned-block.py and bsv-getdata-datareftx.py, both now blocked. The other eleven were already classified B or C on their own merits. So the cost to coverage is two scripts, not thirteen. Most of the absences look deliberate rather than accidental. Compact blocks are a bandwidth optimisation for small-block networks and Teranode addresses the same problem with subtrees; miner ID and dsdetected are features rather than protocol plumbing. The one that reads oddly is sendcmpct being present while the three messages it negotiates are not: a peer that sends sendcmpct and is answered will believe compact blocks are available.

### Plan

UNLOGGED - awaiting review, and mostly this needs a decision recorded rather than work done. Three separable questions. Whether the legacy wire is meant to be a full BSV peer or a sync client that speaks enough to fetch blocks. Everything else follows from that, and it is the same question legacy-block-announcement and headers-announcement-disconnects raise from the announcement side - worth answering once for all three. Whether sendcmpct should be withdrawn. Advertising a negotiation whose follow-up messages are unimplemented is the sort of thing that produces a confusing peer report rather than a clean unsupported. Not measured what a real peer does after negotiating and then getting nothing - worth checking before deciding it is harmless. Whether gethdrsen is worth having on its own merits. It is the only one of the four that is pure protocol rather than a feature, it gives a peer a coinbase merkle proof without downloading the block, and Teranode's subtree model should make producing one cheap.

## no-cpfp-ancestor-fee-accounting

**There is no accepted-but-unmineable state, so child-pays-for-parent cannot work**

- **Kind:** `product-decision` - not ours to make; needs a deliberate decision elsewhere
- **Status:** `open`
- **Holds up:** 2 upstream scripts
- **Blocks:** `bsv-cpfp.py`, `bsv-cpfp-1000-children.py`
- **Found while porting:** `bsv-cpfp.py`

### Impact

Found while porting bsv-cpfp.py, and it is why that script is blocked rather than partly ported. bitcoind keeps two fee thresholds. -mindebugrejectionfee decides what enters the mempool; -minminingtxfee decides what enters the mining candidate. A transaction between the two is accepted, held, and left out of blocks until a descendant arrives paying enough to cover the whole unpaid ancestry - at which point the ancestors become mineable too. That is child-pays-for-parent, and the upstream script walks through it transaction by transaction. Teranode has the same setting name and applies it at a different point. minminingtxfee is a policy check inside ValidateTransaction; its own settings documentation says "Fee checks are POLICY only ... BDK enforces the floor during ValidateTransaction in policy mode". So the threshold decides ACCEPTANCE, not mineability, and there is no state in between. MEASURED: a zero-fee transaction is refused outright with "TX_POLICY (39): transaction fee is too low". A one-satoshi-fee transaction is accepted and appears in the mining candidate immediately - getminingcandidate num_tx goes from 0 to 1 with no intermediate state in which it is held but not mined. There is nothing for a child to rescue, because an underpaying parent never got in. Consequences worth weighing, none of which this exercise can settle. For senders: a transaction that underpays cannot be fixed by a child. On bitcoind it waits in the mempool and a CPFP child rescues it; here it is refused, so the sender must construct and send a replacement. Wallets that implement fee bumping by CPFP - the standard approach, since BSV has no RBF - have nothing to bump. For miners: no ability to hold marginal transactions speculatively in the hope of a paying descendant. Whether that is worth having at BSV fee levels is a commercial judgement, not a technical one. In its favour: one threshold is simpler, gives the sender an immediate and unambiguous answer rather than silent limbo, and removes a queue an attacker could fill with transactions that never become mineable. This is filed as a product-decision rather than a defect because the current design is coherent and may well be deliberate.

### Plan

UNLOGGED - awaiting review, and this one genuinely is a decision rather than a fix. The question is whether Teranode intends to support CPFP at all. If yes, it needs the second threshold: accept above a relay floor, mine above a higher mining floor, and account for unpaid ancestry when a descendant arrives. That is a real feature in block assembly, not a configuration change, and it brings the queue-filling exposure that goes with holding unmineable transactions. If no - which is defensible - then the useful action is documentation rather than code. minminingtxfee shares a name with a bitcoind setting that does something materially different, and an operator migrating from bitcoind would reasonably expect the bitcoind behaviour. Saying plainly that Teranode refuses rather than defers, and that CPFP is therefore not available, would prevent that surprise. Since resolved: bsv-cpfp-1000-children.py was read and depends on the same absent state - its zero-fee parent is refused at validation, so the secondary mempool it then asserts about never contains anything. It is blocked on this gap. bsv-conflict.py ported cleanly, so the double-spend eviction territory is unaffected.

## log-assertions-unreachable

**No port can assert on the node's log output, which costs fidelity on a countable set of entries**

- **Kind:** `test-config` - ours to fix, in this repository
- **Status:** `open`
- **Holds up:** 2 upstream scripts
- **Blocks:** `spamlog.py`, `bsv-log-time-diff.py`
- **Found while porting:** `spamlog.py`

### Impact

Ours to fix rather than a node defect, and quantified rather than asserted, because the cost turned out to be smaller than it first looked. An in-process TestDaemon writes through a ulogger the test cannot inspect. daemon.TestOptions chooses between four hardcoded logger factories (daemon/test_daemon.go:452-480) on UseUnifiedLogger, EnableDebugLogging and EnableErrorLogging, with no way to supply one - so there is no log sink to read and no counter to bound. bitcoin-sv's framework has check_for_log_msg and a log file on disk, and upstream leans on both. MEASURED against the upstream suite and this registry: 35 of the 279 upstream scripts assert on log content or log size. Of those, 12 are bucket A - 8 already ported with a log assertion waived, 4 still todo. The rest are in buckets B and C and are not in scope to port, so the honest figure for what this obstacle costs is about 13 entries: retiring waivers on 8 landed ports, unblocking spamlog.py, which has no other assertion, and improving 4 not yet started. Not the sweeping unlock it appeared to be before counting. Each of those 8 waivers is defensible on its own - in most cases the port asserts the behaviour the log line stands for, and in two cases (bsv-block-bad-count.py, bsv-p2p-invalid-inv.py) it asserts something stronger than the log could show. The loss is narrower than the count suggests: what cannot be checked is that the node says the right thing, as distinct from doing it.

### Plan

NEEDS A DECISION rather than filing, because the fix touches shared test infrastructure rather than this package. Not done unilaterally for that reason. The change is small and additive: a LoggerFactory field on daemon.TestOptions, honoured ahead of the four existing branches and defaulting to today's behaviour when nil. wirepeer would then grow a helper returning a daemon plus a captured-log accessor, and ports could assert on log content the way upstream does. Nothing existing changes behaviour. The reservation worth weighing against it: asserting on log strings couples ports to log wording, which is not a contract and will drift, and several of the current waivers are better than the assertion they replace. If it is done, the guidance should be to assert on logs only where the log is the ONLY evidence - spamlog.py being the clear case - and to keep preferring behavioural assertions everywhere else. Until decided, ports keep waiving log assertions with a reason, which is the current practice and is working.

## tx-validation-timeouts-inert

**The script-validation timeout settings are declared and documented but never loaded or read**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** 1 upstream script
- **Blocks:** `bsv-pvq-timeouts.py`
- **Found while porting:** `bsv-pvq-timeouts.py`

### Impact

Found while establishing why bsv-pvq-timeouts.py cannot be ported. The two settings that script drives - maxstdtxvalidationduration and maxnonstdtxvalidationduration - exist in Teranode by name, with defaults of 3ms and 1000ms and long descriptions in settings/policy_settings.go explaining what they protect against. They do nothing. VERIFIED IN CODE, not inferred from the documentation. Every reference to MaxStdTxValidationDuration outside its own declaration is in policy_settings_test.go, which exercises the getter and setter against each other. The line in settings/settings.go that would populate it from configuration is COMMENTED OUT (settings.go:120-121), so the field is not even loaded - it would read as zero regardless of what an operator set. Nothing in the validator, in GoBDK's Go-side wrapper, or anywhere else consults it. Teranode's own documentation for these fields says so, twice and in almost those words: "Setting exists but timeout enforcement NOT actively implemented in current Teranode (placeholder for future)", and "Primary DoS protection is currently economic (fees scale with complexity)". So this is known rather than hidden. What makes it worth a gap anyway is that the settings are exposed on the configuration surface with plausible defaults and a description promising DoS protection, which is the same shape as legacy-whitelist-inert: a defence an operator can configure, verify is set, and reasonably believe is working. NOT ESTABLISHED, and it decides whether this matters at all: whether GoBDK imposes its own bound on script execution time or operation count. If it does, the economic argument in the documentation plus that internal bound may be adequate and these settings are merely misleading. If it does not, then script validation time is unbounded in a network whose restored opcodes - OP_MUL, OP_SUBSTR and the rest - exist specifically to allow expensive computation. That question was not answered here because it needs a reading of the C++ engine rather than the Go tree.

### Plan

UNLOGGED - awaiting review. Answer the GoBDK question first, because it decides whether this is a documentation defect or a missing defence. If GoBDK already bounds script execution, the fix is to stop advertising settings that do nothing: remove them, or mark them explicitly as unimplemented in the description rather than only in the middle of a long one. An operator grepping for DoS controls should not find a knob that silently has no effect. If it does not, then implementing the timeout is the real work, and the two settings are already specified for it - which is presumably why they were written down in the first place. Note the demotion behaviour bitcoin-sv pairs them with (move a slow transaction to a lower-priority queue rather than rejecting it) is a scheduling design, not part of the timeout itself, and Teranode need not copy it to get the protection. Worth doing either way, and cheap: uncomment or delete settings.go:120-121. A field that is declared, defaulted and documented but never loaded is a trap for the next person who reads the settings file.

## peer-id-empty-under-test-context

**daemon.ConnectToPeer builds an invalid multiaddr under SETTINGS_CONTEXT=test**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** 1 upstream script
- **Blocks:** `minchainwork.py`
- **Found while porting:** `invalidblockrequest.py`

### Impact

Split out of invalidblockrequest-port-red, which is resolved by superseding the test that tripped over this rather than by fixing it. Settings.P2P.PeerID is empty under SETTINGS_CONTEXT=test because settings.conf defines p2p_peer_id for only a few contexts, and daemon.peerAddress interpolates it unconditionally — producing "/dns/127.0.0.1/tcp/<port>/p2p/" with an empty peer ID. Every multi-node test that pairs daemons over Teranode's native libp2p P2P under this context fails to form its ring, and fails before asserting anything, which is the worst shape of failure: it looks like the assertion is wrong. Nothing in this package depends on it any more, but the helper is shared, so any future multi-node port will hit it again. Unrelated to wirepeer, which speaks the legacy wire protocol. No longer only theoretical: minchainwork.py is blocked on it, needing three nodes wired in a line. The two legacy-wire alternatives are themselves blocked by legacy-block-announcement and headers-announcement-disconnects, so there is currently no route by which one node can hand a block to another in a test.

### Plan

Raised as https://github.com/bitcoin-sv/teranode/issues/4776. Report as a harness defect against daemon.peerAddress. The fix is either to define p2p_peer_id.test in settings.conf or to have peerAddress omit the /p2p/ component when PeerID is empty; the second is safer, since it fails loudly rather than silently building an address nothing can dial. Not blocking any port today, so recorded rather than worked.

## setban-address-format

**setban never registers a ban with the legacy service, so operators cannot ban legacy peers**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** 1 upstream script
- **Blocks:** `disconnect_ban.py`

### Impact

Found while porting disconnect_ban.py. handleSetBan validates its argument as a bare IP or CIDR (services/rpc/handlers.go isIPOrSubnet), but the legacy leg compares it against sp.Addr(), which is "IP:port" (services/legacy/Server.go banPeer) — so the comparison never matches and the call returns "tried to ban legacy peer but peer not found". The ban is not merely ineffective, it is never recorded with the legacy service at all: with the legacy service alone setban fails outright, and with the P2P service running the ban lands only in that service's list, which is why listbanned shows it while the legacy peer stays connected. Two ban paths exist and only one is broken — the node still bans legacy peers on misbehaviour (server.BanPeer in peer_server.go, called with the peer object, no address matching involved, and exercised by the bsv-ban-useragents.py port). What does not work is banning on request. Operationally an operator cannot evict or block a legacy peer, which is the main thing setban exists for. Unbanning a subnet separately logs "can't split ban peer 10.0.0.0/24: missing port in address".

### Plan

Raised as https://github.com/bitcoin-sv/teranode/issues/4773. Report as a product defect. The fix is small and localised, because enforcement already keys on the bare host: peer_server.go splits the host off and looks up state.banned by IP, both on admission and in OnBlock. So the ban list already speaks the same language setban does, and only banPeer's comparison disagrees — compare hosts rather than host:port, and CIDR-match subnets, then have handleUnbanPeerMsg accept CIDR too. Not verified: whether a peer is refused on reconnection after an automatic ban. That is read from the lookup being keyed by host, not tested. Until fixed, ports run against a daemon with both peer services (NewLegacyDaemonWithP2P) and waive the disconnect assertion.

## listbanned-duplicate-entries

**listbanned reports every ban twice when both peer services are running**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** 1 upstream script
- **Blocks:** `disconnect_ban.py`

### Impact

handleListBanned concatenates the P2P service's list and the legacy service's list, which are views of the same bans, so each address appears once per service. Upstream tests assert on len(listbanned()); those assertions cannot be ported literally, and any operator tool that counts bans is wrong by a factor of the number of running peer services.

### Plan

Raised as https://github.com/bitcoin-sv/teranode/issues/4775. Report as a product defect; the fix is to deduplicate in handleListBanned. Ports compare the distinct set of banned addresses in the meantime.

## getpeerinfo-omits-txninvsize

**getpeerinfo does not report per-peer inv queue depth, so propagation-queue tests cannot observe anything**

- **Kind:** `product-decision` - not ours to make; needs a deliberate decision elsewhere
- **Status:** `open`
- **Holds up:** 1 upstream script
- **Blocks:** `p2p-txn_propagation.py`

### Impact

Found while triaging p2p-txn_propagation.py. This is an observability gap rather than an architectural one: Teranode's legacy service does maintain a per-peer inv send queue (services/legacy/peer/peer.go, invSendQueue in the queue handler), it just never publishes its depth. bitcoin-sv exports it as getpeerinfo's txninvsize, and the upstream test is built entirely on that field - assert every peer starts at zero, feed 200 transactions, assert the queue is non-empty but bounded, then assert it drains. None of those can be written against Teranode, because the number is not reachable from outside the process. There is also no -broadcastdelay equivalent to slow relay down enough to observe a queue mid-drain. Beyond testing, an operator cannot see whether a peer's relay queue is backing up, which is the first thing worth knowing when propagation is slow.

### Plan

Not ours to decide: it means adding a field to a public RPC response. If it is added, the upstream assertions port almost directly. Until then p2p-txn_propagation.py stays blocked, and note that a port of it would still need funding-shim for the 200 wallet-built transactions and a multi-node mesh for the cross-node mempool checks, so this gap alone does not unblock it.

## reconsiderblock-ignores-ancestors

**reconsiderblock refuses a block whose ancestor is still invalid, where bitcoin-sv clears both**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `bsv-command-line-invalid-block.py`
- **Found while porting:** `bsv-command-line-invalid-block.py`

### Impact

Found while porting bsv-command-line-invalid-block.py, and it is the scenario that script actually runs: invalidate two adjacent blocks, then reconsider the HIGHER of the two and expect the chain to come all the way back. bitcoin-sv can do that because Core's ResetBlockFailureFlags clears the failure flag on the named block's ancestors as well as its descendants, so naming any block in the invalidated span is enough. Teranode refuses: Block failed revalidation: [RevalidateBlock][<hash> (height: 4 ...)] failed block re-validation -> BLOCK_INVALID (11): [ValidateBlock][<hash>] parent block is invalid MEASURED both ways on the same chain shape, five blocks with heights 3 and 4 invalidated. Reconsidering height 3, the lower, restores the chain to height 5 in full - so the descendant walk works and clears the separately-invalidated block above it. Reconsidering height 4, the higher, fails with the error above and leaves the tip where it was. The failed call changes nothing, which is at least clean. So the capability is there and only the ancestor direction is missing. The operational cost is that an operator must know, and name, the LOWEST block they invalidated. Naming any other gets an error that describes the immediate cause - "parent block is invalid" - without saying which block to name instead, and an operator who invalidated several blocks over time during an incident has to reconstruct that order themselves.

### Plan

Raised as https://github.com/bitcoin-sv/teranode/issues/4777. Report as a defect; it is a divergence from bitcoin-sv's documented behaviour for an RPC that exists to undo operator mistakes, which is exactly when a second, confusing error is least welcome. Cross-referenced from the issue: #4717 is adjacent and contradicted on this point. It claims RevalidateBlock leaves auto-inherited DESCENDANTS invalid, but reconsidering the lower of two separately-invalidated blocks restored the full chain, so the descendant direction works and the ancestor direction is the gap. That issue asks to be verified against master; this is a partial verdict on it. Two possible fixes and they are not equivalent. Either walk ancestors clearing the invalid flag, matching Core's ResetBlockFailureFlags, or keep the current restriction and make the error say what to do - naming the lowest invalid ancestor in the message would turn a dead end into an instruction. The first matches bitcoin-sv and is what a ported test would assert; the second is cheaper and may be preferable if the narrower behaviour is deliberate. Worth checking while in there, since it is the same code path and was not established: whether reconsiderblock on a block with NO invalidated ancestors but several invalidated descendants clears all of them, or only the first.

## reconsiderblock-error-contract

**reconsiderblock returns the wrong RPC error code for an unknown block, and leaks the storage layer**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `bsv-command-line-invalid-block.py`
- **Found while porting:** `bsv-command-line-invalid-block.py`

### Impact

Found while porting bsv-command-line-invalid-block.py, which asserts assert_raises_rpc_error(-5, 'Block not found', reconsiderblock, <unknown hash>). MEASURED, Teranode returns code -25 with this message: Block failed revalidation: SERVICE_ERROR (59): [RevalidateBlock][1000...0000] failed to get block -> BLOCK_NOT_FOUND (10): error in GetBlock -> UNKNOWN (0): sql: no rows in result set Two separate problems. The code is wrong for the condition: -5 is RPC_INVALID_ADDRESS_OR_KEY, which is what bitcoind returns for an unknown block, while -25 is RPC_VERIFY_ERROR, meaning the block was rejected on its merits. A caller branching on the code cannot tell "no such block" from "that block is bad", and those call for different responses. And the message says far too much: four levels of internal error chain, the internal service and method names, and "sql: no rows in result set", which tells an unauthenticated-at-the-RPC-boundary caller that the backend is SQL and that the lookup reached it. The RPC surface is credentialed, so this is a disclosure to an operator rather than to the world - but it is still internal detail crossing a documented API boundary, and the same wrapping is likely to appear on other handlers. Worth noting as a contrast rather than a separate finding: this is the opposite failure to opaque-block-reject-reason and opaque-tx-reject-reason, where peers are told nothing useful. Peers get a fixed string with no cause; RPC callers get the whole internal chain. Both come from the same absent boundary - nobody is deciding what each audience should be told - so they are worth reviewing together even though the fixes point in opposite directions.

### Plan

Raised as https://github.com/bitcoin-sv/teranode/issues/4778. Report as a defect, and settle it alongside the two opaque-reject gaps, since the underlying question is one question: what does each audience get told when something fails. For this handler specifically: map BLOCK_NOT_FOUND to -5 with the message "Block not found", matching bitcoind, and keep the internal chain in the log where it already goes - services/rpc/Server.go:1015 logs it in full, so nothing is lost by trimming what crosses the wire. Worth auditing at the same time whether other RPC handlers wrap internal errors into their responses the same way. The pattern here is a generic "wrap and return" rather than anything specific to reconsiderblock, so it probably is not the only one.

## invalidblockrequest-port-red

**The pre-existing TestBSVInvalidBlockRequest port fails under SETTINGS_CONTEXT=test**

- **Kind:** `test-config` - ours to fix, in this repository
- **Status:** `resolved`
- **Holds up:** no tests - found while porting `invalidblockrequest.py`
- **Found while porting:** `invalidblockrequest.py`

### Impact

The only port that predates this exercise did not pass. td.ConnectToPeer builds an invalid multiaddr — "/dns/127.0.0.1/tcp/59641/p2p/" with an empty peer ID — so the three-node ring never forms and the test fails before asserting anything. The cause is that Settings.P2P.PeerID is empty: settings.conf defines p2p_peer_id only for a few contexts and not for "test", and daemon.peerAddress interpolates it unconditionally. That makes this a harness defect affecting every multi-node test run under SETTINGS_CONTEXT=test, not just this port. Unrelated to wirepeer; it uses Teranode's native libp2p P2P, not the legacy wire service.

### Plan

RESOLVED by superseding the test, not by fixing peerAddress. The multi-node test was replaced outright by a single-node port that never calls ConnectToPeer, so nothing in this package depends on the broken helper any more. The replacement asserts all three defects upstream checks: a duplicated transaction, a transaction spending the same outpoint twice, and a coinbase paying above subsidy — plus, over wirepeer, that a valid block is requested via getdata and becomes the tip. One correction to the plan as originally written: it assumed wirepeer would let the port assert the upstream reason strings. Measurement says otherwise — Teranode's wire reject carries the fixed reason "block rejected" for every cause (see the opaque-block-reject-reason gap) — so the reasons are asserted where they are actually distinguishable, on the error ProcessBlock returns. The underlying peerAddress/PeerID defect is untouched and now carried by peer-id-empty-under-test-context.

## opaque-block-reject-reason

**Teranode's wire reject gives the same reason for every invalid block**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `invalidblockrequest.py`
- **Found while porting:** `invalidblockrequest.py`

### Impact

Found while porting invalidblockrequest.py. When block validation fails, netsync/manager.go answers the peer with PushRejectMsg(CmdBlock, RejectInvalid, "block rejected", hash) — the reject code (16) matches bitcoin-sv, but the reason is that one fixed string whatever went wrong, while the specific cause is available at the call site in the error being handled. A peer therefore cannot tell a duplicated transaction from an over-paying coinbase from a bad merkle root. bitcoin-sv sends bad-txns-duplicate, bad-txns-inputs-duplicate, bad-cb-amount and so on, and the functional tests assert on exactly those strings, so this is the single reason the three reject assertions in invalidblockrequest.py cannot be reproduced over the wire. Operationally it costs a peer operator the ability to diagnose why their block was refused without access to our logs. Interesting counter-example: the duplicate-input case does surface upstream's string verbatim, because GoBDK — the same script engine bitcoin-sv runs — produces it. The reason strings exist inside Teranode; they are lost at the wire boundary.

### Plan

Raised as https://github.com/bitcoin-sv/teranode/issues/4772. Report as a product defect. The fix is localised: derive the reject reason from the error already in hand at the PushRejectMsg call site rather than hardcoding it. Care is needed not to leak internal error text to peers — bitcoin-sv's strings are a short closed vocabulary, and matching it is the point — so a mapping from Teranode error to upstream reason string is preferable to passing err.Error() through. Until fixed, ports assert rejection reasons on the ProcessBlock error instead of over the wire.

## getpeerinfo-stalls-without-p2p-service

**RPCs whose handler touches the P2P client take ~10 seconds when that service is not running**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `net.py`
- **Found while porting:** `net.py`

### Impact

BROADER THAN THE NAME. Recorded originally for getpeerinfo, but a second RPC was measured with the same stall while porting bsv-command-line-invalid-block.py: reconsiderblock takes 9.77s without the P2P service and 2ms with it - the same figure, twice, to within a hundredth of a second. invalidateblock is 2ms either way, so the stall belongs to the handlers that touch the P2P client rather than to any one RPC or to the work being done. The name is kept so existing references still resolve. That reframes the operational cost: it is not one status RPC being slow, it is an unknown subset of the RPC surface, including at least one state-changing call an operator would reach for during an incident. Which handlers are affected has not been enumerated - that enumeration is the first thing worth doing. The original finding follows. Found while porting net.py, and measured: with the legacy peer service alone a getpeerinfo call takes 9.757s; with the P2P service also running the same call takes 0.5ms, and returns byte-for-byte the same answer. The cause is that daemon/daemon_services.go always builds a P2P client for the RPC service (GetP2PClient at line 501, not conditional on the service running), and handleGetpeerinfo waits on GetPeerRegistry before answering. With nothing listening, that call is retried by the gRPC interceptor — grpc_max_retries 40 at grpc_retry_backoff 250ms, so ~10s — and only then does handleGetpeerinfo log the failure at info level and return the legacy half of the answer. Two distinct problems: the RPC service should not wait on a service that is not running, and the 9.757s wait exceeds the 5s RPC.ClientCallTimeout that handleGetpeerinfo puts on peerCtx, so the deadline is not being honoured by the retry interceptor. The same p2pClient backs setban, listbanned and isbanned, which pay the same cost. This is not test-only: legacy-only is a supported deployment, and there every peer-administration RPC is ~10s slow and silent about why.

### Plan

Raised as https://github.com/bitcoin-sv/teranode/issues/4774. Report as a product defect. Two fixes, either of which helps: make the RPC service's P2P client construction conditional on the service being configured, and make the gRPC retry interceptor stop retrying once the call context's deadline has passed. Ports run against NewLegacyDaemonWithP2P so that polling assertions are not fighting a 10s floor per poll.

## short-payload-read-as-peer-eof

**A message whose payload is too short to decode is misread as the peer hanging up, and a healthy peer is silently disconnected**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `bsv-empty-payload.py`
- **Found while porting:** `bsv-empty-payload.py`

### Impact

Found while porting bsv-empty-payload.py. A well-framed message whose payload is shorter than its command requires - declared length and checksum both correct, so nothing about the framing is wrong - makes go-wire's decoder hit the end of the payload reader and return a bare io.EOF. Measured against go-wire v1.2.10: a reject frame with declared length 0 returns exactly io.EOF, not a *wire.MessageError and not io.ErrUnexpectedEOF. peer.inHandler (services/legacy/peer/peer.go) then has no way to tell that from the socket dying. isAllowedReadError requires a *wire.MessageError, so it declines. shouldHandleReadError matches err == io.EOF and returns false with the debug line "Remote peer has disconnected (EOF)". Control falls to the else branch, which matches io.EOF again and calls DisconnectWithWarning("Peer disconnected due to: EOF"). Three things are wrong at once. The peer is disconnected though it did nothing but send a short message. It is told nothing - no reject is pushed, because that branch was skipped. And the operator-visible record blames the peer for a disconnect the node itself initiated, at debug level, so a node shedding peers this way looks like a node whose peers keep leaving. bitcoin-sv catches the same deserialisation failure and swallows it, and for the reject message specifically says why: "Avoid feedback loops by preventing reject messages from triggering a new reject message" (net/net_processing.cpp). The failure mode is not confined to reject - any command whose decoder runs off the end of a truncated-but-consistent payload takes the same path.

### Plan

Raised as https://github.com/bitcoin-sv/teranode/issues/4771. Not fixed here; a porting change should not also change node behaviour. The fix is to distinguish "the payload ended early" from "the connection ended", which needs the decode-time EOF wrapped as a *wire.MessageError in go-wire rather than passed through bare - at which point Teranode's existing malformed-message handling applies unchanged and the peer gets a reject instead of a mystery disconnect. TestBSVEmptyPayload asserts the current behaviour in a tripwire subtest, so the day it changes the port fails and its waivers get rewritten.

## unknown-command-disconnects-off-regtest

**Teranode disconnects any peer sending a message command go-wire cannot decode, including nine a real bitcoin-sv node can send**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `bsv-empty-msg-cmd.py`
- **Found while porting:** `bsv-empty-msg-cmd.py`

### Impact

Found while porting bsv-empty-msg-cmd.py. Ignoring unrecognised message commands is a deliberate extensibility rule in the reference implementation, stated in the code: "Ignore unknown commands for extensibility" (net/net_processing.cpp). It is what lets the network add a message type without partitioning itself. Teranode does not follow it. go-wire's makeEmptyMessage returns an error for any command it has no case for, which peer.inHandler treats as a malformed message: a "malformed" reject followed by DisconnectWithWarning. The one escape is isAllowedReadError, a btcd-inherited regression-test affordance that tolerates the error only when ChainCfgParams.Net is RegTestNet AND the peer is 127.0.0.1. Every wirepeer test satisfies both, which is the only reason TestBSVEmptyMsgCmd observes upstream's outcome at all. On mainnet or testnet the same frame costs the peer its connection. This is not hypothetical. Comparing bitcoin-sv's NetMsgType table against go-wire v1.2.10's makeEmptyMessage, nine commands a bitcoin-sv node can put on the wire have no case: blocktxn, cmpctblock, datareftx, dsdetected, getblocktxn, gethdrsen, hdrsen, revokemid, sendhdrsen. dsdetected is the sharpest of them, because it needs no negotiation and no request. On receiving a valid double-spend-detected message a bitcoin-sv node relays it to every peer it has (net/net_processing.cpp, ForEachNode) - so a Teranode legacy peer on a live network is disconnected by an ordinary, correct peer doing the thing that message exists for, and is disconnected again on reconnect the next time one propagates.

### Plan

Raised as https://github.com/bitcoin-sv/teranode/issues/4770. Not fixed here; a porting change should not also change node behaviour. Two separable pieces. The rule itself: an unrecognised command should be discarded and the peer left alone on every network, not only under the regtest affordance - that is the interop-correct behaviour and the smaller change. Then, separately, decide which of the nine Teranode should actually understand rather than merely tolerate; dsdetected and the hdrsen family are the ones with live traffic behind them. It stands on its own merits rather than on this exercise's: the coverage cost here is nil, the cost on a real network is dropped peers.

## validated-tx-not-rechecked-across-activation

**A transaction validated below an activation height is never re-verified above it, so two nodes can disagree on the same block**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `bsv-genesis-pushonly.py`
- **Found while porting:** `bsv-genesis-pushonly.py`

### Impact

Found while porting bsv-genesis-pushonly.py, then diagnosed and measured. The port's waived assertion - that a non-push-only transaction is absent from the mempool after its block is invalidated - looked like mempool bookkeeping Teranode has no counterpart for. It is not. The underlying behaviour is a consensus one. Teranode stores a transaction's validation verdict in the UTXO store keyed by txid alone. Subtree validation script-verifies only the transactions whose metadata is ABSENT: ValidateSubtreeInternal (services/subtreevalidation/SubtreeValidation.go) batch-decorates every hash in the subtree, and routes only the unset ones through processMissingTransactions -> blessMissingTransaction -> validator. A transaction already in the store is accepted on the strength of stored metadata that records no script flags, and therefore no activation height. Measured on one node, Genesis moved to height 6, two transactions of identical shape whose unlocking script is OP_1 OP_1 OP_ADD, each in its own candidate block at height 7. The transaction the node had never seen is rejected: TX_INVALID (31), GoBDK "Only non-push operators allowed in signatures". The transaction the node validated at height 4 is ACCEPTED and becomes the tip. Same node, same rule, same height, opposite verdicts - the only difference is whether the node had seen the transaction before the fork. That is a chain split at an activation boundary, not a bookkeeping divergence: a node that saw the transaction pre-activation accepts the block, a node that did not rejects it. Reaching it needs no invalidateblock - an ordinary reorg does, because a reorg onto a longer competing chain moves the tip FORWARD past the activation height while dropping the block that held the transaction. Block assembly then readmits it: loadUnminedTransactions (services/blockassembly/BlockAssembler.go) filters only on Skip, already-mined-on-best-chain, and optional input-conflict checks (validateUnminedTxInputs, which looks at input existence and double spends, never at scripts), and subtreeProcessor.Reorg re-adds moveBackBlocks' transactions for re-mining. Nothing on the mining path re-checks either: submitMiningSolution runs model.Block.Valid, which is header, PoW, coinbase and merkle structure only, then calls blockchainClient.AddBlock directly. Measured: the readmitted transaction was mined into a block at height 7. bitcoin-sv defends against exactly this and names it. GetScriptCacheKey(tx, flags) puts the flags in the script-cache key, and because per-INPUT flags still are not covered, FinalizeEraCrossing (src/validation.cpp) clears the mempool and the script execution cache whenever a height transition crosses the Genesis or Chronicle line - called from both ConnectTip and DisconnectTip, so in either direction. The comment at src/validation.cpp:2699 states the reason outright. Teranode has no equivalent invalidation. Chronicle makes this live rather than historical: go-chaincfg carries a mainnet ChronicleActivationHeight marked "temporary and subject to change", so the boundary is ahead of the network, not behind it.

### Plan

Raised as https://github.com/bitcoin-sv/teranode/issues/4769. Escalate as a product defect outside this exercise - it is consensus behaviour, and a porting change must not alter node behaviour. The fix has the shape bitcoin-sv already settled on: make a stored validation verdict carry the script flags, or the activation era, it was produced under, and re-verify when the era of the height being validated differs. Note that Teranode has no single cache to flush - the verdict lives in the UTXO store as durable metadata, so FinalizeEraCrossing's clear-everything approach does not transplant directly, and the era needs to be part of what is stored or part of what is checked. Two things left to establish, carried into the issue, neither of which changes the finding above. Whether the same reasoning reaches other height-dependent consensus rules that are checked once at validation time and then trusted (nLockTime finality, coinbase maturity) - the caching mechanism is generic, so the presumption is yes. And whether a node syncing from scratch, which validates every transaction at its block's height, agrees with the pre-fork node or the fresh one, since that decides which side of a split a restarted or rebuilt node lands on. TestBSVGenesisPushOnly's readmission_tripwire subtest asserts the readmission half of the current behaviour, so the port's waiver breaks as soon as any of this is fixed. The full reproduction is not in the tree; it is: Genesis to height 6 via wirepeer.WithGenesisActivationHeight, fund two OP_TRUE outputs confirmed at height 3, mine one of them spent by OP_1 OP_1 OP_ADD in a block at height 4, build three empty blocks from the funding block so the reorg lands the tip at height 6, then offer two blocks at height 7 - one holding the never-seen spend, one holding the already-validated spend - and compare.

## legacy-whitelist-inert

**The legacy peer whitelist cannot be configured, and the per-IP connection cap would ignore it anyway**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `bsv-p2p-max-connections-from-addr.py`
- **Found while porting:** `bsv-p2p-max-connections-from-addr.py`

### Impact

Found while porting bsv-p2p-max-connections-from-addr.py, whose second block asserts that -whitelist exempts an address from -maxconnectionsfromaddr. Teranode grants no such exemption, for two independent reasons. First, and the larger of the two: no whitelist can be in force at all. loadConfig parses config.Whitelists into config.whitelists, the []*net.IPNet list that isWhitelisted actually reads (services/legacy/config.go:523-557, peer_server.go:4026). setConfigValuesFromSettings then overwrites config.Whitelists from the settings map AFTERWARDS (peer_server.go:3530) and nothing re-derives config.whitelists. Since Teranode passes no bsvd command line, legacy_config_Whitelists is the only way to set a whitelist, and it populates the string list while leaving the parsed list empty - so isWhitelisted returns false for every address however the node is configured. settings.conf defines legacy_config_Whitelists in no context, so today no deployment even attempts it. The knock-on: serverPeer.addBanScore skips ban scoring for whitelisted peers (peer_server.go:544), so a peer an operator believes they have whitelisted is in fact still bannable on misbehaviour. Second, and separable: the per-IP cap in handleAddPeerMsg (peer_server.go:2312) never consults sp.isWhitelisted, so even a working whitelist would not lift MaxPeersPerIP for an address. bitcoin-sv's does. MEASURED: with legacy_config_Whitelists=127.0.0.1 set before daemon start, the sixth connection from 127.0.0.1 is still dropped and getpeerinfo still reports 5 - see the third subtest of TestBSVP2PMaxConnectionsFromAddr, which is a tripwire on exactly this. READ FROM CODE, not measured: which of the two reasons is doing the work, because neither can be isolated from a test - and that the settings key name is the one the reflection path matches (config.go:158 declares the field as Whitelists). No observable effect of a working whitelist exists to confirm against, which is the defect. Operationally: several peers sharing one NAT or one Kubernetes egress IP cannot hold more than MaxPeersPerIP (5) connections to a node, and an operator has no supported way to lift that for infrastructure they control. Separately, misbehaviour bans cannot be suppressed for a trusted peer.

### Plan

UNLOGGED - awaiting review. Report as a defect if the user agrees; the two halves are separable and only the first is unambiguously a bug. Fix for the first: re-derive config.whitelists after setConfigValuesFromSettings, or move the whitelist parsing after it. Worth auditing the same ordering hazard for every other config field loadConfig post-processes into an unexported twin - the whitelist is unlikely to be the only one, and this gap is really an instance of "loadConfig validates, then the settings map overwrites what was validated". Fix for the second is a policy call, not an obvious bug: decide whether the per-IP cap should honour the whitelist as bitcoin-sv does. Establish first whether whitelisting is meant to be reachable in Teranode at all. If it is dead by design, the honest change is to remove Whitelists, isWhitelisted and its callers rather than leave a defence that reads as configurable and is not.

## per-ip-connection-count-leaks-on-duplicate-peer

**Replacing a peer that shares an address never returns its slot against the per-IP cap**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `bsv-p2p-max-connections-from-addr.py`
- **Found while porting:** `bsv-p2p-max-connections-from-addr.py`

### Impact

Found while reading the per-IP cap for bsv-p2p-max-connections-from-addr.py. handleAddPeerMsg drops an existing peer that shares Addr() with an incoming one by calling DisconnectWithInfo and deleting it from state.outboundPeers / state.inboundPeers on the spot (services/legacy/peer_server.go:2287-2302). handleDonePeerMsg decrements state.connectionCount[host] only inside `if _, ok := list.Get(sp.ID()); ok` (peer_server.go:2402-2418), so when the replaced peer's done message arrives it is no longer in the list, the decrement never runs, and its slot against MaxPeersPerIP is never returned. CountIP then reports more connections than exist, and after MaxPeersPerIP such replacements the host cannot connect at all. connectionCount is process-lifetime state with no reconciliation, so it only ever drifts one way. NOT MEASURED, and not reachable from this port. Two live inbound connections cannot share a source port, so Addr() never collides for inbound peers, and wirepeer connects inbound only. Outbound and persistent peers dial a fixed remote address and so can collide - whether connManager in practice produces a second connection to an address it already holds is READ FROM THE CODE only, and is the thing to settle before treating this as live. The bookkeeping asymmetry itself is not in doubt; its reachability is.

### Plan

UNLOGGED - awaiting review. Establish reachability first, since that decides whether this is a latent tidiness problem or an operational one: a legacy-package unit test can drive handleAddPeerMsg with two serverPeers sharing Addr(), then both done messages, and assert CountIP returns to zero. That is a cheap, decisive test and belongs in services/legacy regardless of the outcome. The fix, if confirmed: decrement connectionCount where the peer is removed in handleAddPeerMsg, or better, stop removing it there and let handleDonePeerMsg be the single place that unwinds a peer's bookkeeping - one owner for the counter rather than two call sites that must agree.

## max-peers-no-reservation-no-eviction

**The total peer cap reserves nothing for outbound connections and never attempts eviction**

- **Kind:** `product-decision` - not ours to make; needs a deliberate decision elsewhere
- **Status:** `open`
- **Holds up:** no tests - found while porting `bsv-p2p-max-connections.py`
- **Found while porting:** `bsv-p2p-max-connections.py`

### Impact

Found while porting bsv-p2p-max-connections.py. bitcoin-sv's -maxconnections is a total out of which -maxoutboundconnections plus one feeler slot are reserved, and when the inbound slots are full it calls AttemptToEvictConnection before refusing anyone - the log line the upstream script waits for, "failed to find an eviction candidate - connection dropped (full)", is that attempt reporting failure. Teranode has neither half. No reservation: config.MaxPeers is one flat total. state.Count() sums inboundPeers, outboundPeers and persistentPeers (peer_server.go:238) and handleAddPeerMsg refuses whichever peer arrives once that total is reached (peer_server.go:2320), with no notion of which kind of slot is being taken. So inbound peers can consume every slot the node would otherwise use to reach out. MEASURED: with legacy_config_MaxPeers=2, two inbound wire peers fill the cap and a third connection is refused - see TestBSVP2PMaxConnections. INFERRED, not measured: that a full inbound set would likewise refuse an outbound peer. handleAddPeerMsg is the single gate for both, reached from OnVersion for inbound and outbound alike, and the connectNodeMsg path applies the same test (peer_server.go:2705) - but addnode is handleUnimplemented (see net-rpcs-unimplemented), so an outbound connection cannot be driven from a test and the inference stands unproven. No eviction: there is no AttemptToEvictConnection analogue anywhere in services/legacy - "evict" in that package refers only to misbehaviour disconnects and association teardown. MEASURED: with the cap full, every established peer survived and the newcomer was dropped, which is bitcoin-sv's fallback behaviour without bitcoin-sv's attempt. Consequence, reasoned rather than measured, and the reason this is worth a decision rather than a note: eviction is the mechanism by which a node with full slots can still prefer a new peer over an entrenched one. Without it, once MaxPeers (20 in settings.conf) is reached the peer set is frozen until a peer leaves of its own accord. MaxPeersPerIP caps one address at 5, so four addresses suffice to fill a default node, after which no honest inbound peer can connect. That is the shape of an eclipse attack, and bitcoin-sv's eviction plus outbound reservation are the two defences aimed at it.

### Plan

UNLOGGED - awaiting review. Raise as a product decision rather than a defect: neither absence looks accidental, and adding peer eviction is an architectural change, not a fix. What the review should settle is whether the eclipse exposure above is accepted, mitigated elsewhere (Teranode's native libp2p P2P service is a separate peer set with its own policy, which may be the intended answer for a real deployment), or worth implementing on the legacy path. Two things to establish first, neither of which changes the finding. Whether a full inbound set really does block outbound dialling - cheapest via a legacy-package unit test over handleAddPeerMsg rather than by implementing addnode, which would widen the public RPC surface and needs its own proposal. And what MaxPeers should be for a production node at all: 20 is low enough that the question is not academic, and it is not obviously the same number that suits a node whose real peering happens over libp2p.

## no-reject-for-undecodable-version

**A version message the node cannot decode gets a silent hangup, not a reject**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `bsv-p2p-version_msg.py`
- **Found while porting:** `bsv-p2p-version_msg.py`

### Impact

Found while porting bsv-p2p-version_msg.py. negotiateInboundProtocol (services/legacy/peer/peer.go:2675) reads the first message and, if decoding fails, returns the error immediately - nothing is written back to the peer, which sees the connection close with no explanation. The same function handles the adjacent case differently: a first message that decodes but is the wrong type gets wire.NewMsgReject(cmd, RejectMalformed, "a version message must precede all others") before the error is returned. So the reject machinery is present, correct, and skipped on exactly the path where the peer has least information to work from. MEASURED: a version frame carrying eight raw bytes where a serialised association ID belongs - upstream's msg_version_bad, struct.pack("<Q", 0x00000000111111FE) - draws zero bytes and an immediate close. The identical frame minus those eight bytes is answered with the node's own version, which is what isolates the trailing field as the cause rather than the framing. bitcoin-sv answers the same input with a reject and a close, and the upstream script asserts the reject. READ FROM CODE: that this covers every undecodable first message, not only this one, since the error return is common to all of them. Operationally this is the version-handshake instance of the same problem as opaque-block-reject-reason, and slightly worse, because here there is no reason string at all rather than an unhelpful one. A peer operator whose implementation emits a version Teranode's decoder will not take - a trailing field it does not know, an association ID over MaxAssociationIDLen - sees a TCP close indistinguishable from a network fault, a ban, or a node that is down. Diagnosing it requires our logs.

### Plan

Raised as https://github.com/bitcoin-sv/teranode/issues/4784. The fix is small and local, and the call site already demonstrates the pattern. Write a RejectMalformed reject before returning the decode error from negotiateInboundProtocol, as the wrong-message-type branch immediately below already does. Two cautions for whoever takes it. The reason string should not be err.Error(): go-wire's decode errors carry internal detail, and bitcoin-sv's reject reasons are a short closed vocabulary that matching is the point of - the same argument made in opaque-block-reject-reason, and worth settling once for both. And a peer that has not completed a handshake is unauthenticated and cheap to create, so anything written back on this path is an amplification surface: a fixed short string, written once before closing, is the safe shape. Worth checking at the same time, since it is adjacent and cheap: whether Teranode should validate the association ID format at all. peer.NewAssociation accepts the ID bytes without inspecting them, so bitcoin-sv's "Badly formatted association ID" check has no counterpart - any byte string within MaxAssociationIDLen is accepted as an association ID when Legacy.AllowBlockPriority is set, which it is by default. Not measured as a fault, and not obviously one; recorded because this port established it while establishing the above.

## unsupported-inv-type-ignored-not-disconnected

**Unsupported inventory types are free to send: no disconnect, no ban score, a goroutine each**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `bsv-p2p-invalid-inv.py`
- **Found while porting:** `bsv-p2p-invalid-inv.py`

### Impact

Found while porting bsv-p2p-invalid-inv.py. bitcoin-sv treats an inventory vector of type ERROR (0) as a protocol violation - no honest peer sends one - logs "Got invalid inv" and disconnects. Teranode discards the vector and carries on. That much is deliberate: SyncManager.processInvMsg (services/legacy/netsync/manager.go) switches on the type and its default branch is a bare return, and the caller's comment says "Ignore unsupported inventory types". What is not deliberate is the price. handleInvMsg iterates the vectors and, for every one that is not InvTypeBlock, does wg.Add(1) and spawns a goroutine whose body is processInvMsg - so the goroutine is created BEFORE the type switch that will discard the vector. A single inv message may carry wire.MaxInvPerMsg = 50000 vectors. Sending 50000 unsupported vectors therefore costs the sender about 1.8MB and costs the node 50000 goroutine creations plus a WaitGroup barrier, every one of which does nothing but return. Nothing charges the peer for it. There is no disconnect, no reject, and no ban score - and ban scoring on this path is clearly reachable, because the adjacent getdata handler does exactly that: sp.addBanScore(0, uint32(length)*99/wire.MaxInvPerMsg, "getdata") (peer_server.go:1438) charges a peer for an oversized getdata. inv has no equivalent. MEASURED: 200000 unsupported vectors across four full-size messages on one connection. The peer was never disconnected, drew no reject, and the node was still serving headers afterwards. The single-vector and full-size cases are both asserted in TestBSVP2PInvalidInv as tripwires. READ FROM CODE, not measured: the goroutine-per-discarded-vector count, and that this is unbounded across messages. A test cannot count goroutines in the daemon it shares a process with, so the cost is established from the loop rather than observed. That is the part worth confirming with a profile before sizing the risk.

### Plan

Raised as https://github.com/bitcoin-sv/teranode/issues/4781. Two separable pieces, and only the first is unambiguous. Cross-referenced with #4700, which makes the same observation about the ban-score asymmetry from the getheaders side - "getheaders is also not covered by the legacy getdata ban-score". Same missing policy, a different message, worth settling once for the legacy message set rather than per message type. The cheap fix: do not spawn a goroutine for a vector the type switch will discard. Filter on type before the wg.Add(1) - or better, hoist the switch out of processInvMsg into the loop, since the loop already special-cases InvTypeBlock and so already knows the type. This removes the amplification without changing any policy. The policy question: whether to charge a ban score for unsupported inventory types, as bitcoin-sv does by disconnecting. Worth deciding rather than inheriting - the getdata handler shows the mechanism is right there and already used one function away.

## tx-inv-dropped-during-startup-window

**Transaction announcements are silently dropped for a window after startup while block announcements are not**

- **Kind:** `unknown` - needs diagnosis before anything else can be decided
- **Status:** `investigating`
- **Holds up:** no tests - found while porting `bsv-p2p-invalid-inv.py`
- **Found while porting:** `bsv-p2p-invalid-inv.py`

### Impact

Found while porting bsv-p2p-invalid-inv.py, as the reason its transaction assertion needed a precondition upstream does not need. MEASURED, and reproducible: on a freshly started daemon, a peer that announces a transaction gets no getdata, while the same peer on the same connection announcing a block a millisecond earlier does get one. After roughly one to two seconds, transaction announcements are answered normally and every subsequent one is answered. Established by elimination rather than by reading a log: - Not the FSM state as the test sees it. GetFSMCurrentState on the test's own blockchain client returned RUNNING before, during and after the window. - Not the UTXO store. Direct utxoStore.Get calls for the announced hash returned a clean TX_NOT_FOUND throughout, which haveInventory maps to "don't have it, request it" - the path that leads to a getdata. - Not the hash or the message shape: the identical announcement succeeds later, and fresh hashes succeed immediately once the window has passed. The only code path that treats the two vector types differently is the processInvs gate: processInvMsg returns early for InvTypeTx when processInvs is false, and does not consult it for InvTypeBlock. processInvs is set in handleInvMsg from sm.blockchainClient.GetFSMCurrentState, and a failed lookup and a genuine not-RUNNING state are indistinguishable there - the error is logged and processInvs stays false either way. netsync's blockchain client is a different instance from the test's, so the test observing RUNNING says nothing about what netsync saw. NOT ESTABLISHED: which of the two it is. That needs the node's log, which an in-process TestDaemon does not expose. Until it is known this is kind "unknown", not "defect". Why it may matter beyond tests: a dropped inv vector is not requeued or retried, so any transaction announced inside the window is simply never requested from that peer. BSV peers do not generally re-announce the same transaction to the same peer, so it is not obviously self-healing. If the cause is a transient client error rather than a real not-yet-RUNNING state, then the same fail-closed handling would drop announcements on any later blip too, not only at startup.

### Plan

UNLOGGED - awaiting review. Diagnose before deciding anything; this is recorded as a question, not a defect. The decisive step is cheap: log or return the GetFSMCurrentState outcome in handleInvMsg distinctly for "lookup failed" and "state is not RUNNING", then re-run TestBSVP2PInvalidInv without its requireTxInvProcessingLive precondition and read which one fires. If it is a lookup error, the question becomes whether dropping is the right response to not knowing the state - the alternatives being to retry the lookup, or to requeue the vector rather than discard it. If the state genuinely is not RUNNING for a second or two after the FSM reports RUNNING to another client, that is a separate and more interesting question about how the FSM state is propagated to service clients. Also worth settling while in there: whether block announcements SHOULD bypass the processInvs gate. The asymmetry may well be intentional - a node that is still coming up wants blocks - but it is not commented, so it currently reads as an oversight either way.

## no-duplicate-protoconf-guard

**protoconf can be sent repeatedly, and each one redoes peer setup including sync-manager registration**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `bsv-protoconf-violation.py`
- **Found while porting:** `bsv-protoconf-violation.py`

### Impact

Found while porting bsv-protoconf-violation.py. bitcoin-sv allows protoconf once per connection and disconnects a peer that sends a second - deliberately a disconnect and not a ban, per the upstream script's own comment. Teranode has no such rule: serverPeer.OnProtoconf (services/legacy/peer_server.go:695) keeps no record that it has already run for this peer, so it re-executes in full every time. MEASURED: seven protoconf messages on one handshaked connection. The peer was not disconnected, drew no reject, and listbanned stayed empty throughout. The missing disconnect matters less than what each repeat re-executes: 1. numberOfFields and maxRecvPayloadLength are overwritten. No consequence today, and worth stating precisely rather than implying one: maxRecvPayloadLength is stored at peer_server.go:703 and read nowhere in services/legacy - the only two references are the declaration and that store. So a peer can change it freely and nothing observes the change. (That the value is never read is a separate matter and a signpost for the bsv-protoconf.py port, since honouring a peer's declared maximum receive payload length is what protoconf exists to negotiate. Not opened as a gap here - it belongs to that entry, which has not been read yet.) 2. Stream-policy negotiation runs again: assoc.SetPolicy, and for outbound peers openRequiredStreams(). 3. sp.server.syncManager.NewPeer(sp.Peer, nil) is called again, whenever the peer has an association. Item 3 is the reason this is filed as a defect rather than a tolerance note. A duplicate NewPeer is a hazard this codebase has already been bitten by and documented: the comment in handleAddPeerMsg says syncManager.NewPeer is "intentionally NOT called here" because a duplicate newPeerMsg, when a donePeerMsg landed between the two, "left a permanently-disconnected peer pointer in peerStates, which startSync then re-picked forever and the node ground to a halt". That hazard was removed from one call site while remaining reachable, repeatedly and on demand, from another - and from a message a peer controls. READ FROM CODE, not measured, and the thing to settle before sizing this: item 3's reachability. It requires the peer to hold an association, i.e. to have sent a non-empty AssociationID in its version while Legacy.AllowBlockPriority is set - which is the default. wirepeer sends no AssociationID, so the port's peer has no association and the measurement above exercises items 1 and 2 only. Whether repeated registration actually reproduces the grinding-halt failure, or is absorbed harmlessly by peerStates, is not established either way.

### Plan

UNLOGGED - awaiting review. Report as a defect, with the reachability question carried into it rather than resolved first, since the fix is the same either way and cheaper than the investigation. The fix mirrors bitcoin-sv and closes all three items at once: record on the serverPeer whether protoconf has been seen, and on a second one disconnect without banning. That also makes Teranode's wire behaviour match the reference implementation, which is what the upstream test asserts. If disconnecting is judged too harsh for a message some peer might resend benignly, the minimum is to make OnProtoconf idempotent - above all to call syncManager.NewPeer at most once per peer, which is the invariant the handleAddPeerMsg comment already describes as necessary. To establish reachability of item 3: add an AssociationID option to wirepeer so a test peer can hold an association, or drive OnProtoconf twice in a legacy-package unit test and assert how many newPeerMsg values reach the sync manager's channel. The unit test is the more direct of the two and does not widen the harness.

## protoconf-payload-limit-not-honoured

**The protoconf maximum receive payload length is advertised and then ignored in both directions**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `bsv-protoconf.py`
- **Found while porting:** `bsv-protoconf.py`

### Impact

Found while porting bsv-protoconf.py. protoconf exists to negotiate messages larger than the legacy 1 MiB cap: each side declares how much it is willing to receive, and each side is expected to respect the other's number. Teranode declares one and respects nothing, and the number it declares is also not the number it will accept. The value it sends is correct. peer.go:2860 sends wire.NewMsgProtoconf(0, ...), which resolves to go-wire's DefaultMaxRecvPayloadLength of 2 MiB - the same default bitcoin-sv uses when -maxprotocolrecvpayloadlength is unset. Measured: numberOfFields 2, maxRecvPayloadLength 2097152, policies [BlockPriority Default]. The literal 0 also means the value is not configurable; there is no setting. OUTBOUND - the peer's declared limit is ignored. The value arrives and is stored at peer_server.go:703 and is read nowhere in services/legacy: the only two references to maxRecvPayloadLength are that store and the field declaration. Outbound getdata size is governed instead by netsync's maxRequestedTxns / maxRequestedBlocks, both set to wire.MaxInvPerMsg - a vector COUNT, not a byte budget, so no arithmetic anywhere compares a message against what the peer said it would take. MEASURED: a peer that declared 1 MiB (upstream's LEGACY_MAX_PROTOCOL_PAYLOAD_LENGTH, and the lowest value bitcoin-sv permits for the option) announced 40000 transactions and received one getdata of 1,440,009 bytes - 37% over its declared limit. A compliant node would have sent two messages, of 29126 and 10874 items. Why that is an interop risk and not only untidiness: bitcoin-sv BANS a peer whose message exceeds the receiver's declared limit. That is precisely what upstream's run_ban_test asserts, in the opposite direction. Teranode's largest possible getdata is 9 + 50000*36 = 1,800,009 bytes (1.72 MiB), so any bitcoin-sv peer configured below that can ban Teranode. Stated precisely because it bears on severity: a DEFAULT bitcoin-sv peer declares 2 MiB and is safe, so this needs a peer configured below 1.72 MiB, the floor being 1 MiB. Reachable by configuration, not by default. INBOUND - the advertisement exceeds what the node accepts. go-wire refuses an inv above MaxInvPerMsg, so the largest inv Teranode can decode is 1,800,009 bytes, less than the 2 MiB it advertises. MEASURED: a hand-framed inv of 52000 vectors (1,872,003 bytes - inside the advertised limit, above MaxInvPerMsg) drew no getdata, no reject and no ban, and the connection survived. It is discarded in silence. That silence is itself the regtest-and-loopback tolerance in peer.isAllowedReadError, already recorded as unknown-command-disconnects-off-regtest - so this is understated by any test that runs on regtest. On mainnet the same frame is answered with a "malformed" reject and a disconnect. A conforming peer that believes Teranode's advertisement and sends a 1.9 MiB inv would therefore be disconnected by Teranode for doing what Teranode invited.

### Plan

UNLOGGED - awaiting review. Report as a protocol-conformance defect. Three separable pieces, in the order they matter. First, honour the peer's declared limit when sizing outbound messages. The value is already captured per peer, so what is missing is a byte budget where netsync currently counts vectors. getdata is the case measured; the same reasoning covers anything Teranode sizes by count rather than bytes, and inv and headers are the obvious two to audit alongside it. Second, make the advertisement agree with what the node will accept - either advertise 1,800,009 rather than 2 MiB, or raise the inv ceiling to match the advertisement. Advertising more than will be accepted is a trap for a conforming peer, and this is a choice between two numbers rather than a design question. Third, decide whether the advertised value should be configurable. Not needed for conformance, but needed to port upstream's second and third cases, and by any operator wanting messages larger than the go-wire default. One caution for whoever verifies a fix: because the inbound half is masked on regtest by isAllowedReadError, a regtest test will show a silent drop where mainnet shows a disconnect. Verify that half against a non-regtest chain params, or by asserting on the reject path directly.

## protoconf-not-validated

**Nothing about a peer's protoconf is validated, which becomes a problem the moment the values are used**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `bsv-protoconf-versions-compatibility.py`
- **Found while porting:** `bsv-protoconf-versions-compatibility.py`

### Impact

Found while porting bsv-protoconf-versions-compatibility.py. bitcoin-sv validates a peer's protoconf on three axes and refuses the connection on each: numberOfFields must be at least 1 (it throws at deserialisation, src/protocol.h:622), maxRecvPayloadLength must be at least 1 MiB, and the message itself must not exceed LEGACY_MAX_PROTOCOL_PAYLOAD_LENGTH - which it also bans for. serverPeer.OnProtoconf (peer_server.go:695) applies none of these: it stores what arrives and inspects it only far enough to look for a stream policy. MEASURED, each on its own connection, each tolerated with no disconnect, no reject and nothing banned: - numberOfFields = 0. go-wire reads the compact size as 0 and returns a MsgProtoconf with nothing else populated; OnProtoconf stores the 0 and skips every branch guarded by NumberOfFields > 0. - maxRecvPayloadLength = 1 byte. Stored as given. bitcoin-sv's floor is 1 MiB. - a 1,048,576-byte protoconf of filler. Accepted - and this one MATCHES bitcoin-sv, the only case in the script where the two agree. The filler decodes to nonsense rather than failing: 'a' is 0x61, so numberOfFields reads as 97 and maxRecvPayloadLength as 0x61616161, about 1.5 GB, and both are stored without comment. - a 1,048,577-byte protoconf, one byte over. bitcoin-sv disconnects and bans. Teranode does neither, and the connection remains usable - a getheaders on the same connection immediately afterwards was answered. That was checked rather than assumed, because refusing an over-long frame without draining its payload would leave 1 MiB of filler to be read as the next message header and desynchronise the connection permanently. It does not. One thing this script tests that Teranode gets right, worth recording as such: a two-field protoconf with an unknown extra field appended is parsed, tolerated, and the node keeps serving. That is the forward-compatibility property upstream's third case exists to protect, and it holds. Also checked and NOT a finding, because it looked like one: go-wire's protoconf encoding is not incompatible with bitcoin-sv's. The upstream script's bespoke helper classes serialise numberOfFields as a fixed 4-byte int, which makes go-wire's varint read look wrong - but those helpers exist precisely to send malformed messages. The real format is READWRITECOMPACTSIZE (src/protocol.h:615), which is what go-wire reads. Recorded so nobody else spends the time. Why this matters despite every case being harmless today: it is harmless only because the values are stored and never read (see protoconf-payload-limit-not-honoured). The moment anything reads them - which is exactly what fixing that gap means - a peer can declare 1 byte, or the 1.5 GB the filler case produced, and Teranode will size real messages from it.

### Plan

UNLOGGED - awaiting review. Report as a defect, and carry the ordering constraint into it, because it is the substantive point: this should be fixed BEFORE or TOGETHER WITH protoconf-payload-limit-not-honoured, never after. Validating a value nobody reads is housekeeping; reading a value nobody validated is a defect waiting for a peer to find it. The checks to copy are bitcoin-sv's and all cheap. Reject numberOfFields = 0 during decoding, which is where bitcoin-sv does it - note that this is a change to go-wire rather than to Teranode, so it needs coordinating in that repo. Reject maxRecvPayloadLength below LEGACY_MAX_PROTOCOL_PAYLOAD_LENGTH in OnProtoconf, where a node's own policy belongs rather than in the codec. A sensible upper bound belongs there too: bitcoin-sv's ceiling is 1 GB, and accepting 1.5 GB from filler bytes shows there is currently none. The one policy question is whether an over-long protoconf should ban. It is the same question unknown-command-disconnects-off-regtest raises about unparseable frames generally, so it is worth answering once for all message types rather than per type. A caution for verification, as with the sibling gap: the over-long case is masked on regtest by isAllowedReadError, so a regtest test shows tolerance where mainnet shows a disconnect. Verify that case against non-regtest chain params or on the reject path directly.

## unsolicited-addr-accepted

**Addresses a peer volunteers unasked are added to the address manager, and the node dials from it**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `p2p-unsolicited_addr.py`
- **Found while porting:** `p2p-unsolicited_addr.py`

### Impact

Found while porting p2p-unsolicited_addr.py, which exists in bitcoin-sv specifically to assert that this does not happen. serverPeer.OnAddr (services/legacy/peer_server.go:1779) hands every address in the message to addrManager.AddAddresses with no check that the node ever asked for any of them - there is no counterpart to the solicited-only rule bitcoin-sv and Bitcoin Core both apply. The only thing OnAddr rejects is an addr with an empty address list. MEASURED: a peer sent upstream's exact message - 104.20.31.65:10000 through :10009, unasked - and an unrelated later peer's getaddr had those addresses handed back to it. Two of the ten each time, which is not partial acceptance but AddressCache sampling: addrmgr returns len(known) * getAddrPercent / 100 entries after a Fisher-Yates shuffle, and with getAddrPercent = 23 and ten known addresses that is two, chosen at random. Different pairs came back on different runs, so all ten were stored. Why it matters: the address manager is what the node dials from. With legacy_config_ConnectPeers empty - which is the committed default in settings.conf - newServer installs a newAddressFunc that feeds the connection manager from addrManager.GetAddress (peer_server.go:3714), so an attacker who can reach the legacy port can influence which peers the node dials. That is the eclipse-attack shape address-manager poisoning has always had, and the reason upstream wrote the test. Bounded honestly: an operator who sets legacy_config_ConnectPeers gets newAddressFunc left nil and MaxPeers pinned to the number of configured peers, so a node peered explicitly does not dial from the poisoned set at all. How Teranode is actually deployed therefore decides whether this is live or latent, and that is a question for the review rather than something this exercise can settle. A second, quieter finding worth recording with it: upstream's test PASSES against Teranode, and passes for a reason unrelated to what it tests. Its only evidence of acceptance is onward relay, and Teranode never relays an addr unsolicited - pushAddrMsg is reachable only from OnGetAddr and from an outbound-only branch of OnVersion. Anyone porting this script by translating its assertions literally would have recorded a pass and moved on. The acceptance is visible only by asking the node with getaddr, which is what the port does.

### Plan

Raised as https://github.com/bitcoin-sv/teranode/issues/4783, with the deployment question attached, since it decides the severity rather than the existence. Cross-referenced with #4782 in both issues, since that is the read side of the same address-manager exposure and together they mean an attacker can both write to and read back that set. The fix bitcoin-sv and Bitcoin Core both use is to track whether a getaddr is outstanding for the peer and drop an addr that answers no request. Teranode already has the state to hang that on: serverPeer carries sentAddrs for the opposite direction, so a symmetric sentGetAddr flag set where OnVersion requests addresses (the addrManager.NeedMoreAddresses branch at peer_server.go:648) is the natural place. Large unsolicited addr messages are conventionally dropped outright rather than trimmed. Two things to settle in review. Whether production Teranode runs with legacy_config_ConnectPeers set - if it always does, this is latent and can be fixed at leisure; if it can be empty, an attacker chooses part of the peer set. And whether the address manager should be dialled from at all on the legacy path, given that Teranode's own peer discovery happens over libp2p; if not, the safest change is to stop installing newAddressFunc rather than to police what reaches the address manager.

## pre-handshake-message-leak

**A peer that never sends a verack is treated as fully connected and has its requests answered**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `p2p-leaktests.py`
- **Found while porting:** `p2p-leaktests.py`

### Impact

Found while porting p2p-leaktests.py, which exists to enforce one rule, stated in its own docstring: a node should send nothing but VERSION, VERACK and REJECT until it has received a VERACK. Teranode does not implement the rule, because it does not wait for the inbound verack at all. negotiateInboundProtocol reads the peer's version, calls handleVersionMsg and writeLocalVersionMsg, and returns; the caller then starts the message handlers and sends its own verack. Nothing anywhere blocks on the peer acknowledging. MEASURED, on a peer that sent a version and never a verack: - Its ping is answered with a pong. The nonce comes back, so it is unambiguously a reply to that ping and not a keepalive. - Its getaddr is answered with an addr, disclosing entries from the address manager. - It appears in getpeerinfo as an ordinary inbound peer, which is the mechanism behind both: handleAddPeerMsg runs off OnVersion, so the peer is registered in the peer lists before any verack could arrive. - It is also sent protoconf, and intermittently feefilter - present in three of four runs. Recorded as real but not asserted on, since it is not deterministic. - It is never dropped. Added while porting p2p-timeouts.py, which comes at the same defect from the timeout side: peer.Start applies negotiateTimeout (services/legacy/peer/peer.go:63, 30 seconds) only while negotiateInboundProtocol is still running, and that function returns as soon as the version has been handled. So no deadline covers the missing verack. Measured on one connection: a silent peer was dropped at exactly 30.0s while a peer that had sent a version and no verack was still connected, still in getpeerinfo, at 49s. bitcoin-sv drops its equivalent at 60s, and upstream asserts that it does. Two of the three peers upstream attaches behave correctly, and that is worth recording so this reads as a narrow finding rather than a broad one. A peer that says nothing at all is sent nothing at all. A peer whose first message is a verack rather than a version gets a reject naming the reason - "a version message must precede all others" - and the connection is closed immediately, which is stricter than bitcoin-sv, where the equivalent takes ten messages to accumulate a ban score. A methodological point that belongs with the finding: the addr half of this is invisible against a freshly started node. The address manager is empty, so pushAddrMsg has nothing to send and upstream's assertion passes for a reason unrelated to handshake state. It only becomes visible after the node has learned some addresses, which the port arranges deliberately. This is the second gap in this exercise - after unsolicited-addr-accepted - where a literal translation of the upstream assertions would have reported a pass and hidden a real divergence. Consequences, in the order they seem to matter. The address-manager contents are disclosed to anyone who can open a TCP connection and send a version, which combined with unsolicited-addr-accepted means an attacker can both write to and read back that set. Half-handshaked peers occupy slots against MaxPeersPerIP and MaxPeers, which combined with max-peers-no-reservation-no-eviction lowers the cost of filling a node's peer table - a version is enough, no verack needed. And the node does work on behalf of peers that have not demonstrated they can complete a handshake, which is the generic reason Bitcoin Core adopted the rule.

### Plan

Raised as https://github.com/bitcoin-sv/teranode/issues/4782. It is a conformance gap with a disclosure consequence rather than anything acutely exploitable, so the severity call is the reviewer's - filed without a [security] prefix on that basis, and the issue says so explicitly. Cross-referenced with #4783, the write side of the same address-manager exposure. The fix has a natural shape: gate reply-generating handlers on the verack having arrived. peer.Peer already tracks verAckReceived, so the cheapest correct version is to check it in the handlers that answer requests - OnPing, OnGetAddr, OnGetData, OnGetHeaders and the rest - or, more cleanly, to withhold registration with the server until the verack lands, so the peer simply is not a peer yet. The second is closer to what bitcoin-sv does and avoids a growing list of per-handler checks, but it moves handleAddPeerMsg off OnVersion, which the comment there says was placed deliberately - so it needs care and is not a one-line change. Worth deciding at the same time, since it is the same question: whether protoconf and feefilter should also wait for the verack. protoconf is arguably part of the BSV handshake and bitcoin-sv sends it early too, so it may be correct as is; feefilter is not, and upstream's test flags it explicitly.

## block-txcount-preallocation

**An 89-byte block message can make the node allocate tens of gigabytes before reading a transaction**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `bsv-block-bad-count.py`
- **Found while porting:** `bsv-block-bad-count.py`

### Impact

SECURITY-SENSITIVE. Found while porting bsv-block-bad-count.py, which exists to check that a block message declaring an absurd transaction count is refused rather than acted on. The guard it tests is present and works. The finding is that the guard's threshold is high enough to leave a memory-exhaustion vector wide open beneath it. MsgBlock.Bsvdecode reads the 80-byte header, reads the transaction count as a varint, checks it against maxTxPerBlock(), and then - before reading a single transaction byte - does make([]MsgTx, txCount) and make([]*MsgTx, txCount). Nothing relates the count to how many bytes the message actually contains. The declared payload length in the wire header is 89 bytes; the count can claim hundreds of millions. maxTxPerBlock() is (MaxBlockPayload() / minTxPayload) + 1, where minTxPayload is 10 and MaxBlockPayload() returns go-wire's ebs. Teranode sets that to 4e9 via wire.SetLimits(4000000000) at services/legacy/Server.go:255, so the ceiling a peer may declare is 400,000,001. MEASURED, at deliberately small scale: 200,000 declared transactions in an 89-byte frame allocated 13.7 MB; 600,000 allocated 41.2 MB; 1,200,000 allocated 82.4 MB. That is 72.0 bytes per declared transaction, linear across all three, taken from runtime.ReadMemStats around the send - possible because an in-process TestDaemon allocates in the test's own process, on a daemon otherwise idle enough that drift is under half a megabyte. NOT EXECUTED, and deliberately: the ceiling. 400,000,001 x 72.0 bytes is about 26.8 GiB from one 89-byte message. That figure is arithmetic from the measured slope and the ceiling read out of the code, not something this exercise triggered - running it risks taking the host down rather than the node, and the mechanism is already established by the linear measurements above. Reachability is the part that makes it serious. The allocation happens during message decode, so it precedes every policy check: the unrequested-block rejection in netsync is downstream of it, and per pre-handshake-message-leak a peer need not even complete a handshake - a version is enough to have its messages read. The cost is also paid BEFORE the peer is dropped: measured, the connection is closed about 50 ms after the frame, once the truncated payload runs out, so the disconnect is the aftermath rather than a defence. An attacker reconnects and repeats. Note also that MaxPeersPerIP permits five concurrent connections per address. Two related observations recorded while establishing this, both of which shaped the port. First, the streaming block handler (services/legacy/peer/wire_streaming.go) does NOT avoid this: it exists to stop the full payload being buffered as a []byte, and wraps the reader in an io.LimitedReader, but it then calls the same MsgBlock.Bsvdecode, so the pre-allocation is unchanged. Second, ReadVarInt rejects non-canonical encodings, so any count the guard could permit - all of which fit in 32 bits - must be sent in the 0xFE form. A first attempt to measure this used the 0xFF form for a small count and saw no allocation at all, which looked exactly like the guard working. Worth knowing for anyone reproducing it.

### Plan

Raised as https://github.com/bitcoin-sv/teranode/issues/4779, with the [security] prefix per GOAL.md. The fix belongs in go-wire rather than in Teranode, which means coordinating in that repository - the issue cannot be closed by a change in this tree. The issue opens by warning against closing it as a duplicate of #4572, which is the same class of defect (capacity derived from an untrusted count) but was fixed by validating the count against a policy maximum. That remedy does not transplant here: the policy maximum IS the hole at 400,000,001, and the check against it already exists at msg_block.go:88. The fix is the standard one for length-prefixed formats: bound the count by the bytes actually available before allocating, not by a policy maximum. Inside the streaming handler the available length is already known and already used to build the io.LimitedReader, so the information needed is in hand - a count above length/minTxPayload cannot possibly be satisfied and can be rejected outright. Failing that, allocate incrementally with append rather than sizing up front from an untrusted number; the arena in the same function already clamps its own hint to [4 KiB, 4 MiB] for exactly this reason, so the pattern is established one line away. Worth auditing alongside it, since the shape is generic rather than specific to blocks: every other Bsvdecode that sizes a slice from a peer-supplied count. MsgInv is bounded by MaxInvPerMsg and so is only a 50,000-element exposure, but it deserves the same read, as do headers and getdata. One thing not established: whether Teranode's process memory limits or container limits would turn this into a clean crash-and-restart rather than host-level memory pressure. That changes the operational severity and not the defect, and it needs someone who knows how Teranode is deployed.

## porttest-suite-intermittent-failure

**Suite failures on a machine that suspends mid-run, and one earlier failure still unexplained**

- **Kind:** `test-config` - ours to fix, in this repository
- **Status:** `resolved`
- **Holds up:** no tests - found while porting `bsv-peer-flood.py`
- **Found while porting:** `bsv-peer-flood.py`

### Impact

Recorded so it is not lost rather than because it is understood. During the bsv-peer-flood.py tick, one `make bsvporttest` run failed with the last progress line being "Daemon TestBSVNet stopped successfully" and no assertion message captured. It did not reproduce: three subsequent full-suite runs passed, including one with -v, and TestBSVNet run alone passed six times out of six. Not attributable to that tick's port on the evidence available. The same run also contained a genuine failure of TestBSVPeerFlood, whose heap assertion was wrong at the time and has since been fixed, so the tail of that output is not trustworthy as a guide to what else went wrong - it is possible the suite failure was only that test, and the TestBSVNet line was simply the last thing printed before the summary. That is the most likely explanation and it would mean there is nothing here at all. DIAGNOSED on the following tick, and it turned out to be the harness's host rather than anything in the node or the ports. Two more suite runs failed, and the second was instrumented: the test binary reported 163.678s of test time inside 78 minutes of wall clock at 0% CPU. The machine was suspending between tests. Confirmed by a run whose process showed 5.7 seconds of CPU across 35 minutes wall clock in state S - blocked, not busy - and by Go's own -timeout never firing, which is what happens when the monotonic clock stops with the machine. That also explains why it looked like a hang and why it could not be reproduced on demand: nothing was wrong, the machine was asleep. Both failing runs ended immediately after TestBSVP2PTimeouts, which is the one port in the suite whose assertions are about wall-clock deadlines - it samples either side of the node's 30-second negotiation timeout, and a suspend across that window leaves every peer dropped for reasons unrelated to the test. One correction to the record: a SIGQUIT sent to the blocked process to collect a goroutine dump produced "fault 0x18a94750c" instead and destroyed that run's evidence. The right tool was a short -timeout, which makes Go dump every goroutine itself. NOT explained by this: the very first failure, seen the previous tick after TestBSVNet. It is consistent with the same cause, since that run also contained a genuine TestBSVPeerFlood failure whose output masked everything else, but the assertion message was lost and it cannot be attributed now.

### Plan

RESOLVED by hardening the affected port rather than by changing the suite. waitUntil in TestBSVP2PTimeouts now skips, with an explicit message, when the clock jumps more than 10 seconds past a sampling point. Skipping is the honest outcome: a failure there would report that the node behaved wrongly when the test had simply stopped running for a while. Two notes for anyone hitting something similar. Check CPU time against wall clock before diagnosing a hang - 5.7 seconds across 35 minutes is a suspended machine, not a deadlock, and reading it as a deadlock cost real time here. And run the suite with a short -timeout when investigating, never SIGQUIT, for the reason above. No further action unless a suite failure appears on a machine that has demonstrably stayed awake.

## opaque-tx-reject-reason

**Every rejected transaction gets the reason 'rejected', and the real cause is lost twice over**

- **Kind:** `defect` - a confirmed Teranode bug; belongs in the issue tracker
- **Status:** `open`
- **Holds up:** no tests - found while porting `invalidtxrequest.py`
- **Found while porting:** `invalidtxrequest.py`

### Impact

Found while porting invalidtxrequest.py. The transaction twin of opaque-block-reject-reason, and worse, because the detail is discarded at two separate boundaries rather than one. At the wire boundary: netsync/manager.go:1288 answers a refused transaction with PushRejectMsg(CmdTx, RejectInvalid, "rejected", txHash, false) - the same fixed string for every cause. The line above it reads "TODO better rejection code and message from the error", and the specific error is in scope at that point, having just been logged on the previous line. So this is known and the material is in hand. At the validator boundary, and this part is new: the specific GoBDK error does not survive to the caller either. ScriptVerifierGoBDK.go:307 logs it and then mapBDKValidationError wraps it as errors.NewTxInvalidError(errMsgInvalidTx, errVerify), where errMsgInvalidTx is the constant "GoBDK fail to ValidateTransaction". MEASURED across the propagation client: Error() returns "TX_INVALID (31): GoBDK fail to ValidateTransaction", %+v renders the same, and the unwrap chain has exactly one element - so the cause is not recoverable programmatically even in-process. The only place the real reason exists is a log line. MEASURED for reference, on a transaction spending an OP_TRUE output with an unlocking script of a single OP_NOTIF, which is upstream's input: the wire reject carries cmd "tx", code REJECT_INVALID and reason "rejected", naming the correct txid, and the peer is not disconnected. bitcoin-sv sends mandatory-script-verify-flag-failed for the same input, and the upstream test asserts that string. Worth recording that the code half is right: RejectInvalid is 16, which is what upstream asserts, so a peer learns the class of failure and only loses the cause. Also worth recording the counter-example already noted in opaque-block-reject-reason - GoBDK does sometimes surface a specific message, as the bsv-genesis-pushonly.py port measured ("Only non-push operators allowed in signatures") - so the detail exists inside BDK and is being flattened by Teranode's own wrapping rather than never produced. Operationally this costs a peer operator the ability to find out why their transaction was refused without access to our logs, and it costs Teranode's own callers the ability to branch on the reason.

### Plan

UNLOGGED - awaiting review. Report as a defect, and worth reviewing together with opaque-block-reject-reason since the two share a fix strategy and a caution, and a single decision should cover both. The two boundaries need separate work. Inside the validator, stop flattening: either include the BDK error's own text in the TxInvalidError message, or keep it retrievable so errors.As can reach a typed script error. The wrapping is already there, so this is about what NewTxInvalidError renders and unwraps to rather than about plumbing a new value through. At the wire boundary, derive the reject reason from the error already in hand at the PushRejectMsg call site, which is what the TODO there asks for. The caution from opaque-block-reject-reason applies unchanged and is the reason to decide both at once: do not pass err.Error() through to a peer. bitcoin-sv's reasons are a short closed vocabulary and matching it is the point, so a mapping from Teranode error to upstream reason string is what is wanted, not internal error text.
