# Porting bitcoin-sv functional tests to Teranode

This package holds Teranode ports of the bitcoin-sv Python functional test suite
(`bitcoin-sv/test/functional`, 279 test scripts). The goal is to import the
*coverage* of that suite, not to run it.

`registry.yaml` is the tracker and the source of truth. It has one entry per
upstream script. `registry_test.go` enforces its consistency, so the tracker
cannot silently drift away from the code.

Upstream baseline: bitcoin-sv `879fc8b42`.

## Why we port rather than run the Python suite

The two harnesses are not compatible in any shallow way:

- The Python framework launches a single `bitcoind` process with command-line
  flags. Teranode is a multi-service daemon configured through `settings.conf`.
- The suite's most-used RPCs are wallet RPCs (`getnewaddress`, `sendtoaddress`,
  `signrawtransaction`, `listunspent`, `dumpprivkey`, ...). Teranode ships no
  wallet and no wallet RPCs.
- The suite leans on bitcoin-sv test-only hook RPCs — `waitaftervalidatingblock`
  (53 call sites), `softrejectblock` (36), `acceptblock` (27), `setmocktime`
  (23), `getrawnonfinalmempool` (36) — which exist to make the C++ node
  testable and have no Teranode counterpart.
- `mininode.py` assumes one port speaking the BSV wire protocol. Teranode's
  native P2P is libp2p; BSV wire is reachable only through the optional legacy
  service.
- Teranode has no mempool. Transactions go from the validator into block
  assembly, so mempool admission, eviction, and ordering policy have no
  analogue.

Ports target `daemon.NewTestDaemon` and use Teranode's own helpers
(`MineToMaturityAndGetSpendableCoinbaseTx`, `CreateTestBlock`,
`WaitForBlockAssemblyToProcessTx`, ...).

## Triage buckets

| Bucket | Meaning |
|--------|---------|
| **A** | Portable. May need a harness prerequisite (see `needs`). |
| **B** | Portable in principle, but depends on bitcoin-sv test-hook RPCs. Needs a Teranode equivalent or a restructured assertion before it can move to A. |
| **C** | Not applicable. Teranode's architecture has no counterpart — recorded with a reason, never silently dropped. |

Current triage (regenerate the live numbers with the summary command below):

| | Count |
|---|---|
| A — portable | 92 |
| B — blocked on test-hook RPCs | 44 |
| C — not applicable | 143 |
| **total tracked** | **279** |

For the bucket-A breakdown by prerequisite, run `make bsvportingstatus` — it is
printed live and an entry can need more than one, so the counts overlap and do not
sum to bucket A. A hand-written copy used to live here and had already drifted by a
wide margin; the register learned that lesson twice, so this one is a pointer.

Bucket C by reason: mempool policy 45, wallet 31, compact blocks 10,
pre-Genesis activation 9, block files/reindex 9, safe mode 9, node CLI/config 8,
authenticated connections 4, double-spend detector 4, REST 3, miner ID 3,
huge blocks 2, callback service 2, mempool journal 2, associations 2.

## The fidelity contract

A test that passes but asserts nothing has *lost* the coverage it was meant to
import. Counting ported files is therefore not the measure; reproducing
assertions is.

Rules, enforced by `registry_test.go`:

1. `upstream_assertions` must enumerate what the original script actually
   asserts — read the Python, don't infer from the filename.
2. Status `ported` means every listed upstream assertion has a corresponding
   `require` in the Go test. It may not waive anything.
3. Status `ported-partial` means one or more assertions were dropped. Each must
   appear in `waived_assertions` formatted `"assertion: reason"`.
4. Every `Test*` function in this package must be claimed by exactly one
   registry entry, and every `ported_to` must name a function that exists. A
   port cannot be added without a tracker row, and a tracker row cannot point at
   a test that was renamed or deleted.

`bsv_invalidblockrequest_test.go` is the worked example: it predates this
contract, and is recorded as `ported-partial` with all three upstream assertions
waived, because it never builds the malformed blocks the original feeds over
`mininode`. It is the reference for how to be honest in this file rather than a
model port.

## Status vocabulary

| Status | `ported_to` | Meaning |
|--------|-------------|---------|
| `todo` | must be empty | Triaged as portable, not started. |
| `in-progress` | must be empty | Being ported now. |
| `ported` | required | Full assertion fidelity. |
| `ported-partial` | required | Some upstream assertions waived, each with a reason. |
| `blocked` | must be empty | Prerequisite missing; `reason` required. |
| `not-applicable` | must be empty | Bucket C; `reason` required. |

## Workflow

Update the registry entry **in the same commit as the port**. The validator turns
a forgotten update into a test failure, which is the point.

To port a script:

1. Read the upstream script and fill `upstream_assertions` with what it asserts.
2. Set `status: in-progress`.
3. Write the Go test. Its doc comment should name the upstream script and
   restate the assertions being reproduced.
4. Set `status: ported` (or `ported-partial` with waivers) and `ported_to`.
5. Run the validator.

```bash
# validate the tracker and the fidelity contract
go test ./test/e2e/daemon/bsv/ -run TestRegistry

# live progress report
go test ./test/e2e/daemon/bsv/ -run TestRegistrySummary -v

# drift against an upstream checkout (never writes to the registry)
python3 test/e2e/daemon/bsv/tools/upstream_drift.py \
    /path/to/bitcoin-sv/test/functional
```

`TestRegistry*` are pure file/AST checks — no daemon, no Docker — so they are
cheap enough to run on every change.

## Harness prerequisites

The `needs` field names what is missing, drawn from a closed set — `validNeeds` in
`registry_test.go`. Closed because `needs` drives the bucket-A breakdown in `make
bsvportingstatus`, so a typo would not fail anything, it would invent a
prerequisite and quietly split one count into two. Adding a real prerequisite means
adding it there and describing it here.

Two pieces of harness unlock most of bucket A:

- **`wirepeer`** — **built**, see [test/utils/wirepeer](../../../utils/wirepeer).
  A `mininode` analogue: connect to a daemon started with `EnableLegacy: true`,
  send hand-crafted `block`/`tx`/`inv`/`getheaders` messages, wait for or assert
  the absence of a reply, and match reject reasons. There are two escape hatches
  for bytes a conforming encoder would not produce, and malformed-message ports
  need the right one: `raw.go`'s `RawConn` writes to a bare socket, before any
  handshake, while `Peer.SendRawFrame` writes over an already-negotiated
  connection. The distinction is not cosmetic — the node reads the two phases
  with different decoders reached by different error paths
  (`peer.readMessage` during negotiation, `peer.readMessageStreaming` in the main
  loop), so the wrong one measures the wrong thing. Upstream's malformed-message
  scripts connect through `run_node_with_connections`, which handshakes, so they
  port onto `SendRawFrame`; `RawConn` is for input that is meant to arrive before
  the node knows who is talking. Three self-tests in `wirepeer_smoke_test.go` cover the
  outbound handshake, an inbound `getheaders`/`headers` round trip, and reject
  observation; run those first when a port misbehaves, to tell harness faults
  from port faults.
- **`funding-shim`** (unlocks 25) — the wallet-shaped operations the tests
  actually use (`newAddress`, `fundToAddress`, `signRawTransaction`,
  `listUnspent`) implemented over `MineToMaturityAndGetSpendableCoinbaseTx` and
  `CreateTransactionWithOptions`, falling back to a `test/utils/svnode`
  sidecar (real `bitcoinsv/bitcoin-sv` in Docker) where genuine wallet
  behaviour is under test.
- **`frozen-txo`** (unlocks 5 in bucket A) — a way to freeze and unfreeze a UTXO
  from a test. Teranode *has* the feature: [`FreezeUTXOs`/`UnFreezeUTXOs`](../../../../stores/utxo/Interface.go)
  on the UTXO store, `ERR_UTXO_FROZEN` (72) when a spend is refused, and
  [`alert.Node.AddToConsensusBlacklist`](../../../../services/alert/node.go)
  driving it from a signed alert. What is missing is the door: `daemon.TestOptions`
  has no alert-service option, so the alert node is unreachable from
  `TestDaemon`.

  Upstream drives all of this through three RPCs, and Teranode has **none** of
  them — no `queryBlacklist`, no `addToPolicyBlacklist`, no `clearBlacklists`
  anywhere in the tree. Two consequences worth knowing before picking this up.
  Teranode has consensus-level freezing only, with no policy level, so upstream's
  policy assertions have no counterpart rather than merely no plumbing. And with
  no query surface the blacklist cannot be read back at all, which is what most of
  `bsv-frozentxo-freezefunds.py` asserts.

  So this prerequisite is a *choice*, not just work: either expose the alert
  service to `TestDaemon` and port the RPC-shaped assertions that survive, or skip
  the bookkeeping and assert the substance — freeze a UTXO via `td.UtxoStore`,
  spend it, require `ERR_UTXO_FROZEN`. The second is reachable today and tests
  what the feature exists for; it just is not what the upstream scripts check.
Bucket B needs a per-hook decision, recorded in the entry's `reason` when
resolved. `make bsvportingstatus` prints the live hook histogram; see Phase 6 of
the roadmap for the per-hook reading, and the gap register for the two hooks held
outside this exercise.

## Known harness limitations

Findings from building `wirepeer` that constrain what a port can assert. Verify
these still hold before working around them.

- **The test peer cannot reuse `services/legacy/peer`.** That package tracks sent
  version nonces in a package-level MRU map and treats a known nonce as a
  self-connection. A `TestDaemon` shares the process with the test, so a
  `peer.Peer` client trips the node's own check and is dropped with
  "disconnecting peer connected to self". `wirepeer` therefore implements the
  client handshake directly on `go-wire`. Do not "fix" this by disabling the
  node's self-connection defence.
- **Activation heights are movable, and safely.** 58 upstream scripts pass
  `-genesisactivationheight` or `-chronicleactivationheight`, and under
  `SETTINGS_CONTEXT=test` the network is `regtest`, where Genesis activates at
  **10000** — so a port cannot mine to the far side of the fork, it has to move the
  height. Use `wirepeer.WithGenesisActivationHeight`. This looks unsafe and is not:
  `chaincfg.GetChainParams` returns a pointer to a package-level struct, but
  `daemon.NewTestDaemon` copies `RegressionNetParams` and repoints
  `ChainCfgParams` at the copy *before* applying `SettingsOverrideFunc`
  (`daemon/test_daemon.go`). Overrides therefore land on a copy the daemon owns.
  `TestWirePeerChainParamsAreIsolated` locks that in, because nothing at a call
  site would reveal if the copy went away. The same copy sets `CoinbaseMaturity`
  to 1, so ports do not inherit regtest's 100-block maturity.
- **At most 5 test peers per node.** `services/legacy/config.go` sets
  `defaultMaxPeersPerIP` to 5 and `peer_server.go` enforces it with no bypass for
  loopback or whitelisted addresses. Every `wirepeer` dials from `127.0.0.1`, so
  the sixth is disconnected before it sends its version and `Connect` fails with
  "no version from node within 30s" — a timeout that looks nothing like the
  limit that caused it. Ports that need a crowd (`p2p-connections.py` wants 8)
  must scale to 5 and waive the count. Do not raise the limit to make a port
  pass: it is the node's per-IP DoS protection, and a test that switches off a
  defence is testing a node nobody runs.
- **A test peer must present a BSV user agent.** The legacy service bans any peer
  whose agent contains neither "Bitcoin SV" nor "BSV"
  (`services/legacy/peer_server.go`, `OnVersion`), so `wirepeer` defaults to
  `/Bitcoin SV:1.2.2(teranode-wirepeer)/`. Overriding it with `WithUserAgent` is
  how the `bsv-ban-useragents.py` port exercises the rule — and an easy way to
  break an unrelated port by accident.
- **`SETTINGS_CONTEXT` must be in the environment before the process starts.**
  gocore resolves it once, on the first `Config()` call, and `ui/dashboard`'s
  package `init()` makes that call before any test code runs — so `os.Setenv`
  from `init` or `TestMain` is too late (verified: both were tried and both
  failed). Unset, the context defaults to `dev`, which points at Postgres and
  Aerospike, and the daemon dies 30s later with "failed to create postgres
  schema". Run ports with `make bsvporttest`, or `SETTINGS_CONTEXT=test go test`.
  `wirepeer.NewLegacyDaemon` fails fast with this explanation rather than letting
  the daemon time out.
- **A `TestDaemon` can be stopped and replaced within one process, if the
  replacement gets its own data folder.** This bullet used to say it could not:
  that a second daemon started after the first was stopped failed to come up
  within 30s. `TestBSVP2PMaxConnectionsFromAddr` does exactly that and passes —
  two sequential subtests, each starting and stopping its own
  `NewLegacyDaemonWithP2P`, one of them with a different `legacy_config_Whitelists`
  because the legacy whitelist is only read at server start. The distinguishing
  factor is very likely the data folder: `DataFolder` derives from `t.Name()`, so
  daemons created inside *subtests* get one each, while two created in the same
  top-level test body would share one. Not proven — the shared-folder case has not
  been re-measured — so treat "own subtest, own folder" as the supported shape and
  reach for a restart only when a port genuinely needs different node
  configuration, which is what upstream's repeated
  `with run_node_with_connections(...)` blocks usually mean.

  Where a port only needs node *state* reset rather than a restart, resetting is
  still cheaper and clearer: `wirepeer.ClearBanned` exists for exactly this and
  mirrors upstream's own `clearbanned` calls. Reset from `t.Cleanup`, not the end
  of the subtest body, or a failing assertion leaks state into the next subtest and
  fails it for the wrong reason.
- **`t.Cleanup` inside a subtest fires when that subtest returns.** Obvious written
  down, easy to get wrong: a helper that registers `t.Cleanup(peer.Close)` and is
  called from a subtest tears its peers down at the end of *that* subtest, so a
  following subtest that depends on those connections finds none. It cost two
  iterations on `TestBSVP2PMaxConnectionsFromAddr`, whose second step needs the
  per-IP cap still full. Either pass the enclosing test's `*testing.T` explicitly,
  or write the dependent steps as one subtest — that port does the latter, since
  the coupling is real and hiding it behind two names made it worse, not better.
- **A banned peer gets silence, not a closed socket.** After a ban, a reconnect
  from the same address receives no `version` at all, but the node was not
  observed closing the TCP connection within 30s. Ports should assert the absence
  of a handshake (`AssertNotReceived`), not disconnection.
- **A locally mined block is not announced to a legacy wire peer** in the
  in-process `TestDaemon` configuration — tracked as gap
  `legacy-block-announcement`, see the gap register below. Ports that request data
  (`getheaders`/`getdata`) work today; ports that need the node to *volunteer* a
  block do not. Not a `wirepeer` bug: the inbound round-trip self-test passes.

## Gap register

`registry.yaml` has a top-level `gaps:` list alongside `entries:`, validated by
`TestRegistryGaps`. A gap is an obstacle standing between the current triage and
a larger portable set. Tracking them as data rather than prose means the number
of tests each one holds up is *derived* (`make bsvportingstatus` prints it) and
cannot drift from a hand-written figure, and a gap cannot be quietly dropped.

Each gap carries `kind` (`test-config` | `defect` | `product-decision` |
`unknown`) and `status` (`open` | `investigating` | `deferred` | `resolved`).
`kind` matters because it decides who acts: a `test-config` gap is ours, a
`defect` belongs in the issue tracker, a `product-decision` is not ours to make.
A gap must name what it blocks — `blocks` for specific scripts, `blocks_hooks`
for clusters too broad to enumerate — and the validator rejects a script or hook
no entry actually has, so a rename cannot leave a stale plan behind.

Some defects cost no coverage at all: a slow RPC, a wrong log line, something
that is merely wrong on the way past. Those still belong in the register, so a gap
may leave `blocks` empty *if* it sets `found_while_porting` to the script whose
port turned it up. That is the only way to an empty `blocks` list —
`TestRegistryGaps` rejects a gap that neither blocks anything nor says where it
came from, so "blocks nothing" is always a claim someone made on purpose.

**To read the register, open [`GAPS.md`](GAPS.md)** — every gap in full, worst
first, browsable on GitHub and linkable from an issue. It is generated from
`registry.yaml` by `TestRegistryGapsDoc` and checked for drift on every `make
test`, so it is a view of the data rather than a second copy of it. Editing it by
hand fails the build; edit the `gaps:` block and run `make bsvportinggapsdoc`.

The same content from the command line:

```bash
make bsvportinggaps     # every gap in full: title, impact, plan, what it blocks
make bsvportingstatus   # progress counts, with one summary line per gap
make bsvportinggapsdoc  # regenerate GAPS.md after editing gaps: in registry.yaml
```

All three read `registry.yaml` directly. None needs `SETTINGS_CONTEXT` — no daemon
is started.

This section deliberately no longer lists the gaps. It used to carry a table, and a
hand-maintained table is exactly the thing that goes quietly wrong: it was already
the one copy of the register that could disagree with the data. [`GAPS.md`](GAPS.md)
is generated, so it cannot. What follows is commentary on the register, not a
duplicate of it.

The `defect` gaps were found by writing a port, which is the exercise paying
for itself: `setban` never registers a ban with the legacy service at all, so
operators cannot ban legacy peers, and
`listbanned` double-counts. Neither is fixed here — a porting change should not
also change node behaviour — but both are now recorded with the code references
needed to raise them.

`net-rpcs-unimplemented` shows the other thing porting surfaces: how much of the
operator-facing RPC surface is a stub. Of the eight net RPCs `net.py` exercises,
only `getpeerinfo` answers. `TestBSVNet` therefore ends with a *tripwire* subtest
that asserts the other seven still fail — the port breaks the moment one is
implemented, which is exactly when its waived assertions should be written. Copy
that pattern for any port whose waivers rest on "not implemented yet"; a waiver
with nothing watching it silently becomes a lie.

`getpeerinfo-stalls-without-p2p-service` is the third defect the exercise has
turned up, and the only one that costs no coverage: `getpeerinfo` takes 9.76s on a
node running the legacy peer service alone, and 0.5ms once the P2P service is also
running — same answer both times. The RPC service is handed a P2P client whether or
not that service exists, and waits out ~10s of gRPC retries before replying. It
surfaced only because a polling assertion was inexplicably slow, which is worth
remembering: **a test that passes slowly is still telling you something.**

A pattern worth naming, since it now accounts for most of the register: the
blocker is usually **not** a missing capability but a missing way to see it.
Teranode freezes UTXOs, it just cannot be asked to from a test. It keeps a per-peer
inv send queue, it just never reports the depth (`getpeerinfo-omits-txninvsize`).
It bans legacy peers on misbehaviour, but `setban` cannot reach that path. Each
time, the feature is there and the observation point is not — so triaging a script
by whether Teranode *has* the behaviour will overestimate what is portable. Ask
instead what the assertion has to read.

Two notes on the others, since both are easy to mis-read as ordinary to-do items:

- **`legacy-block-announcement` needs diagnosis before implementation.** It is
  currently `kind: unknown` on purpose. If the in-process `TestDaemon` never
  publishes to `blocks-final`, it is a harness fix. If it does publish and the
  consumer ignores it, then a production Teranode also fails to tell its BSV-wire
  peers about blocks it mined — a defect worth escalating on its own merits,
  independent of this exercise. Set `kind` once the answer is known.
- **`submitblock-rpc` is deliberately out of scope.** It is the largest single
  cluster in bucket B and would be mostly adapter code over the existing
  `getminingcandidate`/`submitminingsolution` path, which makes it tempting. But
  it widens Teranode's public RPC surface, and `submitblock` in particular takes
  a fully-formed block from an untrusted caller — a different trust posture from
  the mining-candidate flow. That belongs in its own proposal with its own
  review. Porting tests must not become a vehicle for expanding the node's API.

## Roadmap

Phases 1 and 2 are done: the triage and tracker exist, and `wirepeer` is built
and self-tested.

**Phase 3 — first ports (current).** Request-driven and reject-driven scripts
only, so nothing depends on `legacy-block-announcement`:

1. `bsv-ban-useragents.py` — the mechanism already works; this port proves the
   tracker workflow end to end.
2. `bsv-empty-payload.py`, `bsv-empty-msg-cmd.py` — the `raw.go` group.
3. `invalidtxrequest.py` — first port with real consensus content.
4. Retire the three waivers on `invalidblockrequest.py`, the one entry that is
   `ported-partial` only because it predates `wirepeer`.

**Phase 4 — `funding-shim`.** Unlocks the 25 bucket-A entries needing wallet-
shaped operations.

**Phase 5 — remaining bucket A** in value order: frozen-TXO, chain state,
P2P/ban behaviour, Genesis script rules.

**Phase 6 — bucket B**, one decision per hook, recorded in the entry's `reason`.
`setmocktime` (7) is plausibly worth adding; `waitaftervalidatingblock` (1) may
be replaceable by the existing `WaitForBlockStateChange`; `softrejectblock` (2)
and `acceptblock` (2) are genuine product decisions. `submitblock` and
`getblocktemplate` are held in the gap register instead.

## Adding a bucket C entry

Reclassifying a test as not applicable is a real claim about Teranode's
architecture. The `reason` must say which Teranode design choice removes the
counterpart — not "not supported". Compare the existing C reasons for tone.
