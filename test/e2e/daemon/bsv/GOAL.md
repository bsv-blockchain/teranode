# Goal: port the bitcoin-sv functional test suite to Teranode

A standing brief for Claude. Read this first, then [`PORTING.md`](PORTING.md) for
the mechanics. `registry.yaml` is the source of truth for state; `GAPS.md` is
generated from it and must never be hand-edited.

---

## The objective

Give Teranode e2e coverage equivalent to bitcoin-sv's functional test suite
(279 scripts, baseline commit `879fc8b42`, checked out at
`/Users/sg/bsv-blockchain/bitcoin-sv/test/functional`), and an honest, reviewable
record of every place the two nodes genuinely differ.

**Both halves count.** A gap found and written up well is worth as much as a port
landed. This exercise has already surfaced a consensus-level defect; finding the
next one is a success, not a detour.

## Definition of done

Every bucket-A entry in `registry.yaml` reaches a terminal status:

- `ported` — every upstream assertion reproduced, nothing waived
- `ported-partial` — ported, with each waiver written as `"assertion: reason"`
- `not-applicable` — no Teranode counterpart by architecture, with the reason
- `blocked` — genuinely blocked, with a gap recording the obstacle

Plus: `make bsvportingcheck` and `make bsvporttest` green, and every open gap
carrying an impact, a plan, and a clear line between what was **measured** and
what was **inferred**.

Buckets B and C are not in scope to port. B is blocked on test-hook RPCs
(`submitblock` alone holds up 30 scripts) and widening Teranode's public RPC
surface is deliberately out of scope here. C is not applicable by architecture.
Re-triage an entry only if the code proves the original call wrong.

## Where things stand

Re-derive rather than trusting these — they age:

```bash
SETTINGS_CONTEXT=test go test -count=1 -v -run TestRegistrySummary ./test/e2e/daemon/bsv/
```

At the time of writing: **11 of 92 portable ported** (0 full, 11 partial),
81 todo, 13 gaps (8 `defect`). 7 bucket-A entries declare no prerequisite and no
gap; a further 64 declare `wirepeer`, **which is built** — so those are actionable
too. `funding-shim` (26) and `frozen-txo` (5) are not built; an entry needing
either is `blocked`, not `todo`.

---

## Hard rules

These are not style preferences. Breaking one invalidates the work.

### 1. Never log a bug without explicit permission

Record findings as gaps in `registry.yaml` and **stop**. The user reads and
analyses every gap before anything is filed, and says when to log. Permission for
one batch is not permission for the next — ask each time.

When permission is given: the tracker is **`bitcoin-sv/teranode` (private)** and
`gh` needs `-R bitcoin-sv/teranode` explicitly. Bare `gh issue create` resolves to
the git `upstream` remote, `bsv-blockchain/teranode`, which is the **public**
mirror and the wrong place. Security-sensitive issues take a `[security]` title
prefix. Check for duplicates first; cross-reference the gap `id` both ways.

### 2. Never change node behaviour in a porting change

If a port fails because Teranode is wrong, the port records a gap and asserts what
Teranode actually does — often as a tripwire subtest that fails the day the defect
is fixed. It does not fix the node.

### 3. Never weaken a node defence to make a port pass

The per-IP peer cap of 5, the self-connection check, the user-agent check and
similar exist for a reason. A port that needs one relaxed is a port that needs
rethinking, or a gap.

### 4. Never hand-edit `GAPS.md`

It is generated. Edit the `gaps:` block in `registry.yaml`, then run
`make bsvportinggapsdoc`.

### 5. A thin port is worse than no port

If a script turns out not to be portable, say so and record the reason in the
registry. That has happened several times and is a valid, useful outcome. What is
not acceptable is a test that runs, passes, and asserts nothing of substance —
that is how the old `TestBSVInvalidBlockRequest` sat skipped and worthless for
months.

---

## The fidelity contract

Enforced by `registry_test.go`, so it cannot quietly rot.

1. Read the Python. Enumerate **every** assertion it makes into
   `upstream_assertions` — including the ones implied by the framework, such as a
   later step building on the same parent, which only holds if the rejected block
   never became the tip.
2. Each one is either reproduced, or listed in `waived_assertions` as
   `"assertion: reason"`. The reason must say *why Teranode cannot express it*, not
   that it was inconvenient.
3. `ported` waives nothing. Any waiver means `ported-partial`.
4. If a waiver exists because Teranode is defective, the waiver text points at the
   gap `id`.

**Measure, do not assume.** Two examples from work already done:

- The plan for `invalidblockrequest.py` said to assert upstream's reject reasons
  over wirepeer. Measurement showed Teranode's wire reject carries the fixed string
  `"block rejected"` for every cause, so the reasons were asserted on the
  `ProcessBlock` error instead, and the discrepancy became a gap. The plan was
  written on an assumption; the code disagreed; the code won.
- Two ports were written against `RawConn` per PORTING.md's guidance and measured
  the wrong thing. Upstream's malformed-message scripts handshake first, so they
  needed `Peer.SendRawFrame`. PORTING.md was corrected.

---

## The loop

For each entry, work one script at a time and land it green before starting the
next.

1. **Pick** a bucket-A `todo` entry. Prefer clusters — scripts sharing a harness
   need pay for that harness once.
2. **Read the upstream Python in full.** Enumerate its assertions before writing
   any Go.
3. **Check the mechanism exists.** Grep for the rule in Teranode before assuming
   its absence, and before assuming its presence. Rules live in unexpected layers:
   duplicate transactions are caught by subtree validation, not
   `model.Block.checkDuplicateTransactions`; duplicate inputs come back from GoBDK
   with upstream's exact string.
4. **Write the port**, mirroring upstream's structure so the two can be read side
   by side. Cite the upstream construct in comments.
5. **Run it.** Iterate against real daemons until green. Never claim "should work".
6. **Record** in `registry.yaml`: status, `ported_to`, `upstream_assertions`,
   `waived_assertions`, and any new gap.
7. **Verify** — the full sweep below.
8. **Report** what landed, what was waived and why, and any gap found. If gaps were
   found, say plainly that they are unlogged and awaiting review.

## Verification

Run all of it before claiming anything is done, and loop until green.

```bash
gci write --skip-generated -s standard -s default <changed .go files>
golangci-lint run ./test/e2e/daemon/bsv/... ./test/utils/wirepeer/...
make bsvportingcheck      # registry + GAPS.md drift; no daemon, fast
make bsvporttest          # real daemons, ~60-70s
make bsvportinggapsdoc    # only when the gaps: block changed
```

---

## Harness facts worth not rediscovering

- **`SETTINGS_CONTEXT=test` must be in the environment before process start** —
  gocore resolves it once during package init.
- Under that context the network is **regtest**, and `CoinbaseMaturity` is **1**,
  not 100. Genesis activates at 10000, so move the activation height rather than
  mining to it: `wirepeer.WithGenesisActivationHeight()`.
- **Chain params are already isolated.** `daemon/test_daemon.go` copies
  `RegressionNetParams` and repoints `ChainCfgParams` at the copy *before* applying
  `SettingsOverrideFunc`. `TestWirePeerChainParamsAreIsolated` locks that in. Do not
  add your own copying.
- **Prefer `wirepeer.NewLegacyDaemonWithP2P`.** Without the P2P service,
  `getpeerinfo` and `getinfo` stall ~10s per call — see the
  `getpeerinfo-stalls-without-p2p-service` gap.
- **Use the `try*` helpers** (`tryPeers`, `tryBestBlockHash`, `tryBestHeight`, …)
  inside `require.Eventually` / `require.Never`. Asserting inside a testify polling
  goroutine causes late flakes.
- **Two escape hatches for malformed bytes, and they measure different things.**
  `RawConn` writes to a bare socket before any handshake; `Peer.SendRawFrame`
  writes over an already-negotiated connection. The node reads the two phases with
  different decoders (`peer.readMessage` during negotiation,
  `peer.readMessageStreaming` in the main loop). Upstream scripts using
  `run_node_with_connections` handshake first, so they port onto `SendRawFrame`.
- **Teranode rejects unrequested blocks** from a wire peer and disconnects. To feed
  a block over the wire: announce by `inv`, wait for `getdata`, then send it.
- **`ProcessBlock` vs the wire.** The wire reject carries code 16 but an opaque
  reason. Assert specific rejection reasons on the `ProcessBlock` error; use the
  wire for what only the wire can show (request/response exchanges, disconnects).
- **Don't mock** the blockchain client/store (use `sqlitememory`) or Kafka (use
  `in_memory_kafka.go`). Use `require`, not `assert`. Avoid `t.Parallel()` unless
  the test is specifically about concurrency.

## Stop and ask

Do not decide these alone:

- Filing, editing or closing **any** tracker issue (rule 1).
- Changing node behaviour, even when the fix looks obvious and small.
- Implementing a missing RPC to unblock a port — that widens Teranode's public API
  and belongs in its own proposal.
- Re-triaging an entry between buckets on judgement rather than evidence.
- Adding a new value to `validNeeds` — the closed set is deliberate.
- Anything that would relax a node defence.

Otherwise: keep going. Pick the next entry, port it, verify it, record it, report
it. Ask when blocked on a decision that is genuinely the user's; make ordinary
engineering calls yourself and say what you assumed.
