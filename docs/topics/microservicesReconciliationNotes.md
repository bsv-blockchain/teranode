# Reconciliation Notes — Microservices Guide

Internal working notes, not part of [How Teranode's Microservices Work](microservicesInPrinciple.md).

While writing that guide, statements were checked against the code rather than against
the existing documentation. This file records where reality differed from expectation,
where the existing documentation and the code disagree, and where the code has plumbing
that nothing currently uses.

Nothing here is a claim that the guide is wrong. It is a list of things a reader of the
guide would otherwise reasonably assume, plus a few loose ends for maintainers.

Read this file top to bottom once; afterwards it is a lookup table.

## Conventions

Every entry carries file and line references so each claim can be re-checked rather than
trusted. Line numbers refer to the working tree at the time of writing and will drift.

Entries are marked:

- **Confirmed** — code agrees with expectation; noted only because it is surprising.
- **Divergence** — code and existing documentation say different things.
- **Gap** — the code offers a mechanism that no caller uses.
- **Judgement call** — the guide simplifies in a way the reader should know about.

## Decisions taken

Recorded because a future reader meeting only the findings would otherwise reasonably
propose the opposite.

- **Nothing in these findings is to be ported into existing project files.** The
    discoveries below — dependency levels, the FSM guard, the reputation-versus-ban
    distinction, the overloaded use of "level" — stay in the two new documents. The
    existing documentation set is left exactly as it is, contradictions and all, and is
    not amended to agree with the guide. Where the two disagree, this file is the record
    of the disagreement.
- **Where the examples disagree, the guide treats the production-shaped deployment as "as
    shipped".** Sections 25 and 26 establish that `teranode-cr.yaml` is a development
    fixture, and that `teranode-cr-mainnet.yaml` and the Docker overlays agree with each
    other. Scaling commentary now says which of the two it means, rather than averaging
    them. This is a reasoning choice, not an instruction from a maintainer — open question 8
    keeps the underlying question live.
- **Both new documents stay off the site navigation.** Deliberate for now, not an
    oversight. Neither is listed in `mkdocs.yml`, so neither appears in the built site.
    Worth being explicit about, since a page sitting in `docs/` is easily mistaken for a
    page that has been published.
- **For the interfaces, "published contract" means reading the files, not listing their
    names.** Section 76 finds two of the eight contract files deliberately misnamed
    (`services/propagation/ClientInterface.go`, `services/blockassembly/Interface.go:66`), so
    every count in the interfaces section came from reading interface declarations rather than
    from a filename search. The same choice made the Legacy contract visible: it sits a
    directory deeper than the convention implies (`services/legacy/peer/Interface.go`), which a
    survey by filename reports as "Legacy publishes nothing".
- **A door counts as unauthenticated only if its server start proves it.** Section 78 is argued
    from the last argument passed to each `StartGRPCServer` call — `nil` for five services, an
    options block without a key for a sixth — rather than from the absence of a middleware line.
    Propagation is the reason for the strictness: it builds an auth options block, which reads as
    "authenticated" until the shared installer is checked (`util/grpc.go:83-88` requires either a
    default-deny flag or a non-empty key, and Propagation supplies neither).

This means section 7 describes an overlap that is being *left* in place, and section 8
describes a naming collision that is being *left* unresolved except by the
disambiguation line in the guide. Neither is awaiting a fix. The deployment reading in
sections 25 to 29 is the one place this pass changed the guide's own wording rather than
only recording a disagreement: the guide had the front door listed as wholly unresolved,
and the evidence in this tree settles which door ships even though the intent behind it
stays an open question. The interfaces work moved the guide's prose in two places for the
same reason. Part One used "is this transaction valid?" as its example of a phone call, and section
81 shows that question answered without a phone call in the shipped configuration — Part One now says
the example describes the design rather than the default. And the interfaces section first described
the eight Validator replicas as reached through the Kubernetes door, which section 81 shows they are
not: they are fed by the conveyor belt, which is why the topic carrying that work is given thirty-two
partitions in the same settings file.

---

## 1. An invalid subtree costs a peer reputation, not ban score

**Confirmed, and the asymmetry is easy to get wrong.**

When subtree validation rejects a bundle, the complaint reaches P2P and is charged
against the responsible peer. The natural assumption — which the first draft of the
guide made and which is wrong — is that the peer gets banned.

It does not. The charge is one recorded failed interaction:

- `ReportInvalidSubtree` — `services/p2p/Server.go:3273` — resolves the offending peer
    and finishes by calling `UpdatePeerMetrics(... recordFailure=true ...)`, then returns
    `nil`. No ban score is touched.
- `UpdateMetrics` — `services/blockchain/peer_registry.go:474` — increments
    `InteractionAttempts` and `InteractionFailures`, then recalculates the reputation
    score.

Reputation is computed at `services/blockchain/peer_registry.go:688`: baseline 50,
weighted 60% toward success rate, minus 15 for a failure within the last hour, plus 10
for a recent success, scaled by a response-speed factor. A peer marked malicious is
pinned at 5, and the code comments make explicit that no other factor redeems it.

### The asymmetry worth flagging

An invalid **block** *does* attract ban score:

- `processInvalidBlockMessage` — `services/p2p/server_helpers.go:939` — charges
    `ReasonInvalidBlock` through `AddBanScore`.

An invalid **subtree** does not. The same event class, two different penalties.

This looks deliberate rather than accidental. The comment above the reputation call
reads: *"This will not be too harsh on failures of transient behavior but also track if
we are having trouble fetching data from peers"* (`services/p2p/Server.go`, in
`ReportInvalidSubtree`). A subtree can fail to validate because a piece went missing in
transit, which is a network fault and not an indictment of the peer. A whole block
arriving corrupt is harder to blame on the network.

Worth confirming with the maintainers that this is intended policy and not a wiring
oversight, because it is the kind of thing that reads as a bug.

## 2. `ReasonInvalidSubtree` is defined and never charged

**Gap.**

`ReasonInvalidSubtree = "invalid_subtree"` is declared at `services/p2p/ban_reasons.go:13`
and admitted to the allowlist at `services/p2p/ban_reasons.go:38`.

No production code path passes it to `AddBanScore`. Every caller in the tree:

- `services/blockvalidation/BlockValidation.go:2738` — `ReasonCorruptBlockBody`
- `services/p2p/server_helpers.go:939` — `ReasonInvalidBlock`
- `services/p2p/Server.go:3095` — `applyBanScore`, generic helper, reason supplied by
    caller

So the constant exists, the penalty-point lookup on the Blockchain side is built to
accept it, and nothing fires it.

Care needed when reading the allowlist: `knownBanReasons` is a **Prometheus cardinality
guard**, not a usage inventory. The comment at `services/p2p/ban_reasons.go:31` explains
that its job is to stop an unrecognised incoming reason minting unbounded metric series.
An entry appearing there means "this reason is a recognised label value", not "this
reason is enforced". Grepping that map for evidence of behaviour will mislead.

Consequence: if someone later wires subtree failures through `AddBanScore`, the
allowlist and penalty configuration already accept the reason, so the change is one
call site.

## 3. "Horizontally scalable" needs two answers, not one

**Confirmed; existing documentation already says so, and this guide's first draft did
not.**

Subtree validation is built as a stateless worker drawing from a Kafka topic, which is
the pattern that divides across instances cleanly. As shipped, it runs one instance.

From `deploy/kubernetes/teranode/teranode-cr-mainnet.yaml`: every service that is
enabled is pinned to `replicas: 1` except the Validator at `replicas: 8` (line 183). The
general `teranode-cr.yaml` pins everything to 1.

That needs one correction, which this file got wrong on first pass. `replicas` is not the
whole deployment story, because several services in the same file carry `enabled: false`
and so run *zero* instances — among them the P2P service itself. "One copy" and "the
example does not deploy it" are different statements about the same component. See
section 9.

This means that for most services, throughput available today comes from intra-instance
parallelism across the cores of one machine, not from a fleet. Widening is a deployment
decision rather than a rebuild, but it is not switched on.

`teranode-microservices-overview.md` carries the same caveat in its introduction, so
this is a correction to the new guide's first draft, not to the existing set. Recorded
here because the two documents must keep saying the same thing.

### Where the bottleneck is expected

The Validator at eight replicas, against one of everything else, is a load-bearing hint
about operator expectations: signature checking is the anticipated constraint. It also
means the guide's per-service scaling notes are incomplete on their own — subtree
validation sorts work into parallel levels and then hands the heavy arithmetic to the
Validator, so effective scaling is a property of the pair.

## 4. Who gets blamed for a bad bundle is a real problem, not a formality

**Confirmed, and understated by the existing documentation.**

A subtree is typically *announced* by one peer and *served* by another. The comment at
`services/p2p/Server.go` in `ReportInvalidSubtree` states the situation plainly:
subtrees are *"announced by the miner that built them but served invalid by whichever
peer's DataHub the block came from"*. Charging the announcer for a defect in bytes it
never sent would penalise an innocent party.

Three behaviours follow, all of which the guide now describes:

- Attribution resolves through the DataHub URL the bytes were actually fetched from, to
    the peer registered as its owner.
- Where no serving peer can be identified, nobody is charged and the function returns
    successfully. Under-information produces restraint, not a guess.
- Announcer-versus-server mismatch is logged at info level rather than silently
    reconciled.

Related, and separately useful: bans are held in their own ledger and survive a peer
disconnect on purpose. `Remove()` at `services/blockchain/peer_registry.go:485` keeps
the ban-score entry, with the comment that otherwise *"an offending peer could clear its
own ban simply by reconnecting"*.

## 5. The FSM guard prevents a specific and unattractive failure

**Confirmed; not mentioned in the existing service documentation.**

Peer-announced subtrees are only validated while the Blockchain service reports
`RUNNING` — `services/subtreevalidation/subtreeHandler.go:62`.

The comment explains why the check is not simply "is the service up": an `IDLE` state
can follow an operator `STOP` partway through catch-up, leaving a node holding a UTXO
set far behind the tip. A node in that position lacks the information to judge whether a
bundle is valid, and would confidently reject honest peers' data.

Worth stating in operational terms, since the consequence is uneven: a node restarting
mid-catch-up without this guard would turn against healthy connections at exactly the
moment it most needs them.

## 6. Judgement calls made in the guide

Points where the guide simplifies, so a maintainer is not surprised:

- **"Roughly every ten minutes"** for block arrival is the nominal target, not a
    guarantee.
- Subtree structure is described as transactions *"stacked above"* their dependencies.
    Accurate enough for the dependency-levels argument, but it omits padding rules and
    the leaf-count policy check at
    `services/subtreevalidation/check_block_subtrees.go:1696`.
- **Subtree validation** is described as producing *"receipts"* for downstream
    services. These are `txmeta` records published to Kafka, whose actual shape is the
    `meta.Data` struct at `stores/utxo/meta/data.go:32` — transaction, inpoints, block
    heights, fee, size, locktime, coinbase flag, unmined marker.
- **Memory handling** is attributed to three habits: streaming rather than bulk loading,
    arena pooling, and early release. All three exist —
    `services/subtreevalidation/arena_pool.go`, `readTransactionsFromSubtreeDataStream`
    at `check_block_subtrees.go:1162`, and explicit early-nil hints in
    `processTransactionsInLevels` at `check_block_subtrees.go:1230`. Their relative
    contribution is not measured in the guide and is not claimed.
- **Duplicate suppression** for complaints refers to the de-duplication map in
    `publishInvalidSubtree` at `services/subtreevalidation/Server.go:1017`. The window is
    one minute — an expiring map constructed at
    `services/subtreevalidation/Server.go:204`. The guide does not state the window.

For the P2P section:

- **Announcement size caps** are stated as ten kilobytes for a block and eight for a
    transaction bundle, from `maxBlockMessageSize` and `maxSubtreeMessageSize` at
    `services/p2p/Server.go:119`. Both are compile-time constants, not operator settings;
    the guide implies the same and should not be read as describing a tunable.
- **"Fair share" eviction** in the bounded announcement-attribution map is described
    qualitatively. The mechanism is documented in detail at the head of
    `services/p2p/capped_peer_map.go`, including the two issue numbers behind the design
    (1409 for inline bounding, 1503 for the fair-share direction) and an explicit
    statement that cross-peer washout is bounded rather than eliminated. The guide keeps
    that caveat but does not quantify the residual.
- **The multi-publisher budget** is described as "a handful". The actual default is
    `seenHashMaxPublishersPerHash` in `services/p2p/seen_hash_cache.go`, operator-tunable
    via `p2p_seen_hash_max_publishers`. The guide is deliberately vague about the number
    and explicit that the value is configurable.
- **"Reputation at or above 20"** is the literal viability floor at
    `services/p2p/sync_coordinator.go:193`. Worth knowing that this is a hard-coded
    constant rather than a setting, and that a peer marked malicious is pinned at
    reputation 5 by `calculateAndUpdateReputation` — so the malicious flag excludes a
    peer from sync selection indirectly, by driving the score below the floor, not
    through a separate check.
- **The three-kinds-of-progress discussion** compresses two distinct mechanisms into one
    narrative: the no-progress stall deadline refreshed only from peer-attributable
    delivery, and the backoff multiplier that deliberately does not de-escalate on local
    tip advance. Both are documented with their full reasoning in
    `resetProbeBudgetIfLocalChainWorkAdvanced` at
    `services/p2p/sync_coordinator.go:434`. The guide's shorter version does not mention
    the probe budget itself, which is a third bounded quantity.
- **Pruned-peer fallback** is described as picking "the youngest" peer. That matches the
    setting description at `settings.conf:1082` — smallest lag, to limit the damage from
    coin records already discarded — and is gated by
    `p2p_allow_pruned_node_fallback`, default true.
- **"A single peer identifier, key and connection set"** in the scaling section is a
    paraphrase of per-instance configuration keys such as `p2p_peer_id` and
    `p2p_private_key`, and of the indexed `p2p-1` instance in
    `compose/docker-compose-ss.yml:204`. The guide does not claim multiple P2P instances
    run in any shipped Kubernetes configuration.

## 7. Documentation overlap

The new guide sits alongside material that already covers some of the same ground,
differently:

- `docs/topics/architecture/teranode-microservices-overview.md` — per-service
    responsibilities, endpoints, interaction diagrams.
- `docs/topics/services/subtreeValidation.md` — detailed reference. Covers subtree
    handling, pause coordination and Merkle level calculation, but does not mention
    dependency levels, the FSM guard, or the reputation-versus-ban distinction.
- `docs/topics/architecture/understandingDoubleSpends.md` — conflict and reorg
    handling, which the guide defers to rather than restating.

No contradiction found between these three and the guide's Subtree Validation section.
The gap is additive: the mechanisms in sections 4, 5 and 6 above are absent from
`subtreeValidation.md`. They will stay absent — see **Decisions taken** — so a reader
who needs them should come to this file rather than expecting the reference to grow.

Where the existing documents are misleading, concentrated in one place:

| Claim                                         | Where it is made                                                                                          | What the code does                                                                                         |
| --------------------------------------------- | --------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Services scale by adding machines             | overview, for every service                                                                               | Shipped examples run one replica of nearly everything (section 9)                                          |
| Invalid subtree leads to being banned         | `subtreeValidation.md`                                                                                    | Costs reputation; never reaches the ban threshold (section 1)                                              |
| Ban reason exists, so it is used              | `docs/references/services/p2p_reference.md:230` lists `ReasonInvalidSubtree` at 10 points                 | Nothing charges it (section 2)                                                                             |
| IPC is shared memory                          | overview                                                                                                  | Uses gRPC and Kafka (section 6 of the notes' decision list)                                                |
| Propagation integrates with the P2P service   | `services/propagation/Client.go:22-28`                                                                    | The server holds no P2P client (section 14)                                                                |
| Policy skips are a caller option              | `docs/references/protobuf_docs/validatorProto.md:114` documents `skip_policy_checks` as a request field   | The unauthenticated HTTP route refuses it (section 16)                                                     |
| Queue depth is the pressure signal            | intuitive, and stated as such in places                                                                   | Depth is used as a fraction for pausing ingest and deliberately ignored for stall detection (section 20)   |
| Legacy is a compatibility layer               | overview                                                                                                  | In the mainnet example it is the only transaction ingress (section 9)                                      |

## 8. "Level" means three different things

**Divergence, or at least a collision waiting to happen.**

The guide explains that a bundle's transactions are sorted into **dependency levels** to
enable parallel checking. The existing reference document uses the same word twice over
for unrelated ideas:

- **Merkle tree levels** — depth layers of the hash tree: leaves, then 500 intermediate
    nodes, then 250. Discussed at
    `docs/topics/services/subtreeValidation.md:198` under "Level Calculation for Merkle
    Trees", where the stated purpose is memory pre-allocation.
- **Two-Level Checking** — a name for the pause-flag lookup pattern at
    `docs/topics/services/subtreeValidation.md:172`: fast atomic boolean locally, then a
    distributed lock check. Nothing to do with either of the other two.

The three are unrelated in substance:

| Term                 | Sorts what                        | Purpose                              |
| -------------------- | --------------------------------- | ------------------------------------ |
| Merkle tree levels   | Hash nodes                        | Memory pre-allocation                |
| Two-Level Checking   | Two ways of reading one flag      | Cheap pause detection                |
| Dependency levels    | Transactions, by what they need   | Ordering, so checking parallelises   |

The collision matters practically because a reader trying to confirm the guide against
the reference document will find "level" documented, and may conclude the mechanism is
the Merkle one. It is not. Worth either renaming in one of the two documents, or
stating the distinction explicitly in both.

The guide now carries a one-line disambiguation in the Subtree Validation section.

## 9. The two example cluster configurations disagree about what runs

**Divergence, and the more consequential finding in this file.**

`deploy/kubernetes/teranode/` ships two examples. They are not two sizes of the same
deployment; they disagree about which services exist.

| Service           | General `teranode-cr.yaml`   | `teranode-cr-mainnet.yaml`   |
| ----------------- | ---------------------------- | ---------------------------- |
| P2P (`peer`)      | enabled                      | **disabled**                 |
| Validator         | **disabled**                 | enabled, 8 replicas          |
| Legacy            | disabled                     | **enabled**                  |
| Propagation       | disabled                     | disabled                     |
| Block Persister   | disabled                     | disabled                     |
| UTXO Persister    | disabled                     | disabled                     |
| Bootstrap         | disabled                     | disabled                     |
| Coinbase          | disabled                     | disabled                     |

Three observations:

- **The file named for mainnet does not run the service that talks to the network**, and
    runs the compatibility layer instead. The modern mesh is off and the legacy Bitcoin
    protocol path is on. That is a coherent choice, but nothing in either file says so,
    and it is not the arrangement a reader would predict from the filenames.
- **The general example does not run the Validator**, which the guide's own scaling
    discussion identifies as the anticipated bottleneck and the only component scaled out
    in the other file. Read alone, the general example suggests validation is not
    deployed at all.
- **Block Persister and UTXO Persister are disabled in both.** Nothing in either example
    enables the two services that file blocks and maintain the coin-set checkpoint, so
    neither example is a complete deployment on its own terms. Whether persistence arrives
    by another route in these topologies or the flags are simply stale cannot be
    determined from the YAML.
- **Propagation is disabled in both, and that one has a customer-facing edge.** Old-protocol
    submitters are unaffected — the Legacy door takes transactions itself — but an HTTP
    submitter posts to the asset server's `/tx` route, which is registered and forwards to a
    service with no instance running (section 13). The read side of the same public address
    works normally, which is what makes the gap quiet.

Read with section 25, which establishes that these are not two production shapes but a
development fixture and a production shape, and section 26, which brings in the Docker
examples that section 9 did not consult. The table below is still accurate; two of its
framings are not.

Practical consequence for anyone using these files as a starting point: the safe
assumption is that neither is a working mainnet deployment as written, and the sensible
next step is to confirm the intended service set with the maintainers. Note also
`image: ghcr.io/bsv-blockchain/teranode:v0.14.4` and host
`mainnet-eu-1.example.com` — placeholders, consistent with these being documentation
examples rather than a live configuration.

The guide's P2P scaling section points here rather than repeating the table, since the
guide should not carry a claim that is currently unresolved.

## 10. A setting that currently does nothing

**Gap.**

`p2p_share_private_addresses = false` (`settings.conf:1187`) is documented by its own
comment as having no effect: with `p2p_advertise_addresses` empty, every bound interface
address is advertised *including private ones*, regardless of this value. The comment
tells the operator to set `p2p_advertise_addresses` instead.

Recorded because the interaction is easy to get backwards. An operator who wants to keep
private addresses off the wire would reasonably reach for the setting named for the
behaviour, find it already set to false, conclude the desired configuration is in place,
and continue advertising private addresses. The setting is not wrong, merely inert, and
the failure is silent.

## 11. A default-on behaviour `settings.conf` never mentions

**Gap.**

`legacy_maxFeelerPeers` is read with a code default of `1` (`settings/settings.go:765`), so
feeler probing is enabled and one peer-admission slot is permanently reserved for it. The key
appears nowhere in `settings.conf`, and its only prose lives in the `longdesc` of the struct
field that declares it (`settings/legacy_settings.go:30`). `docs/topics/services/legacy.md`
does not mention feelers at all.

Recorded because the direction of the existing guard makes this easy to miss. The dead-key
test (`settings/settings_conf_dead_key_test.go`) checks that every key *in the file* resolves
to a real setting — file to code — and describes itself as "deliberately narrower" than the
converse. Nothing checks code to file, so a declared setting can be absent from the shipped
configuration with no test complaining.

The concrete cost is small but real: an operator auditing the committed configuration cannot
see that the node spends one admission slot on background probing, and the reservation is
computed against `legacy_config_MaxPeers` (20) and the automatic outbound target, arithmetic
that only exists in `services/legacy/feeler.go` and `services/legacy/peer_server.go:3060`.

## 12. Nothing in this tree appears to send work to Propagation

**Resolved on a second pass. Kept because the first reading was wrong in an instructive way.**

Propagation is gRPC-driven: `ProcessTransaction` takes a transaction over gRPC and publishes a
Kafka validation message (`services/propagation/Server.go:1606`). A search for importers of
the propagation package across `cmd/`, `services/` and `daemon/` returns only propagation's
own files, plus one incidental mention in a comment (`services/p2p/metrics.go:4`), so no other
service in this checkout constructs a propagation *client*.

That looked like a missing data flow. It is not: the callers arrive over the network, not as
Go imports. The Asset Server proxies `POST {prefix}/tx` and `POST {prefix}/txs` to
propagation's HTTP endpoints when `asset_propagation_proxy_enabled` is set
(`services/asset/httpimpl/http.go:399-405`), and that flag is `true` in the shipped
`settings.conf:234`, with a valid `.operator` address at `:238`. So the ingress is: external
submitter → Asset Server HTTP → Propagation. Everything else is external by design — wallets,
mining pools, applications.

Recorded because "no in-tree importer" is weak evidence about a network service, and the first
reading of this treated it as strong. The guide's Propagation section now states the asset
proxy as the ingress, which is checkable.

Related, and verified: Legacy *does* subscribe to the transaction metadata conveyor belt
(`services/legacy/netsync/manager.go:41`, `:508`) and uses the parent hashes those messages
carry to order its announcements (`:3584`). So the compatibility door is downstream of the
checking pipeline's receipts rather than bypassing them.

## 13. The transaction door is advertised, the service behind it is not deployed

**Configuration divergence a customer would notice.**

Both example cluster configurations set `propagation.enabled: false` — see the table in
section 9 — while `settings.conf:234` keeps `asset_propagation_proxy_enabled = true` and
`:238` gives it a working cluster address for the `.operator` context. The asset server
registers the proxy routes on the strength of the flag and a non-empty address, with no probe
of whether anything is listening
(`services/asset/httpimpl/http.go:399-405`).

So as the examples stand, a wallet posting a transaction to the node's public address is
forwarded to a service with no running instance and gets **502 Bad Gateway**, counted by the
asset server's own proxy counter (`services/asset/httpimpl/propagation_proxy.go:24-28`,
`:52-53`, `:270-275`). That is the correct failure mode — loud, and measured — and it is worth
saying so rather than implying the request vanishes. The read side of the same public address
(`GET /tx/:hash` and friends, registered at `services/asset/httpimpl/http.go:395-397`) is
served locally and works normally, which is what makes the gap easy to miss from outside: half
the API is healthy.

The proxy is a plain `httputil.ReverseProxy` (`propagation_proxy.go:16-29`), which matters for
section 3 of these notes in a good way: a reverse proxy copies the backend's response headers,
so the `X-Teranode-Error` verdict survives the extra hop and still reaches the customer.

Two readings of the disabled flag, and the code cannot decide between them. Either real
deployments take transactions some other way (the legacy door, which accepts old-protocol
submissions and needs neither propagation nor this proxy), or these example files have drifted.
Recorded as a question for a maintainer; the guide says the same thing in plain language rather
than picking a side.

## 14. The propagation package promises P2P integration it has no way to perform

**Gap between package documentation and reachable code.**

The client package's documentation says the service integrates "with the validator (which
validates transactions and forwards them to block assembly for inclusion in subtrees) and the
P2P service, ensuring transactions are properly validated and distributed across the network"
(`services/propagation/Client.go:22-28`).

The server holds a blob store, the validator, a blockchain client, a Kafka producer, an HTTP
server, a ban list, three worker ceilings and its UDP sockets
(`services/propagation/Server.go:119-141`). There is no P2P client, and nothing in the
package references the P2P service. Same shape as finding 2: a plausible-sounding capability
sitting in a doc comment with no caller behind it.

Why it matters more than a stale sentence: the claim is about *how a submitted transaction
reaches the rest of the internet*. Nothing in this service answers that, so the answer lives
in another service's code, and a reader who trusts this comment will look in the wrong place
for the reason a transaction they submitted is not being relayed. Whatever does the relaying,
it is not here.

## 15. The broadcast lane's size limit is 512 bytes, and its comment says 100 MiB

**Misleading constant, wrong by five orders of magnitude.**

`maxDatagramSize = 512 // 100 * 1024 * 1024` (`services/propagation/Server.go:79-80`). The
comment is stale; the value is what the code uses — it sizes the receive buffer for the IPv6
multicast listener (`:496`), which is the only thing the constant feeds.

Consequence in operation: a transaction larger than the buffer arrives shortened, fails to
parse as a wire message, and is dropped with a warning (`:499-535`). There is no metric for
this — the metrics in this service count batches, single transactions, invalid transactions
and admission rejections (`services/propagation/metrics.go:76-138`), and nothing counts UDP
receipts at all. So on a machine where the broadcast lane is quietly losing every large
transaction, the evidence is a warning line per loss.

Recorded because the comment states the opposite of the behaviour, and the number that matters
— how big a transaction this lane can carry — is exactly what someone would look up in a hurry.

## 16. An unauthenticated door used to be able to buy a consensus shortcut

**Security finding, fixed in layers, with the residual written down by the fix itself.**

Issue 4840, finding B-022. The validator's HTTP `/tx` endpoint installs no authentication
middleware (`services/validator/http_options_guard.go:5-7`), and until recently a caller
posting there could set the same options an internal service can: skip script validation, skip
policy checks, spend by outpoint without a hash check, suppress metadata publication, claim
the transaction arrived inside a block — and assert the block height to validate against.

The height is the lever, as the guard's own comment puts it: an asserted low height satisfies
the below-checkpoint guard by construction, so the fast path that skips script execution opens
up on a live transaction. `services/validator/Server.go:970-974` now answers any
behaviour-changing field with a 400 that names it.

Three layers exist now, and the code is unusually precise about what each one buys:

- The server route refuses options and accepts transaction bytes only
    (`services/validator/Server.go:959-981`).
- The client refuses before transmission, so this version of the software is not itself a
    route for those flags — "it does not make the property hold generally", because
    "an un-upgraded validator still parses the old query string for anyone who asks"
    (`services/validator/Client.go:721-733`).
- The fast path is bounded independently by the node's own chain tip, not the asserted height:
    "The guard above bounds the height the CALLER asserted; an attacker simply asserts a low
    one" (`services/validator/Validator.go:1448-1471`).

Two things worth surfacing for anyone reading the guide's Validator section:

- **The real boundary is topology, not this guard.** The guard file says the gRPC listener is
    also unauthenticated on the shipped profile (`security_level_grpc` defaults to 0, no auth
    interceptor), so the guard "removes the HTTP route rather than removing the capability from
    unauthenticated callers". Whether a stranger can reach the validator's port decides the
    exposure, and `validator_httpListenAddress = :${VALIDATOR_HTTP_PORT}` binds every interface
    outside the `dev` context (`settings.conf:1755-1756`).
- **The fix has a maintenance contract, and it is the fragile part.** Every
    behaviour-changing field of the request must be listed in `nonDefaultValidationOptions`;
    a new proto field that skips the listing "becomes a silent hole in this guard — the same
    hole issue 4840 (finding B-022) exists to close". Nothing in the build enforces that
    listing.

Also recorded because it is the kind of detail that surprises integrators: the legacy
non-protobuf request shape on the same endpoint **drops** an option it cannot express rather
than rejecting it, while the protobuf shape rejects. The code says so and declines to change
it — "Whether it should reject instead is a behaviour change, and is not made here"
(`services/validator/Server.go:947-957`).

## 17. Transactions can be accepted, then dropped, and the submitter cannot see it

**Config-and-code finding, quantifiable.**

The ingestion path answers "accepted" at Propagation, once the transaction is durably stored
and queued for validation. Downstream of that promise, if the block-assembly hand-off is
rejected for a full queue, the validator retries in place for a bounded window
(`validator_blockAssemblyShedRetryTimeout`, 2s) and then drops the transaction — after
reversing its coin-store work so the drop leaves no trace. The counter's own documentation
spells the consequence: "Propagation has already returned success to the submitter by then, so
these drops are silent from the client's point of view — this is the counter to alert on"
(`services/validator/metrics.go:196-200`).

A second, subtler variant is logged as a counter too: a *block-context* transaction accepted,
unlocked and spendable, but absent from this node's mining template — "not mineable by this
node until the next unmined reload, which nothing schedules", so a rising value means a manual
reset would recover transactions this node is quietly failing to mine
(`services/validator/metrics.go:202-209`). That is a revenue effect on the mining path, not a
consensus one, and it is measurable rather than visible.

Neither exists in the shipped configuration: shedding requires a bounded ingest queue, and
`settings.conf:354` sets `blockassembly_maxQueueItems = 0`, which makes the queue unbounded
and the reject path unreachable. So the default posture trades these two silent failure modes
for an unbounded in-memory queue, and the machinery that would have detected the first one is
dark. Recorded with section 18, which is the same trade seen from the other side.

## 18. The ingest backpressure controller ships switched off, twice over

**Config finding, and a pattern worth naming.**

`validator_kafkaBackpressureEnabled = false` in `settings.conf:1761`, on a line whose own
comment says the setting is not alone: "Enable queue-age-driven Kafka ingest backpressure
(also set blockassembly_maxQueueItems)". Since that second value is `0` (section 17), the
controller would be half-blind if it were enabled: the queue-fullness predicate returns "no
usable signal" whenever the reported cap is not positive
(`services/validator/kafka_backpressure.go:376-396`), which `settings.conf:1779` also
states. So age-driven pausing is off by a switch, and fullness-driven pausing is off by
arithmetic.

What the machinery does when enabled is worth restating here because the guide leans on it:
it pauses the validator's own Kafka fetch when block assembly's queue head has waited longer
than 500ms and resumes below 100ms (`settings.conf:1764-1767`), caps any single pause at 30s,
fails open after 3 consecutive failed reads, and arms a cooldown proportional to how long it
was paused. An earlier revision suppressed backpressure for a whole pause window after a
fail-open, which on an unbounded queue is 30 seconds of growth toward the memory exhaustion
the feature exists to prevent; the comment names that defect (A6) and the divisor that bounds
it.

Two recurring properties show up here and deserve their own note because they appear in other
services too:

- **The controller distrusts the number it measures with.** The drain floor it subtracts from
    head age comes from block assembly's own report, clamped so a negative becomes zero,
    because a bad value there "could invert the control decision" and "a bad peer must not be
    able to steer it". A mismatch with the local `blockassembly_doubleSpendWindow` is logged
    once per distinct value and names the setting to align
    (`services/validator/kafka_backpressure.go:264-277`).
- **The same setting lives in two processes and the code knows which copy it may trust.** The
    hand-off deadline is sized from this process's local copy of
    `blockassembly_queueFullWaitTimeout`, explicitly "a safety margin only and never a control
    decision", which is why that value is not self-reported the way the double-spend window is
    (`services/validator/Validator.go:2272-2300`). `handoff_deadline_total` is the counter
    that points at the skew when the copies disagree
    (`services/validator/metrics.go:226-235`).

This is the same two-context settings trap already recorded for the P2P user agent, and now a
third appearance. If a future section needs a cross-process setting to *drive* a decision, the
established answer in this codebase is to have the owning service report it over the wire, not
to read a local copy.

## 19. The double-spend hold-back is off, and it was also a loop bound

**Config finding with an incident attached.**

`double_spend_window_millis = 0` (`settings.conf:776`) means block assembly holds nothing back:
the queue's eligibility filter lets a batch dequeue immediately, so a transaction is eligible
for mining the moment it arrives. That is a throughput choice, and defensible — but the same
value also disables a safety bound somewhere else, and the setting's name advertises only one
of its two jobs.

The queue drain during or after block movement is bounded twice on purpose
(`services/blockassembly/subtreeprocessor/SubtreeProcessor.go:6442-6472`): a time bound so the
drain only takes what existed when it started, and an item bound as a backstop. The comment
records how both failed:

- With the window at zero the time bound was computed as zero, and the queue's dequeue
    short-circuits its filter when the eligibility time is not positive — so the bound switched
    itself off. It is switched off in the shipped default today.
- The item bound counted batches and compared against a length measured in transactions, with
    roughly a thousand transactions per batch, permitting about a thousand times the intended
    work.

The consequence is in the comment verbatim: with ingest at line rate and neither bound firing,
"the scaling-2 pod sat for 35+ minutes at 558 GB RSS inside this loop". The fix takes one clock
reading for both branches, counts items, and dequeues as it goes so the queue length — and the
validator's backpressure that reads it — stays honest during the drain.

Two smaller points worth recording:

- **The key has no service prefix.** Every other block-assembly knob is `blockassembly_*`; this
    one is `double_spend_window_millis` (`settings/blockassembly_settings.go:41`). An operator
    searching the config for the service name will not find the value that governs both the
    mining hold-back and that loop bound.
- **The queue-stats contract names the ceiling and the window as reported values**, not local
    settings, because the two processes live in independent settings contexts
    (`services/blockassembly/Interface.go:41-58`). Section 18 found the validator honouring
    this; block assembly is where the contract is written down.

## 20. Queue depth means two different things in two monitors of the same queue

**Not a contradiction, but it reads like one, and the difference is the useful part.**

Block assembly publishes ingest queue statistics, and two consumers interpret them
differently on purpose:

- For deciding whether upstream should pause, depth alone is declared unusable — "it cannot tell
    a queue that is draining from one that is stalled, since a large but falling depth needs no
    intervention while a small but static one does" — while depth as a *fraction* of the ceiling
    is usable, and is how the validator's fill predicate works
    (`services/blockassembly/Interface.go:30-35`).
- For detecting a stalled consumer, depth is worse than unusable: "depth is the untrustworthy
    half of the pair", because in the incident on record the blocked handler was draining the
    queue from inside the branch that blocked it, so a low reading coincided with a wedged loop.
    The stall detector keys on staleness alone, whatever the queue holds
    (`services/blockassembly/dequeue_stall.go:87-97`).

Same queue, same numbers, two questions: "should I stop sending" uses age and fractional depth;
"is the consumer stuck" uses time since the consumer last checked in. Both rules are in the
code with their reasoning attached, so the guide states both rather than picking one as "the"
monitoring rule.

Related, and worth noting as good practice: the stall threshold is deliberately **not** a
setting — thirty seconds hard-coded, severity split so routine long blocks log quietly — on the
stated grounds that "guessing at a number now is what the gauge exists to avoid". The companion
gauge is the evidence that would justify making it configurable.

## 21. Restarted pending sets depend on a timestamp column's resolution

**Cross-store behaviour finding.**

On start, block assembly reloads unmined transactions from the coin ledger, sorts oldest-first
by `CreatedAt`, and validates parent chains, dropping any transaction whose parent did not come
first — cascading to its descendants (`services/blockassembly/BlockAssembler.go:2920-2937`).

The comment spells out the trap precisely: `CreatedAt` is stamped per transaction with no
enforced gap, and its resolution is store-dependent — milliseconds on Aerospike, one second on
SQLite, "its CURRENT_TIMESTAMP default emits `YYYY-MM-DD HH:MM:SS`". A parent and child
created close together therefore routinely share a value, most of the time on SQLite. With an
unstable sort, the child can be emitted first, parent-chain validation calls that invalid, and
the family disappears from the mining set — no rejection, no customer-visible error, just
transactions that are no longer candidates.

The fix is a stable sort plus a pass that reorders only runs of equal timestamps so in-set
parents precede children (`stableSortUnminedByCreatedAt`, `orderEqualCreatedAtRunsParentsFirst`,
`orderRunParentsFirst`), leaving unrelated transactions in incoming order. Guarded by tests in
`services/blockassembly/unmined_sort_test.go` and
`services/blockassembly/load_unmined_extsort_test.go`.

Recorded because the interesting property is not the bug, which is fixed and tested, but the
shape: behaviour differed by backing store, so a test suite that runs only on SQLite was
exercising the path where the failure is nearly certain, and production on Aerospike saw it
rarely enough to look like a phantom. Any future tie-broken ordering over stored timestamps
inherits the same hazard.

## 22. The transaction library's definition of a coinbase was wider than the rule

**Dependency-behaviour divergence, fixed locally with the reason recorded.**

Block validation rejects a block whose first transaction is not the reward transaction, and the
tidy way to write that is the transaction library's own predicate. This service does not use it,
and the comment states why: the library's test is a disjunction that "also accepts a 0xFFFFFFFF
sequence number in place of a null prevout index, admitting a transaction svnode rejects"
(`services/blockvalidation/quick_validate.go:292-294`). It uses `model.IsConsensusCoinbase`
instead.

The failure mode is the expensive asymmetric one rather than the visible one: a block this node
accepts and the rest of the network refuses. No error is raised locally, so the divergence only
surfaces as a chain this node is alone on.

Recorded as a class rather than a curiosity. A helper named `IsCoinbase` reads as a definition
of a consensus rule, and it was a convenience predicate with a wider extension. Any other call
site using the library version for a consensus decision inherits the same gap — worth a grep if
that question ever comes up: `grep -rn "\.IsCoinbase()" --include=*.go services/`.

## 23. Two consensus-affecting defaults are absent from `settings.conf`

**Second instance of section 11's pattern, and the pattern is now the finding.**

`blockvalidation_outpoint_only_below_checkpoint` and
`blockvalidation_legacy_below_checkpoint_fail_closed` are both read with a code default of
`false` (`settings/settings.go:436`, `:438`). Neither key appears in the shipped
`settings.conf` — searching that file for `outpoint` returns only `legacy_outpointBatcher*`
and `utxostore_outpointBatcher*`, which are unrelated batching knobs.

The mechanism is exactly the one section 11 described: the dead-key test verifies file to code
and calls itself "deliberately narrower" than the converse, so nothing checks code to file, and
a declared setting may be missing from the shipped configuration with no test complaining.

Why this instance matters more than the feeler-slot one:

- The two knobs are a **pair with an unstated interaction**. The first enables the
    below-checkpoint path that skips script validation and spends by outpoint; the second makes
    that path fail closed on a conflict rather than proceed. Enabling the first without the
    second is a real, reachable configuration that no committed file describes, and the
    combination's meaning lives only in code (`services/legacy/netsync/handle_block.go` carries
    the reasoning on the legacy side).
- The thing being hidden is the largest performance lever in the sync path. An operator
    comparing this node to another and wondering why syncing takes different amounts of time
    cannot find the switch by reading the configuration.
- The guard that reads these values also has a comment defending itself against a future edit
    that would let the fast path apply above the hard-coded checkpoint
    (`services/blockvalidation/quick_validate.go:2010`) — someone is thinking about drift here,
    which makes the missing config entries look like an oversight rather than a decision.

## 24. Catch-up trusts peers in three stages, and each stage closes a different cheap lie

**Mechanism worth recording because the ordering is the security property.**

Catch-up asks strangers for long runs of blocks, so the cheapest attacks are the ones that cost
the peer almost nothing to produce. The code has three distinct guards, and what is worth
capturing is that each one is placed before the expensive step it protects:

- **A self-consistent low-difficulty chain.** Header-fetch validation checks each header's
    proof of work against that header's own claimed difficulty, so a peer can invent a chain of
    trivially-easy blocks. Step 9.5 of catch-up recomputes the difficulty the rules required
    from the header chain and rejects that peer — before full blocks are fetched
    (`services/blockvalidation/catchup.go:357-362`).
- **A fork deeper than the chain can safely abandon.** The limit is coinbase maturity, and the
    comparison operator carries the reasoning in a comment that derives it: a reorganisation of
    depth `d` invalidates a coinbase at height `G` when `G >= H - d + 1`, and for that coinbase
    to have been spent in the old chain needs `G + CoinbaseMaturity <= H`, "which has a
    solution only when d > CoinbaseMaturity. So d == CoinbaseMaturity is safe ... and `>` is
    the correct operator here, not `>=`" (`services/blockvalidation/catchup.go:1034-1041`,
    issue 4592). A one-character error on a safety boundary, now carried with its proof.
- **State churn for a fork that never happens.** The mined-set reset on fork detection must run
    only after the fork-depth and secret-mining checks pass, otherwise "a peer merely
    announcing a deep or secretly-mined fork would un-stamp this node's own still-canonical txs
    for a fork that is then rejected, churning honest tx-mined state for no reorg"
    (`services/blockvalidation/catchup.go:287-290`, issue 1145).

Two supporting details that are good engineering rather than bugs, recorded because they are the
type of decision that gets undone by someone tidying up:

- Concurrent per-subtree fetch failures are collected into a set keyed by peer and charged once
    at the end, specifically so `CatchupFailures` cannot exceed `CatchupAttempts`
    (`services/blockvalidation/catchup.go:75-81`). Two metrics sharing a denominator must agree
    on arithmetic.
- A catch-up trip interrupted by this node's own reorganisation is reported as a service error
    so the peer is not charged (`services/blockvalidation/catchup.go:62-65`), and cleanup of
    temporary subtree files tracks committed-block dependencies at *run* scope rather than
    attempt scope, because per-attempt freshness cannot see that a freshly written hash became a
    dependency while the write was still asynchronous (`services/blockvalidation/catchup.go:83-95`,
    issue 4692).

## 25. The two example cluster files are not two sizes of the same thing

**Divergence, and a correction to the framing of section 9.**

Section 9 compared `teranode-cr.yaml` with `teranode-cr-mainnet.yaml` as though they were
two production shapes and found them contradictory. They are not the same kind of file. The
properties that give it away are not the service flags section 9 read, but everything
around them. Both files configure the same fifteen services — eight switched on in the
development file, nine in the mainnet one — and nine entries in the first carry resource
asks:

| Marker                     | `teranode-cr.yaml`                                | `teranode-cr-mainnet.yaml`                               |
| -------------------------- | ------------------------------------------------- | -------------------------------------------------------- |
| Image pull policy          | `Never` on all nine blocks that set one           | `Never` on one (`alertSystem`, line 14)                  |
| Per-service ask            | `cpu: 100m` with `256Mi`, all nine asks           | `cpu: 1`–`cpu: 2`, `1Gi`–`8Gi` on the heavy ones         |
| Ingress host               | `teranode.local`, every occurrence                | `mainnet-eu-1.example.com`                               |
| Shared storage             | absent                                            | `fsx-lustre`, `4800Gi` requested (line 156)              |

`imagePullPolicy: Never` tells the cluster the image is already on the machine, which is
what you want when testing a build produced on your own laptop and never what you want on
rented hardware. A tenth of a processor per service is a laptop-sized number. 4800Gi on AWS
Lustre is the opposite kind of number.

So section 9's disagreement about the front door is not two teams holding different views of
production. It is a development fixture next to a production shape, and the production one
is corroborated from an unrelated direction by section 26. Section 9's table remains
accurate as a table; its closing advice that neither file is a working mainnet deployment as
written was too strong about one of them.

Two residuals the reframing does not close:

- The stray `imagePullPolicy: Never` on `alertSystem` in the production file, where every
    other service omits the line. On a clean node that one service's image would not be
    pulled from the registry, so alerting fails to start unless the image is baked into the
    machine image. Not verified here — no cluster to schedule against — but the flag does
    not have a second meaning.
- 4800Gi of shared storage is requested while both persisters are switched off. See
    section 29 for what that storage is and is not for.

## 26. The front-door question has an in-repo answer, and it is "Legacy always, P2P by choice"

**Divergence resolved by files sections 9 and 13 did not consult.**

Until now the guide's deployment commentary rested on the two Kubernetes files. There is a
third pair of examples — a shared service catalogue under `deploy/docker/base/` with
`mainnet/` and `testnet/` overlays — plus a set of operator howtos. They change the shape of
the answer from "unknown" to "both, in a fixed pecking order":

| Artefact                                        | Legacy     | P2P (`peer`)                 | Propagation                  | `blockpersister`         |
| ----------------------------------------------- | ---------- | ---------------------------- | ---------------------------- | ------------------------ |
| `teranode-cr.yaml` (development fixture)        | off        | on                           | off                          | off                      |
| `teranode-cr-mainnet.yaml`                      | on         | off                          | off                          | off                      |
| Docker mainnet overlay                          | on         | commented out (line 72)      | not listed                   | commented out (79)       |
| Docker testnet overlay                          | on         | commented out                | not listed                   | commented out            |
| Quickstart default profile (external repo)      | on         | on                           | listed as running            | opt-in profile           |

What follows from the table:

- Three of the five examples run Legacy and none omits it. The inventory line calling it a
    compatibility layer stays the most misleading sentence in the guide, and the guide now
    says so in the section itself rather than only in its inventory row.
- The modern door is *gated*, not absent. The Docker overlays do not delete it, they
    comment it out with an instruction to read the network documentation first, and the base
    catalogue defines it completely — public port, healthcheck, start-up dependency on
    Blockchain (`deploy/docker/base/docker-teranode.yml:154`). Nobody leaves a healthcheck
    and a dependency ordering attached to a half-finished service.
- Propagation is off in both Kubernetes files and unlisted in both Docker overlays, while
    the base catalogue defines it (`:133`) and the quickstart's component table lists it as
    running. So section 13's complaint survives — the shipped Kubernetes and Docker examples
    register an HTTP transaction route with no service behind it — but its open question
    "should the examples deploy Propagation" has a documented answer: the supported install
    does.
- `deploy/docker/README.md:5` states plainly that most operators should not start from these
    files and points at a separate quickstart repository for the supported path. That is
    worth remembering about all four in-tree artefacts: they are the layer *underneath* what
    an operator is told to run, which is a reason to read their flags as defaults-to-override
    rather than as policy.

One caveat on the last row, and it limits how much weight the table can carry. The default
profile `legacy,p2p,monitoring` is quoted from
`docs/howto/miners/docker/minersHowToInstallation.md:121`, which documents a *different
repository*. No file in this tree sets it, so it is verified as documentation and not as
behaviour. Every other cell in the table comes from this checkout.

## 27. The first peers come from a third party's DNS records

**Confirmed, and a dependency a reader cannot infer from the code.**

`settings.conf:1048` sets
`p2p_bootstrap_peers = /dnsaddr/${network}.bootstrap.teranode.bsvb.tech`. The comment above
it records that the list is managed by the BSV Association, resolved dynamically through DNS
TXT records, that any node running full DHT mode with a public address may be added on
request, and that on regtest the domain is skipped because no public regtest network exists
(`:1040-1047`).

So the modern door's ability to find the network depends on a DNS answer from a domain this
repository does not operate. That is an availability and governance dependency of the front
door, not an implementation detail, and it belongs in the guide for the same reason the
block-size caps do.

The same block of settings carries the second half of the finding. `p2p_dht_mode` defaults
to `off` — contact only the peers you were handed — with `.operator = server`, the fullest
mode, and `.dev = client` (`:1099-1101`). `deploy/kubernetes/teranode/teranode-configmap.yaml:7`
sets `SETTINGS_CONTEXT: "operator"`, so any Kubernetes deployment in this tree resolves the
knob to `server`. The operator guide's warning about that mode — it probes a hundred or more
peers, and abuse-sensitive hosts have been known to read that as port scanning and suspend
the machine (`minersHowToInstallation.md:158-162`) — sits in the Docker howto, whose stack
defaults to the quiet end. The loud default is the one with no warning attached, and in this
tree only the development fixture runs the service that reads it.

## 28. `listen_mode` is read by one service, and the other one says so out loud

**Confirmed, and the model for what open question 1 is asking for.**

The name reads like a node-wide switch. It is a modern-P2P setting. Every consumer lives in
`services/p2p/`: the value is validated against three legal strings
(`Server.go:617-618`), enforced centrally on the way out of the publish gate
(`services/p2p/publish_gate.go:204-208`), and consulted for address suppression and status handling at
several further points. The asset service carries the value through in peer status JSON and
nothing else.

Legacy does not read it, and the function that would have been gated by it explains why in
the comment:

> the legacy P2P announce path is intentionally NOT gated by the modern-P2P `listen_mode`
> setting. listen_mode is documented for DataHub URL advertisement on the modern p2p
> service; legacy INV→GETDATA is a bog-standard Bitcoin protocol concern and is opt-in via
> enabling the legacy service itself. See PR #955 review for the rationale.

(`services/legacy/peer_server.go:2157-2161`, with the same note repeated at `:3418` and
`:3494`.)

`settings.conf:946-955` carries the matching operator-facing note, including the history:
the `.dev` override sits at `listen_only`, and since PR #955 removed the legacy-side gate the
override "no longer silently disables legacy tx announce in dev runs — it does only what
its modern-P2P docs say it does".

That is the shape of record open question 1 asks for, and it costs four lines of comment: a
behaviour that looks like an oversight, the word *intentionally*, the reason, and a pointer
to the discussion that settled it. That discussion is reachable and agrees: PR #955 is
merged, titled "fix(legacy): wire AddRebroadcastInventory + drop dev listen_mode override",
which matches the gate coming off the legacy side while the setting line itself stays. Checked
with `gh pr view 955` rather than added to the command block below, so that block stays
runnable without network access or credentials. Worth holding up as the model for the
`ReasonInvalidSubtree` case in open question 1.

One gap in the same area. The three legal values are `full`, `listen_only` and `silent`
(`settings/interface.go:12-14`), and `silent` is documented — `docs/p2p-silent-mode.md` and
`docs/references/settings/services/p2p_settings.md:17` — but the operator howto describes
only two positions (`minersHowToInstallation.md:129-143`). An operator who configures from
the howto will not know the quietest mode exists. Cheap to fix, and the kind of omission that
leaves a key looking like it has two values when it has three.

## 29. Why the persisters are switched off: one documented, one not

**Partly resolves the sub-question section 9 could not settle.**

Section 9 wrote that nothing in either example enables the two services that file blocks and
maintain the coin-set checkpoint, and that whether this was intentional or stale "cannot be
determined from the YAML". For block persister, three places say so in prose:

- `deploy/docker/mainnet/docker-compose.yml:78` comments the service out under the words
    *uses lots of disk space*.
- The operator guide gives the number: archival mode needs several terabytes on mainnet and
    "is not needed for standard validation or mining" (`minersHowToInstallation.md:180-181`).
    It is an opt-in profile (`:176`), and the guide notes it writes in parallel with the
    pruner and leaves pruning behaviour unchanged.
- The live coin set does not depend on it: spent outputs are pruned from the UTXO store
    after 288 blocks (`settings.conf:790`, `global_blockHeightRetention = 288`), which the
    howto restates as the default and warns against raising
    (`minersHowToInstallation.md:166-169`).

So "nothing persists the chain" was the wrong worry, and the guide should not carry it. The
chain is served from the block and subtree stores, the coin set lives in the UTXO store with
bounded retention, and the persister is an archival add-on for indexers and explorers.

What stays open is `utxoPersister`: disabled in both Kubernetes files, and not merely
disabled but unlisted in every Docker compose file in the tree. No howto names it and no
comment explains its absence. Its role — building and restoring the UTXO checkpoint used to
seed a node — is described in its own package and nowhere in the deployment documentation, so
a reader cannot tell whether seeding happens some other way in these topologies or the
service is simply not deployed yet. That is the residual of section 9, and it is the one
still worth asking about.

## 30. A fresh node boots parked, and two documents disagree about whether it can be un-parked

**Divergence between an existing howto and the FSM transition table.**

A node with no stored FSM state does not start working. `blockchain_initializeNodeInState`
(`settings.conf:457-459`) is empty in the default context and `IDLE` in both the `operator`
and `docker.m` contexts, so every deployment this repository ships boots parked — which the
comment explains as deliberate: "Production operator and single-node Docker contexts start
in IDLE so a seed can be inspected before catchup begins." The sync howto agrees, calls the
parked period an "inspection window", and tells the operator to start synchronisation with
an explicit command (`docs/howto/miners/docker/minersHowToSyncTheNode.md:196-223`). It also warns that during that
window two services have not bound their gRPC listeners, so their healthchecks report
*starting* or *unhealthy* on a node that is behaving exactly as designed — a monitoring
false alarm written into the deployment method rather than in the code.

The divergence is in one sentence of that passage:

> Catch-up promotes the node to `RUNNING` after it reaches the active network's highest
> checkpoint. There is no transition from `CATCHINGBLOCKS` back to `IDLE`, so verify the
> seed first.

The transition table says otherwise. `STOP` is declared with sources `RUNNING` **and**
`CATCHINGBLOCKS` and destination `IDLE` (`services/blockchain/fsm.go:34-40`), and the stop
script in the same howto set is documented as asking for exactly that: "`stop.sh` asks
Teranode to enter `IDLE` state, then runs `docker compose down`"
(`docs/howto/miners/docker/minersHowToStopStartDockerTeranode.md:80`, echoed at `docs/howto/miners/docker/minersHowToInstallation.md:211`). So
the sentence denies an operator a transition that exists, is wired, and is exercised on every
documented stop.

Two readings, and the finding survives both. If the author meant "catch-up never *reverts*
to parked on its own", that is true and is also enforced deliberately elsewhere — automatic
catch-up and promotion both refuse to undo a parked state, so parking is an operator's
choice and only an operator can undo it. But the sentence as written is about whether the
operator can go back, and it says they cannot, which will change a decision: an operator
told that catch-up is irreversible inspects a seed much harder before pressing the button
than one told a stop is one command away. Neither reading is recorded anywhere an operator
would find it.

## 31. The peer opinion is not persisted in the shipped configuration

**Config gap, plus a setting whose own description overstates it.**

The Blockchain service holds the one opinion of every peer — reliability, catch-up success
and failure counts, ban scores, ban expiry. That registry can be persisted: there is a
versioned JSON envelope written to a fixed key in a configurable blob store, with
ban scores and catch-up counters included, a save tick, and a load at startup that drops
entries older than the configured lifetime (`services/blockchain/peer_registry_persistence.go:16-46`).

It is switched off in everything this repository ships. Persistence activates only when
`blockchain_peerRegistryStore` names a store, its default is empty
(`settings/blockchain_settings.go:34`), and `settings.conf` contains **no entry for it** —
nor for the save interval or any of the `p2p_peer_registry_*` keys, all of which therefore
run at code defaults. The startup path logs the disabled state as a warning about the save
interval rather than about persistence being off (`services/blockchain/Server.go:555-561`),
and shutdown saves only if a store was constructed (`Server.go:1071-1084`), so with the
shipped defaults the registry is memory-only *even on a graceful stop*. The consequence is
concrete: every restart forgets who has misbehaved, and peers banned for bad data get a
fresh allowance.

The setting's long-form description calls a local on-disk store "default for single-node
deployments". In the configuration that actually ships — which is a single-node deployment —
it is not one. That phrasing will send a reader to look for a setting that is not there.

## 32. One registry, two settings namespaces, and a floor derived from another service's constants

**Confirmed, and a maintenance hazard worth naming.**

The registry lives in the Blockchain service, and five of its knobs are split across two
families: `blockchain_peerRegistryStore` and `blockchain_peerRegistrySaveInterval` in the
BlockChain group, `p2p_peer_registry_ttl`, `_max_size` and `_cleanup_interval` in the P2P
group (`settings/settings.go:375-376`, `:601-603`). Nothing groups them as one object, so
an operator tuning peer behaviour has to know which half they are in.

The TTL floor is worse, because its correctness is arithmetic spread over three constants in
two services. A configured expiry below five minutes is raised to five minutes at startup
(`services/blockchain/Server.go:926-950`). The number comes from: peers republish node
status every ten seconds (`services/p2p/Server.go:2034`), the gossip batcher may coalesce
those refreshes for up to a minute (`services/p2p/peer_registry_batcher.go:31`), and the
floor is set at five times the batcher's window so a missed beat or two is survivable. Change
the batcher's window and the floor's justification changes with it; no test or constant
connects them. The documented failure if the floor were wrong is not mild: an
actively-connected peer is evicted between its own refreshes, then fails the catch-up
health check and is silently refused as a source of blocks.

The handling is good — the coercion is logged rather than silent, and the setting's
description explains the tuning hazard, unlike the two opt-outs (`_max_size` and
`_cleanup_interval`) where zero is a documented permanent choice rather than a value to
avoid. The gap is that the arithmetic is folklore in three files rather than one expression.

## 33. The fast path is off, and the instrument watching it is armed behind it

**Config pair, and the third instance of a pattern this file has now named twice.**

Two settings govern the in-memory route that answers "is this block on the main chain?"
without a database query. As shipped (`settings.conf:466`, `:475`):

| Setting                                      | Shipped     | Effect when set that way                                |
| -------------------------------------------- | ----------- | ------------------------------------------------------- |
| `blockchain_use_in_memory_chain_check`       | off         | route unused; every answer comes from SQL               |
| `blockchain_chain_check_shadow_compare`      | on          | compares accept decisions, samples rejects 1:1024       |

With the route off, the comparison has nothing to compare: the code labels the SQL answer
"the authoritative query already ran and produced this answer. Comparing it with itself
measures nothing" (`stores/blockchain/sql/CheckBlockIsInCurrentChain.go:112-113`). So the
watchdog is armed and dark, and it becomes live only if an operator turns the other knob —
at which point, per the config comment, "enabling the route above with this left on measures
no speedup; turning it off is what makes that fast path fast".

The reachable trap is the ordering: the knob an operator finds first is the one that costs
throughput. Turn on the fast path without turning off the comparison and the node takes the
route's risk while paying the query the route existed to avoid. Section 18 and section 23
record the same shape — two settings where one is inert, misleading, or both until its
partner moves — and this is the case where the shipped values are defensible and only the
ordering is not.

The comment also states an honest gap: the route is discouraged during initial sync because
the insert path holds the main-chain guard for its whole length on a node with it enabled,
and "the catch-up duty cycle where that starts to cost more than it saves has not been
measured". So the shipped off is a measurement not yet taken, not a verdict.

## 34. A stale "no" is another service's permanent decision

**Confirmed, and the clearest cross-service coupling in the service.**

The store's fast path can answer three ways: from the off-chain set by absence, from the
high-water mark as "allocated but not yet committed", or from SQL. Each direction of error
has a different cost, and the cost is not set by the store. Block validation asks this
question when deciding what to do with an old block, and a negative answer is not a retry —
it is an invalidation. The store's own comments carry that knowledge in five places, all
naming the escalation: a false negative on a committed block "is escalated into a PERMANENT
ValidateBlock invalidation; nothing undoes it until the two-minute background refresh
happens to run" (`stores/blockchain/sql/StoreBlock.go:328-336`, and again at
`CheckBlockIsInCurrentChain.go:50-55`, `:130-137`, `:236-243`, `:354-360`).

That is why the awkward-looking behaviours exist. The high-water mark is advanced together
with a rebuild that can observe the new block, never on its own, because advancing it alone
would flip the error direction from "committed block reads as uncommitted" to "fork block
reads as proven on-chain" — and the second one is the false positive that lets the asset
server serve a merkle proof against a block that is not in the chain (`:63-80`). The mark of
zero is not treated as initialised even though a genesis-only chain legitimately has a zero
mark, because guessing the other way would mark every committed block dangling
(`:44-57`). And the allocated-but-uncommitted reject, which looked safe enough to skip in the
shadow comparison, is sampled at one-in-1024 precisely because the window it can be wrong in
— between a commit and the mark being refreshed — is the one that ends in permanent
invalidation (`:129-149`).

The finding is not that any of this is wrong. It is that a rule in Block Validation decides
how the store must behave, and the only place that connection is written down is in
comments inside the store.

## 35. The short-chain fallback guards one difficulty lookup, not the other

**Gap, narrow in reach, and acknowledged in the source.**

Difficulty for a new block is computed between two ancestors: the "suitable" block at the
tip, and the suitable block 144 heights back
(`services/blockchain/Difficulty.go:23`, `:128`, `:143-155`). The second lookup handles a
short chain explicitly: if the ancestor is not found because the chain is too young, it
falls back to the block's own hash and continues, while other failures — timeouts, database
errors — are refused rather than fallen back on, because a silent fallback there "causes
wrong difficulty".

The first lookup has no such allowance, because it does not fail on a short chain; it fails
*after* one. `GetSuitableBlock` walks up three ancestors and requires exactly three results,
returning "not enough candidates for suitable block. have %d, need 3" otherwise
(`stores/blockchain/sql/GetSuitableBlock.go:106`, `:118-119`, `:180-181`). On a chain of one or two
blocks — a fresh chain, or a network seeded with less than three — the short-chain fallback
hands that lookup a block with fewer than three ancestors, and difficulty becomes an error
rather than a degraded answer. Whether that is reachable depends on how new a chain can be
when assembly first asks, and nothing in the tree asserts it; the same function also answers
a missing hash with "no result, no error" and a source comment asking whether it should be a
not-found error instead (`:125`).

## 36. Filing has a two-day fuse, and the fuse is the design

**Confirmed, and the load-bearing fact about this service.**

Nothing in the blob store is permanent merely because it was written. When Block Assembly
files a bundle during assembly it stores the file together with a *delete-at-height*: the
current coin-store height plus `global_blockHeightRetention`, which ships as 288 blocks —
roughly 48 hours (`services/blockassembly/Server.go:740`, `:891`, `:907`; retention at
`settings.conf:790`). Scheduling and cancelling are both performed through the Blockchain
service's deletion scheduler, so there is one queue of pending deletions rather than one per
caller (`stores/blob/file/file.go:1171-1186`).

Block Persister's job is the cancellation. It sets delete-at-height to zero on the bundle and
on the bundle structure once the filing copy exists, and zero is defined as "cancel the
scheduled deletion", with the storage layer recording why that path is treated as critical:
if cancellation fails, "the blob could still be deleted even though we want to persist it
forever" (`services/blockpersister/streaming_process_subtree.go:85-95`, `:231-233`).

Three consequences worth having stated together:

- **The window is the specification.** A deployment without the persister does not lose
    anything that validation needs, and loses the entire archive without an error, because
    expiry is what the working copies were always supposed to do. That is why both Docker
    examples can comment the service out and still be honest about running a node.
- **The fuse is re-lit on rewrite.** Where a bundle already exists, Block Assembly updates its
    delete-at-height rather than leaving the original date, so a bundle that keeps being
    written keeps being postponed (`services/blockassembly/Server.go:889-896`). Liveness of
    the archive therefore depends on write traffic as well as on the persister.
- **Being behind is a different failure from being absent.** Once filing falls more than the
    retention behind, the transactions it needs to write into the archive have been pruned, and
    the failure surfaces as a per-transaction retrieval error rather than as "too far behind".
    Section 39 covers what the loop then does.

## 37. The persister explains its coordination to the wrong service

**Divergence between code comments and code, in a comment written to prevent a race.**

Two long comment blocks in the persister describe a startup race and its fix — Block
Assembly's cleanup could delete transactions the persister still needs, so the persister
publishes its filed height at startup and again per block. Both comments name the consumer as
Block Assembly: "BlockAssembler's cleanup service uses this to track how far we've progressed",
"BlockAssembler reads this state during its startup initialization", and, most specifically,
"See BlockAssembler.startChannelListeners for the notification handler"
(`services/blockpersister/Server.go:313-331`, `:398-410`).

Block Assembly does not do any of it. It has no reader for `BlockPersisterHeight`, no handler
for `NotificationType_BlockPersisted`, and no `startChannelListeners` function. The consumer
is the Pruner, which reads the same state key on startup
(`services/pruner/server.go:251-256`) and handles the same notification to advance
`lastPersistedHeight` (`:156-176`). Searching for the consumer from inside the persister lands
a reader in a service that ignores both signals — and the comment is written in exactly the
detail that makes it trustworthy, including the deletion bound formula.

Worth noting on the other side: the arrangement the comments describe is sound, and the Pruner
uses it correctly, including logging the moment the persister first reports a non-zero height
(`services/pruner/server.go:169-171`). This is a stale pointer, not a broken mechanism. It is
also precisely the kind of pointer that gets followed by someone debugging disk growth.

## 38. The shipped single-node stack runs the deleter without the filer, on the trigger that waits for the filer

**Config gap, quantifiable, and the sharpest instance of the pattern in section 18.**

The Pruner supports two triggers, and the difference is which service proves a block is safe
to prune from: `OnBlockPersisted` waits for filing, `OnBlockMined` waits for the mined-set
flag and exists for deployments with no persister (`services/pruner/server.go:140-144`;
setting documented at `settings/pruner_settings.go:12` with the recommendation "Use
OnBlockMined - Use when Block Persister is not available").

The default is `OnBlockPersisted` (`settings/settings.go:673`), and **`pruner_block_trigger`
appears nowhere in this repository** — not in `settings.conf`, not in the Docker compose files
or their `.env` files, not in the Kubernetes examples. Meanwhile:

| Deployed in the single-node Docker stack       | State                         |
| ---------------------------------------------- | ----------------------------- |
| `pruner`                                       | runs                          |
| `blockpersister`                               | commented out                 |
| `pruner_block_trigger`                         | unset → OnBlockPersisted      |

Walk the consequences through the code. Block notifications are ignored while configured for
`OnBlockPersisted` (`services/pruner/server.go:200-204`). `BlockPersisted` notifications can
only come from the persister (`services/blockpersister/Server.go:408-417`), which is not
running. The startup catch-up pass reads the persisted height and acts only if it is above
zero (`services/pruner/server.go:251-256`, `:322-330`), and that state is written only by the
persister, so it stays zero. The code states the stall plainly in its own justification for
retrying the header lookup: "in OnBlockPersisted mode there may be no subsequent notification
to trigger pruning if this fails" (`:326-329`). With no persister the "no subsequent
notification" case is not a risk at startup — it is the steady state.

So as configured, the shipped stack starts a pruner that subscribes, reports itself healthy,
accepts and executes deletions that were already scheduled, and never generates a new pruning
pass.
The visible symptom is not an error anywhere; it is the coin store growing.

Two honest caveats. The supported install is a separate repository, so its `.env` may well set
the trigger — that is unverifiable here and is the same limitation flagged in section 26. And
with no persister there is nothing to file, so the archive is not the thing at risk; what is at
risk is the disk bound the retention exists to provide.

## 39. The retry pause guards the two branches that do not need it

**Gap, small, and directional rather than hypothetical.**

The persister's main loop has one sleep, `blockpersister_persistSleep`, shipped at ten seconds
(`settings.conf:292`). It is applied on three branches: when the query for the next
unfiled block fails, when there is nothing to file, and — a branch that makes the pattern look
accidental rather than deliberate — when the block it just fetched cannot be serialised
(`services/blockpersister/Server.go:353-377`). It is *not* applied when filing itself fails,
which logs and starts the next pass immediately (`:381-389`), nor when the block cannot be
marked as filed (`:391-396`) — the two branches most likely to fail for a reason that retrying
sooner cannot fix.

For transient trouble this is right — a store that hiccups should be retried at once, and the
block will be filed on the next pass. The shape that is not covered is a block that cannot
succeed. Section 36 describes how one arises: filing more than the retention window behind
means the transactions the archive needs have already been pruned, and the retrieval fails with
a per-transaction error (`streaming_process_subtree.go:185`, `:202`). That error is permanent
until an operator intervenes, and the loop responds by re-querying the ledger, re-fetching from
the coin store, re-reading the bundle structure and re-logging, back to back, indefinitely —
with an error line each time, so the log grows as fast as the retries.

The fix-shaped sentence is already in the code's vocabulary elsewhere in the deployment: retry
with backoff after a failure that will not resolve by trying sooner, and pause at the shipped
interval when the reason is that there is nothing to do. Today the pause is attached to the
branch that needs it least.

## 40. Three keys whose names point at a different service than the one that reads them

**Confirmed, and the third instance of the naming pattern in section 32.**

Each of these is consumed happily; the problem is what a reader looking for the knob, or an
operator trying to predict an effect, is told.

- `utxoPersister_buffer_size` is defined in the **Block** settings group
  (`settings/block_settings.go:21`, default 256KB) and read by three places: the file storer
  (`services/utxopersister/filestorer/FileStorer.go:85`), the coin-difference tracker
  (`services/utxopersister/UTXOSet.go:522`, `:564`, `:714`) and Block Persister
  (`services/blockpersister/streaming_process_subtree.go:271`). Nothing named "utxo persister"
  has to be running for it to matter.
- `blockpersister_skipUTXODelete` is read in exactly one place, and it is in the UTXO Persister
  service (`services/utxopersister/Server.go:568`). Block Persister never reads it. Its
  shipped value is `false` (`settings.conf:291`), which happens to be the code default, so the
  mislabelling has no effect today — which is exactly when a name is cheapest to fix.
- `blockpersister_concurrency` limits workers per block, and the *inner* batch parallelism
  inside each bundle is computed from processor count instead, ignoring the setting
  (`streaming_process_subtree.go:123`: four at minimum, half the cores otherwise). One knob
  governs the outer fan-out and silently does not govern the inner one.

One small sharp edge in the same cluster, and it is worse than "a" fallback. A
`utxoPersister_buffer_size` value that fails to parse is logged at error level and then
replaced — with **two different sizes depending on which code path meets it**. The file storer
and Block Persister's writer both substitute 128KB (`services/utxopersister/filestorer/FileStorer.go:87-92`,
`services/blockpersister/streaming_process_subtree.go:271-275`); the two coin-ledger readers
substitute 4096 bytes, a 32-fold difference (`services/utxopersister/UTXOSet.go:526-531`,
`:568-573`). None of the three is the documented 256KB default. So a typo in that one key
leaves the node running, writing with one buffer size and reading with another, and the only
outward sign is one error line per file handle opened. The key is also present in the shipped
file only as a commented line (`settings.conf:1621`), so the default is what runs, and nothing
here has ever exercised the parse failure.

## 41. The coin archive is fed by the service that every example switches off

**Confirmed, and the dependency runs in the direction that hides it.**

The UTXO Persister's entire input is a pair of per-block files: outputs created, outputs spent
(`fileformat.FileTypeUtxoAdditions`, `fileformat.FileTypeUtxoDeletions`,
`pkg/fileformat/header.go:13-14`). Grepping `services/blockpersister/` for those constants
finds nothing, which invites the conclusion that nothing in this repository writes them. That
conclusion is wrong, and the reason it is wrong is the finding.

The writer is `utxopersister.NewUTXOSet` (`services/utxopersister/UTXOSet.go:151-176`), called
by **Block Persister** when its own `ProcessUTXOFiles` switch is on
(`services/blockpersister/persist_block.go:75-81`), which ships enabled
(`settings.conf:294`). The constants live in the imported package, not the caller, so a
by-package search separates two halves of one hand-off. Its first log line makes the confusion
permanent in the other direction:

```go
logger.Infof("[BlockPersister] Persisting utxo additions and deletions for block %s", blockHash.String())

```

— emitted from the `utxopersister` package (`services/utxopersister/UTXOSet.go:152`). Searching
the logs by this service's name misses the lines that create its input; searching by
`[BlockPersister]` returns lines written by another service's code. Fourth instance of the
mislabelled-door pattern in section 40, and the first one that misleads in the log rather than
in a settings file.

Which sets up the chain that matters operationally. Block Persister is commented out in both
Docker overlays (`deploy/docker/mainnet/docker-compose.yml`, `# uses lots of disk space`) and
`blockPersister: enabled: false` in both custom resources. `utxoPersister: enabled: false` in
both custom resources, and no container in any compose file. So in every example in this
repository the coin archive is off *and* its feeder is off — coherent. But the dependency means
switching on the archive alone does nothing at all: no persister, no ledgers, nothing to
consolidate, and per section 42 the service reports that state with the sentence that means
*caught up*. The two switches are three lines apart in the same files and nothing says one
requires the other.

## 42. "No new block to process" means three different things

**Confirmed, and it is the log line an operator will actually see.**

The main loop converts one specific error into a benign-sounding informational message:
"No new block to process" (`services/utxopersister/Server.go:389-394`). Three distinct
situations arrive at it, and they want different responses.

- **Genuinely caught up.** The bookmark is at or above the processing ceiling, which is reported
    in its own line one moment earlier (`Server.go:468`).
- **The ledger for the next block has not been written yet.** A missing additions file is a
    hard error by design — `consumeAdditions` says so in its own comment, "Unlike deletions, a
    missing additions file is a …" (`services/utxopersister/consolidator.go:265-274`) — and the caller turns that error
    into the idle message. Correct behaviour, wrong noun: the block exists, the block's *record*
    does not.
- **A legal block whose deletions ledger will never appear.** `consumeDeletions` deliberately
    tolerates a missing deletions file, because a block containing only the reward spends
    nothing, and the comment records the intent (`services/utxopersister/consolidator.go:250-262`). The tolerance is
    keyed to `errors.ErrBlobNotFound`, but the file store signals a missing blob with
    `errors.ErrNotFound`, and the error package's `Is` matches by error code rather than message
    (`errors/Error_types.go:13`, `:35`; `errors/errors.go:167-191`). The two names never meet,
    so on the store that ships the tolerance never fires and the block falls through to the same
    idle message. A block that legitimately spent nothing is then indistinguishable in the log
    from a block whose records are still on their way.

The failure mode is not a crash and not a backlog; it is the coin archive stopping while the
node looks healthy. The asymmetry is intended and the mismatch of names is not, so this goes to
open questions rather than being called a bug.

## 43. The archive's lag and the retention that deletes working files are numbers in different repositories — and they apply to different files

**Structural, and unmeasured.**

Two constants govern whether the coin archive can catch up after a stall, and neither is in the
service that needs both:

- The **processing ceiling** is the chain tip minus a hundred blocks, fixed in code
  (`services/utxopersister/Server.go:464`, `const confirmations = 100` at `:46`). Not a
    setting; `docs/references/settings/services/utxopersister_settings.md` does not list it.
- The **retention window** that expires working files is `global_blockHeightRetention`, 288
    blocks (`settings.conf:790`), the same constant behind Block Persister's two-day fuse
    (section 36) and the Pruner's default floor (section 14).

The first version of this note asserted that the coin-difference files carry that retention and
was **wrong**, in a way worth keeping. They do not.

Block Assembly's `height + GlobalBlockHeightRetention` arithmetic is real but applies to the
subtree pair — `FileTypeSubtree` and `FileTypeSubtreeMeta` (`services/blockassembly/Server.go:740`,
and the rewrite path at `:886-896`) — and Block Persister clears exactly those two when it files
(`services/blockpersister/streaming_process_subtree.go:85`, `:91`, `:231`). The additions and
deletions ledgers are a different pair: the file storer writes them with whatever options its
caller passed (`services/utxopersister/filestorer/FileStorer.go:126`), the caller passes none
(`services/utxopersister/UTXOSet.go:155`, `:160`), and the file store's fallback fires only when a
retention was configured on the store:

```go
if dah == 0 && merged.BlockHeightRetention > 0 {   // stores/blob/file/file.go:1105

```

That fallback is set by `options.WithDefaultBlockHeightRetention`
(`stores/blob/options/Options.go:114-118`) — which no non-test code in this repository calls, so
for the shipped file store `merged.BlockHeightRetention` is zero and these ledgers are never
scheduled for deletion at all. Confirmed by the read side too: the two places this service opens
its own ledgers pass `options.WithDeleteAt(0)` explicitly (`services/utxopersister/UTXOSet.go:510`,
`:553`), the convention for "do not schedule anything".

So the margin this note was written to measure does not exist for the archive's inputs, and the
risk is the opposite one: the ledgers accumulate with no expiry and nothing reclaims them, while
everything Block Assembly does schedule — the subtree pair — sits behind the hundred-block lag of
a *different* consumer with a 188-block cushion that no setting ties together. Both halves of the
sentence "retention is configured globally" are true and they apply to different files, which is
why reading one of them from the other was easy to get wrong.

There is a partial mitigation already in the code, and it belongs to the subtree path: when Block
Assembly rewrites a block's subtree it replaces the deletion date rather than keeping the earlier
one, so a block that keeps being re-mined keeps being postponed
(`services/blockassembly/Server.go:889-896`) — the same postponement noted in section 36. It helps
for reorganisations, not for downtime, and it does not touch the ledgers.

## 44. "A checkpoint file for every block" describes the naming, not the behaviour

**Documentation divergence, in the UTXO Persister's reference document.**

`docs/references/services/utxopersister_reference.md:5` states that the service "creates and
maintains up-to-date UTXO file sets for each block". The service does not work per block. It
computes a ceiling of tip-minus-a-hundred, and if that ceiling is above its bookmark it
consolidates **the whole range between** in one pass and writes **one** set covering it
(`services/utxopersister/Server.go:476-540` — `Rolling up data from block height %d to %d` at
`:497`). A node ten blocks behind the ceiling writes one file, not ten; a node that is idle
writes none, and says so.

Why the distinction matters operationally: the file names *are* keyed per block, so a directory
listing looks like a complete per-block history, and the question an auditor asks next — "was
this coin unspent at height N?" — has no answer if N falls inside a rolled-up range. An output
created and spent inside the same range is added to the pending list and removed from it before
the set is ever written, so it appears in no set at all
(`services/utxopersister/consolidator.go:295-300`, `:333-341`). That is correct for "what is
unspent at the end of the range" and silent about everything else. It is the same class of gap
as section 35's `GetSuitableBlock` stub — an honest per-block contract that the implementation
does not promise — but here the gap is not signposted, it is papered over by a documentation
sentence that says the opposite.

## 45. The dev fixture also turns off the Validator, which is the service the guide says money cannot appear without

**Confirmed, and it widens section 25 from a door question to a capability question.**

The `teranode-cr.yaml` fixture is argued in section 25 to be a development cluster rather than
a small production size, on the evidence of `imagePullPolicy: Never` on all nine blocks, 100m
cpu limits, and `host: teranode.local` on every ingress. Its service switches carry a second
tell that matters more for anyone reasoning about what the examples prove:

| Service            | Dev fixture   | Mainnet CR   | Docker mainnet overlay   |
| ------------------ | ------------- | ------------ | ------------------------ |
| `legacy`           | false         | true         | runs                     |
| `peer`             | true          | false        | commented out            |
| `validator`        | false         | true         | not a container here     |
| `propagation`      | false         | false        | commented out            |
| `blockPersister`   | false         | false        | commented out            |
| `utxoPersister`    | false         | false        | absent                   |
| `alertSystem`      | true          | true         | not a container here     |

The row that should stop a reader is Validator. The mainline guide's own summary line for it is
"the only place money is created or destroyed", and the fixture leaves it off while leaving
Block Assembly, Block Validation and Subtree Validation on. A developer using that fixture is
not exercising the path a transaction takes to become spendable, and the propagation door
section 13 found to be advertised-but-undeployed is switched off here as well. The fixture is a
development convenience, which is what section 25 argues; this is the second piece of evidence
for that reading, and it is the one that changes what a developer learns from using it.

The pattern across the table is worth stating plainly: the two Kubernetes examples disagree
about the front door and the Validator, and agree about everything else. Both Docker examples
agree with the mainnet custom resource about the door and additionally run the Pruner. In no
example in this repository do `propagation`, `blockPersister` and `utxoPersister` run at all.
That is four of the guide's fourteen service sections describing machinery that the shipped
examples leave switched off — which is not a defect in the examples and is a real constraint on
any reading of them as evidence of behaviour.

## 46. The gate into the better metering tiers ships empty, so two of three tiers are unreachable

**Confirmed, and the design compensates rather than depends on it.**

The Asset Server meters traffic by tier, with distinct keying per tier
(`services/asset/httpimpl/rate_limiter.go:44-56`): unverified callers by client IP, IPv6 collapsed
to the /64 block and the whole table bounded to 50,000 entries
(`services/asset/httpimpl/rate_limiter.go:19-23`) because "an IPv6 attacker rotating source
addresses across a /64 could grow the map to many GB before the 5-minute cleanup runs"; peers by
libp2p peer ID so two authenticated peers behind one egress IP get independent buckets; miners
either exempt or at their own rate.

Elevation requires membership of an allowlist, and the allowlist ships empty
(`settings/settings.go:234`, `asset_peerAuthAllowlist` default `""`; the key appears nowhere in
`settings.conf`). The consequence is stated in the struct comment
(`services/asset/httpimpl/peer_auth_middleware.go:203-208`):

> An empty allowlist means no peer is eligible: a signed request is rejected at the membership
> check, before the replay claim, the signature verification and the body digest, and stays at
> tierUnverified.

So `asset_httpPeerRateMultiplier` (default 5, `settings/settings.go:230`) multiplies an
allowance nobody receives, and `asset_httpMinerRateLimit` (`:231`) gates a tier that cannot be
reached. Every caller — including Teranode's own peers performing the most expensive reads the
service offers — is metered by source address at the anonymous ceiling.

The design knows this. The five catch-up routes get their burst floored at the catch-up fan-out
because a catching-up peer "fans out many concurrent, **typically tier-unverified**, requests at
once" (`services/asset/httpimpl/http.go:293-295`), which is an admission that elevation is not
relied upon. Two consequences worth recording:

- There is a second, harder dependency upstream: the tier middleware is constructed only if a
    P2P client is supplied (`services/asset/httpimpl/http.go:238-245`). Without one there is no
    verifier at all, and every request takes the `tierUnverified` branch unconditionally
    (`peer_auth_middleware.go:322`). The client is injected by the process wiring outside this
    repository, so which branch ships in a given deployment is not determinable from this
    checkout.
- The header-verified tier assignment is at `peer_auth_middleware.go:349-353`; with an empty
    allowlist, line 322 is the only line that ever runs.

Not a defect — a capability that is switched off, with the compensation already built. The risk
is that "peers get 5x" is read as behaviour.

## 47. Splitting the heavy surface to protect catch-up doubled the anonymous ceiling on it

**Confirmed, documented in the code's own start-up line, and the sort of thing a capacity review
should meet before an incident.**

Two heavy limiters are built from the same configured rate
(`services/asset/httpimpl/http.go:269-312`): `heavy` for heavy routes generally, and
`heavy_catchup` for the five routes peer catch-up depends on, distinguished by metric label. The
purpose is stated plainly — so that "a bulk caller on an unrelated heavy route (e.g. POST
/utxos, up to 1024-way Aerospike fan-out) can't spend the floor a catching-up peer needs".

The cost is stated in the same comment block, and then in the log line the service prints:

> The two limiters are independent per-IP buckets, not a shared budget: one unverified IP can
> draw asset_httpHeavyRateLimit sustained on the catchup routes AND the same rate again,
> separately, on the other heavy routes - double the pre-split sustained budget for that IP
> across the whole heavy surface.

(`services/asset/httpimpl/http.go:276-280`; the start-up line computes and prints
`2*tSettings.Asset.HTTPHeavyRateLimit` at `:310-312`.)

With shipped values — `asset_httpRateLimit` 1024 and `asset_httpHeavyRateLimit` 10
(`settings/settings.go:227-228`) — one unverified address may sustain 20 requests per second
across the heavy surface where it previously had 10, and 1,024 per second globally on top. The
change was correct in intent and is documented honestly; it is recorded here because the
arithmetic is invisible from the setting name, and the setting's own default did not move when
the surface doubled.

## 48. Ten read paths, five of which ship with no ceiling — and one comment that disagrees with its default

**Confirmed, and the mechanism is clean.**

The repository layer builds one weighted semaphore per hot read path — ten in total
(`services/asset/repository/repository.go:159-168`) — with this contract stated in three lines of
comment (`services/asset/repository/repository.go:144-158`):

```go
// 0 = unlimited, -1 = runtime.NumCPU(), >0 = exact limit

```

Against the shipped defaults (`settings/asset_settings.go`, key tags; grep form below) that
produces:

| Path                       | Default   | Effect         |
| -------------------------- | --------- | -------------- |
| get_transaction            | 0         | unlimited      |
| get_transaction_meta       | 0         | unlimited      |
| get_subtree_exists         | 0         | unlimited      |
| get_subtree_head           | 0         | unlimited      |
| get_utxo                   | 0         | unlimited      |
| get_subtree_data           | 2         | 2 concurrent   |
| get_subtree_transactions   | 2         | 2 concurrent   |
| get_subtree_data_reader    | 4         | 4 concurrent   |
| subtree_data_create        | 4         | 4 concurrent   |
| get_legacy_block_reader    | **-1**    | one per CPU    |

The shape is deliberate — expensive bulk reads bounded, cheap point lookups not — and is the
right instinct. Two observations.

First, the cheap paths are not free at the storage. An unbounded number of concurrent
`GetTransaction` calls still contend for the file store's read semaphore, which is a separate
limit owned by the store (default 768 permits, 25-second acquire timeout — section 42). So the
ceiling on those paths exists, it is just not in this service, and it fails later and louder
than a rejection at the door would.

Second, a documentation mismatch: `settings.conf:255` shows
`# asset_concurrency_get_legacy_block_reader  = 0    # default: 0 (unlimited)` while the code
default is `-1` (`settings/settings.go:247`, whose own trailing comment reads
`// -1 = NumCPU(...)`). Same commented-out style as its neighbours, different meaning, and the
two words "unlimited" and "one per CPU" describe different deployments on a 64-core machine. An
operator who leaves the line commented believes the path is unbounded when it is in fact limited
to one reader per core, and one who uncomments it to "set the documented default" of 0 quietly
removes a limit they did not know was active.

## 49. The main-chain cache knows exactly what its own silence means

**Structural, and the most careful cache in the services examined so far.**

`/merkle_proof` and `/txmeta` need "is this block on the current main chain?", which is owned by
the Blockchain service; caching it saves a gRPC round trip per request
(`services/asset/httpimpl/mainchain_cache.go:37-44`). The window size comes from
`global_blockHeightRetention`, defaulting to 288 when absent (`:18-22`) — the same constant that
sets the deletion fuse in sections 36 and 43.

The design is organised by *what absence proves*, in three regions (`:45-69`):

- **Inside the window** the set of main-chain block IDs is complete, so it is authoritative in
    both directions: "present means main chain, absent means NOT main chain. Zero RPC either
    way."
- **Below the window** "missing-means-false does NOT hold", so lookups go to
    `CheckBlockIsInCurrentChain`, deduplicated per block ID by singleflight to prevent stampedes
    on cold IDs, and written back under a generation guard — invalidation increments a counter,
    and a result fetched before it is dropped rather than stored, which the comment names as
    closing "the TOCTOU window where a stale result could outlive a reorg".
- **Above the window** the block is newer than the set and a rebuild is imminent, so results go
    straight to RPC, uncached.

The failure policy is the part worth stealing: if the window has never been populated or a
rebuild fails, it is marked unhealthy and *every* lookup falls back to direct RPC with no
caching, and the below-window cache is consulted only while healthy — "healthy window is the
precondition for any cached answer", "degraded throughput, never degraded correctness"
(`:57-72`). Below-window entries are cleared on every rebuild and on rebuild failure, for
deep-reorg safety (`:104-106`).

Rebuilds are serialised on one consume goroutine and pending notifications are drained first, so
"a burst of block events coalesces into one rebuild" (`:73-77`).

The consequence an operator should know: when this cache degrades, nothing gets wrong. The rest
of the node starts paying a round trip per proof request, which shows up as latency elsewhere
and as an unexplained load increase on the Blockchain service.

## 50. The address the mesh is told to fetch from ships as a placeholder, and the public door's ingress says "internal"

**Confirmed across five files, and it decides whether a node is a participant or a listener.**

The P2P service puts an asset-server URL in its gossip so listeners can fetch weight
(section on P2P). That URL is `asset_httpPublicAddress` (`services/p2p/Server.go:965`), whose
default is empty (`settings/settings.go:217`) and which is commented out in every example:
`settings.conf:228`, `deploy/docker/base/settings_local.conf:5`, and the operator template
`deploy/docker/base/settings_local.conf.template:63`. The catalogue description is explicit that
empty means the internal address is used for everyone
(`settings/asset_settings.go`, `asset_httpPublicAddress` longdesc: "When empty, HTTPAddress is
used for all clients").

The front door checks the address once at start-up and the comment names the shipped state as
the expected case — "the committed default — localhost asset_httpAddress with
asset_httpPublicAddress unset — produces exactly one loud warning instead of an error per publish
tick" — and the warning itself says what peers will do
(`services/p2p/Server.go:2412-2416`, `:2436-2438`):

> DataHubURL %q will be scored as a protocol violation by peers (set asset_httpPublicAddress)

Same comment block, on why announcements are not suppressed instead: "Block and subtree
announcements are useless without a fetchable URL and receivers score an empty DataHubURL just
like a malformed one, so those publish unchanged and the warning below is the operator's
signal". And on the check's own limits: "The SSRF half is a heuristic, not a security property:
validateDataHubURL reads OUR AllowPrivateIPs, which governs what this node accepts from peers,
not what peers accept from us" — with `p2p_allow_private_ips` defaulting to false
(`settings/settings.go:620`).

The operator documentation does tell people to set it — `docs/howto/miners/docker/minersSecurityBestPractices.md:51`
("Set `asset_httpPublicAddress` to the public URL including `/api/v1`"),
`docs/howto/miners/docker/minersHowToTroubleshooting.md:113` — and the local settings file
connects it to the listen-mode question in section 27: "you can change listen_mode to full after
opening your asset_httpPublicAddress" (`deploy/docker/base/settings_local.conf:11`).

The second half of the finding is in the production-shaped Kubernetes example: the asset service's
HTTP ingress is declared with `className: traefik-internal`
(`deploy/kubernetes/teranode/teranode-cr-mainnet.yaml`, the `asset` block), and the same block
notes there is no gRPC ingress because "the asset service serves HTTP and WebSocket only".
Whether *internal* is reachable from the internet is a property of that cluster, not of this
repository — but a reader comparing examples cannot tell, and a node that both advertises an
unreachable address and serves its read door on an internal class is serving its operators, not
the network. That is a legitimate posture and should be stated as one.

Related, and lower stakes: the WebSocket feed mounted on this same server is explicitly
unauthenticated — `readinessMiddleware` "does NOT authenticate the client", justified because
the socket carries "the same public, read-only chain and node telemetry" that the front door
gossips (`services/asset/centrifuge_impl/centrifuge.go:593-599`). Its origin allowlist is empty
(`settings.conf:216`), which means a browser may connect only when its origin matches the host
asked, while non-browser clients — which send no `Origin` header — are unaffected
(`services/asset/centrifuge_impl/websocket.go:488-508`). Rejections are logged at WARN at most
once per interval so "a hostile page can neither flood the log nor use up the warning for good"
(`centrifuge.go:577-580`).

## 51. Delete-at-height is a queue owned by a third service, and an unwired scheduler fails the write

**Confirmed, and it explains several things that look odd elsewhere.**

The file store's own header describes the feature as "Delete-At-Height (DAH) for automatic
cleanup of expired blobs" (`stores/blob/file/file.go:6`, repeated at `:176`). The word doing the
work is *automatic* — the store does not time anything. On a write carrying an expiry
(`stores/blob/file/file.go:1104-1124`):

```go
if dah == 0 && merged.BlockHeightRetention > 0 {
    dah = s.currentBlockHeight.Load() + merged.BlockHeightRetention
}
if dah > 0 {
    if s.blobDeletionScheduler == nil {
        return "", errors.NewConfigurationError("cannot schedule blob deletion: blob deletion scheduler not configured")
    }
    if _, _, err := s.blobDeletionScheduler.ScheduleBlobDeletion(bgCtx, key, string(fileType), s.storeType, dah); err != nil {

```

Three facts follow, none of them visible from the caller's side.

- **The queue is in the Blockchain service, not the Pruner.** The scheduler interface is
    documented "This interface is satisfied by blockchain.ClientI"
    (`stores/blob/options/pruner_client.go:9-11`) — in a file named for the pruner — and the
    daemon's own comment says "Get blob deletion scheduler (blockchain client)"
    (`daemon/daemon_stores.go:423-424`, wired at `:431`, `:484`, `:557`). The Pruner is the
    *consumer*: `services/pruner/blob_deletion_worker.go:105` calls
    `AcquireBlobDeletionBatch` and `:212` calls `CompleteBlobDeletionBatch` on the blockchain
    client.
- **An expiry without a scheduler is a write failure, not a no-op.** Any store created without
    one errors on every retention-bearing write, so a deployment that wires stores but not the
    Blockchain client fails loudly at store creation (`daemon/daemon_stores.go:424-427`), not
    later at delete time.
- **The writer cannot see whether its promise was kept.** Scheduling returns an id and a
    "already scheduled" flag; deletion is asynchronous and owned elsewhere, so section 39's
    persister retry loop and section 54's skipped heights are invisible to whoever set the expiry.

This is the third place this file has recorded the scheduler/executor pair being described
incorrectly — section 37 (persister comments naming Block Assembly for the Pruner's consumer),
section 14 (Pruner inheriting retention it does not own), and now the store's "automatic"
wording plus a filename pointing at one service for an interface satisfied by another. The
mechanism is sound and the naming consistently is not.

## 52. Both preservation phases need the coin store, and a missing one is silent while a disabled one is not

**Confirmed, and it is the difference between an operator decision and an unreported absence.**

The pruner's own comment states the consequence of not running phase 1b: preserved parents "keep
PreserveUntil set and DeleteAtHeight cleared forever and are never pruned"
(`services/pruner/worker.go:229-235`) — holds are a debt that only that pass repays. And both
preservation passes are guarded by the coin store being present:

```go
if s.utxoStore != nil && !s.settings.Pruner.SkipPreserveParents {          // worker.go:212
} else if s.settings.Pruner.SkipPreserveParents {
    s.logger.Infof("[pruner][%s:%d] phase 1: skipped (pruner_skipPreserveParents=true)", ...)  // :226-227
}

```

(`services/pruner/worker.go:212-228`, and the same shape for phase 1b at `:237`.)

There is no `else` for the `utxoStore == nil` case. So:

| Situation                                        | Phase 1 runs   | Logged                    |
| ------------------------------------------------ | -------------- | ------------------------- |
| Normal, coin store wired                         | yes            | DEBUG                     |
| Operator set `pruner_skipPreserveParents=true`   | no             | INFO, names the setting   |
| Coin store not wired                             | no             | nothing at all            |

The third row is the problem. Protection for parents of unmined transactions stops, deletion
continues without it, and the log is indistinguishable from a node that simply had nothing to
preserve — while the second row, a deliberate and reversible operator choice, is the one that
explains itself. The information needed to tell "configured" from "broken" is in the process at
the moment the decision is made.

## 53. The two waits before deleting log at different volumes, and only the rarer one is counted

**Confirmed, and the asymmetry is the wrong way round for an operator.**

Before pruning a height the service waits for block assembly to be running and caught up
(`services/pruner/worker.go:30-70`) and separately waits for the block to be marked mined
(`:72-107`). Both retry a thousand times — one second apart, half a second apart — and on giving
up both skip the phase. What differs is the reporting:

| Wait                                     | On give-up                                                  | Metric                                                |
| ---------------------------------------- | ----------------------------------------------------------- | ----------------------------------------------------- |
| Block assembly ready / caught up         | `Warnf`, names height and reason (`:65`)                    | `prunerSkipped{"block_assembly_timeout"}` (`:66`)     |
| `mined_set=true`, record path            | `Debugf` (`:188`, `:190`)                                   | none                                                  |
| `mined_set=true`, file-deletion path     | `Warnf`, names height (`blob_deletion_worker.go:56-57`)     | none                                                  |

A first reading of this file stopped at the second row and was incomplete: the same wait is
implemented twice, once per half of the service, and the two halves report it at different
volumes. Two further details matter more than the asymmetry itself.

The record path's `continue` at `services/pruner/worker.go:191` skips the rest of the pass, and
the wake-up that starts file deletion comes later in the same block
(`services/pruner/worker.go:200-205`) — so a quiet, uncounted timeout in the record path also
stops the half that would have logged a warning about it. And the file-deletion path's wait is
gated on trigger mode (`blob_deletion_worker.go:53-54`, "only in OnBlockMined trigger mode"), so
in the shipped default of `OnBlockPersisted` that half does not wait for mined status at all, on
the reasoning that a persisted block is already mined. The reasoning is sound; the consequence is
that the loud version of this wait is off in the configuration that ships and the silent version
is the one running.

The mined-set wait is the one that fires in ordinary operation: a block arriving slightly ahead of
validation is exactly the condition the wait was built to absorb, and the code even comments that
the assembly check allows "1 block tolerance for race conditions during rapid block generation"
(`:42-43`). Yet that ordinary, expected skip produces nothing an operator at the shipped `info`
level can see, and no counter, while the comparatively unusual assembly failure is both loud and
counted. A pruner skipping every height because blocks are not yet mined looks, from the metrics,
identical to a pruner doing nothing at all.

Related and unresolvable from this file: signals are coalesced to the latest height, so
intermediate heights are skipped (``worker.go:135-137``, "intermediate heights are naturally
skipped"). Deletion is threshold-based, so a later pass still sweeps earlier records; parent
preservation is per-height, so a skipped height's children's parents are not examined by the pass
that replaced it. Whether that is reachable in practice is a question about pending-transaction
lifetimes, so it is recorded rather than asserted.

## 54. The catch-up guard waits for a state that nothing in the node's own start-up path enters

**Confirmed, and it turns an optional safety into an off switch.**

`pruner_skipDuringCatchup` (default false, `settings/settings.go:674`) makes the pruner skip a
height unless the FSM reports RUNNING:

```go
// Only RUNNING proves no catchup is in flight: an operator STOP
// parks a catching-up node in IDLE while its batch still runs.
if fsmState == nil || *fsmState != blockchain.FSMStateRUNNING {   // worker.go:172-174

```

The comment is correct about the transition table: STOP moves RUNNING or CATCHINGBLOCKS to IDLE
(`services/blockchain/fsm.go:35-42`), so an operator pause really does park a catching-up node in
IDLE. What the comment does not mention is how a node ever reaches RUNNING: the only transition
into it is the explicit RUN event (`fsm.go:17-25`), and the callers of RUN in this repository are
an operator command (`cmd/setfsmstate/set_fsm_state.go:53`), a test daemon
(`daemon/test_daemon.go:157`), a test helper (`test/utils/helper.go:1274`), and the gRPC
convenience method itself (`services/blockchain/Server.go:3112-3115`). No service start-up path
fires it.

Compounding it: the health probe treats RUNNING, CATCHINGBLOCKS and IDLE identically — all three
return 200 (`services/blockchain/fsm.go:149-155`) — so a node parked in IDLE with pruning
switched off looks healthy by every external check.

This is section 30's naming trap with a mechanism attached: the state names describe the last
instruction received, not the work in progress, and a guard written against the *names* will
silently never fire. Enabling the switch converts a catch-up safeguard into a pruning stop-switch
with no log line and no failing health check.

## 55. The pruner is absent from both Kubernetes examples and present in both Docker ones

**Confirmed by enumeration, and it changes what "the deployment does not prune" means.**

The top-level service keys in the two cluster files are:

```text
alertSystem asset blockAssembly blockPersister blockValidator blockchain bootstrap coinbase
legacy peer propagation rpc subtreeValidator utxoPersister validator

```

(`deploy/kubernetes/teranode/teranode-cr-mainnet.yaml`, `deploy/kubernetes/teranode/teranode-cr.yaml`,
identical in both; the only other top-level keys are `envFrom` and `sharedStorage`, which are not
services). No pruner entry, in either file, while the thirteen other inventory services that run
as processes each have one. Two of the fifteen entries — `bootstrap` and `coinbase` — have no
inventory row at all, a gap in the other direction noted for the stores-and-interfaces section.
It is not `enabled: false` as `blockPersister` and `utxoPersister` are; the key is simply not
there. Both Docker
overlays do start it (`deploy/docker/mainnet/docker-compose.yml`, `deploy/docker/testnet/docker-compose.yml`,
each `extends` the base `pruner` service which runs `-pruner=1`).

Combined with section 38, the shipped examples break the deletion pipeline in opposite
directions:

| Deployment                 | Filing service     | Pruning service   | Effect                                                                                         |
| -------------------------- | ------------------ | ----------------- | ---------------------------------------------------------------------------------------------- |
| Kubernetes (both)          | `enabled: false`   | key absent        | expiries are accepted and queued by the Blockchain service (section 51); nothing drains them   |
| Docker mainnet / testnet   | commented out      | running           | pruner waits on `OnBlockPersisted`, which the absent persister never emits                     |

Both shapes grow rather than shrink, but they need different diagnoses and different fixes, and
from inside either example the other failure mode is invisible. Worth noting for anyone reading
the Kubernetes file as "the production shape": the service that would otherwise be filling in for
the disabled persister is not deployed at all, so section 38's one-line fix (`OnBlockMined`) would
not help there — the pruner has to be added first.

## 56. The pruner's connection budget assumes it is the only pruner

**Confirmed, and it is the sharpest scaling caveat in the service.**

`pruner_connectionPoolWarningThreshold` (default 0.7, `settings/pruner_settings.go:11`) is not a
warning threshold in the ordinary sense — it scales the pruner's own parallelism:

```go
recommendedMax := int(float64(connectionQueueSize) * s.connectionPoolWarningThreshold)  // :531
if maxPrunerConnections > recommendedMax {
    adjusted := recommendedMax / (numWorkers + 1)                                        // :535

```

(`stores/utxo/aerospike/pruner/pruner_service.go:531-537`.)

Two things follow. The guard lives in the Aerospike UTXO store's pruner helper, not in
`services/pruner`, so reading the service directory alone does not show that the pruner throttles
itself — a second reason, alongside section 40's misnamed keys, to treat the pruner as a mechanism
spread over three places. And the calculation is per-process: it takes the queue size of the pool
this process opened and keeps its own usage under seventy percent of it. Nothing in the arithmetic
knows about other instances, so two pruners pointed at the same database each reserve seventy
percent and together request a hundred and forty.

That sits in uncomfortable contrast with the lease design in section 55. The work queue is built
for several workers and the batch leases really do keep them from duplicating effort; the shared
resource underneath is divided as if there were one. Adding a second instance is therefore safe
with respect to *what gets deleted* and unsafe with respect to *how many connections it takes to
do it*, and the knob that governs the second part is named as if it were only a warning. Lowering
the threshold per instance, or moving the calculation to a shared limit, are both smaller changes
than rewriting the queue.

## 57. `sendrawtransaction` is a door wearing a window's clothes — and its two storage calls are guarded differently

**Confirmed, and it is the reason the RPC surface has to be read as an ingest path, not a query API.**

The handler does four things in sequence (`services/rpc/handlers.go:830-930`):

1. deserialises the transaction (`:846-857`);
2. writes it to the blob store — `if s.txStore != nil { s.txStore.Set(...) }`, `:863-871`, with the
    comment "following the pattern from propagation service";
3. resolves the inputs from the coin store to compute the fee (`:899`, `PreviousOutputsDecorate`);
4. calls `s.validatorClient.Validate(ctx, tx, 0)` (`:924`), which is what spends the coins and
    hands the transaction to Block Assembly.

So this service is a second Propagation: it does not forward submissions to the ingest door, it
performs the ingest steps itself. Section 12's question — what feeds Propagation — has a shadow:
a transaction submitted here never touches Propagation at all, so any policy that lives in that
service is bypassed by this route. Whether that is intended is the same open question this
repository already carries about the front door, now with a second door attached to it.

The two storage accesses are guarded in opposite ways, which is the part worth recording:

| Dependency                 | Guard                            | If unwired                                                                                     |
| -------------------------- | -------------------------------- | ---------------------------------------------------------------------------------------------- |
| Blob store (`txStore`)     | `if s.txStore != nil` (`:863`)   | write skipped silently; validation and spend proceed anyway                                    |
| Coin store (`utxoStore`)   | none (`:899`)                    | nil method call, recovered as `internal error: RPC handler panicked` (`Server.go:1004-1012`)   |

Neither is reachable in the shipped wiring — the daemon passes both stores
(`daemon/daemon_services.go:575`) — so this is not a live bug. What it documents is an
asymmetry of judgement about the same two lines of work: the write that can be skipped quietly is
the one with the nil check, and skipping it is the failure with the worse consequence, because the
coin is spent and Block Assembly holds a transaction whose bytes were never stored. Nothing in the
handler logs the skip. Recorded because the next reader to reorganise this path will have to
choose again, and the choice made here is easy to copy without seeing.

## 58. The concurrency limit counts requests, closes every connection, and nobody notices when the caller leaves

**Confirmed, and the three facts combine into a door that sheds load by refusing it rather than
delaying it.**

```go
if int(atomic.LoadInt32(&s.numClients)+1) > s.rpcMaxClients {        // Server.go:849
    s.logger.Infof("Max RPC clients exceeded [%d] - disconnecting client %s", ...)
    http.Error(w, "503 Too busy.  Try again later.", http.StatusServiceUnavailable)  // :853

```

- **The counter is of in-flight requests**, incremented and decremented around a single request
    (`Server.go:1353-1359`), so "maximum clients" is really "maximum simultaneous requests". The
    code default is 1 (`settings/rpc_settings.go:14`); `settings.conf:1284` ships 3.
- **No connection survives a response.** Every reply sets `Connection: close` and forces
    `r.Close = true` (`Server.go:1347-1349`), so callers cannot reuse a socket even when the door
    is idle. There is no `Retry-After` on the refusal, so the client's only information about when
    to try again is the phrase "Try again later".
- **No handler ever notices a client giving up.** All twenty-nine implemented handlers take the
    disconnect channel as `_` (`grep -c "_ <-chan struct{}" services/rpc/handlers.go` returns 29,
    matching the twenty-nine `func handle` definitions). The channel is populated for hijacked
    WebSocket connections (`Server.go:1209-1216`) and the only thing that reads it is
    `handleUnimplemented` (`Server.go:389-399`) — the placeholder. The practical bound on a stuck
    command is therefore the per-command timeout (`Server.go:1015`, 30 seconds by default), and a
    submission whose caller has gone can still spend the coins.

The design is coherent for an administrative door: shed rather than queue, so a crowd of callers
cannot pile up work behind an already-busy Validator. It is the wrong shape for machine traffic,
and nothing in the setting's name or description says which of the two it is protecting.

## 59. The limited tier promises forty-four methods and delivers twenty-five

**Confirmed by enumeration, and it is an integration failure that looks like a capacity failure.**

The limited credentials are allowed through only for methods named in `rpcLimited`
(`services/rpc/Server.go:288`), which lists 44 commands — the point being that a read-only account
can be handed to a monitoring fleet or an integration partner without handing them `setban`. The
command table in the same file has 62 entries, 31 of which map to `handleUnimplemented`. Nineteen
of the placeholders are inside the allowlist:

```text
decoderawtransaction decodescript estimatefee getbestblock getblockcount getcfilter
getcfilterheader getcurrentnet getheaders getnettotals getnetworkhashps gettxout gettxoutproof
searchrawtransactions submitblock uptime validateaddress verifymessage verifytxoutproof

```

So a read-only caller passing the tier check on `getblockcount` — the single most commonly polled
code in Bitcoin tooling — receives "method not implemented". Nothing links the two lists, and the
tier check cannot catch the mismatch because it only asks whether a name is allowed, not whether
anyone has written the handler. Two honest options exist: prune the allowlist to what works, or
say in the setting's description that the allowlist is a permission set rather than a capability
list. It is recorded rather than called a defect because the allowlist is not *wrong* — it
forbids exactly what it says — but a reader will assume it also promises.

## 60. Empty credentials shut the door, and the setting's own description says they open it

**Confirmed in code, contradicted in prose, untested.**

The stored hash is computed only when both halves of a credential exist
(`services/rpc/Server.go:1482-1492`):

```go
if rpcUser != "" && rpcPass != "" {
    login := rpcUser + ":" + rpcPass
    auth := "Basic " + base64.StdEncoding.EncodeToString([]byte(login))
    rpc.authsha = sha256.Sum256([]byte(auth))
}

```

Otherwise `authsha` stays as thirty-two zero bytes, and `checkAuth` compares a request's
`sha256(Authorization header)` against it (`:937-951`). A real header hash will not equal the zero
array, and the request path always demands authentication — `s.checkAuth(r, true)` at `:1360`, with
no configuration that passes `false`. So unset credentials make every request fail: the door
locks, it does not fall open.

One document says the reverse, and it is the one written for this exact decision.
`settings/rpc_settings.go:10-11` carries "Leave empty to disable authentication (development only)"
in the `longdesc` of both `rpc_user` and `rpc_pass` — the field text a settings reference is
generated from. `services/rpc/README.md` gets it right ("set both in `settings_local.conf` before
the RPC service will authenticate anyone"), and `docs/references/settings/services/rpc_settings.md`
lists the defaults as empty without characterising the behaviour, so the wrong claim has exactly one
source and no amplifier. `settings.conf:1286-1292` is closer to correct, explaining that while the
credentials are unset "the Asset admin routes reject every request" — the right fail-closed
behaviour, described for the other service and not for this one.

No test covers it: the only credential test asserts that the `test` and Docker contexts resolve to
`bitcoin` (`settings/rpc_credentials_test.go:24-25`). The consequence is operational rather than
security-shaped — fail-closed is the safe direction — but an operator who deploys with no
credentials because the setting's description said that opens the door will find a listening port
answering `401` to plausible requests, with the setting reference on their side of the argument and
the README two directories away.

## 61. Five commands are answered from a ten-second cache, and one of them is held for five minutes

**Confirmed, and the exception is the command an operator reaches for during a reorganisation.**

`rpc_cache_enabled` is on by default (`settings/rpc_settings.go:18`) and gates five commands
(`grep -n "rpcCallCache" services/rpc/handlers.go`):

| Command                 | Cache lifetime             | Line                            |
| ----------------------- | -------------------------- | ------------------------------- |
| `getbestblockhash`      | 10 s (library default)     | `handlers.go:515`, `:527`       |
| `getpeerinfo`           | 10 s                       | `handlers.go:1211`, `:1344`     |
| `getblockchaininfo`     | 10 s                       | `handlers.go:1473`, `:1515`     |
| `getinfo`               | 10 s (library default)     | `handlers.go:1653`, `:1752`     |
| `getchaintips`          | **300 s**                  | `handlers.go:2936`, `:2964`     |

The cache itself is a process-wide TTL map with a 10-second default and
`WithDisableTouchOnHit` (`handlers.go:72-79`), so an entry expires on its own rather than being
extended by readers. Against the three-request ceiling in section 58 this is plainly deliberate:
caching the four cheapest-to-lose answers is what lets a chatty monitor hammer a door that is only
three wide. Nothing here is wrong.

What is worth a second look is the 300-second outlier. `getchaintips` exists to show where the
chain has branches — active, valid-fork, active-fork, invalid headers — which makes it the command
an operator runs when they suspect a reorganisation, and the only one of the five whose whole
purpose is describing volatile state. It is served from a snapshot up to five minutes old, while
the tip itself (`getbestblockhash`) goes stale after ten seconds. So the sequence "run getchain-tips
during a reorg, see a single active chain, conclude nothing happened" is possible on a healthy node
with default settings, and the second-grader's fix — a shorter lifetime — is the obvious one, or an
invalidation on the chain-update path that does not currently exist. Recorded as a question rather
than a defect because the trade is real: walking chain tips is not a cheap query, and if the
intent is that nobody polls this command during a reorg, the lifetime should say so.

## 62. A freeze can report success and freeze nothing, and the failure list cannot tell you

**Confirmed from the code's own comment, and the reporting shape makes it invisible.**

`AddToConsensusBlacklist` derives the coin identity it hands to the store from the transaction
record on this node (`services/alert/node.go:296`, `:325-326`). The comment at `:319-324` states the
hazard outright:

> The two agree only while the stored transaction is byte-faithful. A node bootstrapped from a
> UTXO-set snapshot stores input-less reconstructions that retain no inputs, version or locktime, so
> they hash to something else entirely; the UTXO hash derived from them matches nothing in the
> store, and the freeze then reports success while blacklisting nothing.

The response type cannot distinguish the outcomes. Every failure branch appends the coin and a reason
to `response.NotProcessed` — bad txid (`:291`), missing parent (`:298`), nil output (`:313`), hash
derivation failure (`:329`), store error (`:342`, `:347`) — and the success path appends nothing
(`:339-351`). So a run that froze five coins and a run that matched zero both return an empty
failure list, and neither the caller nor the alert network can tell them apart.

The consequence runs into the next command in the protocol. Reassignment requires the coin to be
frozen: the SQLite store raises "transaction %s:%d is not frozen" when the flag is clear
(`stores/utxo/sql/alert_system.go:167-169`). So on a snapshot-seeded node the freeze silently
matches nothing, and the confiscation that follows fails loudly with an error naming the freeze —
pointing the investigation at the alert service rather than at how the node was seeded.

Worth pairing with the seeding discussion this file has carried since section 9: the trade-offs of
snapshot seeding are documented in terms of disk and cost, and nowhere in terms of the alert service
answering instructions differently on a seeded node than on a synced one.

## 63. Confiscation rewrites the commitment to a locking script it never stores

**Confirmed in both store implementations, and named in the code as issue 1725.**

The alert path builds the new world and hands it over (`services/alert/node.go:456-495`): a public
key is extracted from the confiscation transaction's unlocking script, a P2PKH locking script is
built from it, an amended output is assembled with the original satoshi amount, and a new UTXO hash
is derived that commits to that script. Then:

> Calculate the replacement commitment. ReAssignUTXO does not persist the replacement script, so
> changing the owner currently strands the output for both owners after mandatory re-extension
> (issue 1725).

Both stores confirm it. SQLite updates three columns and nothing else — `utxo_hash`, `frozen`,
`spendableIn` (`stores/utxo/sql/alert_system.go:181-188`) — so the stored locking script is still
the original one while the stored hash now commits to a script that exists only in the alert.
Aerospike dispatches a Lua routine with the old hash, the new hash, a height and a delay
(`stores/utxo/aerospike/alert_system.go:244-251`); no script is among its arguments, so it cannot
store one either.

Operationally that means the previous owner cannot prove the coin, because the commitment moved, and
the new owner cannot spend it, because the script never arrived — and the coin is held for a
thousand blocks on top (`stores/utxo/Interface.go:49`), so the stranding outlasts most incident
windows. The design is sound; the write is one column short. Recorded because the note that explains
it is a comment inside a loop body, and the effect is that an emergency instruction leaves a coin
unusable by anyone.

## 64. The setting for that thousand-block hold works on the development store and is ignored on the production one

**Confirmed, and it inverts the pattern the rest of this file documents.**

`utxostore_reassignedUtxoSpendableAfterBlocks` is declared with a default of 1,000
(`settings/utxostore_settings.go:55`, read at `settings/settings.go:554`) and is absent from
`settings.conf`. Two stores consume it differently:

| Store                           | Behaviour                                                            | Line                                          |
| ------------------------------- | -------------------------------------------------------------------- | --------------------------------------------- |
| SQLite (development default)    | starts from the constant, overrides with the setting when non-zero   | `stores/utxo/sql/alert_system.go:173-176`     |
| Aerospike (production-shaped)   | passes the constant; the setting is never read                       | `stores/utxo/aerospike/alert_system.go:250`   |

An operator who lowers the hold to reopen a confiscated coin sooner gets a shorter hold on the store
used for development and testing, and the full thousand blocks on the store in the mainnet example.
The recurring pattern in these notes is a knob that works where it is deployed and is untested
elsewhere; here the knob works where nothing deploys and does nothing where everything does. Because
the key is not in `settings.conf`, there is also no comment in the shipped configuration marking the
constant as the real value.

## 65. The brake bites at spend time, and the block assembler is never told

**Confirmed by absence, and it explains a latency an operator would otherwise misread.**

Freezing is enforced where coins are spent: the coin store refuses and names the condition —
"transaction is frozen", "UTXO is frozen", "UTXO frozen until block"
(`stores/utxo/aerospike/spend.go:1315`, `:1370`, `:1373`). That is the correct chokepoint, because
every path that spends passes it.

Nothing upstream is notified. The block assembly client is injected into the alert service
(`services/alert/server.go:112`, stored at `services/alert/node.go:56-58`) and its only use in the
package is the readiness check (`services/alert/server.go:164-165`) — `grep -rn
"blockassemblyClient\." services/alert/*.go` finds no call in the alert-handling path at all. So a
transaction whose input has just been frozen remains in the pending set and can still be selected
into a block; it is stopped when the spend is attempted. The coin stops immediately and the block
that contains it may not, which is a gap an operator watching a brake being pulled will read as the
brake being slow.

Whether that is worth fixing depends on whether selection-time filtering would prevent anything
besides wasted work — the recorded position here is that enforcement is correctly placed and the
missing piece is a signal, not a second gate.

## 66. The alert service is absent from every Docker file, which also contains a decoy with its name

**Confirmed by enumeration, and the decoy is worse than the gap.**

There is no alert service in the Docker catalogue: `grep -c "^  alert:" deploy/docker/base/docker-teranode.yml`
returns zero, and the service names in that file are `asset`, `blockassembly`, `blockchain`,
`blockpersister`, `blockvalidation`, `legacy`, `peer`, `propagation`, `pruner`, `rpc`,
`subtreevalidation` — fourteen keys, no alert among them. Both overlays do define `alertmanager:`,
which is Prometheus's alert routing service (`deploy/docker/mainnet/docker-compose.yml:118`,
`deploy/docker/testnet/docker-compose.yml:118`), unrelated to the consensus alert system, with a
comment explaining it exists so Prometheus stops logging unresolvable hosts.

In Kubernetes it is the opposite: enabled in both cluster files, one replica each
(`deploy/kubernetes/teranode/teranode-cr-mainnet.yaml:10-19`). Which is also where the stray
`imagePullPolicy: Never` lives — exactly one occurrence in the production file, on `alertSystem`
(section 25, open question 15).

So the deployment this repository hands to miners runs no emergency brake, and an operator who
searches the compose file for "alert" to check will find a service by that name and reasonably
conclude the brake is present. The gap is a decision someone made for disk or image reasons; the
naming collision is what stops anyone noticing it was made.

## 67. The transaction blob store points at nothing, in every deployment in this repository

**Confirmed from the shipped default, and it is not a deployment choice — there is only one line.**

`settings.conf` names the transaction store once:

```text
txstore = null:///

```

(`settings.conf:1600`, no comment beyond "# Store to use for transaction blobs", and exactly one
occurrence of `^txstore` in the file.) The null store is the backend that keeps nothing, and its
behaviour is precise about it:

| Call              | Null store                            | Source                               |
| ----------------- | ------------------------------------- | ------------------------------------ |
| `Set`             | debug log, keeps nothing, success     | `stores/blob/null/null.go:104-150`   |
| `Get`             | `errors.ErrNotFound`                  | `stores/blob/null/null.go:153-196`   |
| `Exists`          | `false`, unconditionally              | `stores/blob/null/null.go:198-211`   |
| `Del`, `SetDAH`   | success                               | `stores/blob/null/null.go:136-220`   |

Writers exist. Propagation writes every transaction it accepts
(`services/propagation/Server.go:1656`), and so does the RPC door
(`services/rpc/handlers.go:863-871`, section 57). The Pruner has a mapping for the store type and
would delete from it in the ordinary course (`settings/settings.go:843-846`). One production caller
reads it, and it is not inside the node: `compose/chainintegrity/chain_integrity.go:600`, the
integrity checker that lives in the directory this file has already flagged as misplaced
(section 61).

Section 57 noted that the RPC write is guarded by `if s.txStore != nil`. That guard is what makes this
quiet: a null store is a perfectly good non-nil `blob.Store`, so the guard passes, `Set` returns
success, and the bytes are gone. Nothing in the node can distinguish "storage disabled" from
"storage empty", because the interface's success answer means "accepted", not "kept".

What makes it a decision nobody recorded rather than a deliberate one is the contrast with its two
siblings in the same file. The block store has four spellings including a development context and an
operator context (`settings.conf:496-499`); the subtree store has eight, three of them operator
variants (`settings.conf:1488-1495`). The transaction store has one line, no context variants, and it
points at a black hole. So the mechanism that would give it a directory exists, is used twice in the
same neighbourhood of the file, and was not used here.

The fossil confirms the sequence: four test rigs bind-mount a `txstore` directory into the container
(`test/docker-compose-host.yml:148,197,246` and `test/docker-compose.e2etest.legacy.yml:211,239`),
which is where files would go. Those rigs select `SETTINGS_CONTEXT: "docker.host.teranode1"`
(`test/docker-compose-host.yml:139`), a context with no transaction-store line, so the mount receives
nothing. Whatever the default was when those volumes were written, it is not the default now, and no
file says when it changed or why.

Practical consequence for an operator: a node configured like this cannot serve an old transaction's
bytes from blob storage, and loses its whole pending pile on restart with no copy on disk. That is
survivable — peers re-send what the node missed — but it means the node's memory of unmined work is
exactly as durable as its process, and `settings.conf` says so in four words that read like a
placeholder.

Addendum, recorded when the UTXO Store section was written. The sentence above, "no deployment in
this repository puts a submitted transaction on a disk", is too broad and was wrong in a way worth
keeping. It is true of the *transaction blob store*, which is what this section is about. It is not
true of the ledger: the Aerospike UTXO store writes the transaction that created a coin — the whole
transaction, or a compact list of its outputs — into a blob store of its own, configured inside the
UTXO store's connection string as `externalStore=file://${DATADIR}/external` with the inner `?`
percent-encoded (`settings.conf:1629`, written at `stores/utxo/aerospike/create.go:494-518`). So a
submitted transaction that reaches the ledger does reach a disk, in `${DATADIR}/external`, and a
submitted transaction that never gets that far — rejected, or still sitting in the pending pile when
the process stops — reaches nothing.

The distinction matters for anyone troubleshooting, because the two shelves have different names,
different retention, and different failure messages, and `settings.conf` only ever mentions the one
that is empty. See section 71 for what owns deletion on the shelf that is not.

## 68. The rule that turns a hash into a directory is written down twice, joined by a comment

**Confirmed, and the duplicated copy is the one used by the deleting side.**

Two characters of a filename decide which subdirectory a blob lives in, and which characters those are
is configured three ways:

- per-store defaults in the constructors: `+2` for the transaction store (`daemon/daemon_stores.go:415`)
    and the subtree store (`:457`), `0` for the temporary store (`:530`), `-2` for the block store
    (`:583`), and `+2` for the persister store (`:637`);
- a `hashPrefix` query parameter on the store URL, which overrides the default in both places that
    build a store (`daemon/daemon_stores.go:416-419`, `services/pruner/blob_deletion_worker.go:333-338`);
- and a second copy of the same table inside the Pruner, introduced by this comment:

```text
// Default hash prefix per store type (must match daemon_stores.go)

```

(`services/pruner/blob_deletion_worker.go:332`, with the table itself at `:356-374`.)

Two things make that worth recording. First, the duplicated table is not a fallback for exotic
configurations — it is the path taken whenever the URL carries no parameter, which is every store in
the shipped configuration. The one place `settings.conf` sets `hashPrefix` at all is on line 1629, and
it is nested inside an encoded URL (`externalStore=file://${DATADIR}/external%3FhashPrefix=2`) for the
UTXO store's external store, which is not a blob store and not in the Pruner's table. So for all five
blob stores the Pruner's copy of the table decides where deletions land.

Second, no test compares the two tables. `grep -rn defaultHashPrefix --include=*_test.go
services/pruner/` finds nothing. The contract is the word "must" in a comment.

The failure mode is silent, and the code already contains its own misdiagnosis. When every blob in a
batch is absent, the Pruner logs:

```text
blob deletion: all %d blobs in batch were already missing from disk - verify the pruner pod has the correct volume mount for blob store(s)

```

(`services/pruner/blob_deletion_worker.go:204`.) A prefix disagreement between the writer and the
deleter produces exactly that log line, and the sentence sends the operator to the volume mount. The
thing to check is a table in a different package.

There is a live example of the same path with two different minds on it: `blockpersister_store =
${blockstore}` (`settings.conf:286`), so the persister's store and the block store are rooted at the
same directory while the constructors give them opposite prefix signs, `+2` at
`daemon/daemon_stores.go:637` and `-2` at `:583`. Two layouts in one tree is workable — the file types
differ — but it means "which subdirectory holds this hash" has no single answer for that path, and the
answer depends on which service is asking.

## 69. The file-handle budget is tuned for a limit no Teranode container sets

**Confirmed, and the design choice is deliberate and well-argued; the gap is in the examples.**

The file store meters open files with two process-global weighted semaphores — reads and writes
separately, because one shared counter deadlocked (`stores/blob/file/file.go:328-339`) — with defaults
of 768 and 256 (`settings/block_settings.go:25-27`). `settings.conf` explains the arithmetic and then
hands the operator the job:

> These limits protect against Linux ulimit exhaustion by controlling concurrent file descriptors.
> Total: read + write should stay under your system's open file limit (ulimit -n), leaving room …
> The limit itself is the operator's to set (ulimit -Hn, systemd LimitNOFILE, container --ulimit)
>
> — `settings.conf:504-511`

Both knobs are then commented out, so the defaults stand (`settings.conf:516-517`).

What the code does when the limit is smaller than the budget is a considered choice, written at the
point of use:

> The semaphores bound how many file operations run at once, but they cannot bound what the OS allows,
> so fit them under the descriptors it actually leaves us rather than refusing to start (issue 1431).
>
> Clamping, not refusing, is the important choice.
>
> — `stores/blob/file/file.go:438-442`

That is the right shape of decision, and it is why nothing breaks. The gap is that the examples never
set the limit for the services that need it. In the Docker catalogue exactly one container declares
`ulimits`, and it is a database:

```text
    ulimits:
      nofile:
        soft: 15000
        hard: 15000

```

(`deploy/docker/base/docker-services.yml:193-196`, inside the `aerospike-tn` block.) No Teranode
service block sets it, and `grep -rn "ulimit\|nofile" deploy/kubernetes/` returns nothing at all —
the Kubernetes examples carry no `ulimit`, no `securityContext.sysctls` and no annotation mentioning
the open-file limit.

So the number in force is whatever the container runtime hands out, and on a common default of 1024
the clamp reduces both counters at start-up and says so once, at INFO, naming both numbers
(`stores/blob/file/file.go:1759-1781`). Nothing is wrong; the node simply runs with a smaller read
path than the settings text advertises, and the only evidence is a line most operators never read at
INFO level. Two consequences worth stating plainly:

- commissioning should include one command per service — `docker exec <service> sh -c 'ulimit -n'` —
    and the answer belongs next to the throughput expectations the same files set;
- the budget is per process, so this is section 56's arithmetic with file handles instead of database
    connections: N replicas of a blob-touching service ask for N times the descriptors, against a
    limit nothing in these files raises.

## 70. The address of a coin in the ledger includes a number that the shipped contexts disagree about

**Confirmed from the code's own warning and three values in one file.**

With the Aerospike engine, a transaction's outputs are stored in chunks, and which chunk holds an
output is computed from the output index modulo a configured size:

```text
return vout % uint32(s.utxoBatchSize)

```

(`stores/utxo/aerospike/aerospike.go:920`; the read path does the same arithmetic at
`stores/utxo/aerospike/get.go:356`, and chunk construction at `stores/utxo/aerospike/create.go:664-671`.)
The size comes from settings:

```text
utxostore_utxoBatchSize          = 128
utxostore_utxoBatchSize.docker   = 50
utxostore_utxoBatchSize.docker.m = 512

```

(`settings.conf:1731-1733`.) Because the value is part of how a location is computed, it is not a
tuning knob — it is part of the address of every coin already on the disk, and the constructor says as
much, without hedging:

```text
// It's very dangerous to change this number after a node has been running for a while
// Do not change this value after starting, it is used to calculate the offset for the output

```

(`stores/utxo/aerospike/aerospike.go:345-346`.)

Three values ship, and the two that matter are two lines apart. `docker.m` is not an obscure
alternative — it is the default the base catalogue hands the first node
(`deploy/docker/base/docker-teranode.yml:13`, `SETTINGS_CONTEXT: ${SETTINGS_CONTEXT_1:-docker.m}`),
so the same deployment family carries 512 as its default and 50 for any node started under the plain
`docker` context.

What is missing is the record. Nothing in the data directory says which chunk size it was written with,
so nothing can refuse to read it under a different one; the value is taken from settings on every start
and trusted. The blob store next door solves exactly this problem for itself — every file it writes
begins with an eight-byte magic number naming what is inside (`pkg/fileformat/header.go:49-53`), so a reader
can say "that is not a block file" rather than mis-parsing it. The ledger has no equivalent for the
layout parameter, and the failure it would catch is silent: for most output indices, 50 and 512 give
different remainders, so a coin is simply not where the reader looks.

Limits of the claim, stated plainly. Only the Aerospike engine shards this way; the SQL engine keeps
the outputs in a row. And the two shipped contexts that differ are not both pointed at one data
directory in these files — `utxostore.docker.ss.teranode1` is a PostgreSQL address outright
(`settings.conf:1630`). So this is a hazard documented in the code and left unenforced, not a fault
observable in the shipped stack.

## 71. The ledger's own files are deliberately outside the delete-at-height promise, and the setting that appears to govern them does not

**Confirmed at the point of construction, with the reason written down.**

The deletion arrangement this file has described repeatedly — the Blockchain service holds a promise,
the Pruner fulfils it, and a file carries the height at which it may go (§51) — does not apply to the
files the UTXO store writes. The store turns the mechanism off while building its blob store:

```text
// Create external store with DAH explicitly disabled
// External file lifecycle is managed by the Aerospike pruner service, not by DAH files

```

(`stores/utxo/aerospike/aerospike.go:337-338`, passing `options.WithDisableDAH(true)`; individual
writes also pass `options.WithDeleteAt(0)` at `stores/utxo/aerospike/create.go:1157`.)

The reason is sound: the rule for these files is "delete a fully spent parent once every spending child
is mined and comfortably old" (`docs/utxo-safe-deletion.md`), and a height on a file cannot express
"once every child is verified". So the store keeps its own clock, `unminedSince`, and handles the
reorganisation case by stamping a transaction that has fallen back to pending with a timestamp from the
past, so it does not win a fresh full grace period by being re-org'd
(`stores/utxo/pruner_unmined.go:16-35`, `stores/utxo/aerospike/longest_chain.go:77`, both citing
issue 1768).

The consequence for operators is the part worth recording: **the two numbers that decide when the
ledger forgets appear nowhere in `settings.conf`.** `utxostore_unminedTxRetention` and
`utxostore_parentPreservationBlocks` have zero occurrences in the file — `grep -c` on each returns 0 —
and both are computed at load time from constants belonging to other concerns:

```text
UnminedTxRetention:        getUint32("utxostore_unminedTxRetention", globalBlockHeightRetention/2, alternativeContext...),
ParentPreservationBlocks:  getUint32("utxostore_parentPreservationBlocks", blocksInADayOnAverage*10, alternativeContext...),

```

(`settings/settings.go:505-506`, with `blocksInADayOnAverage = 144` at `settings/settings.go:31`.)
Since `global_blockHeightRetention = 288` in the shipped file (`settings.conf:790`), the ledger's
unmined grace period is 144 blocks, derived by halving a number that lives in another service's
section of the configuration. An operator who lowers `global_blockHeightRetention` to buy back disk
space halves the ledger's grace period as a side effect, silently, and will find no hint of the
coupling in the file they edited — which is another instance of the pattern section 32 named, and the
first where the derived value is a deletion deadline rather than a timeout.

## 72. The `aerospike` build tag no longer gates the Aerospike store, but `AGENTS.md` and the Makefile both still say it does

**Confirmed by reading the directives and asking the compiler.**

`AGENTS.md` documents test tags as including "aerospike (tests requiring Aerospike)", and every build
target passes it: `go build -mod=readonly -tags aerospike,${TXMETA_TAG}` appears four times in the
Makefile (`Makefile:102,106,110,114`). For the UTXO store in this checkout the tag selects nothing.
Four files carry the directive commented out — note the second pair of slashes, which turns the
constraint into prose:

```text
// //go:build aerospike

```

(`stores/utxo/factory/aerospike.go:1`, `stores/utxo/process_conflicting.go:1`,
`stores/utxo/aerospike/set_mined.go:1`, `stores/utxo/aerospike/delete.go:1`.) One live directive
remains in the entire repository, and it is on a test
(`stores/utxo/aerospike/native_op_add_deleted_children_integration_test.go:1`). The compiler settles
it: `go build ./stores/utxo/factory/` with no tags at all succeeds, so a binary built without the tag
registers and uses `aerospike://` addresses exactly as the tagged one does.

Two practical consequences. An operator choosing a build because they need Aerospike is choosing for a
reason that no longer applies; and a developer who follows `AGENTS.md`, runs the tagged suite, and
concludes the untagged build was not exercised is describing a distinction the code no longer makes.
Nothing is broken. The tag, the Makefile lines, and the conventions file are three artifacts agreeing
with each other and not with the tree, which is why no test fails.

## 73. The metadata cache's shard count is a build tag, and the Kafka protocol partitions by it

The transaction metadata cache splits its memory into `BucketsCount` shards, and that constant is
selected by build tag:

| Build                    | `BucketsCount` | Location                                                |
| ------------------------ | -------------- | ------------------------------------------------------- |
| Default                  | 8,192          | `stores/txmetacache/improved_cache_const_large.go:10`   |
| `-tags smalltxmetacache` | 32             | `stores/txmetacache/improved_cache_const_small.go:10`   |
| `-tags testtxmetacache`  | 8              | `stores/txmetacache/improved_cache_const_testing.go:10` |

The receiver chooses a bucket with that constant (`stores/txmetacache/improved_cache.go:386`,
`stores/txmetacache/pointer_cache.go:276`). The v2 producer chooses a Kafka partition from the same
constant — `bucket = xxhash(hash) % BucketsCount`, `partition = bucket / (BucketsCount /
NumPartitions)` (`services/validator/Validator.go:2009`, `:2032-2034`) — and its own comment states
the intent: "Each partition therefore owns a contiguous, disjoint range of receiver cache buckets"
(`:1968-1971`).

Nothing puts the constant on the wire and nothing checks that producer and receiver agree. A Validator
built with `smalltxmetacache` and a Subtree Validation built without it will exchange valid messages
forever, with the bucket ranges quietly overlapping: the concurrency the partitioning exists to buy
becomes lock contention, and the property is not observable from either side's metrics. The default
`validator_txmeta_wireFormat` is v1 (`settings/settings.go:487`) and `settings.conf` contains no
`validator_txmeta` key at all, so the overlap is currently unreachable — which is another instance of the
family catalogued in sections 11, 18, 23, 33 and 71: a mechanism complete in code, absent from the
shipped settings, with the failure reserved for whoever turns it on.

## 74. Two partition counts, and the default context has them eight apart

The v2 routing divides by `validator_txmeta_numPartitions`, which defaults to 32
(`settings/settings.go:488`, absent from `settings.conf`). The topic's own partition count comes from a
different variable in a different file: `kafka_txmetaConfig` builds its URL with
`partitions=${KAFKA_PARTITIONS_HIGH}` (`settings.conf:853`), and that is **8** in the shared default
context, 32 only in `docker.m` and `operator` (`settings.conf:38-40`).

So in the plain default context the routing can name partitions 8 through 31 on an eight-partition
topic, in a producer mode documented as having "no fallback" — "Callers MUST set Partition on every
Message they publish" (`util/kafka/kafka_producer_async.go:112-116`). Two numbers that must agree,
nothing comparing them, and the safe thing about the situation is only that v2 is off by default. The
same settings file raises the topic to 32 for the two deployment contexts that would need it
(`settings.conf:39-40`) without touching the producer's number, so the pairing works by coincidence in
those contexts and fails in the one an operator would use for a laptop run.

## 75. The metric that would say whether the cache is working is commented out, and its own doc block still advertises it

`stores/txmetacache/metrics.go:199-207` is a commented-out gauge:

```go
// prometheusBlockValidationTxMetaCacheSize = promauto.NewGauge(
//     prometheus.GaugeOpts{
//         Namespace: "teranode",
//         Subsystem: "tx_meta_cache",
//         Name:      "size",

```

Twelve lines above it, the function's own documentation still lists it as a live capacity metric:
"size: Current number of entries in the cache" (`:180`). What is exported is insertions, hits,
misses, origin reads, evictions, trims and map size (`:211-287`), so an operator can compute a hit
rate and see the map allocation, but cannot see how full the cache is — the one reading that separates
"empty because idle" from "full and churning", which is the distinction a 1 GiB budget
(`settings.conf:1580`) actually needs. Map size is a poor proxy: the shards are pre-divided from the
budget at construction (`stores/txmetacache/improved_cache.go:311`), so it stays flat whether the
cache holds one entry or a million.

The scaling consequence sits in the same package's wiring. The txmeta consumer group gets sixteen
random characters appended to its name "to allow multiple instances to run concurrently and consuming
the same messages independently" (`daemon/daemon_kafka.go:178-180`): Kafka is used as a broadcast, so
each Subtree Validation replica keeps a complete copy of the cache. Adding machines adds memory and
adds hit capacity, and divides nothing — which is a defensible design for a pending-set working
memory, and is worth stating plainly in a guide before someone reads "cache" and assumes a shared one.

## 76. Eight of fourteen services publish a contract, and two of those call it something else

**Structural finding, affects any survey done by filename.**

The convention says a service publishes its surface in a file named `Interface.go`, with a matching
`Client.go` holding the wire handling. Measured across `services/`:

- Eight services have a contract file: Block Assembly, Blockchain, Block Validation, Legacy (as
    `peer/Interface.go`), P2P, Propagation, Subtree Validation, Validator.
- Six have none: Alert, Asset Server, Block Persister, Pruner, RPC, UTXO Persister.
- Eight have a `Client.go`, the same eight.

Two of the eight deliberately misname it. Propagation's is `ClientInterface.go`
(`services/propagation/ClientInterface.go:20`) and Block Assembly's contract is called `ClientI`
(`services/blockassembly/Interface.go:66`) with a second contract `Store` below it (`:232`). A
filename survey for the convention therefore finds six of eight and reports the wrong denominator.
The guide's interfaces table is built by reading files rather than by listing names.

Of the six without one, three are pure wire services — Alert, Asset Server and Pruner each expose a
gRPC contract with no in-process caller in this repository — which is a coherent position. The other
three are not: Block Persister consumes Kafka and writes files with no published surface of any kind,
RPC is reached only over HTTP, and UTXO Persister is reached only over Kafka. For those three, "what
may I ask this service" has no answer in Go.

## 77. The no-protobuf rule broke on the bus, not on a door

**Convention violation, and the one that costs something.**

The convention forbids protobuf types in a published interface, and the reference implementation
named alongside it is P2P. Measured, compliance is close to total where the surface is small and
absent where it matters:

| Service            | Published questions | Accepting a wire type |
| ------------------ | ------------------- | --------------------- |
| P2P                | 26                  | 0                     |
| Validator          | 6                   | 0                     |
| Block Validation   | 6                   | 0                     |
| Subtree Validation | 3                   | 0                     |
| Propagation        | 1                   | 0                     |
| Block Assembly     | 18                  | 3                     |
| Blockchain         | 73                  | 5                     |
| Legacy             | 6                   | 6                     |

The P2P reference is clean, which is worth stating because it is the existence proof that the rule
is workable at this surface size.

The violation that matters is not a count but a type. Blockchain's event bus takes and hands out a
protobuf structure — `SendNotification(ctx, notification *blockchain_api.Notification)`
(`services/blockchain/Interface.go:111`) and `Subscribe(ctx, source) (chan *blockchain_api.Notification, error)`
(`services/blockchain/Interface.go:558`) — and every service in the node subscribes to that channel.
So the wire type the rule was written to keep out of the interior is now the parameter type of the
most widely used interface in the system, and the conversion the rule wanted in one place lives
instead in every consumer. `blockchain.FSMStateType` is the workaround: a hand-written wrapper over
the enum (`services/blockchain/Interface.go:72`), which is evidence the leak was noticed and managed
rather than closed.

## 78. Six of nine gRPC doors accept any caller, and the settings file says so twice

**Security posture, arguable but deliberate.**

One shared credential, `grpc_admin_api_key`, is the only interior authentication in the system. Of
the nine services that start a gRPC server, three install an interceptor that checks it — Blockchain
(`services/blockchain/auth.go:12-19`, with a public-method exception list), Legacy
(`services/legacy/Server.go:787-804`) and P2P (`services/p2p/Server.go:1389`).

The other six start their servers with nothing to check. That is verified by the last argument to
each `StartGRPCServer` call, which is the auth options: `nil` for Validator
(`services/validator/Server.go:483`), Block Assembly (`services/blockassembly/Server.go:1081`),
Block Validation (`services/blockvalidation/Server.go:1233`), Subtree Validation
(`services/subtreevalidation/Server.go:589`) and Pruner (`services/pruner/server.go:301`).
Propagation builds an auth options block and fills in only a ban-list hook
(`services/propagation/Server.go:355-361`), and the shared server helper installs an interceptor only
if `RequireAuthByDefault` is set or an API key is present (`util/grpc.go:83-88`) — neither is true
there, so Propagation's door is open as well.

Nothing encrypts any of it. `security_level_grpc = 0` (`settings.conf:1310`) with no context
override, so no certificate is read at all (`util/grpc.go:62-71` skips the cert loading when the
level is zero, which is also why the shared credential declares it does not need transport security,
`util/grpc_helper.go:66-68`). The settings file documents both halves of this plainly — the shared
key paragraph explains that only the Blockchain's health endpoints are public
(`settings.conf:800-808`) — but nothing in it states that the remaining six doors have no check at
all, which is the sentence an operator would need.

## 79. The credential every service needs is defined nowhere in the repository

**Deployment gap, one-line fix.**

Blockchain treats the credential as a condition of existing: `ValidateRequiredAdminAPIKey` rejects an
empty value, one with padding, a well-known placeholder, or anything under sixteen characters
(`util/grpc_required_auth.go:17-22`, `util/admin_api_key.go:29`), and it is called before the
Blockchain starts its listeners (`services/blockchain/Server.go:524-525`). Its clients check the key
against the live door before accepting the connection and give up if it is refused
(`services/blockchain/Client.go:158-163`).

The requirement is documented: the development cluster file says "REQUIRED: include
grpc_admin_api_key for Blockchain startup and all service clients"
(`deploy/kubernetes/teranode/teranode-cr.yaml:82-83`) and points at a secret named
`teranode-operator-secrets` (`:85-86`). But no `kind: Secret` exists anywhere under `deploy/`, and
the key has no value in `settings.conf` — only the comment describing it (`settings.conf:800-808`).

So a node started from this repository's own manifests cannot start: Blockchain exits, and its
clients exit behind it, because the one setting all of them share has to be created outside the tree.
That is defensible for a secret and normal practice — but the mainnet cluster file states the same
requirement in a comment (`deploy/kubernetes/teranode/teranode-cr-mainnet.yaml:93`) while the
docker examples carry no mention of it at all, and a reader who starts from Docker will get an
unhelpful failure first.

## 80. The resolver written for Kubernetes is registered and then overwritten

**Dead-as-shipped code, provable from two files.**

`pkg/k8sresolver` is a resolver written for this project specifically — four hundred and sixty-seven
lines outside its tests, a thousand one hundred and eighty-four with them. It builds in-cluster
credentials (`pkg/k8sresolver/k8s.go:30-41`), lists and watches the cluster's endpoint records for a
namespace (`:66`, `:100-104`), and caches the answers with a per-name key
(`:53-61`). Its builder registers the scheme `k8s` (`pkg/k8sresolver/builder.go:69-71`).

It is installed in `util/grpc_helper.go:525-537`, in a switch on the resolver setting:

```go
case "k8s":
    resolver.Register(k8sresolver.NewBuilder(logger))
    resolver.SetDefaultScheme("k8s")
case "kubernetes":
    resolver.Register(k8sresolver.NewBuilder(logger))
    kuberesolver.RegisterInClusterWithSchema("k8s")
```

The `kubernetes` branch registers two builders under the same scheme name, and gRPC's registry is a
map assignment keyed by scheme (`google.golang.org/grpc@v1.82.1/resolver/resolver.go:54-56`,
`m[b.Scheme()] = b`), so the second call replaces the first. The shipped production context selects
`kubernetes` (`settings.conf:815`), which means the in-repo resolver is constructed, registered, and
discarded on every start. The branch that would keep it — `k8s` — is selected by nothing: no line in
`settings.conf` and nothing under `deploy/` sets that value.

Two consequences are worth knowing. The in-repo resolver's own logging and caching are not what
production runs on, so behaviour questions about service discovery should be answered from the
upstream package, not this one. And the lookup watches endpoint records
(`pkg/k8sresolver/k8s.go:66`), a permission no manifest grants — not because a manifest grants the
wrong thing, but because the repository contains no `Role`, `ClusterRole`, `RoleBinding` or
`ServiceAccount` object at all. Across `deploy/kubernetes/` the kinds present are Service, ConfigMap,
StatefulSet, PersistentVolume, PersistentVolumeClaim, NetworkPolicy, Deployment and Cray `Cluster`; a
case-insensitive sweep of every manifest in the tree finds no RBAC kind anywhere. The single place the
word appears is Aerospike's StatefulSet, which uses it to turn the mounted token off
(`deploy/kubernetes/aerospike/aerospike-statefulset.yaml:20`, `automountServiceAccountToken: false`)
— the opposite of what a resolver needs, and in a different product's pod. Teranode's pods therefore
run under the namespace's default service account. That permission gap applies to whichever resolver
runs, because the upstream in-cluster resolver needs the same access.

Whether it bites depends on facts outside this repository. A cluster may grant `discovery` read access
to `default` by policy, or an operator may apply RBAC when installing the operator itself. The
repository can only say the grant is absent from its own manifests, which is what this finding claims
and nothing more. It can also say the Docker smoke path would never notice: there the resolver setting
is `dns` and this code is not reached.

## 81. The busiest interface in the design is switched off by a default

**Config finding, changes the meaning of every client table.**

`useLocalValidator = true` (`settings.conf:1602`). The key carries no context suffix, so it is true
in every context including `operator`.

Its effect is in the one function every service uses to reach a Validator: when the flag is set, the
process builds a Validator inside itself instead of dialling one
(`daemon/daemon_stores.go:280-283`), constructing its own UTXO store and its own Block Assembly
client while it does. Four services ask for that client — RPC (`daemon/daemon_services.go:567`),
Block Validation (`:820`), Propagation (`:1085`) and Legacy (`:1164`) — and in the shipped
configuration, not one of them opens a connection to the Validator service.

The Validator's gRPC door is still built, and its five published methods still exist, but nothing in
this repository dials them except the diagnostic commands (`cmd/monitor/monitor.go:200`,
`cmd/diagnose/health_checks.go:139`). The service is reached over the conveyor belt instead, and the
eight replicas in the production-shaped example (`deploy/kubernetes/teranode/teranode-cr-mainnet.yaml:183`)
do block and transaction work arriving on Kafka, split by partition rather than by client-side
spreading — which is the reason the topic that carries that work is given thirty-two partitions in the
same file (`settings.conf:859`).

The address it would dial is also wrong for a split deployment: `validator_grpcAddress` is
`0.0.0.0:${VALIDATOR_GRPC_PORT}` (`settings.conf:1743`), a listen wildcard sitting in a dial slot,
where every other service's dial target is a name to look up. On Linux, dialling `0.0.0.0` means
"this machine", so clearing the flag without correcting that line makes each copy of the Validator
call itself.

## 82. The fast lane has no sender, and its default interface belongs to a Mac

**Interface surface, dormant twice over.**

Propagation can listen for transactions on IPv6 multicast
(`services/propagation/Server.go:435-476`) and the settings file documents the address as a comment
(`settings.conf:828-830`). Every deployment in this repository leaves it unset, which is the first
reason it is dark. It is also receive-only, which is the second and less visible one.

The listener reads and nothing else: `ReadMsgUDP` at `services/propagation/Server.go:499`, with the
socket list otherwise used only to append (`:474-476`) and to close (`:603-609`). A search of the
whole tree for a UDP write finds one file, `services/legacy/upnp.go`, which is talking to a router.
So a transaction arriving by multicast can be heard and cannot be answered in kind — it leaves by the
ordinary gRPC and Kafka routes, which means the lane is an ingest path and not a propagation path,
whatever the service is named after.

Two defaults would catch an operator who switched it on. The interface falls back to `en0`
(`services/propagation/Server.go:439-441`), which is the macOS device name; a Linux host gets a
clean startup failure instead of a guess (`:445-446`). And an empty source allowlist is read as
accept-everything, logged as such (`:456-458`) — with no credential involved, since the key check in
`util/grpc.go:83-88` is not on this path at all.

## 83. The ledger store was written for a second instance, and the settings file hides the ceiling

**Design evidence, plus a shipped-default trap.**

`stores/blockchain/sql` is the only component in this tree whose comments describe more than one
Blockchain process running against one database, and describe it in the past tense of something that
happened.

The design evidence is the durable reservation table. A block needs a number before it commits, and
two paths can bring the same block in during initial sync — Legacy netsync and Block Validation
catch-up. Without a shared authority "each calls nextval and gets a DIFFERENT id, one of which is
written into the block's UTXO mined-info while the block commits under the other — orphaning the
first id (a phantom that later fails checkOldBlockIDs and wedges sync)"
(`stores/blockchain/sql/sql.go:128-136`). The guard has two levels: an in-process cache behind a
mutex, and a table whose entries "survive ttlcache TTL expiry, restart, and a second instance"
(`stores/blockchain/sql/assign_block_id.go:62-63`), inserted with `ON CONFLICT` so a second process
cannot claim a promised number (`:108`). The mutex is explicitly documented as covering only this
process, with cross-process races left to the table (`:43-44`).

The incident evidence is stronger. Schema creation at startup is serialised behind a fixed advisory
lock whose constant carries the comment "must be unique per schema-creation context and stable across
releases" (`stores/blockchain/sql/sql.go:477-479`) — a number two processes have to agree on. And
the fast path exists because of a specific cascade, described in the code: every DDL statement takes
a SHARE lock on `blocks`; a concurrent instance's `on_main_chain` rebuild holds ROW EXCLUSIVE on the
same table for the length of the walk; shared requests queue; Postgres queues fairly, so ordinary
SELECTs arriving afterwards line up behind the queued DDL. "That cascade is what blocks readers
during otherwise idle startup" (`stores/blockchain/sql/sql.go:484-493`). The fix probes the catalog,
which locks no table in the ledger (`:494-500`).

None of that is reachable with one instance. So the store is designed, and has been field-tested,
against a topology no deployment in this repository runs.

The shipped-default trap is separate and smaller. `util.InitSQLiteDB` ignores every pool setting and
fixes the connection ceiling at five, with a comment naming the cause: the driver's working memory
lives in a global arena that never returns pages, which drove "sql.test's bimodal ~2G/~34G swing and
the intermittent OOM (#1051)" (`util/sql.go:283-290`). Defensible — SQLite serialises writes anyway.
But the default ledger store *is* the file engine (`settings.conf:483`), and the same file offers
`postgres_maxOpenConns = 50` (`settings.conf:116`) with a per-service override documented but
commented out (`:130`). So the shipped default runs five database operations at a time, and the
settings file contains two numbers that suggest otherwise.

## 84. The chain-walk cache is stale on purpose, and the freshness setting is off

**Two switches that only make sense together, one of them off.**

The store caches the results of parent-walking queries for ten minutes
(`chainWalkCacheTTL`, `stores/blockchain/sql/sql.go:53`), and clears it on exactly four events —
store-time reorg detection, invalidation, deletion, revalidation
(`stores/blockchain/sql/StoreBlock.go:357`, `InvalidateBlock.go:139`, `DeleteBlock.go:79`,
`RevalidateBlock.go:56`) — and explicitly not when a block is written or a block field updated
(`stores/blockchain/sql/sql.go:1314-1318`). The reasoning is sound and stated: parent links never
change, so only a change of on-chain status can make a cached walk wrong.

The other switch decides whether the walk happens in memory or in SQL:
`blockchain_use_in_memory_chain_check` (`settings/blockchain_settings.go:26`), and the shipped value
is `false` (`settings.conf:466`). So as configured, every membership question goes to the database,
in batches of a thousand because Postgres refuses roughly thirty-two thousand bind parameters
(`stores/blockchain/sql/CheckBlockIsInCurrentChain.go:13-17`), on an engine whose concurrency ceiling
is five by default (section 83). The two settings are individually reasonable and jointly a tuning
conversation nobody appears to have had: the cache that would make the SQL path cheap is the one the
setting switches off.

Worth an operator's attention rather than a code change, and worth measuring before concluding —
neither number is visible in the shipped metrics without looking at the database.

## 85. Open questions

1. **Needs a maintainer, not another reading of the code: is the reputation-only
        treatment of invalid subtrees intended policy?** Should `ReasonInvalidSubtree`
        reach `AddBanScore` the way `ReasonInvalidBlock` does?

    The code comment argues yes — subtree failures can be transient transport faults,
    and the report path says it should "not be too harsh on failures of transient
    behavior". But nothing states it as a decision, and the asymmetry against invalid
    blocks is unexplained anywhere a reader would find it.

    That gap carries a specific risk worth naming. To a newcomer the constant sitting
    there, allowlisted and penalty-configured with no caller, reads like an oversight
    waiting to be tidied up. The plausible well-meant fix is to wire it into
    `AddBanScore`. The consequence of applying that fix if the behaviour was deliberate
    is a node that hands out ban points for network jitter, and sheds healthy peers
    during exactly the periods when transit is least reliable — a failure that would
    surface as unexplained connectivity loss, not as an obvious bug. A line in the
    source, or here, recording the intent would stop it happening.
2. Should shipped cluster configuration raise Subtree Validation above one replica once
    Validator saturation is observed, or is intra-instance parallelism considered
    sufficient headroom?
3. The new guide and `docs/topics/services/subtreeValidation.md` cover overlapping
    ground at different depths, and the guide omits mechanisms the reference documents
    (pause coordination, Merkle level calculation). Left as-is by the decision above.
    Worth confirming that two documents describing the same service differently, neither
    pointing at the other, is acceptable rather than a slow trap for new readers.
4. Should the overloaded use of "level" in section 8 be resolved by renaming in one of
    the two documents, or is the disambiguation line in the guide sufficient? Renaming
    would mean editing an existing file, which the decision above currently forbids.
5. **Partly answered by section 26; the residue needs a maintainer.** Which front door is
    *intended*, as opposed to which one ships? Section 26 establishes that Legacy runs in
    every deployment artefact in this tree, that the modern door is deliberately gated
    rather than unfinished, and that the supported quickstart runs both. What no file says
    is whether the target arrangement is Legacy alone or Legacy plus the modern door, and
    whether the development fixture's peer-on/legacy-off pairing is a step toward
    production or just convenient locally. The guide now argues from deployment evidence;
    the intent is still unrecorded.
6. Does Propagation enforce any policy that a transaction can bypass by arriving through
    the Legacy door instead? Section 12 now identifies the ingress — external submitters via
    the asset server's `POST /tx` proxy — so the question is answerable, but not from config
    alone: it needs the two submission paths walked side by side.
7. Should the examples deploy Propagation? Section 26 found the documented answer — the
    supported quickstart runs it — while every in-tree example leaves it off or unlisted.
    So the question is narrower and cheaper than section 13 left it: should the shipped
    examples say what the supported install does? As written they advertise a transaction
    route that leads nowhere unless an operator also finds the other repository.
8. **Narrowed considerably by sections 25, 26 and 29.** The front door and the block
    persister are no longer open questions: section 26 settles which door ships, and section
    29 shows the persister is an opt-in archival role rather than a stale flag. Two pieces
    remain. `utxoPersister` is unlisted in every Docker compose file and disabled in both
    Kubernetes files, with no comment or howto explaining how a node gets seeded instead —
    is the service simply not deployed in these topologies? And the mainnet example requests
    4800Gi of shared storage with both persisters off, which is either about the block store
    or an inherited line. Both are answerable in a sentence by whoever wrote the example,
    and each has so far cost a paragraph of hedging instead.
9. Should the example deployments turn on ingest backpressure and a bounded block-assembly
    queue? Sections 17 and 18 show the pair is off, that one cannot work without the other,
    and that the shipped trade is an unbounded in-memory queue plus two failure modes the
    submitter cannot see. If the intended answer is "operators set these", the examples should
    carry the pair together rather than in comments.
10. Does anything enforce the validator's guard maintenance contract? Section 16 records that
    every behaviour-changing request field must be listed in `nonDefaultValidationOptions`, and
    that an omission is "a silent hole". A test enumerating the proto fields and asserting each
    is either listed or exempt would replace that with something mechanical. Is one planned?
11. Should `double_spend_window_millis` be renamed, or documented where operators look?
    Section 19 shows one unprefixed key driving two unrelated behaviours — the mining hold-back
    and a drain-loop bound that silently switched itself off when the value was zero, which is
    the shipped default. Both the naming and the default deserve an explicit decision rather
    than an inherited one.
12. Is the unbounded ingest queue an accepted risk now that the drain is bounded? Sections 17,
    18 and 19 together leave ingest never refused, no hold-back, and no backpressure controller
    in the shipped configuration, with a recorded incident at 558 GB RSS. The drain fix may be
    sufficient; it would be good to know which.
13. Should the two hidden block-validation defaults be put in `settings.conf`? Section 23
    describes a pair of knobs where turning one on without the other is reachable and
    undocumented, and the code comment defending the fast path's checkpoint bound suggests the
    boundary is considered load-bearing.
14. Is the coinbase-maturity fork-depth boundary tested at the boundary? Section 24 records that
    the operator was once `>=` and is provably `>` (issue 4592). A test at exactly
    `d == CoinbaseMaturity` and one at `d == CoinbaseMaturity + 1` would make the proof
    executable rather than written down.
15. Is `imagePullPolicy: Never` on `alertSystem` in the mainnet example deliberate? Section
    25 found it on that one service there and on all nine services in the development
    fixture, which is the shape of a copy-paste rather than a decision. If it is inherited,
    alerting does not start on a clean node; if it is deliberate, a comment would stop the
    next reader "fixing" it in the wrong direction.
16. Should the shipped single-node configuration persist the peer registry? Section 31 shows
    the mechanism is complete — versioned envelope, save tick, load with expiry — and
    switched off by an unset default with no entry in `settings.conf`, so bans are forgotten
    on every restart, graceful or not. The setting's own long description calls a local
    on-disk store the default for single-node deployments, which is what the shipped
    configuration is not. Either the examples should set it or the description should stop
    promising it.
17. Should the howto's "there is no transition from CATCHINGBLOCKS back to IDLE" be
    corrected? Section 30 shows the transition exists, is wired, and is used by the
    documented stop path. Correcting it means editing an existing file, which the first
    decision above currently forbids, so the guide left it alone — but this one is a
    sentence that changes an operator's risk assessment rather than a naming preference, and
    it may deserve an exception.
18. Should the peer-registry TTL floor be expressed once? Section 32 shows five minutes
    derived from a ten-second publish interval and a one-minute batcher window living in
    three constants across two services, with the batcher as the load-bearing one. A test or
    a single derived constant would survive someone changing the batcher.
19. Has the catch-up cost of the in-memory chain-membership route been measured? Section 33
    quotes the configuration saying the duty cycle where it starts costing more than it
    saves "has not been measured", which makes the shipped default a held opinion rather
    than a result — and the shadow comparison that exists to answer exactly that is armed
    behind the switch that turns the route on.
20. Should the shipped single-node stack set `pruner_block_trigger`? Section 38 shows the
    Docker mainnet and testnet stacks run the pruner with the persister commented out while
    the trigger stays at its `OnBlockPersisted` default, which is the mode that waits for a
    notification the absent persister will never send — so deletion never starts and the
    retention window stops bounding anything. Either the examples should set `OnBlockMined`,
    which is what the setting's own recommendation says to use without a persister, or the
    examples should run the persister. Both are one-line changes to files the examples
    already own.
21. Should the pruner announce that it has never been triggered? Section 38's failure is
    silent: the service subscribes, reports healthy and does nothing, and the first visible
    symptom is the coin store growing. A startup log line naming the trigger mode, or a
    counter of pruning passes that an operator can alarm on when it stays at zero, turns a
    disk-growth investigation into one glance. The information is already in the process.
22. Should filing failures that cannot resolve be paced differently? Section 39 shows the
    loop pauses on three branches — including one where pausing looks arbitrary, a block that
    will not serialise — and does not pause on the two where a repeat attempt is most likely
    to fail for the same reason. A block whose source data has been pruned is retried
    back-to-back forever, logging each time.
23. Should a missing deletions ledger be distinguishable from being caught up? Section 42 shows
    three situations sharing one log line, and one of them — the deliberate tolerance for a
    block that spent nothing — is keyed to an error name the shipped file store does not emit,
    so the tolerance does not fire and the case joins the other two. Naming one error value in
    two places is a small change; the alternative is that "the coin archive has stopped"
    remains something an operator discovers from disk usage.
24. Should the coin archive's hundred-block lag be a setting, and should the ledgers it waits for
    have a retention policy at all? Section 43 was rewritten after a re-check: the lag is fixed in
    code, the 288-block retention is real but applies to the subtree pair, and the additions and
    deletions ledgers this service consolidates are never scheduled for deletion. So there are two
    questions, not one. Making the lag visible — a setting, or a start-up line naming it — would at
    least let an operator see the coupling instead of discovering it after downtime. And the
    ledgers accumulating with no expiry is either intended (an archive is supposed to be
    permanent) or an oversight; a sentence in the service's own documentation would settle which.
25. Should the development fixture say what it does not exercise? Section 45 finds the
    Validator off in `teranode-cr.yaml` while Block Assembly, Block Validation and Subtree
    Validation are on. That is a defensible choice for a laptop cluster, but nothing in the
    file or its documentation says the transaction path is not being run, so a developer using
    the fixture may reasonably believe they exercised it. One comment line naming the services
    switched off for development — the way the Docker overlay already says "read the docs
    first" about the peer service — closes that.
26. Should the Asset Server say at start-up that its tiering is inactive? Section 46 shows the
    elevation allowlist shipping empty, so the peer multiplier and miner tier cannot be reached
    and every caller is metered by address. That is a defensible default, and the catch-up burst
    already assumes it. What is missing is the announcement: the service logs its heavy-route
    rates but not the fact that no caller can ever be elevated, so an operator who allowlists a
    peer and sees no change has to read middleware source to learn the elevation also needs a
    P2P client to have been wired. One line naming the active tier mode and the allowlist size
    costs nothing.
27. Should the heavy-route ceiling be halved now that the surface is two buckets? Section 47
    records the code's own statement that splitting the heavy surface doubled the sustained
    budget one unverified address can draw across it, with the default left where it was. The
    split was the right change for catch-up fairness; whether 10 + 10 is still the intended
    anonymous total is a number question only a maintainer can settle, and if the answer is "no,
    10 was the total", the fix is one default.
28. Should `asset_concurrency_get_legacy_block_reader`'s comment in `settings.conf` match its
    default? Section 48 shows the file describing `0 (unlimited)` while the code default is `-1`,
    one reader per CPU — and the code's own trailing comment says so. Purely a one-token edit to
    a comment line, and the kind of mismatch that turns a support question into a source reading.
29. Is a node without `asset_httpPublicAddress` meant to serve the mesh at all? Section 50 shows
    the address commented out in every example, the front door warning once at start-up that
    peers "will score" the advertised URL as a protocol violation, and the production-shaped
    ingress declaring a class named *internal*. Either the default posture is "private node that
    happens to gossip" — in which case the warning should say that, rather than sounding like a
    misconfiguration — or the examples should set the address the way they set other
    reachability values.
30. Should the deletion-scheduler interface be renamed to say who implements it? Section 52 shows
    an unwired scheduler correctly failing a write rather than dropping a promise, and shows the
    queue being owned by the Blockchain service while the interface is declared in
    `stores/blob/options/pruner_client.go` with a comment pointing at `blockchain.ClientI`. The
    behaviour is right and the filename is a trap for the next reader, who will look for the
    pruner. One comment at the declaration is the whole fix.
31. Should a missing coin store be as loud as a disabled preservation phase? Section 53 shows the
    operator switch logging at information level naming the setting, and the unwired store logging
    nothing while both silently stop protecting parents of unmined transactions. The two states
    deserve different sentences, and the process knows which one it is in.
32. Should the mined-set wait warn and count the way the assembly wait does? Section 54 shows the
    ordinary, expected skip — a block not yet marked mined — logging at debug with no counter,
    while the rarer assembly failure is loud and labelled. Alongside it: is coalescing heights
    safe for the per-height preservation pass, even though it is sound for threshold-based
    deletion? A parent whose child's height was skipped is the case worth an answer, and the
    pending-transaction lifetime that decides it is not visible from this file.
33. Is `pruner_skipDuringCatchup` meant to require an operator command? Section 55 shows it waits
    for a state only an explicit run event enters, that no start-up path sends it, and that the
    health probe reports all three steady states healthy. If the intent is "prune only when an
    operator has said go", the setting name should say so; if the intent is "pause while
    catching up", it should test for catch-up rather than for permission.
34. Should the Kubernetes examples deploy the pruner? Section 56 finds it absent from both cluster
    files while the Docker overlays run it, leaving the two shapes failing in opposite directions —
    a queue nothing drains, and a drainer waiting for a trigger that never fires. If pruning is
    intended to be off in the cluster topology, that is a decision worth one line; if it is an
    omission, the `blockPersister` comment above it shows the house style for saying so.
35. Should the file store be given a default retention at creation? Section 43 found the fallback
    exists, is documented and is never configured outside its own definition
    (`stores/blob/options/Options.go:114-118`, consumed at `stores/blob/file/file.go:1105`), which
    makes every write without an explicit expiry permanent by default rather than by decision. The
    alternative reading — that permanent-by-default is exactly what an append-only archive wants —
    is reasonable, in which case the option should be deleted, or the store should log once at
    start-up that no default retention is in force, so the next reader does not assume the global
    retention setting covers files it does not reach.
36. Should `maxrawtxfee` be in `settings.conf`, and should the ceiling be shared with the other
    doors? Section 57 shows the fee guard exists only on the RPC entry point — the code says so —
    and the key appears nowhere in the shipped configuration
    while defaulting to ten million satoshis, with a test asserting the default is not zero. A
    caller whose submission is refused above a tenth of a coin in fee can only learn the ceiling
    from an error message. Either the ceiling belongs in the configuration operators read, or the
    policy belongs in the Validator so it applies to every door.
37. Which behaviour is intended when `rpc_user` and `rpc_pass` are empty: refuse everything, as
    the code and `services/rpc/README.md` both do, or disable authentication, as the `longdesc` on
    `rpc_user` and `rpc_pass` says? Section 60 shows `settings.conf` describing the fail-closed
    outcome for the Asset routes and staying silent about the door it configures. One test would
    settle it for good — assert the response to an unauthenticated request when no credentials are
    configured — and the losing sentence in whichever source is wrong can go with it.
38. Is the RPC door meant to serve machine traffic? Section 58 shows three simultaneous requests
    as the shipped ceiling, no queue, no `Retry-After`, `Connection: close` on every response, and
    no handler that notices a caller leaving. That is a coherent shape for an operator with a
    terminal and an incoherent one for a mining pool or an explorer, and the setting name
    ("maximum clients") does not say which is being limited. If machine traffic is in scope, the
    fix is a queue with a bound rather than a refusal; if it is not, the name and the description
    should say so plainly.
39. Should the limited-tier allowlist be pruned to implemented methods, or labelled as a permission
    set? Section 59 shows nineteen of its forty-four names reach `handleUnimplemented`, including
    `getblockcount` and `gettxout`. Pruning is a behaviour change for anyone scripting against the
    list; a sentence in `rpc_limit_user`'s description saying the list grants permission and not
    capability is free.
40. Should `getchaintips` keep a five-minute cache? Section 61 shows it is the only cached command
    whose lifetime is above ten seconds and the only one whose entire subject is volatile chain
    state, so "one active chain" may be a five-minute-old observation on a healthy node with default
    settings. Either the lifetime is wrong for this command, or the chain-update path should
    invalidate it. A third option is honest and nearly free: say in the settings description that
    the answer may be stale, so nobody reads it as an observation of the present.
41. Should the alert service report what a freeze actually matched? Section 62 shows the handler's
    response records failures with reasons and records successes as an absence, so "froze five
    coins" and "matched nothing" are the same reply — and on a snapshot-seeded node the second one is
    the likely answer. Appending the coin to a processed list on success is a two-line change and
    turns a post-incident mystery into a log line.
42. Is confiscation usable at all while the replacement script is unpersisted? Section 63 shows both
    stores leave the original locking script in place while the commitment moves, which the code itself
    says strands the output for both owners. If the feature is not reachable in production today,
    that belongs somewhere an operator would look — the alert service's own documentation, or a refusal
    with a clear message — rather than only in a comment inside a loop body.
43. Should the SQLite and Aerospike stores agree on the reassignment hold? Section 64 shows one honours
    `utxostore_reassignedUtxoSpendableAfterBlocks` and the other passes the constant, so the knob works
    on the development store and does nothing on the production one. Agreeing either way is small;
    the asymmetry is the kind of thing that quietly invalidates a tested tuning change.
44. Should the Docker examples say that the alert service is not deployed? Section 66 shows the gap is
    silent and the compose files contain an unrelated `alertmanager` with a name that answers the
    question wrongly. If the brake is deliberately off in that topology, one comment in the style of
    the block persister's "uses lots of disk space" settles it; if not, the service is missing.

45. Should the transaction store point somewhere, or should `settings.conf` say why it points
    nowhere? Section 67 shows one uncommented line selecting the discorder, no context variants while
    its two neighbouring stores have four and eight, and four test rigs still mounting a directory
    that receives nothing. Either a directory is intended — in which case the operator guide should
    say what breaks without it — or the line should carry the reason the node is not expected to
    remember transactions.

46. Should the hash-prefix table be one constant rather than two copies and a promise? Section 68
    shows the Pruner repeating the constructors' per-store defaults behind a comment reading "must
    match `daemon_stores.go`", no test comparing them, the copy being the path taken for every blob
    store in the shipped configuration, and the code's own warning for the resulting failure blaming
    the volume mount. A shared constant, or a test that walks both tables, costs one file.

47. Should the deployment examples state the open-file limit the file store assumes? Section 69 shows
    a budget of 768 reads and 256 writes documented against `ulimit -n`, both knobs commented out,
    the clamp that saves the node from itself logging a single INFO line, and the only container in
    the catalogue that raises its own limit being Aerospike. One `ulimits` block per blob-touching
    service, or one line in the installation guide, turns an invisible throughput reduction into a
    number an operator chooses.

48. Should the chunk size that forms part of a coin's address be recorded where it is written?
    Section 70 shows `utxostore_utxoBatchSize` used as a modulus at `stores/utxo/aerospike/aerospike.go:920`
    and `get.go:356`, three shipped values in `settings.conf:1731-1733`, a constructor comment saying
    the value must not change after a node has run, and no record anywhere of which value a data
    directory was written with. The blob store beside it writes a magic number in every file for
    exactly this reason. A four-byte marker in the data directory, checked at open, closes the gap.

49. Should the ledger's two deletion deadlines appear in `settings.conf`? Section 71 shows
    `utxostore_unminedTxRetention` and `utxostore_parentPreservationBlocks` absent from the file
    entirely, computed at load time at `settings/settings.go:505-506` from a halved
    `global_blockHeightRetention` and a constant of 144, so an operator shortening retention to buy
    disk silently shortens a deletion deadline they never see named.

50. Should the `aerospike` build tag be removed, or restored? Section 72 shows four directives
    commented out with a second pair of slashes, one live directive left on a test, four Makefile
    targets still passing `-tags aerospike`, and `AGENTS.md` still describing the tag as selecting
    Aerospike work. Either the tag is documentation that should go, or the gating is a design
    decision that was dropped without saying so.

51. Should the cache's bucket count be a setting, or be carried on the wire? Section 73 shows
    `BucketsCount` chosen by build tag at `stores/txmetacache/improved_cache_const_large.go:10`,
    `improved_cache_const_small.go:10` and `improved_cache_const_testing.go:10`, used by the receiver to
    pick a bucket (`stores/txmetacache/improved_cache.go:386`) and by the v2 producer to pick a Kafka
    partition (`services/validator/Validator.go:2009`, `:2032-2034`), with no check that the two binaries
    were compiled the same way. One four-byte field in the v2 header, or one start-up log line naming
    the constant on both sides, removes a class of silent misalignment.

52. Should `validator_txmeta_numPartitions` be checked against the topic's partition count at start-up?
    Section 74 shows the default 32 (`settings/settings.go:488`) against `KAFKA_PARTITIONS_HIGH = 8`
    (`settings.conf:38`), a producer mode with no fallback (`util/kafka/kafka_producer_async.go:112-116`),
    and no reconciliation anywhere. A start-up comparison that logs or refuses costs one metadata query
    and turns a v2 flag-day into a message.

53. Should the cache's occupancy gauge be restored? Section 75 shows `tx_meta_cache_size` commented out
    at `stores/txmetacache/metrics.go:199-207` while the same file's documentation block still advertises
    it at `:180`, and points out that `map_size` cannot substitute because the shards are pre-divided
    from the budget at construction (`stores/txmetacache/improved_cache.go:311`).
54. Should the notification contract be moved behind a native type? Section 77 shows the event bus is
    the one place the no-protobuf rule costs something:
    `Subscribe(ctx, source) (chan *blockchain_api.Notification, error)`
    (`services/blockchain/Interface.go:558`) hands every subscriber in the node a generated wire type,
    and `blockchain.FSMStateType` (`services/blockchain/Interface.go:72`) is already a partial wrapper
    around it. A `Notification` struct in the service's own package, converted once inside the client,
    would restore the rule where it matters most — at the cost of touching every consumer at once.
55. Should six gRPC doors be allowed to stay unauthenticated? Section 78 finds Validator, Block
    Assembly, Block Validation, Subtree Validation, Pruner and Propagation starting their servers with
    nothing that checks a caller (`services/validator/Server.go:483` and the four peers listed there,
    `services/propagation/Server.go:355-361`), while `security_level_grpc = 0` (`settings.conf:1310`)
    means nothing is encrypted either. If the intended posture is "the private network is the
    perimeter", that is worth one line in the settings file, because the current comment describes only
    the Blockchain's policy (`settings.conf:800-808`) and an operator may read it as the whole picture.
56. Should the development cluster file carry the secret it requires? Section 79 finds
    `grpc_admin_api_key` named as REQUIRED for Blockchain startup
    (`deploy/kubernetes/teranode/teranode-cr.yaml:82-83`) with no `kind: Secret` under `deploy/` and no
    value in `settings.conf`. The requirement is right for a secret; what is missing is either a
    commented placeholder manifest or a sentence in the file saying the cluster will not come up until
    it exists.
57. Should the in-repo Kubernetes resolver be wired or retired? Section 80 shows it registered and then
    replaced in the same call under the scheme name both builders claim
    (`util/grpc_helper.go:533-534`, with gRPC's registry a map assignment keyed by scheme), and the one
    setting value that would keep it live — `k8s` — selected by nothing in `deploy/` or `settings.conf`.
    Either the upstream resolver is sufficient and the package can go, or the in-repo one is preferred
    and the duplicate registration should be removed; both readings leave it as dead weight now.
58. Should the fast lane be documented as ingest-only? Section 82 finds the IPv6 multicast listener has
    no sender anywhere in the tree — `ReadMsgUDP` at `services/propagation/Server.go:499` is the only
    socket operation besides open and close — and the service is named for the thing this lane does not
    send. Switching it on would also need an explicit interface, the default being the macOS name `en0`
    (`services/propagation/Server.go:439-441`). A sentence saying it receives and never transmits would
    stop an operator expecting it to halve their egress.
59. Should the ledger store's multi-instance design be used, or said to be unused? Section 83 shows the
    store carries a durable block-ID reservation table, a fixed advisory lock for schema creation, and a
    catalog-probe fast path added after a readers-starvation cascade
    (`stores/blockchain/sql/sql.go:477-500`) — all of it reachable only with two Blockchain processes
    against one database, which no deployment here runs. Either the topology is coming, in which case the
    service section's "one instance is arithmetic" needs a footnote saying the store has already been
    widened for it, or it is not, in which case three mechanisms are being maintained on behalf of a case
    that will not arrive.
60. Should the chain-check setting's default be revisited, or the interaction documented? Section 84 has
    the arithmetic: `blockchain_use_in_memory_chain_check = false` (`settings.conf:466`) sends every
    membership question to SQL in batches of a thousand
    (`stores/blockchain/sql/CheckBlockIsInCurrentChain.go:13-17`), on the file engine whose pool is fixed
    at five connections (`util/sql.go:290`). A short measurement — chain-check latency with the flag on and
    off, one node, mid-sync — would either justify the default or retire it. Absent that, one sentence in
    the setting's description naming the five-connection ceiling would stop an operator tuning the wrong
    number.

## Verifying these notes

The checks that produced the findings above. This file was written across several sittings and
the repository moved underneath it twice (`git log -1` in this checkout currently reports
`bdaa46cdf`); every path, line number and command below was re-run against that revision after
the last edit, and the earlier notes in this file corrected where the re-run disagreed.

```bash
# Where ban reasons are defined, and what actually consumes them
grep -rn "ReasonInvalidSubtree" --include=*.go .
grep -rn "AddBanScore(" --include=*.go services/ | grep -v _test | grep -v "\.pb\.go"

# Reputation model and ban-score lifetime
grep -n "calculateAndUpdateReputation" services/blockchain/peer_registry.go
grep -n "bans must outlive peer disconnects" services/blockchain/peer_registry.go

# Shipped replica counts
grep -n "replicas" deploy/kubernetes/teranode/teranode-cr-mainnet.yaml

# The FSM guard on peer-announced subtrees
grep -n "FSMStateRUNNING" services/subtreevalidation/subtreeHandler.go

# Gossip announcement size caps
grep -n "maxBlockMessageSize\|maxSubtreeMessageSize\|maxGossipMessageSize" services/p2p/Server.go

# Sync candidate viability floor, and the three-progress reasoning
grep -n "func isViableSyncCandidate" -A 3 services/p2p/sync_coordinator.go
grep -n "resetProbeBudgetIfLocalChainWorkAdvanced" -A 26 services/p2p/sync_coordinator.go

# What the two example cluster configurations actually enable
awk '/^  [a-zA-Z_]+:$/{svc=$1; getline; if ($0 ~ /enabled:/) print svc, $0}' \
    deploy/kubernetes/teranode/teranode-cr.yaml
awk '/^  [a-zA-Z_]+:$/{svc=$1; getline; if ($0 ~ /enabled:/) print svc, $0}' \
    deploy/kubernetes/teranode/teranode-cr-mainnet.yaml

# The inert setting and its comment
grep -n -B 3 "^p2p_share_private_addresses" settings.conf

# A default-on Legacy behaviour absent from the committed config, and where it lives instead
grep -c feeler settings.conf || echo "absent from settings.conf (default comes from code)"
grep -n "legacy_maxFeelerPeers" settings/settings.go services/legacy/peer_server.go
grep -n "MaxFeelerPeers" settings/legacy_settings.go | cut -c1-80

# The peer-slot arithmetic the feeler reservation is taken out of
grep -n "func automaticOutboundTarget" -A 6 services/legacy/peer_server.go

# What, if anything, feeds Propagation
grep -rln "services/propagation" --include=*.go cmd/ services/ daemon/ | grep -v _test
grep -n "KafkaTxValidationTopicMessage" services/propagation/Server.go

# The asset server's transaction proxy, and the flag that turns it on
grep -n "PropagationProxyEnabled" -A 8 services/asset/httpimpl/http.go
grep -n "^asset_propagation_proxy" settings.conf

# The proxy is a reverse proxy: backend headers pass through, failures are counted
grep -n "ReverseProxy\|StatusBadGateway\|req.URL.Path = backendPath" services/asset/httpimpl/propagation_proxy.go

# Whether the examples deploy the service the proxy points at
awk '/^  [a-zA-Z_]+:$/{svc=$1; getline; if ($0 ~ /enabled:/) print "  ", svc, $0}' \
    deploy/kubernetes/teranode/teranode-cr.yaml | grep propagation

# The P2P integration claim, and the server's actual dependencies
grep -n "P2P service" services/propagation/Client.go
grep -n "type PropagationServer struct" -A 22 services/propagation/Server.go | grep -v "^\s*[0-9]*\s*//"

# The broadcast lane's buffer, its comment, and the absence of a counter
grep -n "maxDatagramSize" services/propagation/Server.go
grep -n "Name:" services/propagation/metrics.go

# Cheapest-test-first: size before parse, allowlist before parse
grep -n "Check transaction size BEFORE parsing" -A 4 services/propagation/Server.go
grep -n "unauthorized source" -B 3 services/propagation/Server.go

# The one cached hash, and the debug argument that pays for itself
grep -n "Cache the hash on the transaction" -A 10 services/propagation/Server.go

# Acknowledgement is not a verdict
grep -n "This branch decides whether the submitter is ever told the verdict" -A 14 services/propagation/Server.go

# The verdict table, and the retryable-overload reasoning
grep -n "func httpStatusForTxError" -A 34 services/propagation/Server.go

# The header that carries a verdict across an internal hop, and why
grep -n "plain-text HTTP body cannot be classified" -A 10 errors/http.go
grep -n "publicCauseCodes is the allowlist" -A 16 errors/errors.go

# Full queue versus oversized message
grep -n "Only an oversized gRPC message is fixable" -A 12 services/propagation/Client.go

# Ordered per-submission failure slots, and the txid paid for only on failure
grep -n "Errors are reported in stream submission order" -A 18 services/propagation/Server.go

# Two concurrency ceilings, and the always-nil worker return
grep -n "Non-blocking admission control" -A 10 services/propagation/Server.go
grep -n "Acquire server-wide semaphore" -A 8 services/propagation/Server.go
grep -n "response.Errors\[idx\] = errors.WrapPublic" -A 4 services/propagation/Server.go

# Bans enforced as transport middleware
grep -n "banlist.CreateGRPCUnaryInterceptor(ps.banList)" services/propagation/Server.go
grep -n "ps.httpServer.Use(banlist.CreateEchoMiddleware(ps.banList))" services/propagation/Server.go

# Where unmined-but-accepted transactions live, in the service's own words
grep -n "no separate mempool service" -A 3 services/propagation/Client.go

# Legacy consumes the txmeta conveyor belt and orders announcements from it
grep -n "stores/txmetacache" services/legacy/netsync/manager.go
grep -n "orderAnnounceBatch" -A 8 services/legacy/netsync/manager.go | head -20

# The side table that carries parents, its bound, and its graceful failure
grep -n "announceParents holds the parent tx hashes" -A 5 services/legacy/netsync/manager.go
grep -n "txmap.NewSyncedMap\[chainhash.Hash, \[\]chainhash.Hash\]" services/legacy/netsync/manager.go

# Put-after-Close panic guard, and the unjoined listener that motivated it
grep -n "PANICS on Put-after-Close" -A 6 services/legacy/netsync/manager.go

# Parents-first ordering, and the orphan-pool reason for it
grep -n "func ParentsFirst" -B 18 services/legacy/netsync/txorder.go

# Rebroadcast cadence, cap, and the eviction preference
grep -n "maxRebroadcastInventory\|maxRebroadcastTips\|rebroadcastTipDelay" -A 6 services/legacy/rebroadcast.go | head -40

# Checkpoint fast paths: the provenance requirement and the default-off shortcut
grep -n "PROOF that this block belongs" -A 6 services/legacy/netsync/handle_block.go
grep -n "skip script validation" -A 34 services/legacy/netsync/handle_block.go | head -44
grep -n "outpoint-only fast path" -A 8 services/legacy/netsync/handle_block.go

# The degenerate bundle-size case and its fix
grep -n "partitionLegacyBlock" -B 16 services/legacy/netsync/subtree_partition.go | head -24

# Freeze: single writer, many readers, loud write after the fact
grep -n "txMap.Freeze()" -B 8 services/legacy/netsync/handle_block.go

# Namespaced registry identity, and the inbound ephemeral-port asymmetry
grep -n "legacyPeerIDPrefix\|legacyRegistryID" -A 8 services/legacy/peer_registry_sync.go | head -30

# The mirror is visibility-only, and distinguishes "unknown" from "empty"
grep -n "read-only visibility path" -A 4 services/legacy/peer_registry_sync.go
grep -n "nil to mean" -A 3 services/legacy/peer_registry_sync.go

# The validator's trust boundary: route guard, client guard, tip-derived bound
grep -n "unauthenticated (no auth middleware" -A 8 services/validator/http_options_guard.go
grep -n "nonDefaultValidationOptions(req)" -A 4 services/validator/Server.go
grep -n "still parses the old query string" -B 8 services/validator/Client.go
grep -n "attacker simply asserts a low" -B 4 services/validator/Validator.go

# The guard is a hand-maintained list; nothing in the build enforces it
grep -n "MAINTENANCE CONTRACT" -A 8 services/validator/http_options_guard.go

# The two silent failure modes that arrive after "accepted"
grep -n "these drops are silent" -B 4 services/validator/metrics.go
grep -n "nothing schedules" -B 6 services/validator/metrics.go

# Backpressure shipped off, and doubly inert
grep -n "kafkaBackpressureEnabled\|also set blockassembly_maxQueueItems" settings.conf
grep -n "^blockassembly_maxQueueItems" settings.conf
grep -n "queueMaxItems <= 0" -B 12 services/validator/kafka_backpressure.go | head -22

# It distrusts the signal it measures, and knows which settings copy it may use
grep -n "must not be able to steer" -B 6 services/validator/kafka_backpressure.go
grep -n "never feeds a control decision" -B 6 services/validator/Validator.go

# The order of a reversal, and why the inconclusive read fails closed
grep -n "Ordering is load-bearing" -A 12 services/validator/Validator.go
grep -n "the branch whose worst case is node-local" -B 14 services/validator/Validator.go

# The hold-back is off, its key is unprefixed, and it doubled as a loop bound
grep -n "^double_spend_window_millis" settings.conf
grep -n "double_spend_window_mi" settings/blockassembly_settings.go
grep -n "35+ minutes at 558 GB RSS" -B 20 services/blockassembly/subtreeprocessor/SubtreeProcessor.go
grep -n "Take a single clock sample" -A 8 services/blockassembly/subtreeprocessor/SubtreeProcessor.go

# Depth: usable as a fraction, untrustworthy on its own
grep -n "Depth alone is not a usable control input" -A 8 services/blockassembly/Interface.go
grep -n "depth is the untrustworthy half" -B 6 services/blockassembly/dequeue_stall.go
grep -n "is what the gauge exists to avoid" -B 9 services/blockassembly/dequeue_stall.go

# The candidate is small on purpose, and its clock is squeezed by two rules
grep -n "totalFees + blockSubsidy" -B 8 services/blockassembly/BlockAssembler.go
grep -n "floored at the parent chain.s median-time-past" -B 2 -A 4 services/blockassembly/BlockAssembler.go
grep -n "no timestamp satisfies both consensus rules" -B 4 services/blockassembly/candidate_time.go | head -20

# The pending set on disk: exact index in RAM, payloads append-only, readers pin a generation
grep -n "exact in-RAM index" -A 16 services/blockassembly/subtreeprocessor/disk_tx_map.go

# Timestamp resolution differs by store, and ties can drop a family of transactions
grep -n "coarse resolution" -A 12 services/blockassembly/BlockAssembler.go
grep -rln "stableSortUnminedByCreatedAt" services/blockassembly/*_test.go

# The library predicate that was wider than the consensus rule
grep -n "IsConsensusCoinbase rather than" -B 4 services/blockvalidation/quick_validate.go
grep -rn "\.IsCoinbase()" --include=*.go services/ | grep -v _test | head -8

# Two consensus-affecting defaults with no entry in the shipped config
grep -n "OutpointOnlyBelowCheckpoint\|LegacyBelowCheckpointFailClosed" settings/settings.go
grep -n "outpoint" settings.conf

# A self-consistent low-difficulty chain, rejected before full blocks are fetched
grep -n "recomputes the DAA-required nBits" -B 4 services/blockvalidation/catchup.go

# The fork-depth bound, with its arithmetic attached
grep -n "Reject only when fork depth strictly exceeds" -A 8 services/blockvalidation/catchup.go

# Cheap announcement, expensive churn: ordering of the mined-set reset
grep -n "churning honest tx-mined state for no reorg" -B 4 services/blockvalidation/catchup.go

# Blame under concurrency: dedup so failures cannot exceed attempts
grep -n "keeps" -A 6 services/blockvalidation/catchup.go | grep -A 6 "CatchupFailures from exceeding"
grep -n "the peer is not charged" -B 4 services/blockvalidation/catchup.go

# Cleanup at run scope, because attempt scope cannot see committed dependencies
grep -n "cannot see that a hash it wrote fresh" -B 6 services/blockvalidation/catchup.go

# Local bytes that do not match their own key, and why the fallback is unsafe
grep -n "bytes on disk under a key they do not belong to" -B 8 services/blockvalidation/quick_validate_bind.go
grep -n "cannot detect the blob this route just rejected" -B 6 services/blockvalidation/quick_validate_bind.go

# Priority, starvation and the block queue
grep -n "func (item \*PrioritizedBlock) getEffectivePriority" -A 18 services/blockvalidation/block_priority.go
grep -n "additionalSources = append" -B 8 services/blockvalidation/block_priority.go

# The two example cluster files are different kinds of thing, not two sizes
grep -c "imagePullPolicy: Never" deploy/kubernetes/teranode/teranode-cr.yaml
grep -n "imagePullPolicy: Never" deploy/kubernetes/teranode/teranode-cr-mainnet.yaml
grep -o "host: [a-z0-9.-]*" deploy/kubernetes/teranode/teranode-cr.yaml | sort -u
grep -n "storageClass\|storage: 4800Gi" deploy/kubernetes/teranode/teranode-cr-mainnet.yaml

# Exact service and replica state of both Kubernetes examples
awk '/^spec:/{inspec=1;next} inspec && /^  [a-zA-Z]+:/{svc=$1; gsub(/:/,"",svc); next}
     inspec && /^    enabled:/{printf "%-16s enabled=%s\n", svc, $2}
     inspec && /^        replicas:/{printf "%-16s replicas=%s\n", svc, $2}' \
  deploy/kubernetes/teranode/teranode-cr.yaml
# ...and that both examples configure the same fifteen services, eight on versus nine on
svcs() { awk '/^spec:/{s=1;next} s && /^  [a-zA-Z]+:/{v=$1; gsub(/:/,"",v); next}
            s && /^    enabled:/{print v}' "$1" | sort; }
diff <(svcs deploy/kubernetes/teranode/teranode-cr.yaml) \
     <(svcs deploy/kubernetes/teranode/teranode-cr-mainnet.yaml) && echo "identical service sets"
svcs deploy/kubernetes/teranode/teranode-cr.yaml | wc -l
awk '/^spec:/{s=1;next} s && /^  [a-zA-Z]+:/{v=$1; next} s && /^    enabled: true/{c++}
     END{print FILENAME": services switched on = "c}' deploy/kubernetes/teranode/teranode-cr.yaml
awk '/^spec:/{s=1;next} s && /^  [a-zA-Z]+:/{v=$1; next} s && /^    enabled: true/{c++}
     END{print FILENAME": services switched on = "c}' deploy/kubernetes/teranode/teranode-cr-mainnet.yaml

# What the Docker stack defines, and what an operator has to switch on
grep -n 'command: \[ "-' deploy/docker/base/docker-teranode.yml
grep -n "service: \(peer\|legacy\|propagation\|blockpersister\)" deploy/docker/mainnet/docker-compose.yml
grep -n "peer service\|lots of disk space" deploy/docker/mainnet/docker-compose.yml
grep -n "SETTINGS_CONTEXT" deploy/docker/base/docker-teranode.yml deploy/kubernetes/teranode/teranode-configmap.yaml

# The supported install's own service list, defaults and warnings
grep -n "Most operators should not start here" deploy/docker/README.md
grep -n "COMPOSE_PROFILES\|The default is\|probes 100+\|288 blocks\|not needed for standard" \
  docs/howto/miners/docker/minersHowToInstallation.md

# listen_mode: one service consumes it, and the other one says so in the source
grep -rn "P2P.ListenMode" --include=*.go services/ | grep -v _test | awk -F: '{print $1}' | sort | uniq -c
grep -n "intentionally NOT gated by" services/legacy/peer_server.go settings.conf
grep -n "listen_mode" services/legacy/peer_server.go
grep -n "ListenModeFull \|= \"full\"\|ListenModeListenOnly\|ListenModeSilent" settings/interface.go
grep -rn "listen_only\|silent" docs/howto/miners/docker/minersHowToInstallation.md docs/p2p-silent-mode.md | head -6

# Where the first peers come from, and which mode each context gets
grep -n "p2p_bootstrap_peers\|p2p_dht_mode" settings.conf

# utxoPersister is in the Kubernetes examples and nowhere else in deploy/
grep -rli "utxopersister" deploy/ || echo "no deploy/ file mentions utxoPersister"
grep -n "utxoPersister" -A 1 deploy/kubernetes/teranode/teranode-cr-mainnet.yaml

# The live coin set does not depend on the block persister
grep -n "global_blockHeightRetention" settings.conf

# A fresh node boots parked, and the parked exit is documented as absent
grep -n "blockchain_initializeNodeInState" settings.conf
grep -n "There is no transition from\|while parked" docs/howto/miners/docker/minersHowToSyncTheNode.md
sed -n '/Name: blockchain_api.FSMEventType_STOP/,/}/p' services/blockchain/fsm.go
grep -n "asks Teranode to enter .IDLE. state" docs/howto/miners/docker/minersHowToStopStartDockerTeranode.md

# Peer registry persistence: implemented, default off, absent from the shipped config
grep -rn "blockchain_peerRegistryStore" settings.conf deploy/ || \
  echo "no shipped configuration sets blockchain_peerRegistryStore"
grep -n "PeerRegistryStore\|PeerRegistrySaveInterval" settings/settings.go
grep -n "peerRegistryStore != nil" -A 3 services/blockchain/Server.go
grep -n "default for single-node deployments" settings/blockchain_settings.go

# One registry, two namespaces, three constants behind one floor
grep -n "p2p_peer_registry_\|blockchain_peerRegistry" settings/settings.go
grep -rn "minPeerRegistryTTL = \|registryReassertTTL = \|nodeStatusPublishInterval = " \
  services/blockchain/Server.go services/p2p/peer_registry_batcher.go services/p2p/Server.go

# The fast path is off with its watchdog armed behind it
grep -n "blockchain_use_in_memory_chain_check\|blockchain_chain_check_shadow_compare" settings.conf
grep -n "measures nothing" -B 2 stores/blockchain/sql/CheckBlockIsInCurrentChain.go
grep -n "shadowRejectSampleRate = \|Sampled rather than compared" stores/blockchain/sql/CheckBlockIsInCurrentChain.go

# A stale "no" escalates, and the escalation lives in the caller
grep -rn "escalates.*PERMANENT" --include=*.go stores/blockchain/sql/ | grep -v _test
grep -rn "CheckBlockIsInCurrentChain(" --include=*.go services/ | grep -v _test | grep -v mock

# The short-chain fallback guards one difficulty lookup, not both
grep -n "bc.depth < 3\|should this be an ErrNotFound\|len(blocks) != 3" stores/blockchain/sql/GetSuitableBlock.go
grep -n "chain too short to get ancestor" -B 3 services/blockchain/Difficulty.go

# Interface width: the widest contract in the deployment
awk '/^type ClientI interface/,/^}/' services/blockchain/Interface.go | grep -cE "^[[:space:]]+[A-Z][A-Za-z]*\("
grep -c "rpc " services/blockchain/blockchain_api/blockchain_api.proto

# The two-day fuse: where it is set, and where filing cancels it
grep -n "dah := " services/blockassembly/Server.go
grep -n "global_blockHeightRetention" settings.conf
grep -n "SetDAH(ctx" services/blockpersister/streaming_process_subtree.go
sed -n '/if newDAH == 0 {/,/}/p' stores/blob/file/file.go

# The persister's comments name Block Assembly; the Pruner is the consumer
grep -n "BlockAssembler's cleanup service uses\|See BlockAssembler.startChannelListeners" services/blockpersister/Server.go
grep -rn "BlockPersisterHeight\|NotificationType_BlockPersisted" --include=*.go services/ | grep -v _test | grep -v "blockpersister/Server.go"

# Pruner deployed, persister not, trigger left at the mode that waits for it
grep -rn "pruner_block_trigger" settings.conf deploy/ || echo "pruner_block_trigger is set nowhere in this repository"
grep -n "PrunerBlockTriggerOnBlockPersisted," settings/settings.go
grep -n "service: pruner\|#  blockpersister:" deploy/docker/mainnet/docker-compose.yml
grep -n "Skip if using OnBlockPersisted trigger mode" -A 3 services/pruner/server.go
grep -n "there may be no subsequent" -B 2 -A 1 services/pruner/server.go

# The retry pause and the branches that do not get it
grep -n "PersistSleep" services/blockpersister/Server.go
grep -n "Failed to persist block\|Failed to mark block" -A 2 services/blockpersister/Server.go

# Keys whose names point at another service, and the 128KB fallback
grep -n "utxoPersister_buffer_size" settings.conf settings/block_settings.go
grep -rn "Block.UTXOPersisterBufferSize" --include=*.go services/ | grep -v _test
grep -rn "SkipUTXODelete" --include=*.go services/ | grep -v _test
grep -n "concurrency := subtreepkg.Max" -B 1 services/blockpersister/streaming_process_subtree.go

# The forgery gate: two checks on write, one on read-back
grep -n "TxIsSerializable\|TxIDChainHash().IsEqual" services/blockpersister/streaming_process_subtree.go
grep -n "transaction hash mismatch" -B 3 services/blockpersister/streaming_process_subtree.go

# The coin archive's input is written by Block Persister, through the archive's own package
grep -n "FileTypeUtxoAdditions\|FileTypeUtxoDeletions" pkg/fileformat/header.go
grep -rn "FileTypeUtxoAdditions" --include=*.go services/blockpersister/ | grep -v _test || echo "blockpersister does not name the file type itself"
grep -n "ProcessUTXOFiles" -A 4 services/blockpersister/persist_block.go
grep -n "\[BlockPersister\] Persisting utxo additions" services/utxopersister/UTXOSet.go
grep -n "func NewUTXOSet" -A 12 services/utxopersister/UTXOSet.go | grep -n "FileTypeUtxo"

# Three situations, one log line
grep -n "No new block to process" services/utxopersister/Server.go
grep -n "Waiting for %d confirmations" services/utxopersister/Server.go
grep -n "Unlike deletions, a missing additions file" -A 8 services/utxopersister/consolidator.go
grep -n "ErrBlobNotFound is tolerated" -A 10 services/utxopersister/consolidator.go
grep -n "ErrBlobNotFound\s*=\|ErrNotFound\s*=" errors/Error_types.go
grep -n "cur.code == targetError.code" errors/errors.go

# The lag is code; the retention is a setting, and they apply to different files
grep -n "const confirmations" services/utxopersister/Server.go
grep -n "maxHeight = bestBlockMeta.Height - confirmations" services/utxopersister/Server.go
grep -n "global_blockHeightRetention" settings.conf
grep -n "GlobalBlockHeightRetention" services/blockassembly/Server.go | head -4
grep -n "FileTypeSubtreeData, 0)\|fileformat.FileTypeSubtree, 0)" services/blockpersister/streaming_process_subtree.go

# The ledgers carry no expiry: caller passes none, store fallback is never configured
grep -n "filestorer.NewFileStorer(ctx, logger, tSettings, store, blockHash\[:\], fileformat.FileTypeUtxo" services/utxopersister/UTXOSet.go
grep -n "store.SetFromReader(ctx, key, fileType, reader, fileOptions...)" services/utxopersister/filestorer/FileStorer.go
grep -n "if dah == 0 && merged.BlockHeightRetention > 0" stores/blob/file/file.go
grep -rn "WithDefaultBlockHeightRetention" --include=*.go . | grep -v _test
grep -n "exhaust the read semaphore" -A 4 services/utxopersister/consolidator.go

# Per-block naming versus per-range behaviour
sed -n '5p' docs/references/services/utxopersister_reference.md
grep -n "Rolling up data from block height" -A 2 services/utxopersister/Server.go
grep -n "if _, exists := c.additions\[\*outpoint\]; exists" -A 5 services/utxopersister/consolidator.go
grep -n "bookkeeping here was making the harness crash" -B 6 services/utxopersister/Server.go | head -8

# The service switches, across all four artifacts
python3 - <<'EOF'
import re
for f in ['deploy/kubernetes/teranode/teranode-cr.yaml','deploy/kubernetes/teranode/teranode-cr-mainnet.yaml']:
    print(f)
    cur=None
    for l in open(f):
        m=re.match(r'^  ([a-zA-Z]+):', l)
        if m: cur=m.group(1)
        e=re.match(r'^\s+(#?)\s*enabled:\s*(true|false)', l)
        if e and cur: print('   ', cur, e.group(2), '<< commented' if e.group(1) else ''); cur=None
EOF
for f in deploy/docker/base/docker-teranode.yml deploy/docker/mainnet/docker-compose.yml deploy/docker/testnet/docker-compose.yml; do
  printf "%s: " "$f"; grep -oE "^  (blockpersister|peer|pruner|utxopersister|validator|propagation|alert):" "$f" | tr -d ' :' | paste -sd, -
done
grep -n "Read the \[Teranode Network\]" -A 1 deploy/docker/mainnet/docker-compose.yml
grep -n "uses lots of disk space" -A 1 deploy/docker/mainnet/docker-compose.yml
grep -c "imagePullPolicy: Never" deploy/kubernetes/teranode/teranode-cr.yaml

# Tiering exists; the gate into two of the three tiers ships empty
grep -n "asset_peerAuthAllowlist" settings/settings.go
grep -n "empty allowlist means no peer is eligible" -A 4 services/asset/httpimpl/peer_auth_middleware.go
grep -n "typically tier-unverified" -B 3 -A 2 services/asset/httpimpl/http.go
grep -n "peer_tier" services/asset/httpimpl/peer_auth_middleware.go
grep -n "IPv6 attacker rotating source addresses" -B 4 services/asset/httpimpl/rate_limiter.go

# Heavy surface: two independent buckets, and the doubling in the code's own arithmetic
grep -n "independent per-IP buckets" -A 5 services/asset/httpimpl/http.go
grep -n "2\*tSettings.Asset.HTTPHeavyRateLimit" services/asset/httpimpl/http.go
grep -n "catchupHeavyMW()" services/asset/httpimpl/http.go | head -6
grep -n "asset_httpRateLimit\|asset_httpHeavyRateLimit\|asset_httpPeerRateMultiplier\|asset_httpMinerRateLimit" settings/settings.go
grep -nE "^[[:space:]]*#?[[:space:]]*asset_http" settings.conf

# Ten read allowances, five unbounded, and a comment that disagrees with its default
grep -n "0 = unlimited" -A 14 services/asset/repository/repository.go
grep -n "asset_concurrency" settings/settings.go | cut -c1-118
grep -nE "^[[:space:]]*#?[[:space:]]*asset_concurrency" settings.conf

# The cache's three regions, and what each one's silence means
sed -n '18,35p' services/asset/httpimpl/mainchain_cache.go
grep -n "absent means NOT main chain\|does NOT hold\|degraded throughput\|TOCTOU\|coalesces into one rebuild" services/asset/httpimpl/mainchain_cache.go

# The advertised fetch address, from setting to gossip to operator documentation
grep -n "Asset.HTTPPublicAddress" services/p2p/Server.go
grep -n "will be scored as a protocol violation" -B 1 services/p2p/Server.go
grep -n "committed default" -A 3 services/p2p/Server.go
grep -rn "asset_httpPublicAddress" settings.conf deploy/docker/base/settings_local.conf deploy/docker/base/settings_local.conf.template docs/howto/miners/docker/minersSecurityBestPractices.md | cut -c1-110
grep -nE "className|httpIngress|replicas" deploy/kubernetes/teranode/teranode-cr-mainnet.yaml | head -12
grep -n "proxy_cache_path\|proxy_cache_min_uses\|proxy_cache_lock" deploy/docker/base/asset-cache-nginx.conf
grep -n "does NOT authenticate" -A 2 services/asset/centrifuge_impl/centrifuge.go
grep -n "func checkOrigin" -A 10 services/asset/centrifuge_impl/websocket.go

# Who owns the deletion queue, and what happens without it
sed -n '1104,1125p' stores/blob/file/file.go
grep -n "satisfied by blockchain\|blob deletion scheduler (blockchain client)" stores/blob/options/pruner_client.go daemon/daemon_stores.go
grep -n "func (s \*Server) ProcessBlock\|phase 1: skipped\|if s.utxoStore != nil" services/pruner/worker.go

# The two waits and their two volumes
sed -n '60,72p' services/pruner/worker.go
sed -n '98,108p' services/pruner/worker.go
sed -n '165,180p' services/pruner/worker.go
grep -n "FSMStateType_RUNNING\|FSMStateType_IDLE\|SetFSMState" services/blockchain/fsm.go | head -14

# Pruner waits: two halves, two volumes, one metric
grep -n "block_assembly_timeout\|prunerSkipped.WithLabelValues" services/pruner/worker.go
grep -n "waiting for mined_set=true\|mined_set wait timeout\|continue" services/pruner/worker.go | head -8
grep -n "only in OnBlockMined trigger mode" -A 5 services/pruner/blob_deletion_worker.go
grep -n "retry.WithRetryCount" services/pruner/worker.go

# The pool threshold scales concurrency, per process (section 56)

# RPC as an ingest door: store, fee ceiling, validate (section 57)
sed -n '861,872p' services/rpc/handlers.go
sed -n '874,884p' services/rpc/handlers.go
grep -n "PreviousOutputsDecorate(ctx, tx)\|validatorClient.Validate(ctx, tx, 0)" services/rpc/handlers.go
grep -n 'key:"maxrawtxfee"' settings/policy_settings.go | cut -c1-150
grep -c "maxrawtxfee" settings.conf || echo "maxrawtxfee absent from settings.conf"
grep -n "TestMaxRawTxFee_DefaultIsNonZero" -A 3 settings/settings_test.go

# Concurrency: requests not clients, no keep-alive, no disconnect watcher (section 58)
grep -n "numClients)+1" -A 6 services/rpc/Server.go
grep -n 'Connection", "close' -A 2 services/rpc/Server.go
echo "handlers that ignore the disconnect channel: $(grep -c '_ <-chan struct{}' services/rpc/handlers.go) of $(grep -c '^func handle' services/rpc/handlers.go)"
grep -n "rpc_max_clients" settings.conf | cut -c1-60
grep -n "RPCMaxClients " settings/rpc_settings.go | cut -c1-120

# Limited tier versus unimplemented handlers (section 59)
grep -c "handleUnimplemented," services/rpc/Server.go
python3 -c "
import re
src=open('services/rpc/Server.go').read()
def seg(a):
    i=src.index(a)+len(a); d=1; j=i
    while d:
        d += (src[j]=='{') - (src[j]=='}'); j += 1
    return src[i:j-1]
h=dict(re.findall(r'\"([a-z0-9]+)\":\s*(\w+),', seg('rpcHandlersBeforeInit = map[string]commandHandler{')))
un={k for k,v in h.items() if v=='handleUnimplemented'}
lim=set(re.findall(r'\"([a-z0-9]+)\":', seg('rpcLimited = map[string]struct{}{')))
print('methods', len(h), 'unimplemented', len(un), 'limited', len(lim), 'limited and unimplemented', len(lim & un))
"

# Empty credentials fail closed; one description says otherwise (section 60)
sed -n '1482,1493p' services/rpc/Server.go
grep -n "checkAuth(r, true)" services/rpc/Server.go
grep -rn "Leave empty to disable authentication" settings/rpc_settings.go services/rpc/README.md | cut -c1-130
grep -n "the Asset admin routes reject every request" -B 4 settings.conf | cut -c1-130

# The five cached commands and their lifetimes (section 61)

# Alert: freeze matches nothing on snapshot-seeded nodes, and the report cannot say so (section 62)
sed -n '316,330p' services/alert/node.go
grep -c "response.NotProcessed = append" services/alert/node.go
grep -n "is not frozen" stores/utxo/sql/alert_system.go
grep -n "transaction %s:%d is not frozen" stores/utxo/sql/alert_system.go

# Confiscation rewrites the commitment, never the script (section 63)
grep -n "does not persist" -B 2 services/alert/node.go | cut -c1-140
sed -n '179,190p' stores/utxo/sql/alert_system.go
sed -n '242,252p' stores/utxo/aerospike/alert_system.go
grep -n "ReAssignedUtxoSpendableAfterBlocks = " stores/utxo/Interface.go

# The hold's setting: honoured by SQLite, ignored by Aerospike (section 64)
grep -n "key:\"utxostore_reassignedUtxoSpendableAfterBlocks\"" settings/utxostore_settings.go | cut -c1-140
sed -n '172,177p' stores/utxo/sql/alert_system.go
sed -n '244,252p' stores/utxo/aerospike/alert_system.go | grep -n "ReAssignedUtxoSpendableAfterBlocks"
grep -c "utxostore_reassignedUtxoSpendableAfterBlocks" settings.conf || echo "absent from settings.conf"

# Enforcement at spend, block assembly used only for readiness (section 65)
grep -n "UTXO is frozen\|frozen until block\|transaction is frozen" stores/utxo/aerospike/spend.go | cut -c1-120
grep -rn "blockassemblyClient\." services/alert/*.go | grep -v _test | cut -c1-120

# Absent from Docker, present in Kubernetes, with a same-named decoy (section 66)
grep -c "^  alert:" deploy/docker/base/docker-teranode.yml || echo "no alert service in the Docker catalogue"
grep -n "alertmanager:" deploy/docker/mainnet/docker-compose.yml | head -2
grep -n "alertSystem:" -A 6 deploy/kubernetes/teranode/teranode-cr-mainnet.yaml | head -8
grep -c "imagePullPolicy: Never" deploy/kubernetes/teranode/teranode-cr-mainnet.yaml
grep -n "key:\"alert_p2p_dht_mode\"\|key:\"ALERT_P2P_PORT\"" settings/alert_settings.go | cut -c1-110
grep -n "rpcCallCache.Get\|rpcCallCache.Set" services/rpc/handlers.go
grep -n "func newRPCCache" -A 7 services/rpc/handlers.go
grep -n "key:\"rpc_cache_enabled\"" settings/rpc_settings.go | cut -c1-110
grep -n "key:\"pruner_connectionPoolWarningThreshold\"" settings/pruner_settings.go | cut -c1-120
grep -n "recommendedMax\|numWorkers + 1" stores/utxo/aerospike/pruner/pruner_service.go | head -6

# Pruner present in Docker, absent from Kubernetes
grep -n "pruner" deploy/docker/mainnet/docker-compose.yml deploy/docker/testnet/docker-compose.yml
grep -c "pruner:" deploy/kubernetes/teranode/teranode-cr-mainnet.yaml deploy/kubernetes/teranode/teranode-cr.yaml || echo "absent from both cluster files"
grep -nE "^  [a-z]+:" deploy/kubernetes/teranode/teranode-cr-mainnet.yaml | head -20

# The store's default retention is a knob nothing turns (open question 35)
grep -n "WithDefaultBlockHeightRetention" -A 5 stores/blob/options/Options.go
grep -rn "WithDefaultBlockHeightRetention" --include=*.go . | grep -v _test | wc -l
grep -n "merged.BlockHeightRetention" stores/blob/file/file.go
grep -n "options.WithDeleteAt(0)" services/utxopersister/UTXOSet.go

# Where the transaction store actually points (section 67)
grep -n "^txstore" settings.conf
grep -n "func (n \*Null) Set\|func (n \*Null) Exists\|func (n \*Null) Get" stores/blob/null/null.go
grep -n "txStore.Set" services/propagation/Server.go services/rpc/handlers.go
grep -n "txstore" test/docker-compose-host.yml | head -4
grep -c "^blockstore" settings.conf
grep -c "^subtreestore" settings.conf
grep -n "SETTINGS_CONTEXT" test/docker-compose-host.yml | head -2

# The duplicated sharding table and its comment (section 68)
grep -n "hashPrefix := " daemon/daemon_stores.go
grep -n "must match daemon_stores.go" services/pruner/blob_deletion_worker.go
sed -n '356,376p' services/pruner/blob_deletion_worker.go
grep -rn "defaultHashPrefix" --include=*_test.go services/pruner/ | wc -l
grep -n "already missing from disk" services/pruner/blob_deletion_worker.go
grep -n "^blockpersister_store" settings.conf
sed -n '415,420p;583,584p;637,638p' daemon/daemon_stores.go

# The file-handle budget and the limit nobody sets (section 69)
grep -n "filestore_read_concurrency\|filestore_write_concurrency\|ulimit" settings.conf | head -8
grep -n 'default:"768"\|default:"256"' settings/block_settings.go | cut -c1-80
grep -n "issue 1431" -B 3 stores/blob/file/file.go | head -14
grep -n "ulimits" -A 3 deploy/docker/base/docker-services.yml
grep -rn "ulimit\|nofile" deploy/kubernetes/ | wc -l

# The chunk size that forms part of a coin's address (section 70)
grep -n "^utxostore_utxoBatchSize" settings.conf
grep -n "very dangerous to change this number" -A 1 stores/utxo/aerospike/aerospike.go
grep -n "return vout % uint32(s.utxoBatchSize)" stores/utxo/aerospike/aerospike.go
grep -n "spend.Vout % uint32(s.utxoBatchSize)" stores/utxo/aerospike/get.go
grep -n "numBatches := " stores/utxo/aerospike/create.go
grep -n "SETTINGS_CONTEXT" deploy/docker/base/docker-teranode.yml
grep -c "magicUtxoAdditions" pkg/fileformat/header.go

# The ledger's files sit outside DAH, on purpose (section 71)
grep -n "DAH explicitly disabled" -A 2 stores/utxo/aerospike/aerospike.go
grep -n "options.WithDeleteAt(0)" stores/utxo/aerospike/create.go
grep -c "utxostore_unminedTxRetention\|utxostore_parentPreservationBlocks" settings.conf || true
grep -n "UnminedTxRetention:\|ParentPreservationBlocks:" settings/settings.go
grep -n "blocksInADayOnAverage = " settings/settings.go
grep -n "^global_blockHeightRetention" settings.conf
grep -n "BackdatedUnminedSince" stores/utxo/aerospike/longest_chain.go

# The aerospike build tag is decoration (section 72)
grep -rn '^// //go:build aerospike' --include=*.go . | grep -v history
grep -rn '^//go:build aerospike' --include=*.go . | grep -v history
grep -c "tags aerospike" Makefile
grep -n "aerospike" AGENTS.md | head -3
go build ./stores/utxo/factory/ && echo "factory compiles with no build tags"

# The bucket count is a build tag that the wire protocol depends on (section 73)
grep -n "^const BucketsCount" stores/txmetacache/improved_cache_const_*.go
grep -n "idx := h % BucketsCount" stores/txmetacache/improved_cache.go
grep -n "bucket := int(h % uint64(txmetacache.BucketsCount))" services/validator/Validator.go
grep -n "Each partition therefore owns a contiguous" services/validator/Validator.go
grep -c "validator_txmeta" settings.conf || true

# Two partition counts, eight apart in the default context (section 74)
grep -n "TxMetaNumPartitions:" settings/settings.go
grep -n "^KAFKA_PARTITIONS_HIGH" settings.conf
grep -n "kafka_txmetaConfig" settings.conf
grep -n "Callers MUST set Partition on" util/kafka/kafka_producer_async.go

# The occupancy metric is commented out; its doc block still lists it (section 75)
sed -n '199,207p' stores/txmetacache/metrics.go
grep -n "size: Current number of entries" stores/txmetacache/metrics.go
grep -n 'Name:      "map_size"' stores/txmetacache/metrics.go
grep -n "maxBytes / BucketsCount" stores/txmetacache/improved_cache.go
grep -n "random postfix to the consumer group ID" daemon/daemon_kafka.go
grep -n "TxMetaCacheEnabled = false" daemon/test_daemon.go

# Which services publish a contract, and what they call it (section 76)
find services -maxdepth 2 \( -name "Interface.go" -o -name "ClientInterface.go" \) | sort
find services -maxdepth 2 -name "Client.go" | sort
grep -n "^type ClientI interface\|^type Store interface" services/blockassembly/Interface.go
grep -n "^type ClientInterface interface" services/propagation/ClientInterface.go

# Protobuf leaking into published contracts, counted (section 77)
for f in $(find services -maxdepth 2 \( -name "Interface.go" -o -name "ClientInterface.go" \) | sort); do
  t=$(grep -cE "^[[:space:]]+[A-Z][A-Za-z0-9]*\(" "$f")
  l=$(grep -E "^[[:space:]]+[A-Z][A-Za-z0-9]*\(" "$f" | grep -cE "_api\.|emptypb|anypb|timestamppb")
  printf "%-46s %4s %4s\n" "${f#services/}" "$t" "$l"
done
grep -n "SendNotification(ctx context.Context\|Subscribe(ctx context.Context, source string)" services/blockchain/Interface.go

# Which gRPC doors authenticate, by the auth argument each one passes (section 78)
for s in validator blockassembly blockvalidation subtreevalidation pruner propagation p2p legacy blockchain; do
  f=$(ls services/$s/Server.go services/$s/server.go 2>/dev/null | head -1)
  python3 -c "
import sys
src=open('$f').read(); i=src.find('util.StartGRPCServer(')
seg=src[i:]; d=0
for j,ch in enumerate(seg):
    d += ch=='('; d -= ch==')'
    if d==0 and j: seg=seg[:j+1]; break
print('%-18s %s' % ('$s', seg.split(',')[-1].strip()[:34]))"
done
grep -n "RequireAuthByDefault\|else if authOptions.APIKey" util/grpc.go
grep -n "^security_level_grpc" settings.conf

# The required credential that no file in this tree defines (section 79)
grep -n "REQUIRED: include grpc_admin_api_key" deploy/kubernetes/teranode/teranode-cr.yaml
grep -rn "kind: Secret" deploy/ || echo "  no Secret manifest under deploy/"
grep -n "grpc_admin_api_key" settings.conf
sed -n '17,22p' util/grpc_required_auth.go

# Two resolvers, one scheme name, the second wins (section 80)
sed -n '525,537p' util/grpc_helper.go
grep -n "^func Register" -A 3 "$(go env GOMODCACHE)/google.golang.org/grpc@v1.82.1/resolver/resolver.go"
grep -n "^grpc_resolver" settings.conf
grep -rn "grpc_resolver\|GRPC_RESOLVER" deploy/ || echo "  nothing in deploy/ names the setting directly"
grep -n "SETTINGS_CONTEXT" deploy/kubernetes/teranode/teranode-configmap.yaml
grep -rn "endpointslices\|serviceAccountName" deploy/kubernetes/teranode/ || echo "  no endpoint permission named in the cluster files"

# Which context actually selects the kubernetes resolver, and what it costs (section 80)
grep -c "grpcAddress.operator.*k8s:///" settings.conf
grep -oE "^[a-z_]+_grpcAddress\.operator" settings.conf | sed 's/_grpcAddress.operator//' | sort | tr '\n' ' '
grep "_grpcAddress.operator" settings.conf | grep -v "k8s:///" | wc -l | sed 's/^/  operator addresses without the prefix: /'
find pkg/k8sresolver -name "*.go" ! -name "*_test.go" | xargs wc -l | tail -1
find pkg/k8sresolver -name "*.go" | xargs wc -l | tail -1

# No RBAC object exists anywhere in the repository (section 80)
grep -rh "^kind:" deploy/kubernetes/ | sort | uniq -c | sort -rn
echo "  RBAC kinds in deploy/: $(grep -ril 'kind: *role\|kind: *clusterrole\|kind: *rolebinding\|kind: *serviceaccount' --include=*.yaml --include=*.yml deploy/ | wc -l | tr -d ' ') (expect 0)"
grep -n "automountServiceAccountToken" deploy/kubernetes/aerospike/aerospike-statefulset.yaml
grep -rn "func InitGRPCResolver" util/grpc_helper.go
grep -n "InitGRPCResolver" cmd/teranode/daemon.go

# The default that empties the busiest interface (section 81)
grep -n "^useLocalValidator" settings.conf
sed -n '280,283p' daemon/daemon_stores.go
grep -n "GetValidatorClient(" daemon/daemon_services.go
grep -rn "validator.NewClient(" cmd/ | grep -v _test
grep -n "^validator_grpcAddress  " settings.conf

# A listener with no sender, and an interface name from another operating system (section 82)
sed -n '439,441p' services/propagation/Server.go
grep -n "accepting from all sources" services/propagation/Server.go
grep -rn "WriteToUDP\|WriteMsgUDP" --include=*.go services/ | grep -v _test
grep -n "ReadMsgUDP" services/propagation/Server.go | head -2
grep -n "^# ipv6_addresses" settings.conf

# The surface, counted (interfaces section)
for f in $(find services -name "*_api.proto" | sort); do
  printf "%-52s %4s\n" "${f#services/}" "$(grep -cE '^[[:space:]]+rpc [A-Z]' "$f")"
done
cat $(find services -name "*_api.proto") | grep -cE "^[[:space:]]+rpc [A-Z]"
sed -n '152,161p' settings/settings.go | grep -c "getURL"

# One setting, and the comment two lines above it forbidding what the value does
sed -n '857,859p' settings.conf
grep -n "^KAFKA_PARTITIONS_HIGH\|^KAFKA_PARTITIONS_LOW" settings.conf
grep -n "produces by design" services/validator/Validator.go
grep -n "remains deferred" services/validator/Validator.go
grep -rln "orphan" services/legacy/netsync/*.go | grep -v _test

# The two unwired topics, and what an empty address does to a producer
grep -n "^kafka_validatortxsConfig" settings.conf
grep -rn "kafka_txPolicyRejectedConfig" settings.conf deploy/ || echo "  the topic is defined in neither the settings file nor any deployment"
grep -n "cfg := settings.Kafka.TxPolicyRejectedConfig" -A 3 daemon/daemon_kafka.go
grep -n "TxPolicyRejectedCacheEnabled" settings/settings.go | head -2

# The ledger store's four tables, in both dialects (section 83)
grep -hoE "CREATE TABLE IF NOT EXISTS [a-z_]+" stores/blockchain/sql/sql.go | sed 's/.*EXISTS //' | sort | uniq -c
grep -n "parent_id.*REFERENCES blocks" stores/blockchain/sql/sql.go
grep -n "blockchainSchemaLockID int64" stores/blockchain/sql/sql.go
sed -n '484,493p' stores/blockchain/sql/sql.go
sed -n '494,500p' stores/blockchain/sql/sql.go

# The two-level block-ID guard, and what it exists to prevent (section 83)
sed -n '128,136p' stores/blockchain/sql/sql.go
sed -n '43,44p' stores/blockchain/sql/assign_block_id.go
sed -n '62,63p' stores/blockchain/sql/assign_block_id.go
grep -n "ON CONFLICT" stores/blockchain/sql/assign_block_id.go | head -2

# The five-connection ceiling the settings file cannot raise (section 83)
grep -n "SetMaxOpenConns(5)" util/sql.go
sed -n '285,289p' util/sql.go
grep -n "^postgres_maxOpenConns\|^# blockchain_postgres_maxOpenConns" settings.conf
grep -n "^blockchain_store " settings.conf

# The chain-walk cache and the switch that keeps the walk in SQL (section 84)
grep -n "chainWalkCacheTTL = " stores/blockchain/sql/sql.go
sed -n '1314,1318p' stores/blockchain/sql/sql.go
grep -rn "resetChainWalkCache()" stores/blockchain/sql/*.go | grep -v _test
grep -n "^blockchain_use_in_memory_chain_check" settings.conf
grep -n "maxIDsPerCheckBatch = " stores/blockchain/sql/CheckBlockIsInCurrentChain.go
grep -n "32767" stores/blockchain/sql/CheckBlockIsInCurrentChain.go

```
