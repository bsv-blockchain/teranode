# How Teranode's Microservices Work

A plain-language guide to what each part of Teranode does, how it does it, and why it
was built that way. No code, no assumed background beyond knowing that a blockchain
records payments.

## Contents

- [Part One: The Idea](#part-one-the-idea)
    - [The problem with one big program](#the-problem-with-one-big-program)
    - [The sorting office](#the-sorting-office)
    - [Two ways to talk: phone calls and conveyor belts](#two-ways-to-talk-phone-calls-and-conveyor-belts)
    - [A note on what "scalable" means here](#a-note-on-what-scalable-means-here)
    - [A note on which example deployment to believe](#a-note-on-which-example-deployment-to-believe)
    - [Words used throughout](#words-used-throughout)
- [Part Two: The Services](#part-two-the-services)
    - [Inventory](#inventory)
    - [P2P](#service-p2p)
    - [Subtree Validation](#service-subtree-validation)
    - [Legacy](#service-legacy)
    - [Propagation](#service-propagation)
    - [Validator](#service-validator)
    - [Block Assembly](#service-block-assembly)
    - [Block Validation](#service-block-validation)
    - [Blockchain](#service-blockchain)
    - [Block Persister](#service-block-persister)
    - [UTXO Persister](#service-utxo-persister)
    - [Asset Server](#service-asset-server)
    - [Pruner](#service-pruner)
    - [RPC](#service-rpc)
    - [Alert](#service-alert)
    - [Blob Store](#blob-store)
    - [UTXO Store](#utxo-store)
    - [Transaction Metadata Cache](#transaction-metadata-cache)
    - [Chain Store](#chain-store)
    - [The Interfaces Between Services](#the-interfaces-between-services)

---

## Part One: The Idea

### The problem with one big program

The original Bitcoin node is a single program that does everything. One process
receives a transaction, checks it, executes it, files it, and builds blocks. It is a
capable all-purpose clerk sitting at one desk.

That clerk is fine when the mail is light. It becomes the problem when the mail is not.
If transactions arrive faster than one desk can process them, there are only two
options, and only one of them is available:

- **Get a faster clerk.** This is *vertical scaling*. It works for a while and then it
    stops working, because there is a limit to how fast one machine can be. And the
    cost does not rise gently — the next tier of machine is disproportionately more
    expensive than the last.
- **Hire more clerks.** This is *horizontal scaling*. It is what Teranode does. It is
    also much harder, because a single clerk holding everything in their head is easy
    to manage, and a hundred clerks working at once is not.

The hard part of Teranode is not splitting the work up. Splitting it up is
straightforward. The hard part is that a hundred clerks must reach **the same answer**
about every transaction, at the same time, without being able to consult each other on
every step. If they disagree, the ledger is broken.

Almost every design decision in the rest of this document exists to solve that one
problem.

### The sorting office

Teranode replaces the single clerk with a sorting office. The work of running a
blockchain node is divided between specialised teams, and each team is a **microservice
**— a small program with one job, running on its own machine or machines.

There is a team that talks to the outside world. A team that checks transactions. A
team that assembles blocks. A team that files the permanent record. A team that keeps
the master account of who owns what. Each can be given more staff — more machines —
independently of the others, depending on where the pressure actually is.

The commercial consequence is the point: capacity grows by adding ordinary machines, so
cost grows roughly in line with volume, instead of hitting a ceiling and a cliff edge.

### Two ways to talk: phone calls and conveyor belts

The teams have to hand work to each other, and there are two mechanisms. Knowing which
one a handover uses tells you a lot about how that part of the system behaves.

**gRPC is a phone call.** One team rings another and waits for the answer. Used when
the caller genuinely cannot continue without the answer — "is this transaction valid?"
has to be answered before the transaction moves on. Phone calls are fast and precise,
but the team being called has to be available, and if they are busy, the caller waits.

One caveat on that example, because this guide says so elsewhere and the difference
matters: "is this transaction valid?" is the question Teranode answers by phone call *in
its design*, and answers without leaving the room *as configured*. The interfaces section
explains the setting that does it. The distinction is the guide's central theme stated
early — the mechanisms are all built, and the deployment decides which ones are used.

**Kafka is a conveyor belt.** One team puts a job on a belt and walks away. The team
that handles that kind of job takes items off the belt when it is ready. The sender does
not wait, and does not even need to know who is doing the work.

The conveyor belt is the more important of the two for scaling, for two reasons:

- It absorbs surges. A sudden flood of transactions piles up on the belt instead of
    overwhelming a team that can only handle so much at once.
- It divides work automatically. Several workers can stand at the same belt and each
    take a share. Adding workers is adding capacity, with no redesign.

The trade-off is that a conveyor belt does not tell you when the work is *finished*,
only that it was *dropped off*. Teranode spends real effort on knowing where work has
got to, which is a recurring theme below.

#### A note on what "scalable" means here

Worth stating plainly, because it is easy to overstate.

Every service below is assessed on two different questions, and they have different
answers:

- **Could** this service scale by adding machines? Its design question — is the work
    divisible, and is any one instance keeping exclusive state that stops a second
    instance being stood up beside it?
- **Does** it, as shipped today? In the example production configuration that ships with
    Teranode, almost every service runs a single instance. Only the Validator runs
    multiple copies by default.

So for most services, the throughput available *today* comes from parallelism inside
one instance — many checks running simultaneously on the many processor cores of a
single machine — rather than from a fleet of machines. The room to widen outward exists
and was deliberately designed in; opening it is a deployment decision, not a rebuild.

Both readings are given for each service, because "it scales" and "it is deployed at
one copy" are both true of the same component, and a plan built on only one of them will
mislead.

### A note on which example deployment to believe

Everything above says capacity comes from adding machines. Whether that room is actually
open depends on how a node is deployed, and the examples in this repository are easy to
read wrongly.

`deploy/kubernetes/teranode/` holds two cluster files. They look like two sizes of the
same deployment — a small one and a big one. They are not. One is a **development**
cluster: it pulls an image built on the operator's own machine rather than a published
one, asks for a tenth of a processor and a quarter of a gigabyte on the services it
sizes, and gives every public address the made-up hostname `teranode.local`. The other is
shaped like
**production**: real hostnames, one to two whole processors and one to eight gigabytes on
the busy services, and a different set of services switched on. There is a third pair of
examples under `deploy/docker/`, aimed at operators, and on the question that matters most
— which door the internet comes through — they agree with the production one.

That matters because the two Kubernetes files disagree about whether the modern
peer-to-peer door runs at all, and a reader who treats them as two equally valid
production shapes will write a capacity plan that describes neither. Where this guide says
"as shipped", it means the production-shaped file and the Docker operator stack, and it
says so whenever the development file would have given a different answer. The evidence is
traced in the reconciliation notes; what the intended steady state is remains a question for
a maintainer.

### Words used throughout

| Term                           | Meaning                                                                                                                                      |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Transaction                    | A payment. Says "spend these specific existing coins, send the value to these new destinations."                                             |
| UTXO                           | "Unspent transaction output." A specific existing coin that has not yet been spent. The ledger is really a list of these.                    |
| Block                          | A batch of transactions accepted together, roughly every ten minutes, added to the permanent record.                                         |
| Subtree                        | A bundle of related transactions, sealed with one fingerprint. Covered properly in the Subtree Validation section.                           |
| Mempool                        | Transactions that have been received and accepted but not yet put in a block. The queue waiting to be filed.                                 |
| Validation                     | Checking a transaction follows every rule: the coins exist, the spender is entitled, the amounts add up, the signature is genuine.           |
| Chain reorganisation           | When two competing blocks exist and the network settles on the other one, work has to be partly unwound.                                     |

---

## Part Two: The Services

### Inventory

Seven of these do the main path of a transaction through the system. The rest either
file the permanent record, delete what is no longer wanted, or connect Teranode to the
outside world. Every one of them gets its own section, in the order a transaction meets
them — including the two quiet ones, Pruner and Alert, whose behaviour only looks obvious. All
seventeen now have a section, and so do the two things the inventory does not count: the ledger's own
store, and the corridors between the services, in the last two sections.

The table is the catalogue of what the *code* contains, and the deployments do not match it in
either direction: the cluster files name two services with no inventory row and no code in this
repository (`bootstrap`, `coinbase`), and the stores have inventory rows but are not deployable
processes at all. Both are looked at in the interfaces section.

| Service                                        | One-line description                                                                                                 |
| ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| P2P                                            | Talks Teranode's own protocol to other nodes; the optional front door.                                               |
| Legacy                                         | Talks the older Bitcoin protocol; the front door every shipped deployment runs.                                      |
| Propagation                                    | The door third parties hand new transactions through.                                                                |
| Subtree Validation                             | Checks bundles of interdependent transactions, in parallel.                                                          |
| Validator                                      | Decides whether one transaction is legal; the only place money is created or destroyed.                              |
| Block Assembly                                 | Turns the pending pile into a block a miner can work on; it *is* the pending set.                                    |
| Block Validation                               | Decides whether the chain should accept a whole block; brings a lagging node up to date.                             |
| Blockchain                                     | Owns the canonical chain: which block is best, what the ledger says, and whether the node may act.                   |
| Block Persister                                | Turns the node's temporary working copies into the permanent record, before their expiry arrives.                    |
| UTXO Persister                                 | Rebuilds the master list of unspent coins as a checkpointed file, behind the tip.                                    |
| Asset Server                                   | The public read door: answers for transactions, blocks and bundles over HTTP.                                        |
| Blob Store                                     | Stores the raw transaction and bundle data.                                                                          |
| UTXO Store                                     | Holds the live master list of unspent coins.                                                                         |
| Transaction Metadata Cache                     | Short-term memory of recently checked transactions, to avoid re-checking.                                            |
| RPC                                            | The old Bitcoin command interface — a door in old clothes, not a window.                                             |
| Pruner                                         | Deletes data that is no longer needed — by asking, and waiting to be told it may.                                    |
| Alert                                          | Listens to a second network that can freeze coins, reassign them, and ban peers.                                     |

The sections that follow take these one at a time.

---

### Service: P2P

#### What it does

It is the front door. Everything else in Teranode talks to components you chose and
configured; this service talks to strangers. It maintains the connections to other nodes
on the network, learns what they think the blockchain looks like, and brings data in.

#### The problem this removes

The rest of the system can assume co-operation. The open network cannot. A peer on the
internet may be honest and broken, dishonest and well-connected, behind a router that
makes it unreachable, or actively trying to make the node spend more money and memory
than it has. There is no contract with a peer and no way to inspect its source code.

So this service has to do two jobs that pull against each other: absorb as much as
possible from the network, while trusting as little of it as possible.

#### The decision everything else follows from

**Gossip carries announcements. Weight travels separately.**

When a peer has a new block or a new bundle of transactions to share, what arrives on
the gossip network is not the block. It is a small announcement — capped at ten
kilobytes for a block, eight for a transaction bundle — holding the fingerprint, a little
metadata, and an address to download the real thing from. That address is the peer's Asset
Server, its public HTTP endpoint.

So the mesh is used for *awareness* and ordinary web downloads are used for *weight*.

This is the single most consequential choice in the service. A gossip network where every
node forwards every block to every neighbour is a network whose capacity is set by its
slowest link, and it is the design that makes large blocks painful no matter how fast any
individual machine is. Splitting awareness from weight means the mesh keeps carrying a
few kilobytes per announcement while a gigabyte block crosses the network once, by HTTP,
between the two parties who actually need it. The mesh scales with the number of things
happening rather than their size.

Everything below is what it costs to run that split honestly.

#### How it works

Incoming announcements are checked for shape, then placed on the internal queues for the
rest of the system — the transaction bundles to validation, blocks to block validation.
Bulk payloads are pulled over HTTP from the announcing peer's address when the data is
actually needed.

The service also keeps a running judgement of every peer it has met: how reliable, how
responsive, how often it has failed to deliver, whether it has been caught misbehaving.
That judgement decides who gets asked for data. It lives centrally in the Blockchain
service rather than in this one, so every component sees the same opinion of the same
peer.

One dependency is worth naming, because it is outside this repository: the service has to
answer where a node finds anyone to call in the first place. On a fresh node the answer is
a short list of meeting points published as DNS records at a domain run by the BSV
Association, which resolve into the addresses of running peers. How hard a node then looks
for further neighbours is a setting with three positions — off, meaning it only ever
contacts peers it was handed; query-only, meaning it asks the address book but is not listed
in it; and full participant, meaning it advertises itself and helps route other people's
lookups. The Docker stack defaults to off. The Kubernetes settings context defaults to full
participant, which is only a live question in the development example, because the
production-shaped file switches this service off entirely. That combination is worth
knowing twice over: the fullest mode opens a hundred or more connections, and the operator
guide warns that some hosting providers read that as port scanning and complain.

Peer discovery runs through a distributed address book, with three levels of
involvement — full participant, query-only, or off entirely, where the node only ever
calls people it was given a name for. Mining nodes generally run the fuller modes so
other people can find them; development machines generally run the quieter ones.

#### The hard parts

**You must record who told you before you know whether to believe them.**

When an announcement arrives, the node stores which peer sent it. It has to do this
*before* checking whether the announcement refers to anything real, because if it turns
out to be rubbish that record is the only way to know whose reputation to mark down.

The consequence is uncomfortable: the size of that record is controlled by strangers.
Anyone can arrive and start announcing invented fingerprints, and each one is stored on
the strength of being announced. The answer is a hard size limit, enforced the instant
something is inserted rather than by a periodic tidy-up — because a timer-driven sweep
lets a flood balloon memory in the gaps between sweeps, and makes the sweep itself more
expensive in proportion to the attack.

What happens at the limit is the interesting part. The natural instinct is to refuse the
new arrival. This design does the opposite and evicts something already stored. The
reason: refusing new entries lets a bad actor fill every slot with junk in advance, wait,
then announce the one genuinely invalid block they care about and have it land
unattributed — the record full of their own earlier noise, no room left for the truth.

It goes a step further. Each peer is entitled to a fair share of the record — the
capacity divided by the number of peers present. A peer already holding at least its
share evicts *its own* oldest entry; only a peer below its share evicts the oldest entry
overall. So once a troublemaker fills its quota, further pressure lands on its own junk
rather than on everyone else's evidence. The share is computed from the peers actually
present, which means it needs no tuning knob and adjusts as the mix changes. The code is
candid that this bounds the damage rather than eliminating it: a large enough
distributed attack can still clear the record, at the cost of one live connection per
share.

**Suppressing repeats, without losing the fallback you needed the repeat for.**

The same bundle gets announced by many peers, and publishing every announcement would
drown the systems downstream. So repeats are suppressed. But the first announcement is
not automatically the best one — the download address inside it is supplied by the peer,
which could be stale, dead or deliberately hostile. Allowing only the first announcer
through would mean a single bad address costs the node that data.

The resolution is a small budget: a handful of *distinct* announcers per window get
published, and the later ones are collected by block validation as alternative sources
to fall back on. The design admits plainly that a coordinated attacker can burn some of
those slots by racing honest peers for them, degrading the redundancy without removing
it, so the budget is a setting rather than a constant.

**Three different things called "progress".**

When the node is behind and catching up, it must decide whether its chosen peer is
failing. Three signals look alike and mean different things, and conflating them causes
the specific failures that are hardest to diagnose.

- The node's own chain height rising.
- The retry pressure backing off.
- The chosen peer actually delivering blocks.

Only the third is evidence about that peer. The first arrives from *any* peer's ordinary
block chatter, which is precisely why it cannot be used as proof that the peer you picked
is doing its job. Treated carelessly, either one produces a node that waits forever on an
unproductive peer while cheerfully reporting that it is making progress — and since the
network produces blocks continuously, "forever" really means forever.

So the deadline for "my peer has stalled" is refreshed only by delivery attributed to
that peer. Separately, retry pressure deliberately does *not* relax when the local height
climbs, because during a long catch-up the height climbs constantly; relaxing there would
reset the escalation on every attempt, and every failed selection would restart a
backoff whose entry conveniently clears every peer's cooldown — undoing the escalation
exactly in the situation that prompted it. Relaxation happens only when a sync genuinely
completes or a peer's reputation recovers.

**Being ahead of me is not the same as being able to help me.**

Choosing who to sync from sounds like arithmetic — take whoever reports the most work.
That is wrong, because a peer's reported work reflects the headers it has, not its
ability to hand over the blocks behind them. A peer credited with work it has not yet
delivered sits in a penalty window, and during that window its higher total does not earn
it the top tier of selection. Being impressive on paper does not outweigh having
delivered recently.

A candidate must clear four gates: not banned, has a working download address, reputation
at or above 20, and not blacklisted by the operator. Peers running in pruned mode — which
have thrown away old coin records to save space — are usable only as a last resort, and
then the *youngest* one is chosen, the one that has thrown away least, to reduce the
chance it cannot supply a coin some block needs.

Related, and a subtler trap: the question "am I caught up?" and the question "who can I
ask?" must use the same definition of a usable peer. An operator-blacklisted peer sitting
ahead of the node counts as unavailable for one question and available for the other, and
the node waits indefinitely for a peer it will never contact. Catch-up is then stuck
forever while reporting a plausible reason.

**Selection cannot be held hostage by a peer that has stopped answering.**

Checking whether a peer is alive is itself a network call, and dead peers are precisely
the ones that make network calls slow. So probing is bounded three ways: two seconds per
peer, eight peers probed simultaneously, five seconds total for the whole exercise.
Anything unresolved when the overall budget runs out is counted as unhealthy and the
selection proceeds without it. Results are remembered for ten seconds so that one round
of selection does not probe the same peer twice.

The consistent principle: a slow peer must never be allowed to make the whole system
slow while it times out on its own.

**Some lies should reject a message; others should only trim it.**

Peer-supplied text is handled in two categories. Protocol fields have an exact shape every
honest node produces — a fingerprint is 64 characters, a block header is exactly 160 — so
a violation cannot be a formatting difference and is treated as one: the message is
rejected and the sender is marked. Display text is different. A rejected-transaction
reason is free-form text written by another operator's software; it gets truncated rather
than rejected, because discarding the message would throw away the diagnostic at the exact
moment someone needs to read why a transaction failed. Both categories guard against
invisible characters smuggled into logs.

There is a pleasing piece of symmetry here. Before publishing anything, this service
enforces on its own outbound messages the same size limit that receiving peers apply to
incoming ones. The reason is that a message perfectly within every per-field limit can
still exceed the total once it is encoded for the wire, because the encoding expands
certain characters. Such a message would be silently discarded by every recipient with no
error visible on the machine that sent it — the worst class of bug to chase, an opinion
you can only form by looking at someone else's logs.

**Being reachable is a genuine problem, and one fix caused it.**

A node sitting behind a home or office router has only a private address, which the rest
of the internet cannot route to. libp2p has machinery for exactly this: when someone
connects to you, they can tell you the address they saw you coming from, which is your
public address as the world perceives it. The software also filters private addresses out
of what gets advertised.

Those two features interacted badly. The filtering was running early enough that it
suppressed the private addresses the observed-address mechanism needed to work with, so
the node never learned its public address and never advertised one. The protection caused
the invisibility it was meant to accompany. Operators behind NAT can now opt into the
address-discovery service or state their public address explicitly.

Alongside this sits a visibility dial with three positions. *Full* behaves normally.
*Listen only* receives everything but stops publishing announcements. *Silent* goes
further — it also stops advertising its own existence and withdraws from the peer
address book entirely, connecting outward to peers it was told about while remaining
findable by nobody. Silent is the mode for someone who wants the network's data without
becoming a public meeting point.

Two smaller judgement calls in the same area are worth noting because both are traps
avoided. The network penalises peers that cluster many identities behind a single address,
which is a useful proxy for a distributed attack — but a test cluster running several
nodes behind one address would be permanently penalised for the crime of existing, so
local address ranges are exempt. And development setups default to listen-only, because a
development machine's download address is usually unreachable from the public internet, so
advertising it invites other nodes to come and fail.

**A chatty network should not multiply bookkeeping.**

Every peer announcement would otherwise mean a write to the central peer record, which at
network volume is a great deal of traffic for information nobody is reading. Updates are
collected and written once a second, a peer that keeps announcing itself is not
re-registered every time, and the queue of pending updates is capped at ten thousand
peers — past which new updates are dropped and counted rather than growing the queue.
That cap is the deliberate pressure valve against a flood dressed up as many different
sender identities.

#### What it produces

Announcements placed on the internal queues for validation, blocks pulled into the chain,
and a continuously updated opinion of every peer on the network that the rest of the
system consults when it needs to decide who to trust with a download. It also consumes
complaints from validation about bad data and turns them into consequences for the
peer responsible, as described in the Subtree Validation section.

#### Scaling

This service scales differently from the others, and the difference is structural rather
than a matter of degree.

A validation worker is interchangeable and anonymous — it takes work off a queue and
keeps nothing private. A P2P instance has an *identity*: its own cryptographic key, its
own peer identifier, its own set of live connections, and its own accumulated reputation
history. Standing up a second instance means giving it an address the rest of the network
can reach, and its judgements about peers do not automatically transfer to the first one.

The infrastructure reflects that. In the Kubernetes examples, P2P is the service given
public ingress. In the multi-container deployment it is the service with indexed
instances, each with its own identity in configuration.

So P2P does scale out — a mining operation running several ingress nodes in different
regions is the normal pattern, and it keeps any one machine from being the single point
where the internet has to connect. But adding a copy is an act of *network planning*
(addresses, reachability, discovery, whose opinion of which peers matters) rather than
the act of capacity planning the stateless services allow. As shipped it runs a single
instance, and whether it runs at all depends on which door a deployment wants: the two
Docker examples and the production-shaped Kubernetes file leave it switched off and let the
older Bitcoin protocol service take the internet instead, while the supported install
script turns it on alongside that service. So it is optional in every shipped configuration
and standard in the documented one — which is a different, and milder, disagreement than
the one recorded when this section first pointed at the reconciliation notes.

---

### Service: Subtree Validation

#### What it does

It checks bundles of transactions for validity, and it does so in a way that lets a
large number of transactions be checked simultaneously on different machines without
them stepping on each other.

#### The problem this removes

The obvious way to check transactions is one at a time, in the order they arrived:
check one, apply it, move to the next. Simple, and completely serial. Throughput is
capped at how fast one machine can check transactions, no matter how many machines you
own.

The obvious fix — check lots of them at the same time — collides with a awkward fact
about Bitcoin. **Transactions depend on other transactions.** If Alice pays Bob, and
Bob immediately pays Carol, then Carol's payment cannot be checked until Bob's has been
checked, because Carol is spending coins that only exist if Bob's payment is valid.
Check them in parallel naively and you get nonsense: Carol's payment looks invalid
because the coins it spends have not been created yet.

So the dependency chain forces an order, and order is the enemy of parallelism. This is
one of the central obstacles to scaling a blockchain at all, and it is what this service
exists to break.

#### How it works

**Step one: transactions arrive as bundles, not individually.**

When a node on the network has transactions to share, it does not send them one by one.
It groups related transactions into a **subtree**: a bundle of transactions where each
one is stacked above the transactions it depends on, tied together so that the whole
bundle has a single identifying fingerprint — its hash.

The fingerprint is what makes parallel checking possible. Because a bundle's contents
determine its fingerprint, and its fingerprint determines its contents, a node can be
handed a bundle it has never seen and prove immediately that it has not been tampered
with, and can prove to anyone else exactly what it accepted. No trust required, no
negotiation, and — crucially — no need to consult a central authority mid-check.

**Step two: the bundle is sorted into levels.**

These are *dependency* levels — a grouping by what each transaction needs before it can
be checked. They are not the layers of hashes inside the bundle's fingerprint, which are
also sometimes called levels, and which serve a quite different purpose.

The service separates the transactions in a bundle by how deep their dependencies run:

- **Level one** holds transactions that spend only coins already in the permanent
    record. Nothing they depend on is in the bundle, so there is nothing to wait for.
- **Level two** holds transactions that spend only coins created by level one.
- **Level three** spends level two. And so on.

The rule that makes this safe: **no transaction in a level depends on any other
transaction in that same level.**

**Step three: levels are checked in order, and each level is checked all at once.**

Level one is checked as a single parallel sweep — a hundred independent checks running
simultaneously, none able to interfere with another. When level one is finished, level
two runs the same sweep, and so on down the chain.

This is the answer to the awkward fact above. The dependency order is respected exactly,
because nothing is checked before its foundations. And within each sweep, the work is
completely parallel, because the ordering constraint simply does not apply inside a
level. Ordering is enforced *between* the sweeps, parallelism is exploited *within*
them. You get both, which looks like it should be impossible.

In the accounting analogy: an auditor cannot examine a payment before examining the
payment that funded it. But a firm can put all the first-generation payments in one
tray, all the second generation in the next, and then set a hundred auditors loose on a
tray at a time. The discipline of the trays is what buys the freedom to parallelise.

#### The hard parts

**Checking a transaction twice is a real cost, not a rounding error.**

Full validation means fetching parent transactions from a database and executing the
scripts of a transaction. On a busy node that is the dominant cost. And blockchain
traffic is repetitive: the same heavily-used coins get referenced again and again, and
a transaction can arrive a second time from a different peer.

Before doing any work, the service runs two quick questions:

- Is this transaction already in the **metadata cache** — a short-term memory of
    recently checked work?
- If not, is it already recorded in the **UTXO store**, meaning it was checked and filed
    earlier?

Only the transactions that fail both questions get the expensive treatment. In the
common case where most of a bundle has already been seen, most of the work is skipped
entirely. The commercial reading: the expensive check is paid for once per transaction,
not once per sighting.

**A transaction's parent may be missing from the bundle.**

Occasionally a bundle references a transaction that did not arrive inside it. The
service asks the peer that sent the bundle for the missing piece, over HTTP, and works
through the gaps in dependency order rather than scrambling for each one as it is
noticed — so a bundle missing fifty parents causes fifty tidy requests rather than a
storm of them.

**Blaming the right peer when the data arrived through several hands.**

A malformed or dishonest bundle has to be reported, or a peer that keeps supplying
rubbish is never discouraged. The difficulty is that a bundle reaches a node by more
than one route. One peer *announces* that a bundle exists and is worth collecting, and a
peer — frequently a completely different one — actually *serves the bytes*. Charge the
announcer for a fault in the bytes and you have punished an innocent party.

So when a bundle fails, the service publishes a complaint naming the bundle and the
address the bad bytes were actually fetched from. The P2P service reads the complaint and
works out which peer owns that address, and charges that peer. Three refinements are
worth knowing about, because each one is a decision to be restrained rather than
thorough:

- **If the culprit cannot be identified, nobody is charged.** The system deliberately
    prefers to under-punish rather than risk penalising an innocent peer.
- **The mismatch is logged, not smoothed over.** When the announcing peer and the
    serving peer differ, that fact is recorded. The discrepancy stays visible for
    operators instead of being quietly tidied away.
- **The charge is a demerit, not a dismissal.** What gets recorded is one failed
    interaction on the peer's reputation record, held centrally by the Blockchain
    service — not an immediate disconnection. Each peer carries something like a credit
    score, built from its overall success rate, knocked down for a recent failure,
    lifted by a recent success, and discounted if it has been slow to respond. A single
    bad fetch can be a passing network fault, and ending a working relationship over it
    costs more than it recovers, so the score just dips. Repeated failures drag it down
    persistently, and a peer with a poor score gets less trust and less traffic. A peer
    caught doing something genuinely malicious is pinned near the bottom immediately,
    and stays there — that state is deliberately not something good behaviour can quietly
    earn back.
- **Bans are a separate ledger entirely.** Actual bans are recorded apart from
    reputation, and they survive a peer disconnecting on purpose. The reason is a small
    piece of good sense: if clearing a ban were as easy as reconnecting under a new
    session, an offending peer could cancel its own punishment simply by hanging up.

Two further safeguards sit underneath, and both matter more than the reporting itself:

- **Duplicate suppression.** The same bad bundle arriving from five directions produces
    one complaint, not five. Without this, a single piece of bad data generates a flood
    that buries the signal it is meant to carry.
- **The catch-up guard.** While the node is still catching up on blocks it missed, it
    ignores peer bundles entirely and declines to judge them. A node that is behind does
    not yet have enough information to know whether a bundle is valid, and a node that
    judges while under-informed will confidently condemn honest peers. This guard
    prevents a specific and nasty failure mode: a restarting node turning against half
    of its healthiest connections because it is briefly confused about its own position.

**Big blocks exhaust memory, not patience.**

A bundle can contain an enormous number of transactions, and the naive approach holds
all of them decoded in memory at once. Under pressure this is what makes nodes crash
rather than what makes them slow. Three habits address it: transactions are streamed in
and processed as they arrive rather than all being loaded first; scratch memory is
recycled from a shared pool instead of being freshly allocated for every transaction;
and memory is handed back the moment it is finished with, rather than waiting for the
end of the job. Individually minor, together they are the difference between a node
that degrades and one that falls over.

**Fifty transactions wanting the same parent should mean one lookup.**

Each check needs the details of the transactions it spends from. Left to itself, every
transaction independently requests what it needs, and when many transactions in a level
spend from the same handful of parents, the same question gets asked fifty times. The
service instead collects each level's parents, removes the duplicates, and fetches them
once in a batch, then hands the answer to every check that needs it. Related, the
data used to prove a transaction's place in the chain is loaded once before the checks
start, so the parallel workers can all read it without queuing for access to each
other.

#### What it produces

Two things. The checked bundles are written to long-term storage, fingerprinted and
retrievable by fingerprint. And for each transaction, a **metadata record** is published
onwards — a receipt stating that this transaction has been vetted, which blocks it
appears in, the status of the coins it created, its fee and its size.

That receipt is what lets the rest of the system avoid repeating the work. Downstream
services can see that a transaction was validated and trust it, rather than starting
again from scratch. The receipts travel by conveyor belt, so a busy downstream team
never blocks this one.

#### Scaling

Subtree Validation is designed as a *stateless worker*: it keeps no private record that
another instance would need, and it takes its work off a shared conveyor belt, where the
queue divides itself between whoever is standing there. On the design question, it
scales cleanly — add a copy, throughput rises.

As shipped, it runs **one** copy in the example production configuration, alongside most
of the rest of the system. The throughput it delivers today therefore comes from the
parallel sweeps inside each instance, across the cores of a single machine. Widening it
to a fleet is the intended next move when validation becomes the constraint, and it does
not require redesign — but it is not switched on by default.

The one exception in the shipped configuration is the Validator service, which runs
eight copies by default. That is a meaningful hint about where the operators expect the
real bottleneck to be: the sheer arithmetic of checking transaction signatures. Because
Subtree Validation hands most of that work to the Validator, the two scale together in
effect — the bundles get sorted into parallelisable levels here, and the heavy checking
fans out across eight instances there.

### Service: Legacy

#### What it does

It speaks the older Bitcoin protocol — the one the rest of the industry's software has
been built against for a decade — and it is the largest single service in the system.

One correction to the inventory table at the start of this document, which calls it a
compatibility layer and makes it sound like an optional extra. In the example mainnet
configuration, Legacy is the **only** door the internet connects to: the modern peer-to-peer
service described earlier is switched off there, and Legacy is switched on. For that
deployment, every incoming transaction and every incoming block arrives through this
"compatibility" path. The other example configuration does the reverse — but that file, on
inspection, is a development cluster rather than a production one.

On the deployment evidence this repository actually contains, Legacy is the door nothing
shipped leaves out: the production-shaped Kubernetes example runs it with the modern service
off, and the two Docker examples for mainnet and testnet do the same, with the modern door
carefully commented out and a pointer to the documentation to read first. The supported
install script goes further and runs both doors at once. So the honest summary is not
"which one is real" but "this one is always there, and the modern one is the option". See
the reconciliation notes.

#### The problem this removes

A currency is worth what its tools make it worth. Exchanges, wallets, mining pools, payment
processors and block explorers were all written against the protocol Bitcoin has always
spoken, and none of them can be persuaded to change at the same moment. A node that speaks
only a newer, better protocol is a node those businesses cannot use.

So Teranode cannot ask the industry to move. It has to join a network whose habits are
already fixed, by software owned by other companies, which will not change to accommodate
it.

The awkward part is what "accommodate" means here. The older protocol's behaviour is not
only a list of commands you can accept or refuse. A large part of it is *habits* — things
other people's software does that you have to satisfy, or your transactions are quietly
dropped. Nothing errors. No message comes back saying why. A payment simply never arrives,
and the person waiting for it is told the new node is unreliable.

A great deal of this service is the work of knowing those habits precisely, and designing
around them.

#### How it works

**One: it carries a whole classic node inside it.**

Rather than translating at the edge and handing everything to the modern services, Legacy
contains its own copy of the machinery a traditional Bitcoin node has: an address book of
other nodes it has met, with scores for how recently and how reliably they answered; a
connection manager that keeps a target number of outbound links and replaces the ones it
loses; block synchronisation, including the initial download of history and the holding pen
for transactions that turn up too early; and the list of checkpoints described below. It is
not a thin adapter. The reason is that the old protocol is not an envelope — it is a
conversation with a long memory, and you cannot conduct one without remembering the
conversation.

**Two: transactions by announcement.**

A peer does not push transactions at Teranode. It sends short notices — "I have transaction
such-and-such" — a few tens of bytes each, up to fifty thousand in one message. Teranode
asks for the ones it wants. The peer sends them.

**Three: it checks before it repeats.**

An incoming transaction is put to the Validator service for checking before being relayed on
to other peers. The database lookups this needs — finding the existing coins each
transaction spends — are gathered into batches of up to about a thousand at a time, so a
burst of arrivals becomes a handful of large queries instead of a storm of small ones.

**Four: blocks are converted, not merely accepted.**

A block arriving in the old format is re-divided into the bundle structure the modern
machinery works with, the bundles are written out with their fingerprints, and the block is
then handed to Block Validation — the same back end the modern path uses. The two doors
merge at validation, not at the network edge: the compatibility door calls the same Validator
and Block Validation services the modern door eventually reaches.

It is not the same *route*, though. The compatibility door reads the receipts the checking
machinery publishes on the conveyor belt — the same metadata records described in the Subtree
Validation section — and uses them to decide what to tell old-protocol peers about, and in
what order. That ordering detail is one of the better examples of this service's whole
problem, and it is worth its own hard part below.

**Five: it serves miners running older equipment.**

The request "give me a block to mine" is answered here, by asking Block Assembly for a
template and handing it over in the format older mining software understands.

**Six: it reports its peers to the dashboard.**

Old-protocol peers are mirrored into the same central peer list the modern service uses, so
an operator sees one fleet rather than two unrelated halves. The details of how that is done
safely are worth a hard part of their own, below.

#### The hard parts

**The other side's short-term memory is small, and it expires.**

When older software is offered a transaction whose parent it has not yet seen, it parks the
child in an orphan holding pen and waits. That pen is capped in size, and entries expire.
Which means a child transaction can be silently thrown away while waiting for a parent that
was always on its way.

The instinct is to enlarge the pen or lengthen the expiry. Teranode's answer is different:
**lean on the pen a lot less.** When it offers a batch of transactions to a peer, it
reorders them so that every transaction is offered after any of its own parents that are in
the same batch. It does not try to do better than that, and says so: a child whose parent
turns up in a later batch is deliberately not held back, because the pen and the retry
mechanism described below are there to catch it. The reordering is deliberately minimal — a
parent is pulled forward to just
ahead of the first transaction that spends it, rather than to the front of the queue — so
unrelated transactions keep their relative order as far as the dependencies allow. In one
code comment's example, offering a child, then an unrelated transaction, then that child's
parent becomes parent, child, unrelated.

Malformed input cannot hang it either: a transaction is marked as visited before its parents
are examined, so even a nonsensical circular dependency runs to an end.

And the reason the ordering can be wrong in the first place is worth understanding, because it
is not the peer's fault. Teranode's own conveyor belt is split into parallel lanes, and a
transaction that has just been checked travels down one of them while its parent travels down
another. Different lanes are read at different speeds, so the child can arrive, and be
queued for announcement, before its parent — inside our own building. The peer is then handed
a child first, and does what a hundred million lines of older software has always done with
one: takes it, says nothing, and forgets it after a while. So the compatibility service
re-sorts every batch before speaking, on the assumption that its own plumbing gave it the
batch in the wrong order.

**A queue that cannot carry what it needs to sort with.**

To put parents first, the service needs each queued transaction to travel with the list of
transactions it spends from. It cannot have that. The queue identifies items by fingerprint
and removes duplicates, which requires the items it holds to be directly comparable, and a
list of parents is not something you can compare that way.

So the parents travel in a separate side table, keyed by the transaction they belong to, and
taken out again when the batch is sorted. That table has a hard size limit — it is sized at
twice the largest batch the announcement path can request — and losing an entry to that limit
is treated as unimportant on purpose: the transaction simply misses its place in the order and
gets announced in whatever position it happened to arrive. The consequence is that one peer
may park one child in its holding pen for a while, which the retry mechanism below exists to
clean up. That is the point of the design: the expensive bookkeeping is allowed to fail
small, because a cheap safety net sits underneath it.

**Shutting down is as hard as starting up.**

The queue that batches announcements comes from a shared library, and one version of that
library crashes if something is added to a queue after the queue has been closed. Meanwhile,
the reader of the conveyor belt that feeds this queue runs as a background task that shutdown
does not wait for. Which leaves a window: the node decides to stop, drains and closes the
queue, and a moment later the conveyor-belt reader — still running, unaware — hands it one
more transaction. Crash.

The fix is a pair of locks and a flag: adding to the queue takes a shared lock and checks the
flag; closing takes the exclusive lock, which waits for any addition in flight, sets the flag,
then drains. An addition after close becomes an inert no-op instead of a crash. Unremarkable
in the code, and the difference between a node that stops when asked and a node that reports a
fault while being restarted — which is the kind of thing that gets diagnosed as corrupt data
and investigated for a day.

Note that the constraint being designed around comes from a specific version of somebody
else's library, quoted in the comment. If that library ever changes its behaviour, the locks
here become unnecessary rather than wrong, which is the safe direction for this kind of
defence to rot in.

**The other side's memory of refusals is only cleared by a new block.**

Older software keeps a list of transactions it has recently *rejected*, and ignores any repeat
until its end of the chain moves. So a transaction refused for a temporary reason — a coin
not yet visible, a policy detail that has since changed — stays refused even after the reason
has gone away. The sender can retry a hundred times and be ignored a hundred times.

Teranode therefore retries on the *peer's* clock, not its own. A transaction waiting to be
re-offered gets one attempt per new block. It waits about thirty seconds after a block
arrives before making that attempt, because the peer has to finish processing the block and
clear its list first — a retry fired too early is simply swallowed and the attempt wasted.
Blocks that arrive during that pause share the single retry. Each transaction is given six
blocks' worth of attempts, which is roughly an hour on the main network.

The waiting queue is capped, and what happens at the cap is a decision worth reading twice.
When the queue is full, a newly arrived transaction evicts the oldest entry that has **not
yet been retried** — and keeps the entries that have already survived a block unmined. The
reasoning: an entry that has survived a block unmined is a candidate for being genuinely
stuck, and the retry is the one mechanism that might free it, so it keeps its place. A fresh
arrival will simply be re-announced through the normal route anyway. Only when every entry
in the queue has been retried is a new arrival turned away.

There is a capacity fact hidden in that number, and it is the most useful line in the whole
service for judging what this path can carry. The queue holds about four thousand entries and
is drained once every ten minutes. Sustained arrivals above roughly seven transactions a
second fill it with fresh traffic alone, before any stuck transaction gets a look-in. Relay
over the old protocol is the tightest stretch on an otherwise very wide road.

**Background curiosity must not cost you a real connection.**

A node only ever learns that some other node's address actually works by connecting to it.
So its book of *confirmed reachable* addresses slowly goes stale as machines come and go,
and when a peer is lost, the replacement is picked from a list nobody has checked recently.

The fix is a feeler: a short-lived probe that connects to an address the node is not using,
completes just enough of the handshake to prove somebody is home, hangs up, and marks the
address verified. Now replacements are chosen from addresses that answered recently.

The discipline is in who pays for the probe. A connection slot is scarce, and a probe that
borrows one means the probe cost the node a genuine peer. So the probe's slot is
**reserved out of spare capacity, never taken from the number of real connections the node
intends to keep**. The classic software expresses this as arithmetic on its separate inbound
allowance; Teranode runs one joint ceiling over inbound and outbound connections, so the
faithful translation is to lower that joint ceiling and leave the outbound target completely
alone.

It also refuses to reserve a slot in three situations, and writes the reason in the log at
startup rather than leaving an operator to infer it: when the feature is switched off (so the
node never pays for something that is disabled), when the operator has pinned the node to a
fixed list of peers (every slot is spoken for by a peer the node deliberately wants, and the
node has stopped discovering neighbours for itself, so a verified address would have nothing
to contribute), and when the reservation would leave no room for an ordinary peer at all. In
that last case the probe is abandoned rather than the connection target being cut, because a
node in that state would spend its life below target, dialling out and being turned away at
its own door.

**Knowing when a shortcut through history is safe.**

A node catching up from the very beginning must re-check every transaction in history, which
can take days. Older software ships a list of **checkpoints**: points where the accepted chain
is treated as settled, below which the expensive re-checking may be lightened, because that
stretch of history is already agreed by everyone.

There are two shortcuts here, and they are at different stages of trust.

The long-standing one applies to any block at or below the highest checkpoint. It skips the
per-transaction script checking — which is where a signature's arithmetic is actually
verified — skips the step where the bundles inside a block would otherwise be re-verified by
the Subtree Validation service, and pre-assigns the block's identity instead of deriving it
the long way round. The justification recorded for this is that proof-of-work combined with
an unbroken chain of headers running back to a checkpoint is enough to establish that this
block belongs on the chain — the expensive checks were done the first time anyone accepted
this history.

And that sentence contains the whole risk, which is why the code is strict about it. The
requirement is *proof* that this block belongs to the checkpoint-certified chain, not merely
evidence that its height falls inside the certified range — and the code states plainly that
the height-only test is what the gate used to ask, and that this was a weakness since closed.
Two rival histories can both contain block number nine hundred thousand. A block invented on
a rival history, sitting at a certified height, would have got its signature checks waived
for a number. So now every block taking the shortcut must have its header provenance
established, block by block, back to something trusted. Where a caller cannot establish that,
it passes a blank value and the shortcut is denied. When information is missing, the safe
answer is the automatic one.

The second shortcut is newer and **switched off by default**. It goes further still: it stops
calculating transaction fees for those blocks at all, stamps them as zero, and updates the
ledger of unspent coins in a minimal form that records which coins were consumed without the
full checking applied to current blocks. It engages only when an operator asks for it, only
if the storage back end supports it, and only below the highest checkpoint. Anything missing,
and the behaviour is exactly as before.

That last one has a consequence worth flagging to anyone who later wants historical fee
statistics: on that path, recorded fees for old blocks are placeholders, not measurements.

One further touch shows the same care. Such a block would normally wait for its parent to be
formally marked as filed before processing — a wait that exists to support a rule about
relative lock times. On this path that rule is not applied anyway; blocks are processed one
at a time through a single queue, so the parent's coins are already recorded before the child
begins; therefore the wait does nothing except cost a round trip per block, and it is removed
for exactly that path and no other.

**A block can be shaped to break your arithmetic.**

The modern machinery wants a block's transactions divided into bundles of equal size, where
that size is a power of two — 64, 128, 256 — with the final bundle allowed to be shorter. An
early version of the converter picked the largest bundle size that divided the block up
neatly. That is a perfectly sensible rule, and it is attackable: an adversary chooses a
transaction count with almost no even divisors. The example recorded in the code is 900,679
transactions, which drove the bundle size all the way down to two transactions apiece and
produced several hundred thousand bundles for one block. Every bundle carries its own fixed
cost in hashing, storage and bookkeeping, so an ordinary block becomes an extraordinarily
expensive one — by choosing a number.

The fix is to stop requiring the division to be neat. The fingerprint construction already
pads internally at every level, so the final bundle can hold any count up to the standard
size. Once that is allowed, the split becomes boring and fixed: standard-size bundles, plus
one holding the remainder. Cost is then set by the block's real size, not by the arithmetic
properties of an attacker's choice.

**Making a shared table read-only, so it can be read fast.**

While a block is being processed, one stage builds a lookup table of the block's
transactions, and then a number of parallel stages read it heavily.

A read-write lock is the safe reflex, and it stays correct — but every read updates a shared
counter, and on a large block spread across many processor cores, that counter becomes the
bottleneck. Not the data, not the disk: cores arguing over one line of memory. So once the
writing is finished, the table is **frozen**. Readers stop taking the lock at all, and an
attempt to write afterwards does not quietly corrupt the table — it fails loudly, because by
then nothing is supposed to write.

This is safe only because ownership is known in advance: exactly one stage writes, and it
finishes first. The code says so, which is what makes the shortcut reviewable rather than
lucky.

**Two front doors, one peer list, and a naming scheme.**

The dashboard is meant to show every peer, old protocol and new, in one place. Rather than
build a second view, Legacy mirrors its connected peers into the central peer registry — but
under names carrying a distinctive prefix, so an old-protocol entry is recognisable in a log,
in a filter, and on screen, and can never be confused with a modern peer's identifier.

Identity is where the sting is. An **outbound** peer — one Teranode dialled — is registered
under the address it dialled, which does not change, so its history and any ban survive a
disconnection and a reconnect. An **inbound** peer is registered under the network address it
came from, and that address includes a temporary port number chosen by the peer's own
software. So every time an inbound peer reconnects, it looks like a brand-new peer: its
history does not carry over, and a peer that reconnects frequently leaves a trail of separate
entries behind it. The practical consequence is that judgements accumulated about inbound
peers are weaker than judgements accumulated about outbound ones, and any operator reading a
reputation score should know which kind they are looking at.

Three further disciplines in this one small piece of plumbing are worth naming, because they
are the difference between a monitoring feature and a monitoring feature that causes
outages:

- The mirror is **visibility only**. Nothing about syncing, catching up or choosing peers
    reads it back. A reporting feed that starts steering behaviour turns a dashboard problem
    into a networking problem, and makes the cause very hard to see.
- "Could not find out" and "nobody is connected" are treated as different answers. If the
    internal query is momentarily unable to answer, that tick is skipped and the existing
    entries stand. If it answers "nobody", the entries are cleared. Confusing the two would
    empty the operator's view of the fleet's connections because one component happened to
    be busy.
- A failed report does not lose the numbers. Traffic is reported as increments, and when a
    report fails, the previous baseline is carried forward so the next report includes both
    stretches. Otherwise a temporarily unreachable registry makes a busy peer look idle —
    and an idle-looking peer is a candidate for disconnection.

It also reconciles once immediately at startup, so a node with live peers is not shown as
empty for a whole interval, and it puts a deadline on every call it makes, because the
registry client adds none of its own — an unresponsive service would otherwise stall the
loop for as long as a connection takes to give up.

**Not letting one organisation fill the room.**

A node accepts a limited number of peers, and no more than a handful from any single
address. The per-address figure is a compromise in the same spirit as the feeler's budget:
enough for a business that legitimately runs several nodes behind one address, few enough
that one organisation cannot pretend to be the network.

**Risky changes shipped in the dark.**

Several of the behaviours described above sit behind switches that default to off, including
the deeper history shortcut, the stricter treatment of conflicting spends, and a route where
the trusted back-end machinery used for catch-up does the work instead of the path described
here. With a switch off, the code promises the pipeline is byte-for-byte what it was before.
For an operator that is the difference between a change you can try on one node at three in
the afternoon and roll back in seconds, and a change you can only evaluate by betting the
whole deployment on it.

#### What it produces

Accepted transactions enter the mempool and are offered on to other old-protocol peers, in
the careful order described above. Accepted blocks go to Block Validation and then the
Blockchain service, and the bundles derived from them are written to storage so the modern
read path can serve them. Miners receive templates. Peers are mirrored to the dashboard.

#### Scaling

Two answers, and the gap between them is wider here than for any service so far.

On the design question, Legacy is a **front door, not a worker**. It holds its peer
connections inside one process, with its own identity and address book, and its block
download runs through a single queue — one block at a time, in order. Its throughput is set
by that serial path and by its connection allowance. It does not fan out across machines the
way the validation work does.

Adding copies is still possible, and is the standard pattern for this kind of component: put
several behind something that spreads connections, give each its own reachable address, and
accept that each maintains its own view of who is out there. The costs are real — the same
peer may be connected twice, one address can be admitted twice through two doors, and every
copy downloads the chain for itself — but none of it requires redesign.

On the shipped question, the example mainnet configuration runs **one** Legacy instance with
the modern peer-to-peer service disabled, and puts **eight** Validator instances behind it.
That is a deliberate shape: one narrow place where the internet connects, feeding a wide room
where the arithmetic happens. It also says where the first constraint will appear. If
arrival volume grows, the tight place is the door — the single instance and the roughly
seven-transactions-a-second relay cadence — not the signature checking behind it.

The other Kubernetes example turns Legacy off entirely and uses the modern peer-to-peer
service as the door instead. Read as two equal production shapes, that disagreement is fatal
to any capacity plan built on either. Read as what the files are — one development fixture,
one production shape, with both Docker examples siding with the latter — it is a developer's
convenience rather than an operational fork, and the numbers above describe the shipped
posture. What is genuinely still open is narrower: whether the intended steady state is this
service alone, or this service plus the modern door as the supported install script sets up.
The path a transaction takes differs between the two doors, and no document walked them
side by side yet. See the reconciliation notes.

### Service: Propagation

#### What it does

It is the door that strangers use. Everything a customer, a wallet, a mining pool or an
application submits as a brand-new transaction enters Teranode here, and this is the service
that decides what to tell the submitter back.

It is also, on its own, a small lesson in API design: most of its difficult code is not about
transactions at all. It is about what a promise means, and how to keep different kinds of
promise distinguishable when a hundred moving parts are involved.

#### The problem this removes

A business handing in payments needs three things from a node, and they pull against each
other.

It needs to hand transactions in **quickly**, in bulk, without waiting for each one to be
checked before sending the next. It needs to know **what happened** to each one — not
yesterday, now, and not in a log file on a machine it cannot see. And it needs the answer to
be **honest about whether trying again will help**, because an automated pipeline that retries
a payment which can never succeed will retry it until the sun goes out, and an automated
pipeline that gives up on a payment which only needed a moment will lose money.

Meanwhile the node has its own requirements: refuse junk as cheaply as possible, never let one
loud customer starve the others, and never tell an outsider more about the inside of the
machine than it needs to know.

#### How it works

**Four ways in.** A submitter can arrive over gRPC, over plain HTTP, over a high-speed local
network broadcast channel, or — most commonly in a cluster — through the Asset Server, which
accepts a posted transaction on its own public address and forwards it here. That last route
is worth pausing on: the Asset Server is the read interface, the place you go to *ask* for
something, and its one write route is handed across the corridor. From outside, one door; in
side, two services, and the submitter never needs to know.

**What happens to a transaction.** It is checked for size against the policy limit; parsed;
checked for a handful of instant disqualifications; written to the blob store; and then put on
the conveyor belt to the Validator service. The client side of that hand-off usually batches:
a submitter's transactions ride along in groups of up to a hundred, or go as soon as a
timeout of a few milliseconds expires, whichever comes first. That is the mechanism that turns
a stream of individual submissions into a trickle of large messages.

**There is no mempool service.** This surprises people, because every description of Bitcoin
mentions one. In Teranode, a transaction that has been accepted but not yet mined is held as
stored data plus a note in the UTXO store that its coins are unmined, and Block Assembly
assembles it into a bundle when it builds a block. So the word "mempool" in this guide's
glossary describes a *state a transaction can be in*, not a place it sits.

#### The hard parts

**An acknowledgement is not a verdict.**

In the production shape of this system, a normal-sized transaction is published to the
conveyor belt and the submitter is told "accepted" **before the Validator has looked at
it**. The reply means *I have it, and I have kept it* — not *it is valid*. If the transaction
later fails validation, that discovery happens somewhere else, and reaches the submitter,
insofar as it reaches them, by another channel entirely.

Two paths behave differently, and both are unusual ones: the oversized transaction that
cannot fit on the conveyor belt and is sent to the Validator directly, and the
no-conveyor-belt development configuration. Those two wait for the real answer, and are the
only paths that can give a reason.

The consequence for anyone integrating against this is worth stating flatly: the
information-rich path is the path taken by unusually large transactions. A test harness that
submits one big transaction and watches the helpful error message will conclude the API
explains itself, and then go live on a path that says nothing but "accepted". Which path a
given deployment uses is a configuration matter, recorded in the reconciliation notes.

**Some refusals must be retried and some must never be.**

So the service maintains a careful table mapping a refusal to the kind of refusal it was:

- Already-known transaction → **success**, not failure. Resending something the node already
    has is an idempotent act, and reporting it as an error teaches clients to fear their own
    retry logic.
- Frozen output → forbidden.
- Double spend, conflicting spend, coins already spent, coins still being settled →
    **conflict**.
- A parent transaction is missing → unprocessable, which is the most actionable thing a
    submitter can be told: send the parent, then send this.
- The queue behind it is full → **service unavailable**, deliberately retryable, because the
    transaction is already durably stored; resubmitting merely re-drives the hand-off rather
    than starting the work again.
- Bad format, failed policy, lock time not reached → **bad request**, which means: this one
    will never work, stop sending it.

Two of these deserve a moment. "Already known" being a success is what makes a customer's
retry loop safe: a submitter that lost the connection while sending a payment can resend with
impunity, and "I already have that" arrives as a non-event rather than a failure it has to
explain to its own logs. And "queue full" being retryable is a judgement about where the work
was lost — the transaction is durably stored by the time this answer is given, so a retry
re-drives the hand-off instead of repeating expensive work.

The bug that produced this table is recorded in the code, and it is a good one. A policy
rejection arrived from the Validator wrapped in a generic internal-error type, which sorted as
a retryable server error. So a **permanently invalid transaction was reported as "try again
later"** — a customer's retry queue filling with payments that could never succeed, and both
sides paying to shuffle them. The fix also stopped the Validator's internal error chain —
source file, line number, function name — being pasted into a message sent to the public.

**A plain-text answer cannot be judged.**

The reason that bug was possible: when one internal service posts to another over HTTP, the
failure arrives as a body of text. Text has no category. The receiving service then has two
choices, and both are bad — re-parse somebody else's error string, or wrap the whole thing in
an error of its own choosing, which is what happened, and which is how "invalid transaction"
became "try later".

The fix is a dedicated header carrying the *public* verdict — code and message only — from
one service to the next, so the last service before the customer can pass the real answer on
instead of inventing one. Four details make it safe rather than clever:

- Only a short allowlist of verdicts may travel in it, and each on the list meets the same
    three tests: it is a judgement about the submitted transaction, it exposes no internal
    state of this node, and the submitter can act on it. Anything not on the list collapses
    to a blunt outer code. A new kind of internal failure therefore cannot leak by accident.
- The header is bounded to four kilobytes, because many servers cap a single header at eight
    and discard the whole message when it is exceeded. A truncated explanation beats a lost
    one.
- An older peer that does not set the header behaves exactly as it did before, so the change
    can be rolled out in either order.
- The header survives the hop that matters. The asset server forwards a posted transaction to
    Propagation with an ordinary reverse proxy, which copies the backend's response headers
    through, so a verdict raised at the far end of the corridor still reaches the customer who
    asked.

Note the deliberate asymmetry the comment records: more error kinds are allowed to influence
an HTTP status code than are allowed to carry a message to a customer. A status sorts a
request into a coarse bucket; a sentence is held to a higher bar.

**When a hundred go in and three fail, say which three, in order.**

The bulk endpoint reads a stream of transactions from one request body — up to a thousand and
twenty-four of them, or thirty-two megabytes — and processes them in parallel. Parallel
processing has an obvious hazard for a customer: work number 7 may finish before work number
2, so a reply that lists failures in the order they were discovered is useless for matching a
failure back to the payment that caused it.

The answer is a pre-allocated row of slots, one per submission position, each worker writing
only to its own slot, and the list assembled by walking the slots in order afterwards. The
slot row is made at full width and never resized, because resizing a shared list while
workers are writing to it is precisely the race that produces silently wrong answers.

The same discipline shows up in a smaller place, and it is the sort of thing that only matters
at volume: alongside the failures sits a row of pointers to the transactions themselves, so a
failure can name its transaction. The transaction's fingerprint — which costs a double hash of
the whole serialised transaction — is calculated *only for the slots that failed*. Ninety-seven
successful transactions pay nothing for an explanation nobody asked for.

**Do the cheap test first, always.**

A stranger can push junk at this door all day, and every rejection should cost almost nothing
before it costs something:

- Size is compared against the policy limit **before parsing**, so an oversized submission
    never pays for the arithmetic of reading it.
- On the broadcast lane, the source address is checked against the allowlist **before
    parsing**, so an unauthorised packet costs one address comparison.
- A transaction's fingerprint is computed once, at the top, and cached on the transaction.
    Without that, every later use re-serialises and double-hashes it — the coinbase check, the
    storage key, the conveyor-belt key, every error message, and one debug log line whose text
    is built whether or not debug logging is switched on. A message you have turned off still
    pays for its own handwriting.
- Instant disqualifications — no inputs, no outputs, the same coin spent twice within one
    transaction, a coinbase transaction — are settled locally, before any lookup in the coin
    ledger.

**A full queue and an oversized message are different problems wearing one badge.**

When a transaction is too large for the message transport, the client retries it over HTTP,
which fixes it. The trouble is that the transport reports "your message is too big" with the
same generic code that block assembly uses to report "my incoming queue is full". Retry the
second one and you have just thrown a full round of validation work at a machine that announced
it is saturated — the feedback loop that turns a congestion dip into a collapse.

So the client checks for the size problem specifically, and says out loud when it declines to
retry: "block assembly shed the transaction (queue full); not retrying over HTTP". And the
code is candid that today the server relabels a shed as yet another sort of error before it
reaches the client, so this check is not fixing a live bug — it is nailing the client's
behaviour down so that when the server is changed, this mistake does not have to be made a
second time.

**Two size limits, and a fork between them.**

There is the limit above which a transaction is refused outright, which is a commercial and
consensus rule. Then there is a lower limit, the largest message the conveyor belt will carry,
which is a property of plumbing. Transactions between the two are perfectly acceptable, but
cannot ride the normal route.

Rather than lower the commercial limit to match the plumbing — which would be letting a
message queue set the price of a product feature — the service forks: small transactions go on
the belt, larger ones go to the Validator over HTTP and wait for the answer, and the
configuration without a belt at all goes the same way. It is also the fork that produces the
acknowledgement asymmetry above, which is the honest cost of it.

**One transaction per packet, and a window half a kilobyte wide.**

For a machine room full of nodes that all need to hear every transaction, the broadcast
channel is deliberately crude: a single transaction per packet, no conversation, no
acknowledgement.

The reason given is a read pattern rather than a network one. Batching ten transactions into
one packet would mean that to find any one transaction later you would have to keep the
packets and look inside them — a database lookup to recover something you already have stored.
One packet each, and the stored copy is the only copy that matters.

The window is narrow: the receive buffer is five hundred and twelve bytes. A transaction too
large for it arrives shortened, fails to be read as a transaction, and is dropped with a line
in the log. There is no counter for this, so on a busy machine the only sign that the fast lane
is losing traffic is a warning among other warnings. That is a reasonable trade for a lane
intended for small, ordinary transactions inside a trusted network — but the trust part is
real. The source allowlist is optional, and if it is left unset the service says so in the log
and accepts from anywhere on that network.

This lane is switched off in the shipped configuration; you have to name a broadcast address
to turn it on. Which is the right way round for a feature whose security model is "the people
who can put packets on this network are the people I trust".

**Two limits on concurrency, not one.**

The bulk endpoint refuses work rather than queueing it: if too many batch requests are being
handled, a new one is turned away immediately with "server at capacity", and told it is a
temporary condition. Refusing costs the caller one round trip; queueing costs memory and the
latency of everyone behind it. Those refusals are counted, which is worth noticing as a matter
of housekeeping — the failures a customer can see are instrumented, and the ones only this
machine could see, like the dropped broadcast packets above, are not.

Behind that first door is a second, wider limit on the total number of per-transaction workers
*across all* batch requests. The reason is arithmetic: one limit on batches, and a caller can
send fifty batches of a hundred transactions and ask for five thousand workers, because the
first limit counted the wrong thing.

And a batch must not let one bad transaction cancel its neighbours. The parallel-work helper
used here treats a returned error as "stop everything else" — so each worker records its
failure in its own slot and always reports success to the helper. Otherwise one malformed
transaction in a batch of a hundred silently aborts the ninety-nine that were fine, and the
symptom is a customer whose bulk submission half-fails with no error that explains itself.

**Banning happens at the door, not inside the room.**

The ban list is installed as a filter on both network interfaces, ahead of the handlers. A
banned address is refused before its submission is parsed, stored, or counted — which is the
only order of operations that makes banning worth anything under load.

#### What it produces

A stored transaction in the blob store, a message on the Validator's conveyor belt (or a
synchronous validation, on the two paths that take it), and a reply to the submitter whose
exact meaning depends on which path the transaction took — which is the single most important
thing for an integrator to understand about this service.

#### Scaling

On the design question this is close to the ideal shape: it keeps almost no private state of
its own — transaction bytes go to the blob store, validation goes elsewhere — and it is
reachable at an address, so adding another instance means pointing more submitters at it. Its
limits are per-instance ones: worker ceilings, batch admission, and the fact that the broadcast
lane only exists where someone has configured a network for it.

On the shipped question, both example cluster configurations run **zero** instances of this
service, while the Asset Server's transaction-forwarding switch stays on. So the door is
advertised and the room behind it is empty: a submitter posting a transaction to the public
address is forwarded to a service that is not deployed, and gets back "bad gateway" — loudly
recorded under its own counter, which is the correct behaviour for a door that is open onto
nothing. The read side of that same address answers normally, which is what makes the gap easy
to miss from outside. Whether this reflects a deployment that takes transactions some other
way, or an example that has drifted, is recorded in the reconciliation notes as an open
question — it is the one finding here that a customer would notice without reading any code.

### Service: Validator

#### What it does

It answers one question about one transaction: *is this legal, right now, on this chain?* And
it turns the answer into paperwork another service can act on — the receipt this guide has
been calling a txmeta message.

It is also the only place in the whole system where money is created or destroyed. Every other
service moves paperwork, files it, or forwards it. This is the service that reads the coins a
transaction claims to spend, proves they exist and add up, marks them gone, and puts new coins
in their place. When the guide says a transaction "entered circulation", this is the moment it
happened.

#### The problem this removes

Two problems, and the second is the one that shaped the code.

The first is throughput. Checking a transaction means running its scripts — small programs
attached to each coin, proving the owner authorised the spend. On one machine, script checking
is a hard ceiling on how many transactions the whole network behind it can accept. The obvious
answer is to run more machines doing it, and that mostly works, which is why this is the one
service the mainnet example runs eight copies of.

The second is harder: validation is not reading a rulebook, it is reading a **ledger that
everyone is writing to at the same time**. Has this coin been spent? What time does its height
imply? Which chain is it on? Two submissions that spend the same coin a millisecond apart must
not both be told yes, and the answer has to survive the node being busy, restarting, or
catching up from behind. Most of what follows is the cost of that second problem.

There is a third, quieter one. The rules were written down decades ago and implemented in
somebody else's C++ library, which this service still uses for script checking. Everything
*around* the scripts — heights, locktime clocks, coin lookups, ordering — is re-implemented
here. A large share of the difficulty is simply staying byte-for-byte faithful to the
reference behaviour on the awkward cases, and the code records each one.

#### How it works

**Arrive.** A transaction comes from Propagation's conveyor belt, from a direct call by another
service, or from an HTTP post. Which route it took matters more than the transaction itself,
because the route decides how much may be skipped and what happens on failure.

**Settle the cheap stuff.** Coinbase transactions are refused immediately, before anything is
looked up, matching the reference implementation's order. Fast options are checked for
coherence: the fastest mode is only legal in the part of the chain where it is safe, and
refusing there costs nothing.

**Fetch the history.** For each input, the coin being spent is read from the coin ledger — one
batched read per transaction rather than one per input, and on the catch-up path whole
bundles of parents arrive pre-fetched by the caller.

**Overwrite what the submitter claimed.** Every input's coin data is replaced with this node's
own copy. This is not a paranoia measure; see the first hard part.

**Run the rules.** Go-side checks first: value ranges, inflation, the total weight of coin
history a transaction drags along, and policy. Then the script library executes the scripts and
signatures. Then relative locktimes (BIP68), which need to know the median timestamp of the
block each spent coin was created in — served from a pre-loaded table, not fetched per input.

**Write, hand off, unlock.** The spent coins are marked spent and the new coins created in a
state I will call *held*, then a summary goes to Block Assembly, and only then are the new
coins released. That pause is deliberate and is explained below.

**Publish the receipt.** Either way — accepted or refused — a receipt goes on the conveyor belt
that other services, and the legacy door, read from.

#### The hard parts

**Never believe a submitted coin.**

A transaction names the coins it spends, and it is allowed to *carry along* what it says those
coins look like: the unlocking conditions and the amount. The tempting optimisation is to trust
that and skip the lookup. The cost of trusting it was a published security advisory, and the
mechanism is beautiful in the worst way.

The commitment stored against a coin is a fingerprint of its unlocking conditions followed by
its amount. Nothing in that recipe marks where one ends and the other begins. So a shorter
condition string paired with a bigger amount can produce **exactly the same fingerprint** as the
real coin. A submitter could present a trivially-satisfiable condition and an inflated amount
against a coin that genuinely could not be spent that way, and pass the check while creating
money. The fix is not to validate the claim better. It is to stop reading it: every input is
overwritten from this node's own stored copy, unconditionally, on every path.

Translated for anyone who has ever accepted a reimbursement claim: the paperwork matched. That
was never evidence the claim was real.

**Who is allowed to ask for a shortcut.**

Validation has a set of switches — skip the scripts, skip the policy checks, spend a coin by
reference without proving it, treat this transaction as having arrived inside a block. Each one
is legitimate somewhere and dangerous in the wrong place, and they arrive over the network from
callers.

The HTTP submission route is unauthenticated: no credentials are checked. So in principle
anyone who can reach it could ask for the fast path — including the fast path that skips script
checking, which is meant to be reserved for ancient history the node has already been told to
trust. Worse, the height the transaction is validated against was itself supplied by the
caller, and that is the lever: assert a low enough height and the "we are below the checkpoint"
guard is satisfied by construction, so the shortcut unlocks on a live transaction.

Three separate layers now close that:

- The HTTP route accepts transaction bytes **and nothing else**. A request carrying any
    behaviour-changing switch is refused with a message naming the field, and the gRPC route is
    named as where it belongs.
- The client refuses those requests before sending them, so this version of the software is not
    itself a route for the flags regardless of what the far end accepts.
- Independently of both, the fast path is checked against **this node's own chain tip**, not the
    height in the request. The comment is explicit about why the earlier guard was not enough:
    it bounded the height the caller asserted, and an attacker simply asserts a low one.

Two honest caveats are recorded with the fix, and both matter more than the fix. The gRPC route
is *also* unauthenticated on the shipped profile, so the guard "removes the HTTP route rather
than removing the capability from unauthenticated callers" — the real boundary is which network
the address is reachable from, not this code. And the client-side refusal only stops this
client; the property against an arbitrary caller lands when the far end is upgraded, because an
older validator still honours the old query string for anyone who asks.

There is a maintenance contract attached, which is how this class of bug stays closed: every
field that can change validation behaviour must be listed in the guard, and a new field added
to the request without a listing becomes a silent hole.

**When the larger door cannot carry the luggage.**

Oversized transactions cannot fit the message transport, so the client falls back to HTTP. But
the HTTP route has just been made to accept transaction bytes only. The collision is resolved
by refusing rather than quietly dropping the switches: "cannot be sent over the HTTP
fallback". Silently shedding a *skip script validation* flag because the packet was too big
would be a much worse failure than an error.

**A promise made before the verdict was in.**

Propagation answers "accepted" the moment the transaction is durably stored and on its way to
this service. Everything below is what that promise costs.

Handing the transaction to Block Assembly can fail with *queue full*. On the conveyor-belt
route that failure is retried in place, for two seconds, because moving the queue position
forward past a transaction that never reached a mining template would lose it. If the retry
window runs out, the transaction is dropped — and it is dropped **silently**, because the
submitter was told "accepted" several services ago. The code names the one counter to alert
on for exactly this, and says plainly that from the client's side the drop is invisible.

The block-shaped version of the same event is more interesting: a valid transaction from a
block, accepted, spendable, correct — and simply absent from this node's mining template until
somebody reloads the pending set, which nothing does on its own. A counter exists so an
operator can see that a reset would recover transactions this node is otherwise silently failing
to mine. That is a revenue leak, not a correctness bug, and it is measured as such.

**Undoing it in the right order.**

When the hand-off finally fails, the work already done has to be reversed: the coins this call
spent must be freed, the records it created removed. The goal is that a failed hand-off leaves
no trace, so a resubmission is an ordinary first submission rather than an "already have that"
success for something that reached no block. About a hundred and seventy lines of comment sit
behind a hundred and forty lines of code here, because the order matters:

- **Remove the record, then free the coins.** The reverse order leaves the record existing and
    locked while its coins are free — and in that window a competing double-spend can take those
    coins while the surviving record can still be lifted into a mining template. That produces an
    invalid block, which the rest of the network sees. The chosen order's failure mode is
    coins temporarily unspendable on this node, recoverable from a log line that names every
    one. Consensus-visible versus locally annoying is the whole decision.
- **Check that the delete actually deleted.** The coin store is reached through an interface,
    and a decorator in front of it — a caching layer whose delete only touches its own cache is
    the example given — could report success while the record is still in the store. Trusting
    that would turn the safe failure into the dangerous one *while believing it worked*. So the
    record is read back, and anything other than "genuinely gone" stops the unspend. The comment
    is careful to say this is a general guard, one that holds for decorators not yet written.
- **Read it back twice, for two different questions.** The first read asks whether the delete
    reached the record. The second, immediately before freeing the coins, asks whether the record
    has come *back* — deletes leave no trace behind, so a concurrent resubmission of the same
    transaction recreates it, and its coins look identical to this call's own.
- **Remove the master record first, then its attachments.** Only the master can be lifted into a
    mining template, so it must be the first thing to go. The reverse leaves a master whose
    outputs live on attachments that no longer exist: a transaction this node will happily mine,
    and then be unable to serve most of the outputs of. The residue the chosen order can leave —
    orphaned attachments, locked and unreachable — is accepted with a phrase worth keeping:
    unreachable residue over wrongly reachable residue.
- **An inconclusive read fails closed.** A failed read proves neither that the record survived
    nor that it is gone, and the two ways to guess have different worst cases: freeing anyway
    can hand a double-spend a live transaction's coins, which the network sees; aborting leaves
    coins unspendable on this node until an operator acts on the log. Under uncertainty, take the
    branch whose worst case is local. The comment adds the line that makes it a principle rather
    than a judgement call: *that does not change because the probability shifts.* Only the size
    of the window changes, so the read is retried a few times first.

And the residual is stated rather than smoothed over: nothing serialises this against a
concurrent submission of the *same* transaction, so a duplicate arriving mid-flight is told
"accepted", performs no hand-off of its own, and then has its record deleted by this unwind.
Nothing resubmits it. There is a counter for it, and closing it properly needs per-transaction
serialisation this does not have.

**Pressure that reaches backwards.**

This service sits in front of Block Assembly's queue. When that queue backs up, the clean
options are bad: keep up and watch the queue grow without bound, or let the queue reject work
and throw away transactions that Propagation has already promised.

So the validator watches the queue and, when it gets hot, **stops fetching from the conveyor
belt** — pushing the wait back into the durable log, where bursts are cheap, rather than
letting them arrive and be shed. The detail is in what "hot" means:

- It measures **how long the item at the head has been waiting**, not how many items are
    queued. Age is what hurts a customer.
- It pauses at one watermark (half a second) and resumes at a lower one (a tenth of a second),
    because pausing and resuming at the same number makes a queue oscillate.
- The head's age is discounted by the window Block Assembly itself says it holds back for, so a
    healthy hold-back does not read as a stall. That discount is a trust boundary: an absurd
    reported value could invert the decision, so a negative arrives as zero, and if the two
    processes disagree the log names the setting to align.
- **Age alone is blind in exactly one regime**, and the comment spells it out: a burst arriving
    faster than the drain rate pins the queue at its cap, yet each batch only waits for the next
    drain pass, so head age stays calm while the queue rejects every call it receives — the
    precise situation the durable log exists to absorb. So queue fullness is a second trigger.
    Pausing takes **either** signal; resuming requires **both** to be calm. A genuine drain means
    both, everywhere that phrase is used, because an age-only test would be satisfied one tick
    after a fail-open and immediately re-pause — the busy-toggling the whole cooldown exists to
    prevent.
- Every pause is bounded (thirty seconds), and every resume that was *not* an observed drain — a
    timeout, or the signal going dark after three failed reads — is a fail-open that arms a
    cooldown proportional to how long it was paused. An earlier revision suppressed
    backpressure for a full pause window, which on an unbounded queue meant thirty seconds of
    growth toward the memory exhaustion the feature exists to avoid.
- The probe itself must not cause the thing it measures: the queue statistics are served from
    atomic counters precisely so the read cannot touch the processing loop.

In the shipped configuration none of this runs: the controller is switched off and the queue it
monitors is unbounded, which also means nothing is ever shed. Two switches have to be set
together, and the comment says so.

**A coin's history has to be cheap.**

Relative locktime rules need the median timestamp of the block each spent coin came from. Done
literally that is a network round trip per input per transaction, on the hottest path in the
system. Instead the validator pre-loads a run of those medians once per block, and afterwards
every lookup in every parallel worker is a read of a shared table rather than a round trip. The
load overlaps the previous range and corrects values that disagree in place, which is what a
small reorganisation requires. If the pre-load was skipped, the lookup reports an error naming
the step that was missed rather than inventing a timestamp.

**Skipping the script engine means inheriting its duties.**

Below the newest hard-coded checkpoint, skipping script execution is a legitimate and large
saving: proof-of-work plus checkpoint linkage has already established that stretch of chain.
But the script engine was not only checking scripts — it also enforced that no value is out of
range and that inputs do not exceed outputs. Skip it and those invariants silently vanish, so
they are re-implemented on this side, in the same era-dependent order the engine uses, because
the order determines which error a transaction that fails both checks reports.

Beneath that is a small integer trap worth a paragraph. A parent transaction that has not been
mined yet is marked with a sentinel height meaning "no height". Under 32-bit arithmetic that
sentinel wrapped around to minus one and slipped through a comparison unnoticed; under the
wider arithmetic this code now uses it stays a very large number and gets the transaction
rejected — with a generic relative-locktime error that would send an engineer to the wrong
place. So the sentinel is now rejected explicitly and early, to produce the true reason instead
of a technically defensible lie.

**Refusing to accept an ambiguous hand-off deadline.**

Each attempt to hand a transaction to Block Assembly runs under a deadline. Set that deadline
shorter than the queue-wait Block Assembly itself promises and something unpleasant happens:
every queue-full rejection arrives here as a cancellation instead, the code that reverses a
rejection never runs, and the transaction is left stranded in the *held* state. So the floor for
a single attempt is the remote's wait plus a round-trip allowance — and when that conflicts with
the retry window the code says it should advertise, the floor wins, because losing the rejection
classification is a correctness failure while overshooting a time budget is a bounded resource
overshoot.

The honest wrinkle: the wait value used here is this process's **copy** of a setting that belongs
to another service, and the two configuration contexts are independent. That is acceptable
because it only sizes a safety margin and never drives a decision — which is exactly why the
conveyor-belt controller *does* ask the other process to report its own number, since a wrong
value there could invert a control decision. If this copy under-reports, the margin is short and
the failure degrades to a stranded-then-recovered transaction, counted, never to a wrong
reversal. A separate counter exists whose whole purpose is to point at that configuration skew.

**Counters that stay useful.**

The failure paths of this service carry their own family of counters, and the interesting
engineering is that they are deliberately *not* merged. Two of them describe a record reappearing
during a reversal and a record that was there all along; folding them together, in the code's
words, would destroy the first as a signal that per-transaction serialisation is needed in this
deployment. Another one counts resubmissions that found a transaction held and unmined — a state
the store cannot tell apart between an in-flight conflict resolution and an interrupted
submission — and its purpose is stated as collecting the field data that should decide whether
that state ever needs a real behavioural answer. The instrumentation is written to answer a
question, which is the only way it stays worth reading.

#### What it produces

A receipt for or against the transaction, published on the conveyor belt for everyone
downstream — including the flag saying whether it arrived inside a block, which is what stops
the node re-announcing a transaction it merely heard about inside somebody else's block. Coin
ledger updates. A queue entry for Block Assembly. A verdict for the caller, on the routes that
still have a caller.

And one thing that is easy to miss: **provenance is stated, never inferred.** Whether a
transaction came from a block could be guessed from the fact that policy checks were skipped —
but external submitters are allowed to ask for that skip on genuinely fresh transactions, so
guessing would mark submitted transactions as already-mined and quietly suppress their relay.
It is carried explicitly for that reason.

#### Scaling

On the design question this is the best-shaped service in the system: it holds almost no state
of its own, everything durable lives in the shared stores, and transactions can be spread
across instances by transaction. It is the one component the mainnet example scales out —
eight copies, against a single configured replica for each of the thirteen others. The general
example, oddly, does not run it at all.

On the shipped question, that is the whole picture: eight copies in one example file, none in
the other. Nothing here is switched on by default.

And the honest limit, which the code itself keeps circling: spreading validation across eight
machines does not remove the bottleneck, it moves it into the shared coin ledger, which all
eight read and write through the same store. Worse, the one case the store genuinely cannot
distinguish is two concurrent submissions of the *same* transaction — its ownership record holds
the transaction's identifier and which output of it is being spent, so two copies of one
transaction look identical. That is why the residual window in the reversal logic above is
measured and named instead of argued away. Sharding by transaction is sound. Sharding the same
twice is not, and the design knows it.

### Service: Block Assembly

#### What it does

It holds everything the network has been told about and has not yet put in a block, and it
turns that moving pile into a block a miner can actually work on.

It is also, despite the modest name, the thing the rest of the system calls a mempool. There is
no mempool service in Teranode; this service *is* the pending set — the queue it arrives
through, the dependency structure it is organised into, the disk files that let the pile grow
bigger than memory, and the reload path that rebuilds it after a restart. When this guide says
a transaction is "waiting to be mined", this is the room it is waiting in.

It is the service a miner talks to: it offers a candidate block, and takes back a solution to
be published.

#### The problem this removes

Mining puts two demands on this service that pull directly against each other.

The first is speed. A block takes the transactions that are worth taking, and every moment the
candidate is stale is revenue forgone: someone else's block gets the fees that were on offer a
second ago. Under load this service is handed transactions faster than it can organise them.

The second is caution. A block is not paid for transaction by transaction — a block containing
one bad transaction pays nothing. So the assembler must not grab a transaction that might be
about to be contradicted, must not lose track of a coin it already committed to somebody else,
and must not fall over at the exact moment the machine is busiest, which is when it is most
likely to.

On top of that, the pending set is big, and it has to survive a restart. A node that loses its
pending transactions when it restarts does not lose money that exists — it loses money that was
*promised*, and by then the customer has been told "accepted".

#### How it works

**Thin intake.** When the Validator finishes with a transaction it hands this service a summary:
the transaction's identifier, its fee, its size, and which coins it spends. Not the transaction
itself — the bytes stay in the blob store, once. This service works with receipts all the way
through, and only the bytes for the block actually being built get fetched.

**A queue with a waiting rule.** Summaries land in a lock-free ingest queue as batches. Each
batch carries an eligibility time, and the dequeue step will not hand over a batch until it is
old enough. That "old enough" rule is where a lot of the interesting behaviour lives.

**Organisation into bundles.** Transactions are grouped into the same power-of-two bundles used
everywhere else in the system, each transaction filed at the position in the bundle tree that
belongs to it. That structure is what makes a block buildable in pieces: the tree hashes that
block validation will want are already implied by the arrangement, and the fee total for the
block is collected from the bundles that went into it rather than re-added from scratch every
time a miner asks.

**Offering a candidate.** A miner asking to work gets a deliberately small package: the header
fields it needs, the reward on offer (block subsidy plus the fees in the candidate), the number
of transactions and their size, the identifiers of the bundles included, and a merkle proof. The
proof is the point — it lets a miner attach its own coinbase transaction and complete the block
without the node shipping the whole block across the network on every request. There is a second
route for whoever does want the complete block. A solution comes back through a submission
queue, which is deliberately built to fail fast during shutdown rather than block forever on a
hand-off whose other side has already gone away. The completed block then goes onward for
validation and announcement, and this service waits for the confirmation that it landed before
it clears those transactions out of the pending set.

**After a block lands.** Everything that made it into the winning block is removed from the
pending set and marked genuinely mined in the coin ledger. Everything else stays, and the
bundles are re-arranged against the new tip. If the new tip is a reorganisation, work is undone
and redone rather than assumed.

#### The hard parts

**The queue is a quarantine, not a buffer.**

The reason a batch has to sit and be a certain age before it can be mined is not buffering
capacity. It is that a transaction spending a particular coin can be contradicted by another
transaction spending the same coin, arriving a moment later over a different route. Holding a
transaction back for a short, configurable window — the double-spend window — gives that
competitor time to turn up while the transaction is still cheap to drop, instead of after it is
inside a block that peers will reject.

That is the theory. In the shipped configuration the window is zero, so nothing is held back
and a transaction is eligible the instant it arrives. The protection is opt-in, and it trades
directly against the first demand: a miner who sets a one-second window is paying one second of
candidate staleness per block for the chance of dodging a mined double spend.

The window has a second, less obvious job, described under the next heading — which is where it
stopped being theoretical.

**A drain that must not chase its own tail.**

When a block lands, the pending structure has to be rebuilt: everything in the queue is drained
into it. That drain has to be finite. If it simply takes everything, and transactions keep
arriving at line rate while it is working, it will keep taking those too — the drain never
ends, and the service never gets back to offering a block. This is a failure that only happens
under load, which is the worst possible time to discover it.

So the drain is bounded twice, and the code keeps a memory of the incident where both bounds
failed at once. The old form had a time bound and an item bound:

- The time bound was supposed to be "everything that existed when I started". But when the
    double-spend window was zero, the value used for it was also zero, and zero means "no time
    filter" to the queue — so the bound quietly switched itself off. In the shipped default
    configuration, it is switched off today.
- The item bound counted the wrong thing. It compared batches processed against a queue length
    measured in *transactions*, and a batch holds on the order of a thousand transactions. So
    the drain was permitted to do roughly a thousand times the work intended before the bound
    bit.

With ingest at line rate and neither bound firing, the process sat in that loop for over thirty
five minutes at 558 gigabytes of memory. The fix has three parts worth naming, because each one
is a general lesson: take the clock reading **once** (two readings would let batches arrive in
the gap and be swept into "already existed"), count items rather than batches, and dequeue as
you go — which also matters for the next point.

**Depth lies in one direction and tells the truth in the other.**

This service's queue statistics are read by the Validator, which uses them to decide whether to
stop fetching transactions from the conveyor belt. Two claims about those numbers sit in the
code a few lines apart, and they look like contradictions until you notice they answer different
questions.

For "should upstream stop sending", queue depth on its own is useless: a large but falling queue
needs no intervention, while a small but motionless one is an emergency. What is usable is depth
as a *fraction* of the ceiling, alongside how long the item at the head has been waiting —
because head age is blind to exactly the case where a queue is pinned at its ceiling while every
batch still waits only until the next drain pass.

For "is the consumer stuck", depth is worse than useless — it is actively misleading. In the
incident on record, the blocked handler was draining the queue from *inside* the branch that was
blocked, so a check that looked at depth would have seen a low number and concluded everything
was fine while the loop was wedged. So the stall detector deliberately ignores depth and keys on
staleness alone: has the consumer touched its dequeue step recently, whatever the queue holds.

One queue, two monitors, two different signal sets, and the reasoning behind each written down
where someone under pressure will read it. The stall threshold itself is deliberately *not*
configurable — thirty seconds, with the severity split so that cases where thirty seconds is
arguably routine report quietly rather than paging anyone. The comment says the guess is what the
new companion measurement exists to avoid: if the numbers show warnings on ordinary blocks, that
is the evidence for making it a setting.

**Numbers owned by another process are reported, not guessed.**

The queue statistics are served as a named structure holding four values, and the reason given
for the structure is that two of them are durations with completely different meanings — as
adjacent return values they would be silently swappable at every call site forever. Naming them
costs one type declaration and deletes a whole class of bug.

The ceiling and the window are reported by the process that *enforces* them rather than read
from the reader's own configuration, because the two processes read independent settings files
and cannot be assumed to agree. A reported ceiling of zero or less means "there is no fill
signal", not "the capacity is zero" — an easy mistake to make, and the difference between
ignoring a signal and acting on a nonsense one. This is the same two-file trap the guide hits
elsewhere, and this service is where the mitigation is spelled out.

**A restart, a timestamp column, and a vanished family of transactions.**

On restart, the pending set is reloaded from the coin ledger, sorted oldest-first by the time
each transaction was created, and re-validated — including a rule that a parent must come before
its child, with anything violating it dropped along with everything downstream of it.

Here is the trap. That creation time is stamped per transaction, with no enforced gap, and its
resolution depends on which store you are using: milliseconds on Aerospike, **one second** on
SQLite, because that is what its default timestamp function emits. A parent and its child
submitted together therefore routinely share the same value — on one store often, on the other
nearly always. A sort that is not stable is then free to emit the child first, the parent-chain
rule calls that invalid, and the child plus every descendant is dropped. The transactions are
not rejected with a reason anyone can see; they simply stop being candidates. Same code, same
inputs, and whether you get bitten depends on the store underneath.

The fix is a stable sort — so ties keep the order they arrived in — followed by a pass that
reorders *only* the runs of identical timestamps so parents precede children inside each run,
leaving unrelated transactions untouched. Two details make it safe: the pass knows that only
same-timestamp transactions can be misordered, and it preserves relative order for anything
without a parent relationship in the run, so the incoming order is not silently reshuffled.

**Telling a wedged loop from an idle one.**

The main listener loop for this service spends its life waiting: waiting for a block
notification, a miner's request, a reset. A loop that is healthy and a loop that has stopped
can both look exactly like a process doing nothing. So the loop beats a heartbeat on every pass
through its wait, and liveness asks when the last beat was.

The trap is configuration, and the code refuses to leave it to the operator: set the stall
timeout close to the heartbeat interval and the check cannot distinguish "waiting normally" from
"stuck", so a stall timeout within twice the tick is reported as too tight. And the ticker that
implements the interval panics on a non-positive value — from inside a goroutine, which takes the
process down rather than failing tidily — so a zero interval falls back to five seconds instead
of trusting every construction path to have set one.

**A timestamp with two rules squeezing from opposite directions.**

The time written into a new block is governed by two consensus rules at once: it must be later
than the median timestamp of the recent parent chain, and it must be no more than two hours in
the future. Normally the wall clock sits comfortably between them, and nobody thinks about it.

But this service does the arithmetic rather than hoping, because the window can close. If this
node's clock is running behind the network, the floor moves past where its own clock is. If an
upstream miner has been stamping blocks slightly in the future, the floor moves too. Once the
floor sits above the ceiling, **no legal timestamp exists** — and block production stops, not
slows. The error says so and names both candidate causes in the order an operator should check
them: a local clock at least this many seconds behind, or a future-stamped parent chain.

There is also a quieter branch for chains where the median rule is treated as advisory: in that
case the two-hour bound wins and the wall clock is served, with a warning, rather than stopping.
Getting this wrong in either direction is expensive in a way that is invisible until it isn't — a
timestamp too early makes the block invalid to peers, a timestamp too far ahead makes it
unacceptable either, and a node that stops mining is simply not earning.

**The pending set is bigger than memory.**

The structure that maps a transaction to the coins it spends is the large thing on the heap, and
it has been moved off the heap: an **exact** index stays in RAM — a hash, where its payload
lives on disk, and which bundle it belongs to, about eighty bytes an entry — while the payloads
themselves go into append-only log files. The common operations, "have I seen this transaction
before" and "which bundle is it in", are answered by the index alone and never touch disk at
all. Payloads are spread across sixty-four segments per directory so parallel workers are not
queuing behind each other on one file.

There is a rotation trick underneath it worth understanding, because it is where the safety
comes from. The data is kept in two generations and the processor recycles one half at a time.
Nothing is compacted in place: deleting a transaction just forgets the hash, and adding one
appends. A reader **pins** the generation its entries point into, and a rotation or a close
waits for those pins before unmapping — which is what lets a background writer read from one
half while the processor rotates the other.

The cost is stated plainly rather than hidden: the operations on this map have no error return,
so the first storage failure since the last check is recorded quietly and collected separately by
the code that owns it. That is a deliberate trade — a simpler, faster API for a hot path — and it
means a disk failure surfaces one step away from where it happened.

**Undoing a reorganisation without amnesia.**

When a reorganisation lands, transactions that were mined into the losing branch have to go back
into the pending set, and transactions invalidated along the way have to stay invalid — without
re-adding things that were correctly dropped. That bookkeeping is written down as *conflict
intents*, and after a restart it is replayed against the current chain, with a staleness check
that asks whether the block involved actually ended up on the longest chain before acting. A
node that replays its old conclusions blindly either resurrects transactions the network already
rejected or loses ones that were still good.

#### What it produces

A mining candidate, and behind it the bundles with their fee totals that let Block Validation
check a proposed block in parallel. A coinbase to fill it. And the mined-set record — the list
of which transactions made it into the accepted block — which is what tells the coin ledger that
those coins are now spent for real rather than tentatively.

#### Scaling

This is the opposite shape to the Validator. Validation can be spread across machines because
each transaction is checked independently against shared storage; this service cannot be, because
it is the single place that holds the pending set and issues the candidate. Two instances would
be two competing mempools, each offering a different block.

So the scaling levers are all inside one instance: how fast the drain runs, how much of the
pending set is in RAM versus on disk, how quickly the tip change is absorbed. The mainnet example
gives it one replica, which is not a limitation of the example but the correct number for what
this service is.

And the shipped defaults are worth restating here because three of them interlock: no bound on the
ingest queue, so ingest is never refused; no double-spend hold-back, so nothing waits; and no
backpressure controller, so nothing would notice the queue growing anyway. The trade being struck
is throughput now, in exchange for unbounded memory if the drain ever falls behind — which is
precisely the shape of the incident described above.

### Service: Block Validation

#### What it does

It is the gate in front of the chain. A block has been proposed — by this node's own miner, or
by a peer — and this service decides whether the chain moves forward to include it. Nothing
becomes part of the record without passing here.

It has a second job that turns out to be harder. A node that starts up, or that has been off for
a week, or that has fallen behind, has to travel from wherever it was to the current tip — and
it has to do that by asking strangers for hundreds or thousands of blocks, deciding which of
them to believe, and not paying an enormous price for guessing wrong. That catch-up work is
where most of the tricky logic in this service lives, because it is where the node is both far
behind and actively surrounded by parties who might be lying.

#### The problem this removes

A block is the unit of consensus, and it is also the unit of loss. Transaction by transaction
does not apply: a block containing one bad transaction is a bad block, and everything built on
it is built on nothing. So the cost of accepting a block that should have been rejected is not
one transaction — it is the whole block, and the work of every service that touched it.

The other direction is expensive too. Reject a block that was fine, and if that keeps happening
on one route but not another, the node ends up on a minority chain, spending real money on
transactions the majority does not honour.

And the catch-up problem has its own economics: to get from a week behind to the tip as fast as
possible, the node wants to pull blocks from the fastest willing peer. The fastest willing peer
is frequently the one with something to gain, and the naive way to do this hands a stranger a
very cheap way to make an honest node burn hours of work and gigabytes of memory on a chain that
isn't real.

#### How it works

**Take the header first.** A block arrives, or its announcement does. The header is small and
mostly cheap to judge: proof of work, the header signature, the claimed structure. Much can be
decided, or at least discounted, before the megabytes arrive.

**Then the body, in pieces.** A block's transactions are held as the same power-of-two bundles
used everywhere else, so checking them is the same fan-out the Subtree Validation service does
for pending transactions — a bundle at a time, many at once, receipts back.

**Below a checkpoint, take the shortcut.** Deep in the past, where a hard-coded checkpoint has
already certified a stretch of chain, script execution is skipped entirely: proof of work plus
checkpoint linkage has established the history, so re-running every script is pure cost. That
shortcut is what makes syncing a node in hours rather than weeks. It is also the shortcut the
Legacy service's whole design is built around, and it is guarded hard — see below.

**Accept, or reject with a name attached.** Accepting extends the tip, updates the coin ledger,
and tells Block Assembly which transactions are now genuinely mined. Rejecting produces a
verdict class — bad body, incomplete, invalid — and a peer to hold responsible.

**Catch up when behind.** Choose a peer that is far enough ahead and actually reachable, fetch a
run of headers, judge them, fetch the blocks, validate them, and hand the chain to the rest of
the system as it goes.

#### The hard parts

**A peer can hand you a chain that is internally consistent and completely fake.**

Each block header claims the difficulty it was mined at, and a validation step that checks a
header's proof of work against *the difficulty the header itself states* will pass a chain where
every block declared the difficulty trivially easy to hit. A peer can invent that chain offline
in seconds. It hangs together perfectly, and it is worthless.

So before this service spends anything on fetching full blocks, it recomputes what difficulty
the rules actually *required* at each height from the header chain, and rejects a peer that
supplied a self-consistent low-difficulty chain. The ordering is the point as much as the check:
this is a step performed on headers only, deliberately placed before the expensive part, so
being lied to costs a header fetch rather than a block download and a discarded day of work.

**Where the limit on a reorganisation comes from, and why it is one block wider than it looks.**

A peer can offer to lead this node onto a fork further back than the node thought sensible. How
far back is too far? "Deep" is not an answer anybody should have to argue about at 3am, so the
rule is anchored to a number in the consensus parameters — the coinbase maturity window, the
same rule that says a mined reward cannot be spent for a while — and the comment derives why
that is the right anchor rather than asserting it:

A reorganisation of a given depth invalidates the coinbase rewards in its range. For a problem
to exist, one of those invalidated rewards must already have been spent in the chain being
abandoned. Working through the arithmetic of when a reward can be both spent and inside the
reorganised range, the answer is that the danger starts only when the depth is *greater than*
maturity — so a reorganisation exactly maturity-deep is provably safe, and the comparison
operator in that guard is `>`, not `>=`. The comment states that explicitly, because someone
previously wrote `>=`, and issue 4592 is the evidence.

That is a one-character difference on a safety boundary that would have sat there rejecting
legitimate catch-ups at exactly one depth, invisibly, forever.

**Do not pay for state changes that a lie could invalidate.**

When a peer supplies a block body, the cheapest lie is a body that does not hash to the header
being certified. The order of operations therefore matters more than the check itself: this
service proves the supplied body hashes to the checkpoint-certified header *before* entering the
processing pipeline, so that no block identifier assignment and no coin-ledger mutation can ever
be driven by a body the header does not commit to. Issue 4838.

The same instinct appears one line earlier, in a quieter form. Whether the below-checkpoint
fast path applies is computed **once** for the block and then carried down through every stage,
rather than each stage deciding for itself — "so every phase agrees". A mode that is re-derived
at each seam is a mode that can drift mid-block, and a block half-processed on one assumption
and half on another is the kind of bug that produces a corrupt coin set rather than a clean
error.

There is even a guard written against a future change of mind: a check that fails if some later
edit ever lets the fast path apply above the hard-coded checkpoint, so the assumption cannot be
broken by someone who reasonably extends the feature.

**The library's idea of a coinbase was too generous.**

Every block must start with the reward transaction, and there is a tidy helper in the
transaction library for spotting one. This service stopped using it. The library's test is a
disjunction — it also accepts a particular all-ones sequence number in place of the empty
reference that genuinely marks a reward transaction — which means it would wave through a
transaction that the reference node rejects. A dependency's convenient definition of a consensus
rule was slightly wider than the rule, and a block that this node accepted and peers rejected is
exactly the kind of bug that costs money without ever producing an error.

The replacement is an explicitly consensus-shaped test, and the reason for the change is written
next to it, because "just use the obvious helper" would otherwise return in six months.

**Expensive state churn belongs behind the cheap checks.**

When a fork is detected, this node must undo the "mined" marking on transactions in the blocks
the fork abandons — otherwise honest transactions look settled inside a branch that is going
away. It sounds like the reset should happen as soon as a fork is seen.

It does not, and the comment says why: the reset has to run *after* the fork-depth and
secret-mining safety checks pass, because otherwise a peer only has to *announce* a deep or
secretly-mined fork to make this node un-stamp its own still-valid transactions — churn in
honest state, for a fork that gets rejected a moment later, and no reorganisation ever happens.
A one-line announcement should not be able to cause that much work, which is issue 1145.

**Blame has to survive being decided by many workers at once.**

The guide has already met the question of who to blame for bad data; this service is where that
got decided. Catch-up fetches run several blocks and several subtrees concurrently, so the
question "which peers actually failed me on that trip?" has many simultaneous answers, and they
overlap.

So failures are recorded into a set keyed by peer, where a repeat just updates the message, and
the set is drained once at the end to charge each peer exactly once. The reason given is a
dashboard, not a philosophy: deduplication keeps the failure count from exceeding the attempt
count when several concurrent subtree fetches fail against the same peer. Two counters that
share a denominator have to agree about arithmetic or every rate on the dashboard is wrong.

And a related distinction that only a careful design makes: when a catch-up trip is cut short by
*this node's own* reorganisation, the trip is reported as a service error, so the peer is not
charged. A node that blames its neighbours for its own bookkeeping learns the wrong lessons
about whom to trust.

**Deleting temporary files when the fact you need is not in front of you.**

Catch-up writes subtree files to disk and cleans them up afterwards. The obvious rule — delete
what this attempt wrote — is wrong here, and it took issue 4692 to establish it.

The problem is that a hash this attempt wrote freshly may, by the time cleanup runs, be a
dependency of a block that has already committed during the same catch-up run. Nothing in the
attempt's own records can see that, because several blocks are being fetched concurrently and
the write that turned the file into a dependency is itself asynchronous — so two attempts in one
run can both record the same hash as "mine, freshly written, safe to delete", and the one that
must survive is not the one that looks newest.

The fix is to widen the bookkeeping to the run rather than the attempt: a set of committed
blocks' dependencies, registered at the single point where a block succeeds, consulted wherever
files are deleted. The comment is honest about a related piece of defensive work — a mutex whose
protection "is not load-bearing yet", kept so that a future concurrent reader cannot be
introduced unsafely. That is a rare thing to find in code: a guard labelled with its own
current uselessness, and the reason for keeping it anyway.

**Sometimes the corruption is on our own disk.**

Subtree blobs are stored on disk under a key derived from their contents. Sometimes those bytes
stop matching their key, and this service can tell — the nodes do not hash to the key, or the
transactions inside do not match the bundle's structure. Two shapes of that fault are treated as
one problem: bytes on disk filed under a name they do not deserve.

What happens next is the interesting part. The bad blobs are named individually and deleted —
*named*, rather than the fault being described generically, because a handler that re-resolved
which file to delete could pick a different sibling and leave the bad one in place. And if the
deletion cannot be confirmed, the service aborts rather than falling through to the normal
validation route, because that route's loader only checks the bundle header's own claimed root
and therefore cannot see the blob this route just rejected. Continuing would not be a retry; it
would be quietly accepting the thing that was just found to be corrupt.

**A queue of blocks, most of which cannot be processed yet.**

Blocks arrive out of order constantly: a peer announces height 900 while this node is at 887.
Almost everything in the queue is blocked waiting for a parent, and the service has to keep
working productively anyway. It does that with three priority classes — extends the current
chain, near a fork point, deep in the past — and within a class, lowest height first, since the
gaps have to be filled from the bottom up.

Then it fights starvation, because strict priority starves the deep stuff forever. A block that
has been sitting for more than a minute has its priority raised by one class per further
minute, capped at the top class so a very old block cannot outrank a fresh chain-extender, and a
block that has been skipped more than ten times jumps to the top outright. Each dequeue attempt
that finds an item blocked bumps its skip count, and those bumps are collected and applied in
one batch under a single lock rather than one at a time — an attention to lock traffic on a path
that runs constantly.

Each queue entry also remembers the other sources that announced the same block. When one peer
fails to deliver, the work moves to the next rather than being dropped and re-announced — the
same de-duplication instinct seen at the front door, met here from the other end.

#### What it produces

An extended chain tip, or a rejection carrying a specific verdict class that other services
route on. Coin-ledger updates and the mined-transaction confirmation that lets Block Assembly
clear its pending set. A record of which peers failed a catch-up and why, which is what feeds
reputation. And for a lagging node, the blocks it needs, in order, at the rate the network will
support.

#### Scaling

One replica, in both example cluster configurations, and that is the correct number rather than
an unfinished job. A node has exactly one chain tip, and two instances deciding independently
whether to accept a block is two nodes, not one scaled up. The parallelism is real but it lives
*inside* one decision: sixty-four subtrees of a single block checked at once, a hundred and
twenty-eight concurrent passes over transaction metadata, several peers fetched simultaneously
during catch-up. The unit that cannot be sharded is the judgement.

What it means in practice for capacity planning is that block validation scales the way mining
does — vertically, and by making the checks inside one block faster — and that the shared
stores underneath it, not this service, are where the ceiling usually appears first.

One default is worth knowing here because it is the same pattern the guide has met twice
before: the aggressive below-checkpoint fast path and its stricter failure mode are both
switched off in code, and neither one appears anywhere in the shipped configuration file. An
operator reading the config cannot see the knob exists, let alone what it trades.

---

### Service: Blockchain

#### What it does

It owns the answer. Everything else in this guide can be asked what it *believes* — the
validator believes this transaction is legal, block validation believes this block is good,
the front door believes this peer is unreliable. This service is asked what **is**. Which
block is the best one. How high the chain reaches. What difficulty the next block must
beat. Whether a particular block belongs to the history this node will defend. Whether a
transaction was actually mined. It is also the keeper of the single shared opinion of every
peer on the network: the P2P section said that judgement lives centrally rather than in the
front door, and this is the centre.

It holds one more thing, small to describe and large in consequence — the node's permission
to behave as a live node. Three states: parked, catching up, running. Other services ask
this one before relaying a transaction, building a block, or serving a proof. Two facts a
business owner would naturally assume are identical turn out to be separate here: what the
node knows, and what it is currently allowed to do about it.

This is the largest service in the deployment and the widest interface in the system —
seventy-two questions in its client contract and ninety-two remote procedures, against
twenty-six for the next-largest. That is not excess; it is what happens when a component is
the place where agreement is kept.

#### The problem this removes

"Hire more clerks" only works if the clerks agree. If each of a hundred workers keeps its
own idea of which block is the tip, then each is computing against a different ledger, and
every one of them can be locally correct while the combined result is a payment counted
twice. Agreement is not something the system can afford to check afterwards, because
afterwards is when the money has moved.

The second problem is quieter and more embarrassing. A node three days behind the network
can give a fluent, internally consistent answer about which transactions are confirmed,
and be wrong about almost everything that has happened since. Rather than leave every
component to judge for itself whether the node is up to date — which guarantees that
half of them guess optimistically — the question gets asked in one place, and the answer is
a single gate that everything downstream has to pass through.

#### The decision everything else follows from

**The best chain is the one carrying the most accumulated work, and the ranking never has
to ask anyone.**

Every block adds a quantity of work to the running total of the branch it extends, and the
quantity is derived from the difficulty that block had to solve: harder means more work.
The best chain is the branch with the highest total. Where two branches tie, the winner is
chosen by a fixed rule — work first, then the identity of the peer that reported the block,
then the order the blocks arrived. So three honest nodes holding the same blocks pick the
same winner without speaking to each other, which is the property the entire architecture
was built to buy.

Two consequences are worth stating plainly.

- **A fork is bookkeeping, not an emergency.** A block that does not win is not turned away.
    It is stored, marked as off the main chain, and left there. If enough work later lands
    on top of it, it *becomes* the chain and the current one steps aside; nothing has to be
    unwound, only re-flagged. This is why a node can accept competing versions of history
    without anxiety.
- **"Best" is a claim about work, not about truth.** The rule is a bet that the branch
    honest miners are building on accumulates work fastest. It is a very good bet, and it
    is a bet, which is why the deeper history is only trusted once it has enough work under
    it — and why this service holds a hard-coded list of checkpoints and refuses to let the
    node behave as live until its own chain has passed them.

The second half of the decision carries as much weight: **being willing to act on that
answer is a separate fact, and it is written to disk.** What the chain looks like and
whether this node may currently relay, mine and serve are tracked apart, changed only
through a guard, persisted before the change is made, and restored on restart. An operator
can park a node deliberately and automation will not un-park it.

#### How it works

A validated block arrives. Its height is its parent's plus one; its work is the parent's
total plus its own. If it extends the current best block, it is a single database write
that already carries the right flag. If it does not — a fork, a stale arrival, a competing
block at the same height — the write and the flag repair happen together in one
transaction. Then caches that depended on the old tip are dropped, a notification goes out
to every subscribed service, and on the conveyor belt the finished block is published for
anything that listens but does not ask.

Reads are served from a small set of caches held deliberately in memory: the current tip,
recent difficulty calculations, and the median timestamp of every block height — roughly
four bytes per block, so about five megabytes for a chain of well over a million blocks.
That is a considered trade rather than an accident: a bounded pile in memory bought against
a database round trip on the most frequently asked question in the system.

Three separate "done" markers are kept per block for other services to work through —
subtree data written, mined set confirmed, block filed — each with a matching question for
"which blocks are not done yet", because those are somebody else's job and the job has to
be re-offered after a restart.

#### The hard parts

**Winning the race is not being on the chain.**

Two blocks at the same height arrive simultaneously and both try to extend the same tip. The
natural guard — "I got the highest reference number, so I am the head" — proves something
narrower than it sounds: it proves you are at the head of the *numbering sequence*, not that
you won the ranking. Two writers can each read the same best tip, each conclude it is
extending that tip, each write a row marked "on the main chain", and each finish without
error. The numbering race is blind to the fact that work-then-peer-then-arrival makes one of
them a fork block.

The answer is to put the whole insert path behind one lock, so the second writer is forced
to read the first one's row as the new tip, discover that its own parent is no longer the
tip, and be reclassified as the fork it is. The price is one uncontended lock acquisition on
the ordinary path. What it buys is the one property everything else assumes: no two blocks
at the same height both claiming to be the chain.

**Writing a flag you may have to take back.**

The fast path writes "on the main chain" inside the insert itself, which saves a second
database round trip on the common case. It is also a prediction — true of the row at the
moment of writing, and not yet settled by the ranking. So on the fork path the row and the
flag repair share a single transaction, kept small by being bounded to recent ancestry and
by touching only block rows, never transactions or coins. The reason for pairing them is a
specific dread: a block committed with a flag that nothing will ever revisit, because the
statement that would have corrected it never ran. A wedge there is not a delay; it is a
permanently wrong answer that looks like a healthy row.

**Absence as proof, and what it costs to earn that.**

There is a faster route for the most common question in the system. Instead of asking the
database "is this block on the chain?", it keeps an in-memory set of the blocks known to be
*off* the chain and answers by absence: not on that list means on the chain. One memory
lookup instead of a query, on the hottest path in the node.

That inversion is the whole design and the whole risk, because absence is only proof if the
list is trustworthy — and the failure mode at startup is vivid. The list is built
asynchronously, and the code states plainly that this build is *expected to time out* on a
cold cache during catch-up. If the guard that keeps readers away is released whether the
build succeeded or not, the list is empty, absence is universal, and every fork block and
every invalidated block on the node reads as on-chain — with no query, no error, and no
hint. From that state the asset server will happily hand a customer a merkle proof for a
block that is not in the chain.

So the fast path carries a list of conditions under which it declines to answer and asks the
database instead: a high-water mark that has not been initialised, a list that has never
successfully been built, a rebuild in progress. The first of those is instructive. The code
refuses to treat a mark of zero as valid, even though a chain holding nothing but the genesis
block genuinely does have a zero high-water mark, because guessing "valid" there would
declare every committed block dangling. And a wrong "no" here is not a shrug — see the next
item.

**One component's stale answer is another component's permanent decision.**

The block validation service asks whether an old block is on the best chain, and if the
answer is no, it does not queue a retry — it declares the block invalid. Store code that
returns a false answer therefore does not cause a delay, it causes an invalidation that
nothing undoes until a two-minute background refresh happens to run. That fact lives in a
different service, and it changes what the store is allowed to do under uncertainty: it is
why the high-water mark is advanced only together with a rebuild that can see the new block,
and why advancing it alone is described in the code as trading one defect for the opposite
one. This is the sharpest example in the guide of a rule that no single file states: the
caller's policy is load-bearing in the callee's design, and neither can be reviewed alone.

**Permission is stored, guarded, and refused generously.**

The three-state gate is written to storage *before* the state changes. If that write fails,
the in-memory state stays where it was and a flag marks the outcome uncertain, because the
failed write may have committed anyway — and the code explicitly refuses to attempt a
rollback it cannot prove safe. The uncertainty is repaired quietly the next time anyone asks
for the state the node already believes it is in: the state is re-persisted and the flag
cleared.

The gate itself has one rule with an instructive history. A node will not promote itself to
running while its chain tip sits below the highest hard-coded checkpoint for its network,
because below a checkpoint the node is guaranteed to be in deep history even if some
worker believes it has finished a chunk — and a node in that state relaying like a live peer
gets disconnected by peers that remember the rules of the present. That gate used to exempt
one path, on the reasoning that a fresh node always boots into catch-up and so could never
reach it from parked. The exemption made a safety property depend on a default — and the
default had already changed once. Now every promotion is gated, and a node that *remembers*
being running but is below the checkpoint resumes in catch-up instead of failing.

Two related touches, both about not guessing. A stored state the software no longer
recognises stops the node with an instruction to repair the stored state rather than picking
something plausible; and when a state was deleted from the list entirely, a node persisted
into it would have woken up with no way out, so that legacy value is mapped forward and
re-saved. And automatic catch-up and promotion both refuse to undo a parked state: parking
is an operator's decision, and resuming takes an operator.

**A slow listener must not slow down everybody.**

Every service in the deployment is subscribed to this one, and a subscriber that stops
draining would otherwise stop the broadcast for all of them. Each subscriber therefore gets
a sixty-four-deep buffer and its own delivery worker; a full buffer means the subscriber is
marked dead rather than waited on. Because the underlying transport offers no way to cancel
a send in progress, each send is raced against a five-second timer, and the stranded worker
that leaks as a result is bounded to one per stuck connection.

The same reasoning sets the lifetime of a peer record. Peers refresh their entry every ten
seconds, but those refreshes are coalesced in batches of up to a minute elsewhere in the
stack, so an expiry shorter than that gap would evict an actively-connected peer between its
own heartbeats — after which the peer fails a health check and is quietly refused as a
source of blocks. A configured expiry below five minutes is therefore raised to five
minutes, and *logged*, because a setting silently corrected is its own small outage.

#### What it produces

The tip and its height, which set what every other service considers current. The
confirmed-or-not verdict on a block or transaction that the read path quotes to customers.
The difficulty target and median timestamps a miner must respect. The shared peer opinion
that decides who gets asked for a download. The three "done" tallies that let filing,
subtree writing and mined-set confirmation catch up after a restart. And the gate: the
answer to "may this node behave as though it is live?", which is the reason a node that is
partly caught up does not quietly serve stale promises as current ones.

#### Scaling

One instance in every configuration that ships — both Kubernetes examples and both Docker
stacks — and here that number is arithmetic rather than habit. There is exactly one best
chain per node and one opinion of each peer. Two instances answering "which block is best"
independently is two nodes, not one node scaled up, and the peer opinion would arrive split
in half, which is the identity problem the P2P section described rather than a way around
it. Capacity for this service is bought by a faster machine and a faster datastore, not by
another copy.

The shape of the load inside that one instance matters for planning. Reads dominate by far,
and the caches exist to keep them out of the database; block *insertion*, by contrast, is
serialised behind the single lock described above and does not fan out at all. So the
throughput ceiling for a node filing many competing blocks sits in one queue, while the
throughput for the queries everyone else depends on is high and parallel by design.

Two deployment facts are worth more than the replica count. The ledger lives in PostgreSQL
in every operator-facing configuration, while the setting's own default is a file-based
SQLite database — a node started with nothing configured is not comparable to one started
from the examples, and the comparison is easy to make by accident. And the peer opinion is
not persisted at all in the shipped configuration: its storage location has no entry in the
committed settings file, so an operator gets a fresh memory of who has misbehaved on every
restart, ban scores included, and has to point the setting at a store to change that. The
setting's own description calls a local on-disk store the default for single-node
deployments; in the configuration that ships, it is not one.

---

### Service: Block Persister

#### What it does

It turns temporary copies into the permanent record.

Almost everything this node writes while doing its job is written with an expiry date. When
the pending pile is assembled into a block, the transaction bundles it produces are filed
with instructions to delete them roughly two days of blocks later. That is not carelessness;
it is what keeps a node from filling its disk with work in progress. This service is the one
that reads those working copies back, checks them, writes the filing versions, and then —
the part that matters — cancels the deletion date, so the data stops being disposable.

It keeps a second set of books as well: for every block, the coins created and the coins
spent, written out as files. Those are what let a new node be seeded with the coin set
instead of replaying years of history, and they exist only because this service writes them.

It is switched off in every configuration this repository ships. A node that only has to
validate does not need it. A node that has to answer questions about last month does.

#### The problem this removes

The first problem is the one every long-running node has: storage that only grows stops being
affordable, and an attacker can make it grow faster than you can pay for. The design answer
across Teranode is to make disposability the default — write everything with an expiry, and
delete unless something asks to keep it. But that answer needs somebody on the other side of
the transaction, asking. Without this service, "delete unless something asks" quietly becomes
"delete".

The second problem is a race that nobody can see from the outside. The service that deletes
spent data and the service that files history are working on the same objects, both start up
at their own pace, and if deletion runs ahead of filing, the data needed for the archive is
gone before it was ever copied. So filing publishes its progress — once at startup, then on
every block — precisely so the deleting side can bound how far it is allowed to go.

#### The decision everything else follows from

**Nothing is kept because it was written; it is kept because something cancelled its expiry
date.**

When a bundle of transactions is filed during assembly, it is stored together with a height
at which it should be deleted: the current chain height plus the configured retention, which
ships as 288 blocks — about two days. Both halves of that arrangement are routed through the
Blockchain service's deletion scheduler rather than performed locally, so there is one list of
pending deletions rather than one per service. Cancelling a scheduled deletion is treated as
critical work: the storage layer's own words are that if cancellation fails, "the blob could
still be deleted even though we want to persist it forever".

The consequence is the thing to hold onto. **Filing has a fuse.** Every block's worth of
working data is scheduled for destruction about two days after it is written, and this service
has to reach it first. A persister that is not deployed loses the archive quietly and
completely — nothing fails, because the working copies were always meant to disappear.

#### How it works

It asks the Blockchain service for the lowest block that has not been filed, one block at a
time, and works through the chain in height order. When there is nothing to do it waits ten
seconds and asks again — it polls rather than listening.

For each block it writes the block file, then works through the block's bundles with eight
parallel workers. Each worker takes a batch of transaction identifiers, fetches the
transactions themselves from the coin store, and hands them to a writer that holds them until
it is their turn, so that fetching happens in parallel and writing happens in exact order. If
a filing copy already exists, it is read back and validated rather than trusted; if it does
not parse, it is deleted and rebuilt.

Then, if coin-set files are enabled, it streams the file it just wrote back off disk one
transaction at a time and records which coins the block created and spent — so the reading
side gets to check what the writing side produced. Then it cancels the expiry date on the
working copies. Then it tells the Blockchain service the block is filed, which closes the
third of the ledger's tallies and lets the deleting side advance.

#### The hard parts

**Fetched out of order, written in order.**

Batches come back from the coin store in whatever order their storage answered, but the filing
format is positional — the eleventh transaction in the file has to be the eleventh transaction
in the bundle. So the writer accepts batches in any order and holds the late arrivals in a
small holding area, releasing strictly in sequence. The interesting part is what happens at the
end. If the writer is closed while batches are still being held, the file is *aborted*, not
finalised: a file that is short but otherwise well-formed is worse than no file at all,
because everything downstream reads it as complete. The same rule applies to the block file and
to every failure along the way — abort rather than finalise something incomplete.

**A plausible forgery produced by your own storage.**

Transactions are fetched from the coin store to be written into the archive. Most of the time
that is a retrieval. But a transaction that arrived from a coin-set snapshot is stored as its
live outputs only, and asking that record to serialise itself has two possible outcomes, one
obviously fatal and one quietly worse. It can panic — and there is no recovery on this path,
so the process goes down. Or it can serialise cleanly into a short transaction with no inputs,
which does not hash to its own fingerprint and would be written into the archive as a perfect
impostor, indexed under the identifier it no longer matches.

So before writing, the code asks two questions in a fixed order: is this record serialisable,
and does the result hash back to the identifier it was fetched under. The order is not
stylistic — the second question dereferences the same missing field that makes the first one
necessary. And the check is repeated on the way out: when the finished file is streamed back
to record the coin changes, every transaction's fingerprint is compared against the bundle's
index again. Written once with a check, read back with a different one. The block's reward
transaction is the one exemption on the read side: it is not listed in the bundle's index, so
there is nothing to compare it against, and it is identified by shape instead.

**Reused memory, with the safety argument written down because it is narrow.**

Decoding transactions one at a time from a file would mean allocating each one separately, so
the service borrows a large reusable workspace from a pool and decodes into that — two
megabytes to start, reclaimed if a one-off oversized decode leaves it bloated. That is safe
here for a specific reason, and the code spells the reason out rather than leaving it to be
rediscovered: everything downstream copies what it needs on its way to storage, so nothing
borrowed survives the call. The comment names the counterexample explicitly, because it lives
in the same package — a different coin-difference type that *retains* pieces of the decoded
transaction in an in-memory map. If this path ever switched to that type, the reuse would keep
working and quietly start corrupting, which is the worst possible failure to introduce.

**The deleting side and the filing side cannot see each other.**

Deletion is allowed to advance up to the point filing has reached, and nothing knows that
distance except by being told. So on startup the service publishes its last filed height, and
after each block it sends that height again. The comments explaining this describe the consumer
as Block Assembly's cleanup service and point at its notification handler; the actual consumer
is the Pruner, which reads the same state key at startup and listens for the same
notification. The arrangement works; the map is wrong. And the Pruner has a second trigger
mode for deployments with no persister at all — which is exactly the case the shipped
single-node stack is in, where the pruner runs, the persister is commented out, and the
correct mode has to be chosen with a setting that appears nowhere in the shipped configuration.
See the reconciliation notes.

**A block it can never file will be retried as fast as the loop can turn.**

The main loop has a ten-second pause, and it guards exactly two situations: nothing to do, and
cannot reach the Blockchain service. A failure *during* filing logs an error and starts the
next pass immediately. Under ordinary trouble — a store momentarily unavailable — that is
fine, and immediate retry is what you want. But a block whose working copies have already been
pruned is not momentarily trouble; it can never succeed. The loop will re-query the ledger,
re-fetch from the coin store, and re-log the same error back to back for as long as the process
runs, and the pause that would have made that sustainable is attached to the only branch where
it is not needed.

#### What it produces

The archive: block files and bundle files whose expiry has been cancelled, plus the per-block
record of coins created and spent that makes seeding a fresh node possible. Filed timestamps
that close the Blockchain service's third per-block tally. A published progress height that
bounds how far deletion may go. And a definite answer to a question the rest of the deployment
cannot otherwise settle — which of the things this node holds are history rather than drafts.

#### Scaling

The design splits one way and refuses to split another. Inside a block it is parallel: eight
workers over that block's bundles, and inside each bundle a further batch concurrency derived
from the machine's processor count. Across blocks it is strictly serial — the poll loop takes
the lowest unfiled block, files it completely, and only then asks for the next — because the
archive is a sequence and a gap in it is a hole in the record.

Adding copies is possible and unrewarding. Nothing corrupts if two instances run: both would
ask for the lowest unfiled block, one would find the file already present and skip it, and the
store already answers "already exists" as a normal outcome rather than a fault. What they do
not do is divide the work, because nothing hands out ranges — the second instance mostly
re-checks the first one's output. Filing throughput is bought by making one instance's parallel
fetch faster and its storage quicker, not by adding instances.

As shipped it does not run at all: disabled in both Kubernetes examples, present but commented
out in both Docker examples with the reason given as a line of prose — *uses lots of disk
space* — and offered as an opt-in profile by the supported install, which says frankly that
archival mode needs multiple terabytes on mainnet and is not needed for standard validation or
mining. That is an accurate description of the cost rather than a hedge: filing adds a
permanent second copy of bundles and blocks while the hot store keeps pruning on its normal
schedule, so the service consumes disk and very little else.

Two numbers to know if it is switched on. The outer concurrency is configurable and the inner
one is not — the batch parallelism inside each bundle is computed from processor count and
ignores the setting entirely, so on a large machine filing will use more of it than the
configuration appears to allow. And on an all-in-one node, where several services share one
process, the outer number halves itself automatically so filing cannot starve its neighbours.

One door to be aware of. This service can also serve archive files over ordinary web
requests, and it is the only place in the deployment where the raw file store is exposed as a
network service rather than behind a purpose-built API.
Without an authorisation token it accepts reads and refuses every write, which is the sensible
default and the reason the shipped configuration ships it that way. With a token, the service
warns at start-up if it is reachable from outside the machine, because the token then travels
in the clear — the archive door has no encryption of its own, so putting it on a network means
putting something in front of it, not simply opening a port.

---

### Service: UTXO Persister

#### What it does

It keeps the master list of coins that nobody has spent yet as a file on disk, rebuilt as
the chain grows, so that a node can be started from a *fact* instead of by re-reading every
transaction that has ever happened.

Two things get confused here, and the difference is the whole service. The live coin list —
the one a node consults thousands of times a second to refuse a double-spend — lives
somewhere else, and in this repository it lives nowhere: it is a different component, run by
a different part of the product. What this service makes is the **archive** of that list. A
complete snapshot of every coin in existence as of a particular block, deep enough in the
past that nobody disputes it.

Snapshots are what make these possible: adding a machine over the afternoon instead of in six
days; answering "what coins existed at that moment" for an auditor; replacing a corrupt coin
store without a full replay; and proving to a stranger which coin set you believe, without
handing them the whole thing and asking them to trust the arithmetic.

#### The decision everything else follows from

**The archive is built a hundred blocks behind the tip, on purpose.**

This service will not build a snapshot for anything closer to the front of the chain than a
hundred blocks. That number is fixed in the code, not in configuration.

The reason is reorganisation. A snapshot built at the tip is a snapshot of a state that might
not have happened: the block it ends on can be pushed aside, and with it every coin the
snapshot says exists. Unwinding a coin archive is the expensive thing to do twice, so the
service refuses the risk and waits instead. A hundred blocks on a healthy chain is about
sixteen hours, which is far past the point where anyone reorganises.

Two consequences follow, and both surprise people.

The first is that the archive is always about sixteen hours old, on a node that is perfectly
up to date. Freshness is not a fault here; it is the design. A node whose coin archive
matches its chain tip exactly is a node that has stopped being updated.

The second is that the archive is not one snapshot per block. Because it works in ranges —
from wherever its bookmark sits up to the tip-minus-a-hundred — a healthy node that is a few
blocks behind writes **one** snapshot covering those few blocks, not a snapshot each. The
phrase "checkpoints each block's coin set", which appears in the reference documentation, is
true of the naming scheme and not of the behaviour, and reads like a guarantee about
granularity that the service does not make. Each snapshot is keyed to the block it ends on,
so it looks like per-block history in a directory listing.

#### How it works

It is pushed and pulled at the same time, which is how it survives a lost notification. It
subscribes to the chain, it has a timer — a minute when the subscription is in place, ten
seconds when there is no subscription to rely on — and after every pass it kicks itself again,
so a range is chewed through in successive passes rather than waiting for someone to ask.

Only one pass runs at a time. A trigger arriving during a pass is dropped rather than queued,
which is safe only because the pass re-kicks itself at the end; the two facts have to stay
together.

A pass goes like this. Ask how high the chain reaches and subtract a hundred. If the bookmark
is already at or above that, report that there is nothing to do and wait a minute. Otherwise
walk the blocks between the bookmark and that ceiling, and for each one read its two coin
ledgers — outputs created, outputs spent — folding both into a single pending list. Then open
the previous snapshot, stream through it, apply the pending list as you go, and write out the
new snapshot. Record the snapshot's digest beside it. Then delete the snapshot that was
replaced, and move the bookmark.

The folding is the interesting arithmetic. An output created and spent inside the same range
never appears in the snapshot at all: it is added to the pending list and then removed from
it. That is correct — the snapshot describes the end of the range, and a coin that lived and
died inside it does not exist at the end — but it means the archive cannot answer "was this
coin spendable at block 812,400" if 812,400 was inside a range. The snapshots are only
answers at the heights where snapshots happen to exist.

#### The hard part: an empty ledger and a missing ledger sound the same

Every block creates coins — the miner's reward is created in every block — so a block with no
"created" ledger is a block whose ledger has not been written yet. The service treats that as
*not ready*: it stops the pass and waits for the next trigger. Fair enough.

A block with no "spent" ledger is different, because that is legal. A block containing nothing
but the reward spends nothing. So the code tries to tolerate a missing "spent" ledger and get
on with the pass. What it cannot do is tell that apart from the other kind of absence, and the
tolerance is keyed to a name in the error vocabulary that this repository's file store does
not use — the store reports a missing file under a different name. So on the store that
actually ships, a missing spent-ledger falls through to the same place a missing
created-ledger does: the pass stops, and the message in the log is the one that means
*caught up*. "No new block to process."

Which is harmless until you notice who writes those ledgers. **This service's own package
writes them, on behalf of the Block Persister.** The call comes from the filing service
(`services/blockpersister/persist_block.go:78`), and the first line it logs says so —
"Persisting utxo additions and deletions for block …", tagged `[BlockPersister]` — but that line
is emitted from inside this service's package (`services/utxopersister/UTXOSet.go:152`). So a
search of the code by package name separates the two halves of one hand-off, and a search of the
logs by this service's name misses every line that created its input.

What that means operationally is the part to keep. Every example deployment here switches the
archive off *and* the filing service off, which is coherent. But the two switches sit a few lines
apart in the same files and nothing says one is pointless without the other: a deployment that
switches on the archive alone gets a service that reports itself healthy, accepts work, logs the
sentence that means *caught up* once a minute forever, and never writes a snapshot. There is no
log line that says "I have never been given a ledger to consolidate".

#### The hard part: the lag and the fuse are numbers in different buildings

Working files in this system are commonly written with an instruction to delete themselves a
certain number of blocks later, and the default window is 288 blocks — roughly two days of
healthy mining, as discussed under Block Persister. This service waits a hundred blocks before
it consumes its input. If its inputs carried that same two-day instruction, the slack between
"safe to read" and "deleted before I got to it" would be about 188 blocks, and a service that
stalled for that long would lose the data it needs and could not catch up without a rebuild from
somewhere else.

Nothing states that relationship. A hundred is a constant inside this service; 288 is a global
in the settings file, and it reaches the data through two service-specific retention values and
through the expiry stamped on transaction bundles during assembly. The coin ledgers themselves
carry no expiry at all — the code that writes them passes none, and the file store's fallback
retention is a store option that nothing in the node's start-up sets. So for these particular
files the risk is not the fuse burning out but the opposite: they are never scheduled for
deletion, and nothing reclaims them. The relationship between a hundred and 288 remains
unexamined for everything that *is* scheduled, and the two numbers still live in different
places with nothing tying them together.

The same service also holds more than one file open at a time, and the file store lets a
limited number of readers into the disc at once — several hundred by default, shared with
everything else in the process. Each ledger held open occupies one of those places until it is
closed, and the loop walking a range has to hand each one back before moving to the next
block. When that was not done, the allowance ran out within a few hundred blocks of walking, at
which point every read in the process — not just this service's — waited twenty-five seconds
and then failed as *service unavailable*. Fixed, and the comment says why the ordering
matters, which is the kind of comment that saves the next person a day.

#### The hard part: the write cannot be repeated, so nothing after it may fail the pass

Writing a snapshot is a one-way step. Once the file exists, walking into that step again fails
with *already exists* — it is not a step that can be retried. So the service writes the
snapshot first, and treats everything that follows as optional, because returning an error
after that point would wedge the service: the retry would come back to a step that can no
longer run, and the coin archive would stop permanently on one block because of a
transient failure in something auxiliary.

What sits after the point of no return is the interesting part:

- **The digest.** A running hash of the snapshot's contents is written beside it. If that
    write fails, the log says so and says what is lost — the checkpoint for that block is
    unavailable until someone backfills it — and the pipeline moves on.
- **The signed checkpoint.** Given a key, the service signs three facts together: the height,
    the block hash, and the snapshot digest. That is a claim about the coin set rather than a
    copy of it, small enough to hand out and strong enough to verify. The signature is bound to
    the network's own identifier, so a checkpoint from testnet cannot be replayed as a
    checkpoint for mainnet — the same class of protection the front door uses on its own
    identity, applied to data instead of to a peer.
- **The seed package.** The snapshot is cut into chunks by content rather than by size, and
    only chunks not already present are written. A fresh node bootstrapping from your archive
    therefore copies the parts that changed, not the whole coin set again, and the manifest
    names the chunk set so the recipient can verify what it assembled. Chunking by content is
    what makes that work when the file is enormous and changes only slightly block to block.

Two smaller things worth knowing, both about going backwards. The service keeps a bookmark of
how far it has built and will not move it down on its own: asked to build to a lower height it
refuses, and the message tells the operator to delete the bookmark file to override. And there
is a separate one-shot path for building an exact height on demand — used to prepare a
checkpoint or repair a gap — which deliberately ignores the hundred-block window, builds the
height it is told, and never deletes an existing snapshot. The continuous service is allowed to
be conservative because the manual path exists.

The pre-pass check on the previous snapshot is worth naming honestly, because its comment is a
useful story. The service opens the previous snapshot and reads its header, then closes it.
That is the check: it can be opened and its header decoded. It does not confirm the coin count
or the digest. Those are checked later, while streaming the old snapshot to build the new one,
which is after the pass has committed but before anything is deleted. The comment on the check
explains why it does not read further: doing so would consume eight bytes that belong to the
body, which would then fail as an unrecognised format and take the whole process down through
the service manager. A verification step whose own documentation is a warning about how it
could crash the node is a fair description of the state of the art here.

#### Scaling it

**By design: one process, and the range is the unit of work.** This is a chain, not a fan.
Each pass reads the previous snapshot whole and writes the next snapshot whole, so a second
instance cannot be handed half the keyspace the way the live coin store can — it would need
the same starting snapshot, and would produce a competing result rather than a share of one.
The way to get more through it is to make each pass cheaper or wider: bigger ranges, more
internal parallelism while sorting and writing, larger buffers. Not more instances.

**As shipped: switched off everywhere, and starving if you switch it on.** Both Kubernetes
examples set it to false; no Docker file gives it a container at all. That is coherent, given
that its input is written outside this repository — the deployment and the dependency agree.
The risk is the halfway state: an operator who reads "maintains the master list of unspent
coins" and enables it on a node without the writer gets a healthy-looking service that logs
the same idle sentence forever.

Two numbers to know if you switch it on. **One snapshot exists at a time** — each pass deletes
the one it replaced, so this is a rolling window of exactly one, and "keep the last few" is not
a setting. There is a flag that stops the deletion, and it is named for a different service, so
searching for it by the name of *this* service finds nothing; its catalogue description talks
about a live coin store growing unbounded and holding spent outputs, which describes a
different mechanism than the one that reads it. What it actually does here is keep every
snapshot ever written — for the largest single file in the system, once per pass. That is a
disk decision of a different order than the description suggests.

And the hundred blocks: on a chain mining ten-minute blocks that is sixteen hours of lag, and
on a chain that has been quiet for a week it is nothing at all, because the ceiling is a height
and not a time. A node that has not seen a new block does not get a staler archive; it gets the
same one.

---

### Service: Asset Server

#### What it does

It is the library desk. Everything in this guide so far has been the node doing its own work;
this is the node being useful to somebody else. Ask it for a transaction by its identifier and
it returns the transaction. Ask for a block and it returns the block, in raw bytes, in hex, or
as structured data. Ask for a bundle of interdependent transactions and it returns the bundle.
Ask whether a thing exists and it answers without fetching it. Ask for the old-style
Bitcoin-format block and it converts one for you.

It is also two other things that are easy to miss. It hosts the live feed the dashboard watches,
over a persistent connection. And it is the address that every announcement on the network
carries: when the front door tells the world that it has a block, the address attached to that
announcement is this service. That is the second half of the system's most consequential design
choice — gossip carries awareness, weight travels by ordinary web request — and this is the door
the weight leaves through.

So the honest summary is: it is the only place in the node where traffic that nobody vouched for
turns into work on the storage.

#### The decision everything else follows from

**Strangers are allowed to ask, so the answer has to be bounded, not just correct.**

Every other service in this guide can assume a colleague asked. This one cannot. A request
arriving here may be a partner's node catching up, a block explorer indexing the chain, a
customer checking their own transaction, or somebody learning how much a gigabyte block costs
to download. The correctness of the answer is the easy part — the stores know the answer.
Everything interesting about this service is about *how much* it will answer, for whom, and what
happens when the answer is expensive.

Which gives the service a different shape from everything before it. It has a route table, a
set of metering tiers, a cache for the one consensus question it is asked constantly, and a
number of deliberate ceilings on how much of itself it will give away at once.

#### How it works

A request comes in over HTTP under the service's API prefix. Two exceptions sit at the root of
the address space — a liveness answer and a health answer — because the things that probe those
are not clients of the API.

Everything else is a read. Transactions, transaction metadata, bundles, bundle data, bundle
transactions, blocks by hash, ranges of blocks, header chains, walks to and from a common
ancestor between two chains, unspent-coin lookups, legacy blocks, and two routes that simply
forward a submission to the transaction door for services that would rather talk to one host
than two.

Before the read happens, three things may stop or shape it:

- A **tier** is worked out from the request: whether the caller presented credentials the service
    recognises, and if so what class of caller it is.
- A **meter** is applied for that tier, measured in requests per second, held against the
    caller's identity if it has one and against its network address if it does not.
- A **concurrency allowance** is taken for the specific kind of read, of which there are ten,
    each with its own limit — and five of the ten ship with no limit at all.

The read itself goes to the stores through a repository layer, and large objects go out as
streams rather than being assembled in memory first.

#### The hard part: absence is proof only inside a window

One question this service answers far more often than anything else is not really a data
question: *is this block on the chain we are on?* The Merkle-proof and transaction-metadata
routes both need it, because a proof about a block that got reorganised out is not a proof of
anything. Asking the service that owns that answer costs a network round trip to another
process, and these routes are asked constantly.

So this service keeps a cache. The interesting part is not that it caches — it is that the
cache knows precisely what its own silence means, and it is organised into three regions by that
question.

**Inside the window** — the last few hundred blocks, sized from the same retention number that
sets the deletion fuse elsewhere — the cache holds the complete set of main-chain block
identifiers. So inside the window an answer either way is free: present means yes, absent means
no, and no one is asked. This is the region that carries almost all real traffic, because
people ask about recent things.

**Below the window**, absence means nothing. A block from three years ago is main chain and is
simply not in the window, so "not found" here is not evidence. Those lookups go to the service
that owns the answer — but with the requests for the same block glued together so that a
thousand simultaneous questions about one old block become one question, and with a guard so
that if a reorganisation invalidates the cache while an answer is in flight, the stale answer is
discarded rather than written back. The comment in the code calls that window explicitly: the
period in which a stale answer could otherwise outlive a reorganisation.

**Above the window**, the cache cannot speak either: a block that arrived seconds ago is above
the top of the set and the set is rebuilt shortly. So those go straight to the owner, uncached,
deliberately — caching them would be caching a fact about to change.

And if the window has never been built, or a rebuild fails, the whole cache declares itself
unhealthy and every lookup goes to the owner with no caching at all. The code states the
bargain in five words: degraded throughput, never degraded correctness. Note what that means in
practice — the failure mode of this cache is not a wrong answer, it is the rest of the node
paying full price for every proof request until the rebuild succeeds.

This is the same principle the Blockchain section met from the other side: absence is only
proof inside a boundary you can demonstrate is complete, and the moment you cannot demonstrate
it, you have to ask.

#### The hard part: three tiers, two of which are unreachable

The metering is tiered, and the tiers are well thought out.

An unverified caller is metered by network address — and for IPv6 by the address block rather
than the address, with the whole table capped at fifty thousand entries, because an attacker
rotating through a range with 18 quintillion addresses would otherwise grow the metering table
itself for the five minutes between cleanups. The thing being protected from was going to run
out of memory protecting the disc.

An authenticated peer is metered by its peer identity, so two honest nodes behind the same
office connection get separate allowances rather than sharing one address's allowance, and get a
multiplied allowance on top. A miner is either given its own allowance or exempted entirely.

There is one gate into those better tiers: an allowlist of which peers may be elevated. It ships
empty. With it empty, a signed request is rejected at the membership check — before the
signature is examined, before the replay check, before the content digest — and the caller stays
unverified. So the multiplied allowance for known peers, and the miner exemption, are both
unreachable in the configuration this repository ships, and every caller — including
Teranode's own peers doing expensive catch-up work — is metered as a stranger by address.

That is not an oversight; the design assumes it. The routes that catching-up peers hammer get a
burst allowance floored at the size of the catch-up fan-out precisely *because* those peers are
typically unverified. The system does not rely on peers being allowlisted; it relies on
recognising their traffic pattern instead.

But it means the phrase "per-tier rate limiting" describes a capability rather than behaviour.
One consequence is worth naming: the heavy routes are metered as **two independent buckets** —
the five routes catch-up depends on, and everything else heavy — each with its own ceiling for
the same address. The split exists so that an indexer sweeping bulk searches cannot spend the
allowance a catching-up peer needs. The cost, stated in the service's own start-up line, is that
one address can now draw the heavy allowance twice, once in each bucket, so the anonymous
ceiling across the heavy surface doubled when the protection was added. Both halves of that are
documented; it is still the kind of thing an operator should find out before a capacity review,
not during one.

Two smaller courtesies: the burst a caller is allowed is separated from the sustained rate, so a
client designed to make N requests at once is not punished for its design; and a rejection
cannot advertise a wait longer than an hour, because a misconfiguration that computes a
six-day "try again" is worse than useless to a client that obeys it.

#### The hard part: the address it advertises is a placeholder

The front door's announcements carry a fetch address so that a listener can pull the weight. That
address comes from one setting: the public HTTP address of this service. In every example in
this repository it is commented out.

When it is unset, the service advertises its own internal address — a container name or a
cluster hostname or localhost. The front door knows this is a problem and says so once at
start-up, in a line that ends with the setting name to fix: peers "will score" this as a
protocol violation. Because for block and bundle announcements the address is not decoration —
an announcement without a fetchable address is an announcement nobody can act on, and listeners
score an unusable address the same as a malformed one.

So a node installed straight from the examples is a poor thing to download from. It hears the
network; the network cannot easily use it; and it is marked down for advertising an address
nobody can reach. The operator documentation knows this — the security guidance says to set that
value, and the local settings file carries the example commented out at the top, next to a note
that once the address is opened up the node can also advertise its real network location.

One honesty note in the code deserves repeating, because it is the kind of caveat that tends to
get lost: the check that flags the bad address is a heuristic about what *this* node would
accept from others, not a prediction of what others accept from us. A node configured strictly
can withhold an address its more relaxed peers would happily have used. The code judged the
asymmetry acceptable because the opposite mistake gets the node scored.

#### Scaling it

**By design: this is the best candidate in the system for adding machines.** It owns nothing.
Two instances asked the same question return the same answer, its caches are derived and
rebuildable, no election or coordination is involved, and the work is per-request. Everything
else in this guide has a reason it cannot fan out; the read door has none.

**But the ceilings are per instance, which means replicas multiply the protection away.** The
request meters, the ten concurrency allowances and the cache all live inside one process.
Run two instances at the shipped settings and twice the traffic reaches the same storage, while
the storage's own allowance for simultaneous readers is also per-process — so the queue moves
from the door, where rejection is immediate and polite, to the disc, where it becomes a
twenty-five second wait followed by an error. Scaling this service horizontally is fine and is
the intended answer to load; it requires lowering the per-instance numbers as you add machines,
or the admission control silently relocates to the worst possible place.

**As shipped: one instance, with a cache in front of it rather than a second instance behind
it.** Both Kubernetes examples run a single replica. Both Docker examples for mainnet and
testnet do something more interesting: they put a caching proxy in front of the read door. That
cache is small and short-lived — a few hundred megabytes, entries evicted after ten unused
minutes, and caching from the very first request rather than the second. It also holds only one
trip upstream per popular item while everyone else waits for it, which is the same
stampede-avoidance instinct the origin applies to consensus questions about a single old block.
Two places in this system independently decided that when a thousand people ask the same thing,
one person should go and look.

That is the operator's answer to the scaling question, and it is a better one than the
configuration comments admit to: caching in front of one origin keeps the disc's work flat while
the front door's traffic grows, and it costs a small memory budget instead of a second node's
worth of duplicate reads.

The one thing the deployment evidence cannot tell you is how open the door actually is. In the
production-shaped example the ingress for this service is declared with a class named
*internal*. That may be perfectly correct in that cluster, or it may mean the public read door
is not reachable from outside at all — the file does not say, and the name suggests it is worth
asking. Combined with the unset public address above, a node deployed from that example may be
serving only its own operators, which is a legitimate choice and should be a stated one.

---

### Service: Pruner

#### What it does

It makes the node forget.

Everything else in this guide is about acquiring and keeping: connections learned, transactions
validated, blocks filed, coins counted. A node that only ever adds stops being a piece of
software and becomes a storage bill. This service decides what may be released and — the harder
question — what must not be.

It does two kinds of work. It prunes transaction records from the databases, which is where the
volume lives. And it deletes whole files from the file store — blocks, bundles, coin ledgers —
that were written with an instruction attached saying when they could go.

#### The decision everything else follows from

**Nobody deletes anything; they ask for it to be deleted, and somebody else does it later.**

When a service writes a working file it can attach an expiry: "this may be removed at block
412,500". What happens next is the part that surprises people. The file store does not keep that
promise and does not put the file on a timer. It makes a call to the Blockchain service, which
owns the queue of things to remove, and the write *fails outright* if there is nowhere to make
that request — an unconfigured expiry is an error, not a silent no-op.

This service is the one that later comes to the queue, takes work off it in batches under a
lease, deletes the files from whichever store holds them, and reports back which ones succeeded
and which failed.

So intent and execution are separated, with a third service holding the intent in between. Three
consequences run through everything below: the deleting service can be restarted without losing
the promises, the promises can accumulate unbounded if nobody drains them, and the service that
asked for the deletion has no idea whether it ever happened.

#### How it works

It listens for new blocks — either when they are mined or when they are filed, set by one
configuration value, and the two are mutually exclusive, so each mode discards the other's
messages.

For each height it works through, in order:

1. **Wait for colleagues.** It will not prune a height until the block assembly service reports
    itself running and caught up to that height, with a tolerance of one block, and until the
    block is marked as mined. Both waits are bounded and retried steadily rather than with
    growing delays.
2. **Preserve parents.** Old transactions still sitting unmined in the pending pile have parents,
    and those parents may be old enough to be due for deletion. If a parent goes, the child can
    never be mined or resubmitted. So the parents get a hold put on them first.
3. **Release expired holds.** A hold is not permanent, and — this is the subtle part — putting
    one on clears the record's expiry. So this pass finds holds that have run out, asks whether
    the record is genuinely safe to remove now, and only then gives it a new expiry, deliberately
    set in the future so that the deletion pass running moments later does not delete what has
    just been re-stamped.
4. **Delete.** Records past their expiry and not on hold are removed.

Alongside that, the file-deletion half works the queue: it asks for a batch of deletions that are
now due — measured against a height a little behind the current one, a safety margin of ten
blocks by default — under a lease with a timeout, deletes them from the right store, and completes
the batch, naming the failures so they can be retried a limited number of times.

One courtesy in that loop is worth noting: stores that are failing — a file service answering
"not authorised" is the example in the code — get excluded from batch collection for that pass,
so a broken store cannot keep taking work it will fail and starving the healthy ones.

#### The hard part: a hold is a debt, and forgetting it means the record lives forever

A preservation is not a delay with a timer behind it. Putting a hold on a record sets the hold
marker and clears the expiry, which means the deletion pass will now walk straight past it —
forever, unless something remembers.

That is what step three exists for, and the code says the consequence plainly: without it,
preserved parents keep their hold marker and no expiry and "are never pruned".

Which makes this the most quietly dangerous coupling in the service, because steps two and three
have a dependency the other steps do not: both only run when the coin store is wired in. The
configuration switch that skips them announces itself in the log at information level. A missing
coin store does not announce itself at all — the branch simply doesn't run, and the log looks the
same as a node that had nothing to preserve.

So the failure mode is: a node with no coin store attached runs its pruning, silently omits the
two phases that protect parents of unmined transactions, and deletes records that a pending child
might still have needed — with no line in the log explaining why the protecting step was skipped,
because "switched off by an operator" and "dependency missing" are different situations being
reported by only one of them.

#### The hard part: the two waits speak at different volumes

Both of the waits in step one are patient and both are bounded: about a thousand attempts each,
one at one-second intervals and one at half-second, with no growing delays. When they give up,
both skip the height. How loudly they say so is another matter.

Waiting for block assembly gives up with a warning naming the height and the reason, and
increments a labelled counter. Waiting for the block to be marked mined is not one wait but two,
and they disagree with each other.

| Wait                                             | Which half                 | When it applies                                      | On give-up                                                                              |
| ------------------------------------------------ | -------------------------- | ---------------------------------------------------- | --------------------------------------------------------------------------------------- |
| Block assembly running and caught up             | both halves                | always                                               | warning naming height and reason, plus a labelled counter                               |
| Block marked mined                               | record pruning             | whenever an assembly client is wired                 | debug only, no counter, and the file-deletion pass is skipped along with it             |
| Block marked mined                               | file deletion              | only when pruning is triggered by mining             | warning naming the height, no counter                                                   |

So the same condition — a block that is not yet marked mined — produces a warning in one half of
the service and nothing visible in the other, and in the mode this repository ships the
file-deletion half does not wait for it at all. That is backwards in the specific way that
matters to an operator: the ordinary case, a block still being validated when the pruner came
round, which is exactly what the wait exists to tolerate, leaves no trace in the record path, and
the only counter that moves is labelled with the other cause. Nothing tells you that pruning is
being skipped routinely; the metric that would have said so is filed under the wrong reason.

There is a related question the code leaves open in a benign-looking way. Signals are coalesced:
only one pruning operation runs at a time, and the pending height is always replaced with the
latest, so intermediate heights are "naturally skipped". For the deletion pass that is sound —
deletion eligibility is measured by height threshold, so a later height covers everything
earlier. For the preservation pass it is less obvious, because the parents of the children in a
skipped height are not examined by the height that replaced it. Whether that matters depends on
how long transactions wait unmined, which is a question for whoever owns the pending pile rather
than something this file can answer.

#### The hard part: the guard that waits for a state nobody grants

There is an optional safety mode: pause pruning while the node is catching up, so that validation
cannot mark transactions mined faster than the pruner can protect their parents. It is off by
default, and when on it asks one question — is the node in the running state? — and skips
everything if the answer is no.

The comment explains the choice well: only that state proves no catch-up is in flight, because an
operator pause can leave a catching-up node parked in the idle state while its batch is still
running. The difficulty is how a node gets into the running state at all. Looking at the state
machine, that state is only entered by an explicit run event, and nothing in the node's own
start-up path sends it; the callers of that event in this repository are an operator command, a
test harness, and an application-programming interface a client could call. Meanwhile the health
endpoint reports the running, catching-up and idle states all as healthy, identically.

So turning this switch on does not make the pruner cautious; it makes it silent and inert, on a
node whose dashboard stays green. And it is one more instance of the trap described under
Blockchain: the state names read like a description of what the node is doing, and they are
really a record of which instruction it was last given.

#### The hard part: what it says when the feeder never arrives

This service is the second half of a coordination story told under Block Persister, and it is
worth repeating from this side because the symmetry is instructive.

In its default mode it prunes when blocks are *filed*. When the filing service is not running —
which is the state of every example deployment in this repository — the trigger never fires, and
the pruner simply waits. What it does about that is asymmetric in the useful direction and the
useless direction at once.

It logs at information level, once, when the filing service first appears: "block persister is
now active", with the height. That is a genuinely good operational line, and it is the kind of
thing that saves an hour of debugging when a service starts late.

It says nothing at information level when the filing service never appears at all. The messages
that would explain it — discarding a notification because this mode waits for a different one —
exist, and are logged at debug.

So the announcement is built for good news. A node that has never had a filing service logs
nothing at the level operators read, and the first symptom is that the disk is bigger than
expected. Given that "block persister is now active" already exists, its counterpart — "waiting
for block persister, pruning paused, no notification received yet" — is a small addition, and it
is recorded as an open question rather than a criticism, because the hard part is already done.

#### Scaling it

**By design: this is the one service in the guide that is explicitly built for more than one
instance, and the lease is why.** The file-deletion half does not assume it is the only worker.
It asks the queue for a batch under a timeout, and reports the batch back when finished with the
failures named. That is the shape of a work queue with a visibility timeout — the same pattern a
message system uses to let several consumers share a queue without stepping on each other. Two
instances would take different batches from the same queue, and a batch abandoned by a crashed
instance becomes available again when its lease expires. The internal knobs are all about the
size of a mouthful rather than the number of mouths: chunk sizes, chunk groups processed
concurrently, batch sizes for reading and for deletion, and the number of attempts before a
deletion is given up.

There are two honest caveats. The record-level half is not lease-based — it is driven by block
signals and one operation at a time — so a second instance would duplicate that work rather than
share it, and the preservation phase in particular would run twice over the same heights. And the
budget guard for the coin database is calculated inside a single process: it takes the connection
queue size, multiplies it by a threshold that ships as 0.7, and cuts its own parallel batch limit
to fit that share (`stores/utxo/aerospike/pruner/pruner_service.go:531-537`). The arithmetic
assumes it is the only pruner there is. Two instances would each reserve seventy percent of the
same pool and together ask for a hundred and forty — exhaustion by construction rather than by
accident. It is the clearest illustration in this guide that a service can be safe to run in
several copies and still need its shared resources divided up by hand.

**As shipped: one instance where it runs at all, and it does not run in the Kubernetes
examples.** Both Docker examples for mainnet and testnet start it. Neither Kubernetes example
mentions it — not disabled, *absent*: there is no entry for it in either cluster file, while the
thirteen other inventory services that run as their own process each have one.

Put that next to the trigger problem and the picture is that each example deployment has exactly
one half of the deletion pipeline broken, in opposite directions:

| Deployment                           | Filing service           | Pruning service           | What happens                                                                |
| ------------------------------------ | ------------------------ | ------------------------- | --------------------------------------------------------------------------- |
| Kubernetes examples                  | off                      | not deployed              | expiry requests are accepted and queued; nothing ever drains them           |
| Docker mainnet and testnet           | off                      | on                        | the pruner waits for a filing trigger that never arrives                    |

Both roads end with the node growing rather than shrinking; they differ in whether the promises
to delete are sitting in a queue or in the writer's assumption that somebody would.

That makes this the service whose scaling answer is most clearly *not* the interesting question
for the shipped system. It scales well by design and would happily take a lease-per-batch share of
a busy node's cleanup. What it needs first is to be present, and for the trigger that starts it to
match what else is running — which is a deployment conversation, not a capacity one, and is the
subject of open questions rather than of this section.

---

### Service: RPC

#### What it does

It speaks the old Bitcoin command language, so existing tools do not have to learn a new one.

Sixty-two command names behind one HTTP port: ask what the best block is, ask for a block by
height, ask what peers are connected, hand over a finished transaction, ask what the node thinks
the state of the chain is. Wallets, mining pools, block explorers and the scripts that babysit
them all speak this dialect already, and none of them were written with Teranode in mind.

That makes this service the translator on the border. Everything behind it is new — services
talking over remote procedure calls and event streams — and everything in front of it is a JSON
body naming a command that was designed a decade and a half ago.

#### The problem this removes

Without it, the outside world needs to be rewritten. A wallet vendor, a mining pool and an
indexer all have software that posts `{"method": "getblock", ...}` and parses what comes back. If
Teranode cannot answer that, it is not a drop-in node, and the cost of adopting it is measured in
other people's engineering budgets. This service pays that cost once, inside the node.

#### The decision everything else follows from

**The RPC door is not a window onto the node. It is a door, and it does a door's work.**

The obvious way to build this service is as a thin shell: accept the old commands, read some state,
format the answer. That covers most of the sixty-two names. It does not cover
`sendrawtransaction`, and the way that one command is handled decides what this service has to be.

Handing over a transaction is an act of ingestion, and this service performs it end to end. It
decodes the bytes, **writes the transaction into the block store itself**, resolves the inputs
against the coin store to work out what the transaction actually pays in fees, refuses the call if
the fee looks like a typing mistake, and only then asks the Validator to check the transaction —
which is the step that spends the coins and hands them to block assembly. Four steps, three
services, one HTTP request.

Two consequences run through the rest of this section. The first is that this service holds a
direct line to more of the node than anything else here: the coin store, the block store, the
Validator, block assembly, the Blockchain service, and both peer-facing services for the commands
that ask about connections. The second is that a transaction arriving here has not passed through
the door built for transactions, and does not get whatever politeness that door enforces.

#### How it works

One port, one route, one HTTP method: everything is a POST carrying a command name and some
parameters. Then, for each request:

1. **Admission.** Every request to this service counts toward a cap on in-flight requests. Over
    the cap, the caller is refused outright with a "too busy" reply.
2. **Authentication.** A username and password arrive in the standard header. The server holds a
    hash of the exact credential it expects and compares against it — one administrative
    credential, one limited one, and nothing else.
3. **Permission.** A caller using the limited credential may only run commands on an allowlist of
    forty-four names. Everything else is refused without being parsed.
4. **Execution.** The command runs in its own goroutine under a thirty-second deadline, with a net
    underneath that turns a crash inside a command into an ordinary error reply instead of a dead
    connection.
5. **Reply.** The answer is written, and the connection is closed — deliberately, on every
    response.

Some of the read-only questions are answered from a short-lived cache rather than from the coin
store, which is what keeps a chatty monitoring tool from becoming the busiest thing on the node.

#### The hard part: the fat-finger guard exists only at this door

Bitcoin transaction fees are not printed on the way out. Send a transaction with a fee a hundred
times larger than intended and nobody stops you; a miner collects the difference and it is gone.
This service has a ceiling for that — a fee above a tenth of a coin gets the call refused, with an
error naming the limit — and the comment above it is admirably blunt that the ceiling exists only
here, is not consensus and not policy, and is deliberately invisible to the Validator and the peer
paths.

That is a sensible place to put it: the ceiling protects a human from a mistake at the point the
mistake is made, which is exactly where such a guard belongs, and applying it as consensus would
change which transactions the network accepts. Two things about it are worth carrying forward.

**The limit is not in the shipped configuration file.** The ceiling has a default, and a test
asserts that the default is not zero — so it is on, always, in every deployment that has not
thought about it. But it is one of the settings that lives in the code and never appears in
`settings.conf`, so an operator reading the configuration to find out why a submission was refused
will not find the number there. They will find it in the error message, which means after the
fact, and one at a time.

**The check deliberately refuses to trust the caller's arithmetic.** To work out a fee the service
needs to know what the inputs pay, and a caller can simply state that figure in the request. The
handler clears the stated values, re-reads the real ones from the coin store, and pays for an extra
database read per submission so that the answer comes from the node rather than from the party
asking to spend the money. That is the right way round, and it is worth naming because the cheap
version of this code — trust the stated input values — would pass every test in the file.

#### The hard part: the door that closes when you forget the password

An administrative interface with authentication has to answer an awkward question: if no credential
is configured, does the door open for anyone, or shut for everyone?

This one shuts. The expected credential is computed only when both a username and a password are
configured; otherwise the server holds thirty-two zero bytes, no real credential can ever hash to
that, and every request is refused — and the request path always demands authentication, with no
setting that makes it optional. Failing closed is the right instinct, and it is what the README
says when it tells operators to set credentials before the service will authenticate anyone.

What is wrong is the settings description, which tells operators that leaving the password empty
"disables authentication (development only)". That sentence is the opposite of the behaviour, and
it sits in exactly the place an operator looks when deciding whether they can skip setting a
password. The consequence is a specific and miserable debugging session: a developer following the
description starts the node with no credentials, the port is listening — nothing told them it
wouldn't — and every request comes back "unauthorized", while the documentation says
authentication is off. Nothing in this repository tests the empty-credential case, so the sentence
can stay wrong indefinitely.

The shipped deployment makes this real rather than theoretical. In the mainnet and testnet
contexts the credentials are deliberately blank, and `settings.conf` says so carefully: committing
a password here would put a world-readable administrative credential on every deployment that
inherits it, which is the right call. The same comment explains the consequence for the Asset
Server's admin routes — they reject every request — and does not mention that the RPC door does the
same thing. So the production-shaped answer is that the old Bitcoin interface is deployed, enabled,
configured with a single replica, and refuses to speak to anyone until an operator edits a file that
this repository does not ship.

#### The hard part: three requests wide, no queue, and nobody notices when a caller leaves

The cap on this door is not a rate or a queue depth; it is a count of requests currently being
answered. The code default is one, `settings.conf` raises it to three, and an arriving request that
would make four is refused immediately rather than waiting. Combined with the closing of every
connection on reply, the shape is a door three people wide with a sign saying try again — and the
refusal carries no hint of how long "again" means, so a polite client has to guess.

For a tool run by an operator this is fine, and the ten-second answer cache absorbs the chatty
polling that monitoring tends to produce. It is worth being clear about what it is not: a
front door for machine traffic. A mining pool polling for work, an indexer walking blocks and two
operators with scripts is a queue of four that never forms, and the failures will land at random
on whichever caller happened to arrive third.

There is a second looseness pointing the other way. When a caller gives up — closes the connection,
cancels, walks away — the handler running on their behalf is not told. The mechanism for noticing
exists and is wired up, and not one of the command handlers looks at it; the only code that reacts
to a departed caller is the placeholder that answers unimplemented commands. So a request abandoned
by its caller keeps spending, keeps reading, and keeps counting against the three-wide door until
its thirty-second deadline expires. That is a modest amount of waste per incident and it is also
exactly the kind of thing that makes a door look overloaded while nobody is visibly using it.

One consequence of the cache deserves a line of its own. `getchaintips` — the command whose entire
purpose is showing where the chain has forks and dead ends — is answered from cache for three
hundred seconds, five minutes. Every other cached command has a ten-second window. So the one
question an operator asks during a reorganisation can be answered from a snapshot taken before it.

#### Scaling it

**By design: yes, and unusually cleanly for a component this old.** It owns no state that would
have to be agreed between copies — everything it reports lives behind it in the coin store, the
block store or the Blockchain service, and every mutation it triggers is delegated to a service
that arbitrates it. Two instances answering `getblock` do not need to know about each other, so
the ordinary tools apply: put them behind a load balancer and add capacity. The design even has
the two-tier credential split, which is exactly what scaling this service usually needs — a
limited, allowlisted account for the high-volume read traffic and an administrative one kept for
the few callers who change things. Each instance also publishes its own timings per command, so a
slow instance is visible rather than suspected.

Three caveats, all inherited from how the door is metered rather than from what it does. The
three-request cap is per process, so capacity scales with instances but the *shape* of the failure
stays the same: no queue, refusal under load, no hint of when to retry. No connection survives its
reply, so every replica change costs callers a fresh handshake. And nineteen of the forty-four
commands the limited account is allowed to call are placeholders, so the tier that looks designed
for a fleet of read-only pollers cannot actually answer `getblockcount` or `gettxout` — which
means a fleet pointed at it will report errors that look like capacity problems and are really
unfinished work.

**As shipped: deployed everywhere, sized at one, and locked.** Both Kubernetes examples enable the
service with a single replica; the mainnet one gives it a full core and a gigabyte, which is a
sizable amount of machine for a translator, and about right for a service that reads the coin store
on the submission path. Both Docker overlays run it, and the examples hand the test networks a
shared throwaway password while leaving mainnet and testnet blank by design.

So the honest summary of this service as deployed is: one copy, three requests wide, speaking to
nobody. That is a coherent position — an internal administrative door that refuses unauthenticated
callers by default is doing its job — but it is not what the settings description implies, and it
means the scaling question an operator would actually ask, "how many of these do I need", is
answered by their traffic pattern rather than by anything in these files. Whoever points a wallet
or a pool at the mainnet example first will find the credential question and the three-wide door at
the same moment.

One last thing for whoever writes the health checks: this service's port serves exactly one route,
and readiness and liveness live on the shared diagnostics port instead. A monitor that only speaks
the old command language has no way to ask this door whether it is alive.

---

### Service: Alert

#### What it does

It listens to a second network, and that network can stop your money.

Teranode can join a separate peer-to-peer network of alert nodes — its own protocol name, its own
port, its own cryptographic identity, its own address book. Messages on that network arrive signed
by keys the node already trusts, and they instruct the node to do things the blockchain alone never
would: make specific coins unspendable, hand a coin to a different owner, throw away a block, cut
off a peer.

It is the emergency brake. When something goes wrong at the speed of an incident rather than the
speed of a release, this is the service that acts.

#### The problem this removes

Consensus is slow by design and blind to context. If a class of transaction turns out to be
malicious, or a signing key is compromised, or a batch of coins is known to be stolen, the chain
carries on processing them, because the chain only knows whether a transaction is *valid*. The
alternatives are worse: wait for a software release, or ask every operator to intervene by hand.

This service is the third option — a signed instruction, propagated on its own network, applied by
every listening node within minutes, without anyone changing their software.

#### The decision everything else follows from

**Money can be stopped by a message that arrives on a different network from the blockchain.**

Everything about this service follows from putting that instruction path outside consensus. The
alert network has its own identity and transport: a peer-to-peer stack on port 9908 by default, a
protocol identifier of its own, a topic name that picks up the network's name as a suffix, and a
bootstrap list that defaults to the public IPFS peers. It keeps its own database — a SQLite file by
default, separate from every store the blockchain uses. Authority comes from a signature check
against five public keys committed in the shipped configuration, and the service refuses to start
if that list is empty.

And then the part that matters: the instruction is applied by **writing directly into the shared
coin store** — the same store the Validator spends from. A freeze is not a note attached to a
transaction. It is a flag on the coin's record in the database that everything else consults.

#### How it works

Every five minutes — a cadence fixed in the code, not a setting — the service works through what the
alert network has given it. Peer discovery runs separately every ten minutes, and that one *is*
configurable.

For each instruction it acts through a small adapter that reaches four places in the node:

- the Blockchain service, to invalidate a block;
- the coin store, to freeze a coin, unfreeze it, or reassign who owns it;
- the modern peer-facing service and the Legacy door, to ban and unban peers — the ban goes to
    both, and the Legacy copy is expressed as roughly a hundred years.

Nothing in the node asks this service for anything. Its entire inbound interface is one health
procedure. It is a listener that acts, not a service others call.

#### The hard part: a freeze that reports success and freezes nothing

To freeze a coin, the service has to derive that coin's identity from the transaction stored on
this node. The comment above the derivation is the most candid paragraph in the service, and it is
worth rendering at length instead of summarising.

The identity is computed from the stored transaction, keyed by the transaction the caller named.
The two agree only while the stored transaction is byte-for-byte faithful to the original. A node
seeded from a coin-set snapshot does not hold originals: it holds reconstructions with no inputs,
no version and no lock time, which hash to something else entirely. The derived identity then
matches nothing in the store and — in the comment's own words — "the freeze then reports success
while blacklisting nothing".

Three things make this more than a corner case.

The reporting cannot tell the cases apart. Every failure path in the handler appends the coin and
the reason to a "not processed" list; the success path appends nothing. A run that froze five coins
and a run that matched zero both return an empty failure list, and whoever sent the instruction
sees a handled message either way.

What happens next points at the wrong door. Reassignment requires the coin to be frozen already, and
the store answers "is not frozen" when it isn't. So on a snapshot-seeded node the freeze silently
finds nothing, and the confiscation built on top of it fails loudly with an error naming the freeze.
The trail leads to the alert, not to the seeding.

And this repository argues at length about snapshot seeding — whether the service is deployed, how a
node gets filled, what it costs — and nowhere in that argument is the fact that a seeded node
answers alert instructions differently from a synced one.

#### The hard part: confiscation changes the promise about a coin, not the coin

Reassignment is the boldest instruction in the protocol: an alert can move an unspent coin to a new
owner. The service takes a public key out of the confiscation transaction's unlocking script, builds
a fresh locking script from it, and derives a new identity that commits to that script.

Then it hands the pair to the coin store, and the store updates three things: the identity, the
frozen flag, and a delay before the coin may be spent. It does not store the new locking script. The
code says so itself — the replacement script is not persisted, so "changing the owner currently
strands the output for both owners".

That is not speculation. The SQLite path is a visible update of exactly those three columns and
nothing else; the Aerospike path calls a stored routine with two identities, a height and a delay,
and is never given a script at all. So the coin's record now commits to a locking script that exists
only in the instruction that asked for the change, while any later spend is checked against the
script still on disk. The old owner cannot prove the coin because the commitment moved; the new
owner cannot spend it because the script never arrived. On top of that the coin is held for a
thousand blocks — about a week — which is a sensible amount of caution for something this invasive,
and also makes the stranding hard to notice, because everything about it looks deliberate.

The mechanism is right and the write is incomplete. What is missing is not cleverness; it is the
last column.

#### The hard part: the knob for that one-week wait works on the development store, not the production one

The thousand-block hold has a setting, and it defaults to a thousand.

The SQLite store honours it: it starts from the constant and overrides that with the setting when
the setting is non-zero. The Aerospike store — the one in the production-shaped deployment — passes
the constant and never reads the setting.

So an operator who shortens the hold gets what they asked for on the store used for development and
testing, and gets a week anyway on the store they deployed. It is the pattern recorded several times
in the companion notes, inverted: usually it is a knob that works in production and is untested
elsewhere. Here the knob works where nobody deploys and does nothing where everybody does. The
setting is not in the shipped configuration file either, so there is no comment there to catch the
assumption.

#### The hard part: the brake bites at the spend, not at the selection

A freeze takes effect where coins are spent — inside the coin store, which refuses the spend and
says the coin is frozen, or frozen until some later block. That is the right place, because it is
the one place every path must pass.

It also means nothing upstream is told. The block assembly service is handed to this service and
used for exactly one purpose — deciding whether the service counts as ready — and is never called
when an alert is applied. A transaction whose input was just frozen can stay in the pending set and
can be selected into a block, and is stopped only when something tries to spend the coin. That is
safe. It is also why pulling the brake appears slow: the coin stopped immediately, and the block
holding it may not have.

#### Scaling it

**By design: several instances can run, and the design leans on the shared store instead of on
coordination between them.** Each instance holds its own alert database and its own peer identity,
each independently receives the same signed instructions, and each applies them to the same coin
store. There is no leader election and no shared log, because the effects are small, repeatable
writes triggered by messages the whole network sees.

What adding machines does not buy is the network itself. The addressing mode defaults to *client*,
meaning an instance asks questions and does not answer them, so a second instance makes discovery
no better for anyone — it is one more caller. Identity is generated fresh on each start unless a key
is supplied, and the shipped configuration deliberately leaves it unset, so every restart is a new
peer to the alert network. Both defaults are reasonable for a handful of nodes and neither improves
with growth.

**As shipped: one instance in Kubernetes, and entirely absent from Docker.** Both cluster files
enable it with a single replica. There is no alert service in the Docker catalogue at all — the only
"alert" in those files is Prometheus's alert manager, an unrelated monitoring component that an
operator searching for this service will find first and misread. So the deployment this repository
hands to miners runs no emergency brake: coins cannot be frozen, blocks cannot be invalidated and
peers cannot be banned by alert, and nothing in the compose file mentions the gap.

Running one replica also keeps an awkward question off the table. This service defaults to a SQLite
file, on a deployment that provisions shared network storage, with its database connection pool set
to a single open connection. One instance and one file is unremarkable; several instances on one
file across a network filesystem is a well-known way to lose a weekend. Nothing documents which
shape is intended, so the answer today is "don't find out". That same cluster file carries the stray
development image policy the notes keep tripping over, and it sits on this service and no other — a
small coincidence worth knowing about before the first person who scales it assumes the file is the
problem.

---

### Blob Store

*The one part of the node that is not a service. It is a library, which means it runs wherever
bytes are touched — inside every process that stores or reads a transaction, a block file or a
subtree — and it is the reason the transaction store can be switched off without anything
appearing to break.*

#### What it does

The blob store is the node's shelf for opaque objects: things identified by a hash and read back
whole. A transaction, a block file, a subtree bundle, a list of coins added or spent — each is a
blob, and the store's job is to turn "give me this hash's bytes" into a file path, and to turn new
bytes into a file that appears complete or not at all.

What makes it worth its own section is that the shelf is *interchangeable*. Five implementations sit
behind one interface: ordinary files on a disk, an in-memory map for tests, an object store, an
HTTP client that talks to another node's blob endpoint, and a null store that accepts everything and
keeps nothing. A sixth piece of machinery sits on top of whichever one is chosen: a batching layer
for many small writes, a logging layer, and the thing that decides how many files may be open at
once.

The decision this design embodies is that *where* bytes live is a configuration choice, not a code
choice — the same call that stores a transaction against a local disk can store it against a bucket
or a peer, without the caller knowing. That is a good decision, and it has a consequence this
section is mostly about: when the choice is a discorder, nothing downstream can tell.

#### How it works

**A name is a path.** A blob is identified by a hash and a file type. The store turns that into a
filename by hex-encoding the hash in reverse — so the file's name reads the way a hash is normally
written — and appending an extension that says what kind of object is inside. Files are then spread
over subdirectories by taking a couple of characters from the name, so no directory holds millions
of entries. Which two characters, and from which end, is the sharding rule, and it is the one detail
that has to agree everywhere in the node; the hard part below is about it.

**A file appears complete or not at all.** A write does not create its file. It creates a temporary
sibling, writes that, closes it, flushes it to the storage device if told to, and only then renames
it into place. A reader therefore never sees a half-written blob: the name either exists with the
whole object behind it or does not exist. Optional checksum sidecar files give a second check on the
bytes after the fact.

**A read looks in more than one place.** Because a node may keep recent objects in a working area and
older ones in an archive, a lookup falls back: the working directory, then the persisted subdirectory,
then, if configured, a long-term store which may be a bucket or another node. This is what lets the
Block Persister's archive and the hot working set coexist without the caller knowing which is which.

**Writing is metered per process.** Opening a file is a slot, and the store keeps two counters —
768 for reads and 256 for writes — so a flood of reads cannot starve writes of file handles. Those
counters are per process, which is the same arithmetic as the Pruner's database pool and has the
same consequence when you add machines.

**Retention is somebody else's business.** When the store is asked to delete a blob now, and when an
expiry date is attached to one, the handoff is the same described for the Pruner: the file store does
not schedule anything itself (§51). It exposes the means; the Blockchain service holds the promise.

#### The hard part: the transaction shelf is a discorder as shipped

In `settings.conf` there is one line about where transactions go, it is uncommented, and it has no
comment of its own:

```text
txstore = null:///                                                  # Store to use for transaction blobs

```

(`settings.conf:1600`.) The null store is the implementation that keeps nothing. Its behaviour is
precise and it is all bad news for anyone who later wants a transaction back:

| Call                 | Null store's behaviour                                    | Source                                   |
| -------------------- | --------------------------------------------------------- | ---------------------------------------- |
| Store a blob         | Logs at debug level, keeps nothing, returns success       | `stores/blob/null/null.go:104-150`       |
| Does it exist        | Always no, unconditionally                                | `stores/blob/null/null.go:198-211`       |
| Get bytes            | "not found"                                               | `stores/blob/null/null.go:153-196`       |
| Delete, expire       | Success                                                   | `stores/blob/null/null.go:136-220`       |

So every write to the transaction store succeeds. Nothing complains, because success is the honest
answer to "did you accept this" — the store did accept it, it just has nowhere to put it.

Now follow the money. The Propagation service writes every transaction it accepts into the
transaction store (`services/propagation/Server.go:1656`). The RPC door does the same before handing
a transaction on (§57). The Alert service keeps its own history, in its own database rather than on
this shelf, so nothing there depends on it — and that history is switched off by default for reasons
the Alert section gives (§62). The Pruner's retention policy has an entry for the transaction store
and would delete from it in the ordinary course (`settings/settings.go:843-846`) — a schedule with
nothing to delete, whose deletions would still be reported as completed, because the null store says
yes to everything.

The readers are the interesting part. Grepping the repository for code that *asks* the transaction
store for bytes turns up one production caller: `compose/chainintegrity/chain_integrity.go:600`, a
diagnostics tool that lives in the `compose/` directory — which the repository itself has already
flagged as an arrangement problem (§3, §61). Everything inside the node that needs a transaction's
bytes either still holds it in memory or asks the Asset Server, which keeps its own index (§54).

And the way this happened is more revealing than the fact itself. The other blob stores have
per-deployment paths, carefully written: the block store has a default, a development path, a Docker
path and an operator path under the client name (`settings.conf:496-499`), and the subtree store has
eight variants including three operator spellings (`settings.conf:1488-1495`). The transaction store
has exactly one line in the file, no context variants, and it points at nothing. Whatever the reason,
the mechanism for pointing it at a directory was there, is used twice in the same neighbourhood of the
file, and was not used here.

| Store                                   | Lines naming a path in `settings.conf`              | Deployment override                          |
| --------------------------------------- | --------------------------------------------------- | -------------------------------------------- |
| Block store                             | `496-499`                                           | yes, including `blockstore.operator`         |
| Subtree store                           | `1488-1495`                                         | yes, three operator spellings                |
| Transaction store                       | none — `1600` only, and it is `null:///`            | none anywhere in the repository              |

No deployment in this repository therefore puts a submitted transaction on a disk. Not the
Docker examples, not the development fixture, not the operator deployment — the config map sets a
network, a Kafka topology, a client name and a listen mode, and no transaction store
(`deploy/kubernetes/teranode/teranode-configmap.yaml:5-13`), which leaves the default standing.

What that costs, in plain terms: a node configured like this can answer "what was in that
transaction" only while it still holds the transaction in memory or has put it in a block. Restart it
and the pending pile is gone with no copy on disk; the node rejoins and its peers tell it what it
missed, which is exactly what a Bitcoin node does anyway. So this is not a data-loss bug in the ledger
— the ledger lives elsewhere — it is the loss of the node's own recollection, and of the ability to
serve an old transaction's bytes from blob storage, which is one reason the Asset Server's index exists
at all (§54). The configuration is coherent for a miner that does not need to remember. It is not
coherent for an archive, and nothing in the file says which one you are getting.

The relic that shows it was once different: four test rigs bind-mount a `txstore` directory into the
container (`test/docker-compose-host.yml:148,197,246`, `test/docker-compose.e2etest.legacy.yml:211,239`),
which is precisely where files would go. Those rigs run under `SETTINGS_CONTEXT: "docker.host.teranode1"`
(`test/docker-compose-host.yml:139`), a context with no transaction-store line, so the mount has nothing
to receive. An operator who added a disk for it would find it unused, and would be right to wonder
whether something was broken.

#### The hard part: the sharding rule lives in three places

The rule that turns a name into a directory — take two characters from the start of the file name, or
from the end — has to agree between whoever writes a file and whoever later deletes it. It is set in
three ways:

- as a per-store default in the code that builds each store (`daemon/daemon_stores.go:415,457,530,583,637`),
  where the transaction store and subtree store take two characters from the start, the temporary
  store takes none, and the block store takes two **from the end**;
- as a query parameter on the store's URL, which overrides the default — and the only place the
  shipped configuration sets that parameter at all is inside an *encoded nested* URL, the external
  store hanging off the UTXO store's Aerospike address
  (`settings.conf:1629`, where the `?` itself is written `%3F`);
- and as a second copy of the same table inside the Pruner
  (`services/pruner/blob_deletion_worker.go:358-374`), whose comment says it exists "matching the
  defaults in `daemon/daemon_stores.go`".

A comment, not a constant, holds the two tables together, and there is no test that compares them.
If they ever disagree — or if someone puts a `hashPrefix` parameter on one store's URL and the Pruner
starts up before reading it — the Pruner computes a path the writer never used and deletes a file that
isn't there. That failure already has its own log line, which is evidence someone has met it:

```text
all %d blobs in batch were already missing from disk — verify the pruner pod has the correct volume mount

```

(`services/pruner/blob_deletion_worker.go:204`.) The message blames the volume mount. The mismatch it
would actually be reporting is a configuration disagreement about how names become paths — which an
operator reading that sentence has no reason to suspect.

#### The hard part: the file-handle budget is tuned for a limit nobody sets

The two counters — 768 reads, 256 writes — exist because they were once one counter and that caused a
deadlock, and the reasoning is written down at the point of use (`stores/blob/file/file.go:437-452`),
including why a too-small system limit *clamps* the numbers instead of refusing to start:

> Refusing to start while the node can be served by a smaller budget was the behaviour before and it
> caused an outage (issue 1431).

`settings.conf` does the arithmetic for the operator and then hands over responsibility:

> Total: read + write should stay under your system's open file limit (`ulimit -n`), leaving room …
> The limit itself is the operator's to set (`ulimit -Hn`, systemd `LimitNOFILE`, container
> `--ulimit`)   — `settings.conf:504-511`

Both knobs are commented out in that same block, so 768 and 256 apply (`settings.conf:516-517`,
`settings/block_settings.go:25-27`). And in the Docker catalogue the only container that sets
its own file-handle limit is one of the Aerospike variants (`deploy/docker/base/docker-services.yml:193-196`);
no Teranode service does, and nothing in `deploy/kubernetes/` mentions the limit at all. So whether
the clamp fires on a given host depends entirely on the container
runtime's default, and when it does the only trace is an INFO line naming both numbers. Worth one
command at commissioning — `docker exec <service> sh -c 'ulimit -n'` — and worth knowing that the
answer changes the node's throughput without changing its correctness.

#### Can it scale by adding machines

**Yes, and the answer is about storage rather than computers.** The store has no machines of its own:
it is compiled into processes that already exist. Scaling it means choosing a backend that can be
reached by more than one process — an object store, or the HTTP backend, or a shared volume — and the
file backend's atomic-publish-plus-shards design is safe for that as long as names are unique per
key. Two deliberate limits travel badly: the per-process handle budget (N copies ask for N × 1024
handles), and the fact that the HTTP backend is *read-only unless given a token* — writes are refused
outright when no token is configured (`stores/blob/server.go:66,75,149-155`), which is a sensible
default but makes "point the workers at the shared blob server" a two-setting change, not one.

**As shipped: yes, on one shared volume, and never over the network.** Every deployed blob URL in
this repository is a `file://` URL; no example uses the object-store or HTTP backends. The operator
deployment mounts 4,800 GiB of shared Lustre storage (`deploy/kubernetes/teranode/teranode-cr-mainnet.yaml:156-159`)
at the path the operator context uses for every blob store, and the block store's operator path hangs
that client name underneath it: `DATADIR.operator = /data` at `settings.conf:10`, and `settings.conf:499`
puts the block store at `/data/<client name>/blockstore`. One volume, one namespace, every process
looking at the same tree, which is precisely the arrangement the Pruner's shared-filesystem warning
wants (§51). The base Docker catalogue is the exception that
proves the rule: its default blob paths resolve nowhere because the env block that gives `DATADIR` a
value is commented out (§52), so a node built from the catalogue alone fails its first blob write.

The honest summary for a store: **the shelf scales; what is on the shelf does not.** The transaction
shelf holds nothing at all, in any deployment in this repository, and the shelf that does hold
everything — the block store, several terabytes and growing (§12) — scales only by getting a bigger
volume, because nothing in these files can spread it across two.

---

### UTXO Store

*Everything else in the node ferries, files, or deletes. This is the part that knows what exists. It
is also the only component where a wrong answer cannot be fixed by asking a peer again.*

#### What it does

The UTXO store holds the master list of unspent coins, and it is what the rest of the node asks when
it needs to know whether a coin is real and unspent. Two halves live in it. The confirmed half is the
ledger as everyone agrees on it. The pending half is the same shape — coins that would exist if the
transactions waiting to be mined were mined — and it is the reason a second node cannot simply be
bolted on beside the first.

The interface is deliberately small on the outside and enormous underneath (`stores/utxo/Interface.go`).
A coin is identified by a transaction hash and an output index. A coin carries a state — spendable,
spent, locked, or frozen by the alert service (`stores/utxo/Interface.go:12-16`) — and the store is
the only place those states change. The one operation that matters is the atomic one: spend this
transaction's inputs and create its outputs, in a single call (`stores/utxo/Interface.go:22-26`).
The ledger is never allowed to be observable in a half-updated state, because half-updated is the
exact shape of "money created without being spent".

There are two production implementations behind that interface and the choice is a connection string:
a family of SQL engines (SQLite and PostgreSQL through the same code, plus an in-memory variant for
tests) registered unconditionally (`stores/utxo/factory/sql.go:75-84`), and Aerospike
(`stores/utxo/factory/aerospike.go:78`). Anything else is refused by name: a scheme the registry does
not know ends in an error that quotes it back — "utxostore: unknown scheme" — rather than falling
back to something quieter (`stores/utxo/factory/utxo.go:197`).

#### How it works

**A coin's location is arithmetic.** With the Aerospike engine a transaction's outputs are stored in
chunks, and which chunk an output sits in is worked out from its index modulo the configured chunk
size (`stores/utxo/aerospike/aerospike.go:907-920`, `stores/utxo/aerospike/get.go:356`). That size is
a setting — `utxostore_utxoBatchSize`, 128 by default (`settings.conf:1731`). The hard part below is
about what happens when that number moves.

**Coin state and coin bytes are stored apart.** The Aerospike record carries the facts the node needs
fast — heights, flags, which outputs are spent. The transaction that *created* the coin goes to a blob
store, either as the whole transaction or as a compact list of just its outputs
(`stores/utxo/aerospike/create.go:494-518`). That blob store is configured inside the connection
string, as a nested URL (`settings.conf:1629`, where the `?` of the inner address is written `%3F`),
and it has a small memory cache in front of it for the common case of a child spending a coin that was
created moments ago (`settings.conf:1710-1717`).

**Deletion has to wait for children, and a date is not enough.** When a parent transaction is fully
spent the node would like to delete it, but not while anything that spends it might still be undone.
The rule is documented in the repository as "safe deletion": every spending child must itself be
mined and comfortably old (`docs/utxo-safe-deletion.md`). The store implements this with a timestamp
rather than a height, and then handles the awkward case where a transaction goes from confirmed back to
pending during a reorganisation by giving it a timestamp from the *past* — back-dated so that a child
which has just been thrown off the chain does not automatically win a fresh lease of life
(`stores/utxo/pruner_unmined.go:16-35`, where the reasoning cites issue 1768).

**The engines are not identical.** They implement one interface and they disagree on at least one rule:
the hold that keeps a coin the alert service has re-assigned out of spending range for a thousand
blocks is read from configuration by the SQL engine (`stores/utxo/sql/alert_system.go:173-175`) and
taken as a fixed constant by the Aerospike engine (`stores/utxo/aerospike/alert_system.go:250`).
Section 64 followed that disagreement into the alert service; here it is worth noticing that the
disagreement is *structural* — two engines behind one contract, and nothing checks that they still
agree.

#### The hard part: the number that decides where a coin lives is set three ways

`utxostore_utxoBatchSize` is 128 in the defaults, 50 in the `docker` context, and 512 in the
`docker.m` context (`settings.conf:1731-1733`). Since the chunk an output belongs to is computed from
that number, the setting is not a tuning knob — it is part of the address of every coin on the disk.
The code says so, in the constructor, without hedging:

> It's very dangerous to change this number after a node has been running for a while. Do not change
> this value after starting, it is used to calculate the offset for the output
>
> — `stores/utxo/aerospike/aerospike.go:346-348`

And yet three values ship, and the two of them that matter sit two lines apart in the same file. A node
that ran with the `docker` context and is later started with `docker.m` would look for coins in chunks
computed with a different modulus — 50 versus 512 — and would find, for most coins, nothing. The
deal-breaking detail is that nothing at start-up records which size a data directory was written with:
the value is read from settings each time and trusted each time.

To keep the claim honest about its limits: only the Aerospike engine shards this way, and the shipped
deployments that use `docker.m` and `docker.ss.teranode1` do not share a data directory with each other
— the second is a PostgreSQL address entirely (`settings.conf:1630`). So this is a hazard rather than a
live fault: two documented contexts, one of them the default for the base catalogue's first node
(`deploy/docker/base/docker-teranode.yml:13`), pointing at different chunk sizes with no recorded
warning that they must not describe the same disk. The cheap fix is a file in the data directory that
says how it was written, which is what the block store's header magic does for blob files.

#### The hard part: the expiry date is deliberately missing from the ledger's own files

The Blob Store section described the arrangement where a file carries the height it may be deleted at,
and the Blockchain service's queue is the promise and the Pruner the fulfilment (§51). The UTXO store
opts out of that arrangement, on purpose, at the point it opens its blob store:

```text
// Create external store with DAH explicitly disabled
// External file lifecycle is managed by the Aerospike pruner service, not by DAH files

```

(`stores/utxo/aerospike/aerospike.go:337-338`, passing `WithDisableDAH(true)`.)

That is a coherent choice — a height is the wrong instrument for a rule that says "wait until every
child is mined and old", which is why the store keeps its own timestamp and its own sweeper — but it
splits the node's deletion policy into two regimes that an operator has to know about separately. For
ordinary blob files, disk pressure is relieved by the height retention setting and can be reasoned
about in blocks (§12). For the ledger's files, deletion is decided by the safe-deletion rules inside
the store, and a Pruner that is not running (§55) is not what is holding them back. The consequence for
capacity planning is that `global_blockHeightRetention` — the number the whole deployment discussion
has been built around — simply does not govern this data.

#### The hard part: the build tag that no longer gates anything

`AGENTS.md` tells developers that the `aerospike` build tag marks "tests requiring Aerospike", and
`Makefile:102-114` dutifully passes `-tags aerospike` to every build target. In this checkout that tag
no longer selects the Aerospike UTXO engine at all. The four files that used to be gated by it carry
the directive *commented out* — `// //go:build aerospike` at `stores/utxo/factory/aerospike.go:1`,
`stores/utxo/process_conflicting.go:1`, `stores/utxo/aerospike/set_mined.go:1`, and
`stores/utxo/aerospike/delete.go:1` — and one command settles what that means:

```bash
go build ./stores/utxo/factory/   # succeeds with no tags, so aerospike:// compiles in anyway

```

Exactly one live directive remains in the tree, on an integration test
(`stores/utxo/aerospike/native_op_add_deleted_children_integration_test.go:1`). So the practical truth
is that any binary built from this checkout can open an Aerospike address, the Makefile's flag is
decorative, and a developer who follows `AGENTS.md` and runs a tagged test suite believes they are
exercising a different build than they are. Not dangerous — but the kind of documentation drift that
survives because nothing contradicts it.

#### Can it scale by adding machines

**Yes, on the side that matters, and the shipped examples know it.** The store is a client library; the
database is a separate thing whose nodes can be added. The concurrency on the client side is generous:
the mined-set update runs across a worker pool sized from `utxostore_maxMinedRoutines`, 128 by default
(`settings.conf:1646`, consumed at `stores/utxo/aerospike/longest_chain.go:66`,
`model/update-tx-mined.go:215`, and `services/blockvalidation/quick_validate.go:2083`), with batchers
stacked in front of reads, spends, stores and increments (`settings.conf:1640-1668`). That 128 is a
statement about a cluster.

**As shipped, it is one node and the client is throttled to suit.** The example cluster is a
single-node, single-namespace deployment — `replicas: 1` (`deploy/kubernetes/aerospike/aerospike-statefulset.yaml:9`),
a configuration file that describes itself as "a single-node, single namespace developer environment"
(`deploy/kubernetes/aerospike/aerospike-cm0-configmap.yaml:5`), `replication-factor 1` (`:62`), and a
20 GiB volume (`deploy/kubernetes/aerospike/aerospike-claim1-persistentvolumeclaim.yaml:12`). And the
defaults are cut down where they are deployed, with the reason written in the settings file:

```text
utxostore_maxMinedRoutines             = 128
utxostore_maxMinedRoutines.docker.m    = 8
utxostore_maxMinedRoutines.teratestnet = 4 # local aerospike can't always cope with high concurrency against a single AS node

```

(`settings.conf:1646-1648`.) Cutting 128 to 8 is a sixteenth of the client's own default, and to 4 is a
thirty-second; those are not tuning preferences. They are a deployment admitting
deployment admitting that the client was sized for a cluster that this deployment does not have. Note
also the two volumes in the production-shaped deployment: 4,800 GiB for the blob store
(`deploy/kubernetes/teranode/teranode-cr-mainnet.yaml:156-159`) and 20 GiB for the ledger — which
belong to different growth curves, since the blob store grows with history and the ledger grows with
live coins.

There is one more honest limit on this answer: **the ledger's address is not in this repository.**
There is no `utxostore` line for the `operator` context, and the config map explains why — it is a
secret the operator creates, alongside the consensus store and the administrative key
(`deploy/kubernetes/teranode/teranode-configmap.yaml:15-22`), with the installation guide showing the
shape as `utxostore: "aerospike://AEROSPIKE_EXAMPLE_URI_CHANGE_ME"`
(`docs/howto/miners/kubernetes/minersHowToInstallation.md:197`). That is a sound security decision with
one side effect worth naming: the question "how big is your ledger's cluster, and how many replicas?"
has no answer anywhere in this checkout, and every scaling statement above is about the example
cluster in `deploy/kubernetes/aerospike/`, not the one a real deployment points at.

It also means the file's own default for the ledger is a deliberate dead end. `utxostore = null:///`
(`settings.conf:1624`) names a scheme the registry does not implement, so a node started with the file
as-is and no override fails at start-up quoting that scheme back — which is the right behaviour for a
ledger, and a pleasing contrast with the blob store, where the same string means "accept everything
and keep nothing" and no one notices for days (§67). Same six characters, opposite philosophies.

---

### Transaction Metadata Cache

*The inventory calls this a cache, and it is one. But it is also the only thing in Teranode with a
wire protocol of its own, a Kafka topic of its own, and a size that is decided twice over: once by an
operator in a settings file, and once by which Makefile target compiled the binary.*

#### What it does

Validating a transaction means reading its parents. Not the pending ones — the *parents*, the
transactions whose coins this one spends, which in a busy node are often transactions that arrived
seconds ago and have not been mined yet. Every one of those reads is a round trip to the ledger.

The metadata cache sits between Subtree Validation and the ledger and remembers what was just read:
for each transaction, the things a child's validation needs — the scripts and values of its outputs,
and the list of hashes its inputs came from. When the next child arrives and asks for the same parent,
the answer comes from memory.

Two things make it more than an in-process dictionary.

The first is that it is **fed from outside**. The Validator, having checked a transaction, publishes
what it learned onto a Kafka topic called `txmeta` (`daemon/daemon_kafka.go:68`). Subtree Validation
consumes that topic and writes the records into its cache
(`services/subtreevalidation/Server.go:264-271`, `services/subtreevalidation/txmetaHandler.go:63`).
So the cache is not only what this process happened to read; it is what the node as a whole has
learned, arriving over the bus.

The second is that it **forgets on purpose, early**. A record is only cached while the transaction is
unmined (`stores/txmetacache/txmetacache.go:468-472`), and the moment a transaction is mined the
entry is evicted (`:641-648`). This is not a ledger cache; it is a working memory for the pending
pile. That is why a gigabyte is enough for it, and why the inventory's "short-term memory of recently
checked transactions" is, unusually, exactly right.

#### How it works

**One process owns it.** The cache is constructed inside Subtree Validation, wrapped around that
service's handle on the ledger, and only when `subtreevalidation_txMetaCacheEnabled` is true
(`settings.conf:1518`, which is the shipped default). Other services that call the same ledger
method — the Block Persister (`services/blockpersister/streaming_process_subtree.go:160`) and the
Asset Server (`services/asset/repository/GetLegacyBlock.go:626`) — get no benefit from it, because
they hold their own unwrapped handles in their own processes. It is one service's private memory,
not a shared utility.

**The memory is fixed and sharded.** At construction the cache takes its budget
(`txMetaCacheMaxMB`, 1,024 MiB in `settings.conf:1580`) and divides it by a constant number of
buckets, then reserves a ring buffer per bucket. Writes go to the bucket chosen by hashing the
transaction hash; when a bucket is full the oldest entry in *that bucket* is overwritten
(`stores/txmetacache/improved_cache.go:386`, `stores/txmetacache/pointer_cache.go:276`). Eviction is
by insertion order within a shard, not least-recently-used across the whole cache, which is cheaper
and needs no bookkeeping on the read path.

**The read path prefers honesty to cleverness.** On a miss the cache reads the real ledger, and then
caches the answer *only if* the record looks like an unmined transaction — no block attached, not
marked conflicting (`stores/txmetacache/txmetacache.go:468-472`). The same test guards the batched
path (`:582-584`). So a mined transaction is never re-cached, and the cache cannot quietly grow into
a stale copy of the ledger.

**The wire carries two formats at once.** Messages on `txmeta` are batches, and two layouts share the
topic (`stores/txmetacache/wire.go:11-25`):

```text
v1   [4 bytes] entry count
     then per entry:  [32 hash][1 action][4 length][payload]

v2   [1] magic 0xFF  [1] version 0x02  [2] reserved  [4] entry count
     then per entry:  [8 xxhash][32 hash][1 action][4 length][payload]

```

Actions are just "here is the metadata" and "forget this hash" (`wire.go:27-32`). The producer picks
the format from `validator_txmeta_wireFormat`, whose default is **v1**
(`settings/settings.go:487`) — and `settings.conf` never mentions the key at all, so the shipped
format is v1 and the extra eight bytes per entry that v2 offers are not being sent.

**v2 exists to make the receiver's writes lock-free.** In v2 mode the producer puts the receiver's own
bucket number to work: it hashes the transaction hash, turns that into a bucket index, and routes the
record to the Kafka partition that owns that bucket's range
(`services/validator/Validator.go:2032-2034`), publishing with manual partitioning so nothing second-
guesses it (`daemon/daemon_kafka.go:81-83`, `util/kafka/kafka_producer_async.go:112-117`). Each
partition then owns a disjoint run of the receiver's buckets, so Subtree Validation can handle several
partitions at once without two of them fighting over one shard
(`services/subtreevalidation/txmetaHandler.go:88-93`).

**And every instance is told everything.** The consumer group name gets sixteen random characters
appended, "to allow multiple instances to run concurrently and consuming the same messages
independently" (`daemon/daemon_kafka.go:178-180`). Kafka is being used as a broadcast here, not as a
split: each consumer receives every record and keeps a complete copy.

#### The hard part: a build tag is part of the wire contract

The bucket number that v2 routing is computed from is not a setting. It is a compile-time constant,
chosen by build tag:

| Build                       | Bucket count | Where                                                   |
| --------------------------- | ------------ | ------------------------------------------------------- |
| Default (production-shaped) | 8,192        | `stores/txmetacache/improved_cache_const_large.go:10`   |
| `-tags smalltxmetacache`    | 32           | `stores/txmetacache/improved_cache_const_small.go:10`   |
| `-tags testtxmetacache`     | 8            | `stores/txmetacache/improved_cache_const_testing.go:10` |

The producer divides *its* constant into partition ranges (`services/validator/Validator.go:2009-2034`)
and the receiver indexes *its* constant to choose a bucket
(`stores/txmetacache/improved_cache.go:386`). Nothing in the message says which constant was in force,
and nothing checks that the two binaries agreed. Build the Validator with `smalltxmetacache` and
Subtree Validation without it, and every record still arrives, still validates, still gets cached —
the ranges simply stop being disjoint, and the concurrency the partitioning exists to buy turns back
into lock contention. Correct, slower, invisible. Section 32 found a floor derived from another
service's constants; this is the same coupling with a compile step bolted on.

There is a second mismatch of the same kind, and this one is armed in the file today. The producer's
partition count is `validator_txmeta_numPartitions`, default **32**
(`settings/settings.go:488`), which appears nowhere in `settings.conf`. The topic's actual partition
count comes from `KAFKA_PARTITIONS_HIGH` (`settings.conf:853`), which is **8** in the shared default
context and 32 only in the `docker.m` and `operator` contexts (`settings.conf:38-40`). So in the
plain default context, the number the routing divides by and the number of partitions that exist are
different — the routing would address partitions 8 through 31 of an eight-partition topic, in a mode
documented as having "no fallback" (`util/kafka/kafka_producer_async.go:113-116`). What holds it back
is that v2 is off by default. Enable v2 without raising `KAFKA_PARTITIONS_HIGH` in the same context
and the two numbers meet for the first time.

#### The hard part: the number that says whether it is working is commented out

The cache reports insertions, hits, misses, origin reads, evictions, trims and map size
(`stores/txmetacache/metrics.go:211-287`). The gauge that would have reported how many entries it is
actually holding is commented out (`:199-207`) — while the documentation block a dozen lines above
still advertises it: "size: Current number of entries in the cache" (`:180-182`). So the operator can
see traffic and can see the size of the internal maps, but cannot see occupancy, which is the one
number that distinguishes "cache is nearly empty because the node is idle" from "cache is full and
churning". Hit and miss counters exist; the thing you would divide them by to reason about capacity
does not. Section 49 met a cache that knew exactly what its own silence meant; this one is quieter.

#### Can it scale by adding machines

**By design it scales by copying, not by splitting — and the code says so out loud.** The random
consumer-group suffix (`daemon/daemon_kafka.go:178-180`) means every Subtree Validation instance
receives every metadata record and holds a full copy. Adding a second instance doubles the memory
spent on the cache and halves nothing; what it buys is a second process that can answer parent reads
from memory, which is the thing that actually relieves the ledger. That is a sound choice for a
working set this small — a gigabyte of pending metadata, not the chain — and it keeps the design
honest: no cache is ever authoritative, because every miss goes to the ledger anyway
(`stores/txmetacache/txmetacache.go:455-467`).

**As shipped there is one copy, and the harness most developers run does not have it at all.** Every
example deployment runs a single Subtree Validation instance, so there is one cache of 1,024 MiB
(`settings.conf:1580`), fed at v1 volume, sized by a bucket count chosen when the binary was compiled.
The 32 GiB alternative sits commented out in both places an operator would look — `settings.conf:1579`
and `deploy/kubernetes/teranode/teranode-configmap.yaml:30` — with no note about when it should be
raised. And the in-process test harness turns the cache off entirely
(`daemon/test_daemon.go:438`), which is correct for tests and means the cached path is exercised by
deployment-shaped runs rather than by `make test`. Worth knowing before concluding from a green test
suite that the cache is covered.

One more asymmetry worth recording: the v2 format exists to make this cache scale better, and it also
raises the per-entry overhead by eight bytes on a topic that every consumer receives in full
(`wire.go:43-47`). For a broadcast consumer group, better locking on the receiver is paid for by more
bytes on every reader. That trade is only favourable at the replica count the design was written for,
and nothing in the settings file says what that count was.

---

### Chain Store

*The inventory lists three stores, and the ledger's own store is not one of them — it belongs to the
Blockchain service and has never had a section. It is where the chain actually lives, and it is the
one component in this system whose scaling answer is written in its own comments.*

#### What it does

Everything the previous section describes as "what the ledger says" is rows in four tables:

- **`blocks`** — one row per block ever seen, on the main chain or not, linked to its parent by a
    column. The chain, in other words, is a self-referencing table.
- **`state`** — a small key/value table, and the row that matters is the FSM state, the permission
    slip the Blockchain service writes before it acts.
- **`block_id_reservations`** — numbers handed out to blocks that have not been committed yet.
- **`**scheduled_blob_deletions**`** — the promises about when raw block data may be thrown away.

Two engines can hold those tables: a single file (SQLite) or a server (PostgreSQL). The shipped
default is the file (`settings.conf:483`), and every multi-container configuration points at the
server (`:484-485`) — the same split the Blockchain section describes, seen from the side where it is
a connection string rather than a policy.

#### The problem this removes

A blockchain is the worst possible thing to keep in a general-purpose database. It is a graph that
only ever grows, questions about it are "is this on the path from the tip?" — which means walking —
and the answer changes when the chain reorganises, but only for a moment and only for some blocks.
Meanwhile a node answers those questions thousands of times a minute while it validates, and the
same table is being written by whichever path is currently bringing blocks in.

The store's job is to make that combination survivable: to answer chain questions without walking the
chain every time, to hand out block numbers without two writers colliding, and to keep its own memory
bounded while doing both.

#### How it works

**Chain membership has two engines too**, in the same dialect as the rest of the system. The quick one
keeps the off-chain set in memory and answers in one lookup. The slow one asks the database — reading
an `on_main_chain` column when the flags are trustworthy, and falling back to a recursive walk of the
parent links when a rebuild is in progress (`stores/blockchain/sql/CheckBlockIsInCurrentChain.go:312-330`).
The switch is a setting, and as shipped it is off: `blockchain_use_in_memory_chain_check = false`
(`settings.conf:466`). So the default configuration asks the database, in batches capped at a thousand
identifiers because Postgres refuses more than about thirty-two thousand bind parameters
(`stores/blockchain/sql/CheckBlockIsInCurrentChain.go:14-17`).

**Block numbers are reserved twice over.** Before a block is committed it needs a number, and two
paths can be bringing the same block in at the same moment — the Legacy sync service and Block
Validation's catch-up, both active during initial sync. The store's own comment spells out what
happens without a guard: each asks the database for a number, gets a different one, and the block's
spending records are written against the first number while the block itself commits under the second.
The first number becomes a phantom that fails a later consistency check and stalls sync
(`stores/blockchain/sql/sql.go:128-136`). The guard is two-level: an in-process cache for the
common case, and a durable table whose entries survive a restart and are claimed with an
insert-or-ignore so that a second process cannot take a number already promised
(`stores/blockchain/sql/assign_block_id.go:32-82`, `:108`).

**Chain walks are cached, and deliberately stale.** A dedicated cache holds the results of the
parent-walking queries for ten minutes (`stores/blockchain/sql/sql.go:53`, `:1314-1318`). It is not
cleared when a block is stored or a block field is updated — only when the chain's shape changes:
on reorganisation, on invalidation, on deletion, on revalidation
(`stores/blockchain/sql/StoreBlock.go:357`, `InvalidateBlock.go:139`, `DeleteBlock.go:79`,
`RevalidateBlock.go:56). The reasoning is in the comment: parent links never change, so the only thing
that can make a cached walk wrong is a block changing its mind about being on the chain.

#### The hard parts

**A schema change can starve readers, and the code says so.** Bringing two instances up against one
database means both may run the table-creation sequence. Every statement in that sequence takes at
least a shared lock on the blocks table, and if another instance has started its background rebuild —
which holds a row-exclusive lock on the same table for the length of the walk — the shared requests
queue. Postgres queues fairly, which means the ordinary read queries that arrive afterwards line up
behind the queued DDL and wait. The comment calls this out precisely: "That cascade is what blocks
readers during otherwise idle startup" (`stores/blockchain/sql/sql.go:484-493`). The fix is a fast
path that asks the database's own catalog — which takes no lock on the blocks table at all — whether
the schema is already current, and returns without touching it if so (`:494-500`). Schema creation is
also serialised behind a fixed advisory lock, one number chosen once and frozen because two instances
must agree on it (`:477-479`).

That is a genuinely distributed-systems problem, solved, and worth noting for what it implies: the
failure was only reachable with more than one instance against one ledger, so somebody met it.

**The default engine has a hard concurrency ceiling that no setting raises.** The connection pool for
the file-based store is fixed at five, in code, ignoring every pool setting in the settings file
(`util/sql.go:290`). The comment is candid about why: the SQLite driver accumulates working memory in
a global arena that never gives pages back, so under test load the process's memory use climbed
unpredictably — "the main driver of sql.test's bimodal ~2G/~34G swing and the intermittent OOM
(#1051)" — and five is ample anyway because SQLite writes one at a time (`util/sql.go:285-289`).
Reasonable, and it has a consequence worth stating plainly: a node running the shipped default
configuration can have five database operations in flight, whatever its settings say about fifty.

**A number can be reserved and never used.** Reservations exist to be claimed, and a block abandoned
halfway through ingestion leaves a number behind. Entries expire rather than accumulate, which bounds
it (`stores/blockchain/sql/sql.go:136`) — but it also means the blocks table has gaps after a rough
sync, and anything reading those numbers has to treat a gap as normal rather than alarming.

#### Scaling

**By design: this is the component that was built for a second instance, and the evidence is the
durable reservation table.** Its comment says the entries survive "ttlcache TTL expiry, restart, and a
second instance" (`stores/blockchain/sql/assign_block_id.go:62-63`), and the cross-process race is
closed with an insert-or-ignore rather than a promise that only one process will ever run
(`:43-44`, `:108`). The
readers-starvation cascade above is not a hypothetical either — it is a bug someone met. So two
Blockchain processes against one ledger is a case this store was written for, which is more than can
be said for most of the system.

What does not divide is the write path. One database accepts the writes; SQLite serialises them at five
connections and Postgres serialises the worst of them behind table locks. So the honest design answer
is the same one the Blockchain service reached: many readers, effectively one writer, and widening
means a bigger database rather than more databases.

**As shipped: one instance, and on the default engine, five open connections.** Every deployment in
this repository runs a single Blockchain, so the multi-instance machinery in this store — the durable
reservations, the advisory lock, the catalog fast path — is exercised in tests and in somebody's
incident, not in the shipped topology. And the default file-based engine caps concurrent database work
at five while the settings file offers fifty (`settings.conf:116`), a difference an operator can only
discover by reading the code that ignores it.

---

### The Interfaces Between Services

*Every section above describes a room. This one describes the corridors, and the rules about who is
allowed to knock on which door. It is the last section because it is the one that ties the rooms
together, and because the honest answer to "is this a system of services or one program in
several costumes" lives here rather than in any individual service.*

#### The two descriptions every service carries

Each service that can be asked to do something carries two descriptions of itself, and the
difference between them is the whole design.

One is written for the program. It is a Go interface: a list of the questions a caller may ask, with
plain Go answers — an error, a flag, a list of identifiers. It is compiled into every process that
uses the service, so a caller that asks a question the service never learned to answer fails at
build time, not at two in the morning.

The other is written for the network. It is a protobuf contract: the same questions, described in a
language-neutral way, together with a port to send them to. It is what a process on another machine
— or a command line tool, or the dashboard — works from.

The house rule is that the first must never mention the second. A service's published questions take
and return plain Go types; the translation to and from the network format is buried inside that
service's client code, where it can only be reached through one door. The reference implementation
named in the project's own conventions is P2P, and P2P keeps the rule exactly: twenty-six questions,
none of them speaking a wire type.

Why the rule is worth having: wire formats are the part of a system that gets negotiated with other
people, so they change for reasons the code cannot control. If a wire type has leaked into a
published Go question, then changing that format is no longer a change to one file — it is a change
to every caller of that question, everywhere.

#### Who publishes a description at all

| Service            | Question list | Client side | Wire contract | Questions taking wire types |
| ------------------ | ------------- | ----------- | ------------- | --------------------------- |
| P2P                | Yes           | Yes         | Yes           | 0 of 26                     |
| Subtree Validation | Yes           | Yes         | Yes           | 0 of 3                      |
| Validator          | Yes           | Yes         | Yes           | 0 of 6                      |
| Block Validation   | Yes           | Yes         | Yes           | 0 of 6                      |
| Propagation        | Rename        | Yes         | Yes           | 0 of 1                      |
| Block Assembly     | Yes           | Yes         | Yes           | 3 of 18                     |
| Blockchain         | Yes           | Yes         | Yes           | 5 of 73                     |
| Legacy             | Nested        | Nested      | Yes           | 6 of 6                      |
| Asset Server       | No            | No          | Yes           | —                           |
| Pruner             | No            | No          | Yes           | —                           |
| Alert              | No            | No          | Yes           | —                           |
| RPC                | No            | No          | No            | —                           |
| Block Persister    | No            | No          | No            | —                           |
| UTXO Persister     | No            | No          | No            | —                           |

Two things in that table are worth more attention than the rest.

**Half the services have no published question list.** (§76.) Six services — Asset Server, Pruner, Alert,
RPC, Block Persister, UTXO Persister — have no Go interface at all. Three of them are reached only
over the network or over the conveyor belt, which is defensible: nobody inside the program calls
them, so there is no in-process question list to publish. The awkwardness is the other three, and
Legacy, whose list exists but lives a directory deeper than anyone looks for it
(`services/legacy/peer/Interface.go`). Propagation renamed its list to `ClientInterface.go`, so
there are now two spellings of the same idea, and a search for the house convention misses it.

**Every question the door answers today is inside a wire type.** This is the leak that costs
something, and it is not a mistake in a corner; it is the event bus. Blockchain publishes
notifications — "the tip moved", "we are catching up", "this block was invalidated" — and both ends
of that pipe are typed with a generated wire structure:

- `SendNotification(ctx, notification *blockchain_api.Notification)`
    (`services/blockchain/Interface.go:111`)
- `Subscribe(ctx, source) (chan *blockchain_api.Notification, error)` (`services/blockchain/Interface.go:558`)

The consequence is precisely the one the rule exists to prevent. Every service in the node subscribes
to that channel, so every service in the node is now welded to a protobuf-generated type — and the
translation the rule wanted in one place has instead been pushed out to every consumer. Blockchain
carries five such leaks out of seventy-three questions; Block Assembly carries three out of
eighteen. Those eight signatures are the corridors' debt, and they are concentrated in the one
service everyone talks to (§77).

#### The wire surface, measured

Across eleven contract files, the system offers **162 remote methods**. The Blockchain service
supplies 92 of them — more than half the total, from one component. P2P has 25, Block Assembly 19,
Legacy 7, Block Validation 6, Validator 5, Propagation and Subtree Validation 3 each, Pruner and
Alert 1 each.

That distribution is the architecture's centre of gravity stated as an integer. Everything else is
arranged around not being wrong about what the ledger says.

The Asset Server is the one contract with no remote methods at all: its `.proto` file carries an
enum of message types for its WebSocket feed and nothing else. It is an HTTP and streaming service
that happens to own a protobuf file, which is worth knowing before looking for its API in the
wrong place.

Before any of those surfaces carries a call, someone has to turn a name into an address, and the
node does that two different ways depending on which configuration context it starts in. The setting
is `grpc_resolver` and the plain default is `dns` (`settings.conf:814`); in the `operator` context —
the one the Kubernetes ConfigMap actually sets (`deploy/kubernetes/teranode/teranode-configmap.yaml:7`)
— it is `kubernetes` (`settings.conf:815`). The chosen resolver is installed once, at daemon start
(`cmd/teranode/daemon.go:79`).

The two dialects are visible in the addresses themselves. Ordinary contexts write a host and a port
in the slot. The `operator` context writes nine addresses beginning `k8s:///` — Block Assembly,
Blockchain, Block Validation, Coinbase, Legacy, Pruner, Subtree Validation and Validator among them
(`settings.conf:334`, `:440`, `:532`, `:684`, `:907`) — where the service name sits in the URL's path
rather than its host, because it is not a hostname. Something has to look it up, and lookup means
asking the Kubernetes control plane which pods currently back that name. That is the honest reason
this system can scale in Kubernetes at all: a second Block Assembly becomes reachable without anyone
editing a configuration file.

Two things about that lookup are worth knowing before trusting it. This repository contains its own
resolver for the job, four hundred and sixty-seven lines of it (`pkg/k8sresolver/`) — and in the
`operator` context the registration order means a general-purpose upstream resolver quietly takes the
slot instead, so the code a reader finds here is not the code answering the question (§80). And the
lookup is an API call requiring permission to read the cluster's endpoint records, which no manifest
grants — not because a manifest grants the wrong thing, but because the repository has no `Role`,
`ClusterRole`, `RoleBinding` or `ServiceAccount` object anywhere in it. The one place a service
account is mentioned at all is Aerospike's pod specification, where it is switched
off (`deploy/kubernetes/aerospike/aerospike-statefulset.yaml:20`). A refused lookup fails at startup
rather than degrading, and the Docker path — where the resolver is DNS and none of this runs — will
never show it (§80).

Four surfaces carry all of it:

- **gRPC**, for anything that needs an answer. Twenty-nine ports are named in the port group of the
    settings file (`settings.conf:88-141`), and nine services listen.
- **Kafka**, for anything that can wait. Ten topics are configured
    (`settings/settings.go:152-161`); two are unwired as shipped, discussed below.
- **HTTP**, for the doors the outside world uses — Asset Server, Blockchain, Legacy, Propagation,
    Validator, and a health endpoint, all bound to every interface. Block Persister is the one
    exception and binds to the machine's own loopback only (`settings.conf:287`), which is a
    sensible default for the service that holds the permanent archive.
- **UDP multicast**, for the fast lane. Receive-only, IPv6, switched off as shipped, and — as it
    turns out — with no code anywhere that sends on it (§82).

#### How one service finds another

This is the part that has been thought about properly, and it deserves to be described plainly
because it is the mechanism that makes "move it to another machine" true.

Every service has a dial target — where to call — and a listen address — where to answer — and both
change with the deployment context, so that the same binary needs no edit to run on a laptop, in a
container, or in a cluster. Four dialects of the same name appear in the settings file, using the
Blockchain service as the example:

| Context               | The line the caller reads      | Shape of the answer                 |
| --------------------- | ------------------------------ | ----------------------------------- |
| Development           | `settings.conf:445`            | a name only this machine knows      |
| Plain Docker          | `settings.conf:437`            | a placeholder filled in at start-up |
| Multi-service Docker  | `settings.conf:438`            | the container's own network name    |
| Kubernetes            | `settings.conf:440`            | a cluster address in its own scheme |

Two things in that table deserve a pause.

The Kubernetes answer is not a hostname the operating system can resolve. It carries its own scheme,
`k8s:///blockchain.namespace.svc.cluster.local:8087`, and resolving it means asking the cluster which
pods are running behind that name and spreading calls across them. That is how any service the node
really dials can be widened without a load balancer in front of it: the caller does the spreading.
Whether the node exercises this for a given service is a separate question, taken up below — the short
version is that the one service the production example widens to eight copies
(`deploy/kubernetes/teranode/teranode-cr-mainnet.yaml:183`) is not reached through this door at all in
the shipped configuration, and is fed by the conveyor belt instead.

Whether that trick is switched on is itself a setting:

```conf
grpc_resolver          = dns
grpc_resolver.operator = kubernetes
```

(`settings.conf:814-815`)

In the default context the resolver is plain DNS, which cannot read a `k8s:///` address at all — so
the Kubernetes dialect is only speakable in the context written for it, which is correct, and worth
knowing before copying a cluster address into a local run.

The system also carries its own resolver, about twelve hundred lines that watch the cluster's
endpoint records directly and cache the answers (`pkg/k8sresolver/k8s.go:30-41`, `:66`). It is
installed at start-up (`util/grpc_helper.go:525-537`) and, in the one context that turns it on, it
is installed twice over: the in-repo builder is registered under the name `k8s`, and then the
standard one is registered under the same name, which replaces it. So the production path uses the
standard resolver, and the in-repo resolver runs only under a setting value nothing in this
repository selects. It is live code that shipped configuration steps around, and the reconciliation
notes record it.

For the last of those the service name is not resolved by the operating system. The cluster address
carries its own scheme, and which resolver answers is itself a setting:

```conf
grpc_resolver          = dns
grpc_resolver.operator = kubernetes
```

(`settings.conf:814-815`)

The Kubernetes answer matters, because a cluster service name is not one machine. Whoever resolves
it gets back the list of running copies and the caller spreads its requests across them, which is
how the eight Validator replicas in the production-shaped example actually receive work without a
proxy, a mesh, or a load balancer in front of them
(`deploy/kubernetes/teranode/teranode-cr-mainnet.yaml:183`). Resolution is client-side, so a caller
that can see the cluster can use all of it.

Teranode also ships its own resolver for this — `pkg/k8sresolver`, about twelve hundred lines that
watch the cluster's endpoint records and cache the answers
(`pkg/k8sresolver/k8s.go:66`, `:100-104`) — and there is an awkwardness in how it is installed, in
the reconciliation notes.

#### Who is allowed to ask

One shared secret guards the interior: `grpc_admin_api_key`. It is a single value handed to every
service, and it is treated in three different ways depending on which service is asked (§78).

- **Blockchain** makes it a condition of existing. Startup is refused outright without a real key
    (`services/blockchain/Server.go:524`), one public method aside
    (`services/blockchain/auth.go:15-17`), and its HTTP admin routes check the same secret on a
    header (`services/blockchain/auth.go:22-27`). Its clients verify the key works before they will
    use the connection, and refuse to start if it does not
    (`services/blockchain/Client.go:158-163`).
- **P2P and Legacy** fail closed without stopping the node: an unset or well-known placeholder key is
    ignored and a random one generated, so the node keeps validating and relaying while its admin
    methods go unreachable until a real secret is configured
    (`services/p2p/Server.go:1389`, `services/legacy/Server.go:787-804`,
    `util/admin_api_key.go:59-66`).
- **The other six** start their gRPC servers with no authentication at all: Validator, Block
    Assembly, Block Validation, Subtree Validation and Pruner pass nothing
    (`services/validator/Server.go:483`, `services/blockassembly/Server.go:1081`,
    `services/blockvalidation/Server.go:1233`, `services/subtreevalidation/Server.go:589`,
    `services/pruner/server.go:301`), and Propagation passes a policy hook without a key
    (`services/propagation/Server.go:355-361`). Anything that can reach the port can ask anything of
    them.

The install scripts and cluster files make the key a prerequisite — the development cluster file says
it plainly: "REQUIRED: include grpc_admin_api_key for Blockchain startup and all service clients"
(`deploy/kubernetes/teranode/teranode-cr.yaml:82-83`) — and there is no secret definition anywhere
under `deploy/`, so the operator must create it out of band (§79).

And on the wire, that key travels in the clear. Encryption is off in every context
(`settings.conf:1310`), the code knows this and warns rather than refusing
(`util/admin_api_key.go:84-88`), and its own comment gives the reason for warning instead of
stopping: "internal deployments on trusted networks legitimately run without TLS"
(`util/admin_api_key.go:70-72`). That is a coherent position for a node whose interior lives on a
private network. It is worth confirming that the network is private before relying on it.

There is also a shared client-side convention for carrying a secret on every call — a small typed
credential with its metadata plumbing (`util/grpc_helper.go:49-68`), declared to be fine without
transport security (`:66-68`). No code in the repository constructs it. The three clients that do
send the key build the header themselves.

#### The door that changes its answer

Worth standing in front of for a moment, because it is the clearest example in the system of two
components agreeing on the interface and disagreeing on the promise.

A new transaction arrives at Propagation. What Propagation tells the submitter depends on whether a
certain Kafka topic has an address:

- **With the topic wired**, Propagation files the transaction, puts it on the belt, and answers
    "accepted" before anyone has checked it. Whether it is valid reaches the submitter later, or
    through another channel entirely.
- **Without it**, Propagation calls the Validator directly, waits, and answers with a verdict.

Both are correct against the same interface, and the switch is one line:

```conf
kafka_validatortxsConfig                      =
kafka_validatortxsConfig.operator             = ${KAFKA_SCHEMA}://${KAFKA_HOSTS}/...partitions=${KAFKA_PARTITIONS_HIGH}...
```

(`settings.conf:858-859`)

The empty first line is every deployment in this repository. Only the operator context fills it in.
So as shipped, the public transaction door answers truthfully and synchronously; in the production
shape it answers quickly and asynchronously. The code comment at the branch says so in as many
words, and names the two paths that can still report a reason
(`services/propagation/Server.go:1403-1417`).

The same setting also changes how much work arrives out of order: the topic that appears only in the
production shape is given the high partition count, while its own comment two lines above says
transaction validation order is critical so it cannot have multiple partitions
(`settings.conf:857-859`), and `KAFKA_PARTITIONS_HIGH` is 32 there (`settings.conf:40`). The
Validator has already written code for the consequence: a transaction whose parent has not landed
yet is counted and dropped rather than gossiped about, because out-of-order delivery is "produces by
design", and the proper fix — holding orphans until their parent arrives, which the Legacy service
already does — "remains deferred" (`services/validator/Validator.go:709-719`).

#### The corridor most of the system skips

There is one setting that quietly rewrites the map of who calls whom:

```conf
useLocalValidator = true
```

(`settings.conf:1602`, and with no context suffix, so it is true in every context including
production)

Its effect is at the moment a service asks for a Validator client: instead of opening a connection,
the process builds a Validator inside itself (`daemon/daemon_stores.go:280-283`). RPC, Block
Validation, Propagation and Legacy all ask for a Validator client
(`daemon/daemon_services.go:567`, `:820`, `:1085`, `:1164`) and in the shipped configuration none of
them dials one.

Read from the interfaces' point of view, this means the Validator's five remote methods and its
listen address are, for the synchronous path, a door the node opens onto its own hallway. The
service still runs in the production-shaped example, still eight copies wide, and it is reached —
over Kafka for blocks, and over gRPC only by the diagnostic commands
(`cmd/monitor/monitor.go:200`, `cmd/diagnose/health_checks.go:139`). The address it would dial, if
asked, is `0.0.0.0` in the port's slot (`settings.conf:1743`), which on Linux means "this machine",
so a split deployment that turned the shortcut off without correcting that line would find each
copy calling itself.

This is not a bug. It is a performance decision — signature checking is the hot path in a Bitcoin
node, and doing it in the caller's own process avoids a round trip and a serialisation for every
transaction. It is worth naming here because it changes what the interface inventory means (§81): the
most-called service in the design is, as configured, not actually called over the interface that was
built for it.

#### Scaling

**By design: yes, and this layer is the reason the claim is true.** The question lists are compiled,
so a broken handover is caught at build time rather than at runtime. The wire contracts are
language-neutral, so a caller outside the program is a first-class citizen rather than a hack. Dial
targets, listen addresses, and the resolver are all per-context, so the same binary runs on a
laptop, in one container, in a split container set, or in a cluster with client-side spreading
across replicas. Nothing in this layer has to be redesigned to add a machine — the corridors are
already drawn between machines.

Two design costs are worth carrying forward. Every consumer of every event is now inside a wire type,
so the format that is most likely to be negotiated outward is the one most deeply embedded inward.
And one shared secret protects an interior that is only partly authenticated and, as configured,
unencrypted — which is a design that presumes a private network rather than one that enforces it.

**As shipped: it depends which surface.** gRPC scales as far as the deployment gives it addresses,
and the production shape does this properly: cluster service names, the Kubernetes resolver, eight
Validator replicas. Kafka scales on its own terms, by partitions, and here the shipped files are
less consistent — the topic that only exists in the production shape is configured against a comment
that forbids the setting it uses. HTTP scales behind any load balancer, and the dashboard's streaming
feed is built for exactly that. UDP does not scale because it is not running: the fast lane is
documented, configured off, and receive-only in the code (`services/propagation/Server.go:499`),
with no sender anywhere in the tree.

And the honest summary of this layer's scaling is the one this guide has reached for every service:
the mechanism is complete and the deployment is conservative. Everything here is built to be crossed
between machines. Whether it is, is set by whoever deploys the node.

---
