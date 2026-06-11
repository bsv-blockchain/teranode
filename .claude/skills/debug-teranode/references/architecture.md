# Teranode Architecture & Data-Model Reference

Dense reference distilled from `docs/topics/**`. Deployment-agnostic — no Kubernetes,
Docker, namespaces, or ports here (that lives in `debug-teranode-k8s` /
`debug-teranode-docker`). Source doc paths are cited in parentheses so claims can be
verified. This is a *map*, not the territory: where it matters for consensus-critical
work, confirm against the cited doc or the code. Points where the docs themselves are
unclear are tagged **[AMBIGUITY]** and collected in §10.

## Table of Contents

1. [What Teranode Is](#1-what-teranode-is)
2. [Service Map](#2-service-map)
3. [End-to-End Transaction Lifecycle](#3-end-to-end-transaction-lifecycle)
4. [Block Lifecycle](#4-block-lifecycle)
5. [Data Model Schemas](#5-data-model-schemas)
6. [Stores](#6-stores)
7. [Communication & Messaging](#7-communication--messaging)
8. [Key Design Decisions & Invariants](#8-key-design-decisions--invariants)
9. [Glossary](#9-glossary)
10. [Debugging Anchors & Flagged Ambiguities](#10-debugging-anchors--flagged-ambiguities)

---

## 1. What Teranode Is

Teranode is the BSV Association's horizontally-scaled Bitcoin (BSV) node, built as a set
of Go microservices to overcome the throughput ceiling of monolithic node software
(`docs/topics/teranodeIntro.md`, `docs/topics/architecture/teranode-overall-system-design.md`).
Instead of scaling one machine vertically, work is distributed across services that scale
independently and communicate via gRPC (sync), Kafka (async), HTTP/WebSocket (external),
and UDP multicast (high-perf tx ingress). Combined with an **unbounded block size** and a
novel **subtree** abstraction, the design targets a sustained **>1,000,000 transactions/second**.
There is **no mempool**: validated transactions flow continuously into subtrees rather than
sitting in a memory pool (`docs/topics/architecture/teranode-architecture-brief.md`).

---

## 2. Service Map

Ordered along the transaction → block pipeline. "Core" services run the consensus
pipeline; "overlay" services are auxiliary (`docs/topics/architecture/teranode-microservices-overview.md`).

| Service | Responsibility | Consumes | Produces / Sends | Transport(s) |
|---|---|---|---|---|
| **Propagation** | Tx ingress gateway; sanity-checks and stores raw txs, hands off to Validator | Txs from network/clients | Stores raw tx in Tx (blob) store; tx → Validator | In: gRPC, HTTP (`/tx`,`/txs`), UDP multicast. To Validator: Kafka `validatortxs` (normal) / HTTP (large tx) / direct call (embedded) |
| **Validator** | Validates each tx against consensus + policy rules; updates UTXO store | Txs from Propagation / Subtree Validation / Legacy | UTXO updates; tx-meta → Subtree Validation; validated tx → Block Assembly; rejection → P2P | In: direct call (local) or gRPC. Out: **direct gRPC `Store()` → Block Assembly**; Kafka `txmeta` → Subtree Validation; Kafka `rejectedTx` → P2P |
| **Block Assembly** | Groups validated txs into subtrees; builds mining candidates; handles reorgs | Validated txs (gRPC from Validator); block/subtree notifications (Blockchain) | Subtrees → Subtree store + announce; mining candidates → miners; assembled block → Blockchain | gRPC (in/out); subscribes to Blockchain notifications |
| **Subtree Validation** | Validates subtrees received from peers; decorates with metadata (TxInpoints); persists | Subtree notifications (Kafka `subtrees`); tx-meta (Kafka `txmeta`); requests from Block Validation | Validated+decorated subtree → Subtree store | In: Kafka `subtrees`, Kafka `txmeta`, gRPC. Calls Validator for missing txs |
| **Block Validation** | Validates full blocks (PoW, merkle root, subtree contents, tx ordering); adds to chain; marks txs mined | Block announcements (Kafka `blocks`); blocks from Legacy | `AddBlock` → Blockchain; coinbase → UTXO+Tx store; `SetMinedMulti` → UTXO | In: Kafka `blocks`, gRPC. Calls Subtree Validation (`CheckBlockSubtrees`); calls Blockchain |
| **Blockchain** | Owns chain state, headers, FSM, chainwork/longest-chain; persists blocks; fans out notifications | `AddBlock`/`InvalidateBlock` (gRPC from Block Assembly + Block Validation) | Block/Subtree/MiningOn notifications; finalized block → Block Persister (Kafka `blocksFinal`) | gRPC server; pub/sub; Kafka producer |
| **Asset Server** | Read facade over all stores; serves blockchain data to peers + clients; WebSocket feed | UTXO store, Blob store, Blockchain store | HTTP/REST; WebSocket (Centrifuge) push | HTTP/HTTPS, WebSocket. Also exposes FSM endpoints |
| **Alert** | Alert system: freeze/unfreeze/reassign UTXOs, invalidate blocks, ban peers (court-order recourse) | Alert messages (private P2P alert network) | UTXO freeze/reassign; Blockchain `InvalidateBlock`; peer bans | libp2p (alert net), gRPC |
| **Block Persister** *(overlay)* | Post-processes confirmed blocks: writes `.block`/`.subtree`/`.utxo-additions`/`.utxo-deletions` files | Polls Blockchain for unpersisted blocks; reads Subtree + UTXO stores | Files → block-store; `BlockPersisted` notification | Polls Blockchain; writes blob store. **[AMBIGUITY]** also described as consuming Kafka `blocksFinal` — see §10 |
| **UTXO Persister** *(overlay)* | Builds a full on-disk UTXO **set** file per block (seedable into new nodes) | `.utxo-additions`/`.utxo-deletions`/`.block`; headers from Blockchain | `.utxo-set` file per block; `lastProcessed.dat` | Reads/writes blob store. Lags tip ≥100 blocks (finality) |
| **Pruner** *(overlay)* | Event-driven UTXO pruning (Delete-At-Height); preserves parents of old unmined txs | `BlockPersisted` (primary), `Block` (fallback) | Deletes UTXO records + external `.tx`/`.outputs` blobs | gRPC; subscribes to Blockchain; checks Block Assembly state |
| **P2P** *(overlay)* | libp2p transport: peer discovery, gossips blocks/subtrees/rejected-txs, ban list, reputation | Blockchain notifications; Validator rejected-tx; network gossip | To net: GossipSub publish. To node: block→Block Validation (Kafka `blocks`), subtree→Subtree Validation (Kafka `subtrees`) | libp2p/GossipSub/Kademlia DHT; gRPC; HTTP/WS |
| **Legacy** *(overlay)* | Bridges traditional BSV (SV Node) network ↔ Teranode; converts blocks↔subtrees both ways | BSV `inv`/`block`/`tx`; Teranode subtree notifications | Legacy blocks→Teranode (Kafka `blocks` + Asset-compatible HTTP); Teranode txs→BSV net | BSV P2P; Kafka `blocks`/`legacy_inv`; HTTP |
| **RPC** *(overlay)* | Bitcoin JSON-RPC compatibility (`getminingcandidate`, `submitminingsolution`, `sendrawtransaction`, `getblock`, freeze/reassign, setban…) | JSON-RPC requests | Routes to Block Assembly, Blockchain, Propagation (Distributor), UTXO store, P2P | HTTP/HTTPS + JSON; basic auth |

**Explicit data-flow (X sends Y to Z via W):**

- Propagation → **new txs** → Validator via Kafka `validatortxs` (or HTTP / direct call).
- Validator → **validated tx** → Block Assembly via **direct gRPC `Store()`** (NOT Kafka).
- Validator → **UTXO tx-meta** → Subtree Validation via Kafka `txmeta`.
- Validator → **rejected-tx notice** → P2P via Kafka `rejectedTx`.
- Block Assembly → **new subtree announcement** → P2P **via the Blockchain service**.
- P2P → **peer blocks** → Block Validation via Kafka `blocks`; **peer subtrees** → Subtree Validation via Kafka `subtrees`.
- Block Validation / Block Assembly → **AddBlock** → Blockchain via gRPC.
- Blockchain → **finalized block** → Block Persister via Kafka `blocksFinal`.
- Block Persister → **BlockPersisted** → Pruner and UTXO Persister.

---

## 3. End-to-End Transaction Lifecycle

The path a single tx takes from arrival to being mined (`docs/topics/transactionLifecycle.md`):

1. **Ingress — Propagation.** Tx arrives via gRPC / HTTP (`/tx`, `/txs`, batches up to 1,024) / UDP. Basic format check. Raw tx stored in **Tx (blob) store** keyed by tx hash, in received format (standard or Extended/BIP-239). (`docs/topics/services/propagation.md`)
2. **Hand-off to Validator.** Normal tx → Kafka `validatortxs`; tx over the Kafka message limit (~1MB) → Validator HTTP; embedded-validator mode → direct method call.
3. **Validation — Validator.** (`docs/topics/services/validator.md`)
   - If not extended, **auto-extend**: look up each input's parent output (satoshis + locking script) from UTXO store (parallel batched; `ErrTxMissingParent` if a parent is absent).
   - Run consensus rules (structure, value ≤ inputs, 0–21M BSV bounds, finality/locktime, coinbase scriptSig 2–100B, no *new* P2SH) + policy rules (min fee, sigops, dust; skippable via `SkipPolicyChecks`). Script verification via **GoBDK** (`ValidateTransaction`).
   - **Double-spend check**: inputs must reference unspent UTXOs (first-seen wins).
4. **UTXO updates + propagation (Validator, post-validation).**
   - Mark input UTXOs **spent**; create new output UTXOs with **`locked=true`** (two-phase commit phase 1) and `unminedSince`=current height.
   - Send tx to **Block Assembly** (direct gRPC `Store()`); send tx-meta to **Subtree Validation** (Kafka `txmeta`).
   - Reject path: notify P2P via Kafka `rejectedTx`; rejected tx is discarded (not stored).
5. **Subtree assembly — Block Assembly.** (`docs/topics/services/blockAssembly.md`) Subtree Processor dequeues txs FIFO (parent-before-child), appends to the current subtree. At target size (power of 2, default 1,048,576) it is persisted to the **Subtree store** (with finite DAH/TTL) and announced (→ Blockchain → P2P → network). On unlock, Validator sets `locked=false` so outputs become spendable pre-mining (enables tx chains). A timer also announces the current subtree at least every 10s.
6. **Mining candidate.** Block Assembly continuously builds candidates = {known subtrees, coinbase tx, header template, merkle proof} on the longest chain. Miner pulls one (`getminingcandidate`).
7. **Solution submission.** Miner submits nonce via RPC `submitminingsolution` → Block Assembly `SubmitMiningSolution` → builds the final block, validates, persists coinbase, calls Blockchain `AddBlock`.
8. **Mark mined.** When the block is added, `SetBlockSubtreesSet` fires; Block Validation (exclusive owner) calls UTXO `SetMinedMulti` for every tx → sets `blockID`/`blockHeight`/`subtreeIdx`, `unminedSince=0`, `locked=false` (two-phase commit phase 2). The tx is now mined.

In parallel, the tx's subtree is validated network-wide (§4) so block validation is fast ("pre-approval").

---

## 4. Block Lifecycle

(`docs/topics/services/blockAssembly.md`, `blockValidation.md`, `blockchain.md`, `subtreeValidation.md`, `docs/topics/datamodel/block_data_model.md`)

**Assembly (local mining):**

1. Validated txs → Subtree Processor → power-of-2 subtrees. Subtree 0 reserves **position 0 as a coinbase placeholder** (`0xFF...`).
2. Completed subtrees persisted + announced.
3. Mining candidate built; miner solves PoW.
4. On `SubmitMiningSolution`: build block, validate, store coinbase, `AddBlock` → Blockchain. Subtree DAH stays finite until Block Persister promotes it to permanent.

**Validation (peer blocks):** (`docs/topics/services/blockValidation.md`)

1. Block announced by P2P (header in message → quick check) → Kafka `blocks` → Block Validation priority queue (High/Medium/Low).
2. Fetch full block from the announcing node's Asset server.
3. **Parent unknown → catchup** (11-step process in `catchup.go`): fetch headers via block-locator, find common ancestor, enforce coinbase-maturity & secret-mining thresholds, verify checkpoints, concurrent-fetch + sequential-validate. FSM goes `RUNNING → CATCHINGBLOCKS → RUNNING`. **On error the FSM stays in CATCHINGBLOCKS** (no auto-revert).
4. **Parent known → validate**: for each subtree, if unknown → request Subtree Validation. Validate via `Block.Valid()`: PoW hash < target (nBits); merkle root matches subtrees; coinbase structure; coinbase value ≤ subsidy + fees; timestamp > MTP(11) and < now+2h.
   - **`validOrderAndBlessed`**: tx ordering (children after parents), no duplicate inputs, and **re-presentation detection** via **bloom filters** (one per recent block) plus a two-phase already-mined check (in-memory recent block IDs, default 50,000 → fallback `CheckBlockIsInCurrentChain`).
5. `AddBlock` → Blockchain → `SetBlockSubtreesSet` → Block Validation `SetMinedMulti` marks txs mined (two-phase commit phase 2). On failure: block invalidated.
   - **`optimisticMining`**: optionally add block *before* full validation, validate in background, `ReValidateBlock` on failure (≤3 retries). **[AMBIGUITY]** default — see §10.
6. **Merkle-root caveat:** subtree 0's stored hash uses the coinbase **placeholder**; to recompute the block merkle root you must substitute the real coinbase for tx 0 in subtree 0. Subtree hashes ≠ merkle-tree nodes directly. (`docs/topics/datamodel/block_header_data_model.md`)

**State management — Blockchain service** owns the canonical chain, selects the longest chain by **cumulative chainwork** (not height), handles reorgs (depth-limited `blockchain_maxReorgDepth` default 6), persists headers/blocks to the blockchain (SQL) store. *Building on the strongest chain* is **Block Assembly's** job, not Block Validation's.

**Persistence:** Blockchain → Kafka `blocksFinal` → Block Persister writes `.block` + per-subtree `.subtree` + `.utxo-additions` + `.utxo-deletions`, sets `persisted_at`, fires `BlockPersisted`. UTXO Persister folds additions/deletions into a rolling `.utxo-set` file (≥100 blocks behind tip). Pruner deletes DAH-expired UTXO records up to the safely-persisted height.

---

## 5. Data Model Schemas

### Transaction (`docs/topics/datamodel/transaction_data_model.md`)

Standard Bitcoin tx **or** Extended Format (BIP-239). Extended adds marker `0000000000EF` after version and, per input, the **previous output's satoshis + locking script** (so validation needs no UTXO lookup). Teranode **accepts both**, **stores in received format**, auto-extends standard txs in-memory during validation.

- Core fields: Version(4B), input count(VarInt), inputs, output count(VarInt), outputs, nLockTime(4B).
- Input: prevTxHash(32B), prevTxOutIndex(4B), scriptSig, sequence(4B) [+ EF: prevSatoshis(8B), prevScriptLen, prevLockingScript].
- **Debug-relevant:** `IsExtended()`; `ErrTxMissingParent` (CPFP / parent not yet validated / same-block parent).

### UTXO record (per-tx, in UTXO store) (`docs/topics/datamodel/utxo_data_model.md`)

One record per **transaction**, holding all its outputs as a `utxos` array. Aerospike record cap ~1MB.

| Field | Meaning (debug relevance) |
|---|---|
| `utxos` | Array of variable-len entries. **32B = unspent** (hash only); **68B = spent** (hash + 32B spendingTxID + 4B input index LE); **68B all-`0xFF` = frozen**. Size alone tells the state. |
| `locked` (bool) | **Two-phase commit.** `true` at create (outputs unspendable). `false` after block-assembly add OR `SetMinedMulti`. |
| `unminedSince` (uint32) | Height when first stored if **unmined**; `0` = mined on longest chain. Drives recovery + cleanup. |
| `blockIDs`/`blockHeights`/`subtreeIdxs` (arrays) | Where the tx was mined. Usually 1 entry; **multiple = fork**. |
| `conflicting` (bool) | Tx is a double-spend (terminal; gets a TTL/DAH). |
| `conflictingChildren` ([]hash) | On the first non-conflicting parent: lists conflicting child txs (poison-pill propagation). |
| `frozen` (bool) | Frozen by alert system. |
| `spendingDatas`/`spentUtxos`/`recordUtxos` | Per-output spend tracking + counters; `spentUtxos==recordUtxos` → DAH set for cleanup. |
| `spendingHeight` (uint32) | Coinbase only: `coinbase_height + 100` (maturity). 0 otherwise. |
| `preserveUntil` (uint32) | Protects a parent from deletion (Pruner phase 1). |
| `deleteAtHeight` / TTL | **DAH**: record deleted when chain height ≥ this. |
| `reassignments` | Audit of UTXO reassignments. |
| `external` (bool) | Full tx data in an external blob (large tx) rather than inline. |
| `tx`, `fee`, `sizeInBytes`, `isCoinbase`, `txInpoints` | Raw tx + metadata. |
| `utxoSpendableIn` (map offset→height) | Conditional/time-locked spendability; reassigned UTXOs spendable only after +1,000 blocks. |

- **Pagination:** >20,000 outputs (`utxoBatchSize`) → master record (index 0) + child records keyed `hash(txID+i)`. Multi-record creation uses a **lock record** (index `0xFFFFFFFF`, TTL 30–300s) + `creating=true` flag for atomicity.
- **UTXO Meta Data** (`stores/utxo/meta/data.go`): convenience struct `{Tx, ParentTxHashes, BlockIDs, Fee, SizeInBytes, IsCoinbase, LockTime}` returned by `GetMeta`.
- **TxInpoints** (`go-subtree`): `{ParentTxHashes []hash, Idxs [][]uint32}` — which parent outputs a tx consumes.

### Subtree (`docs/topics/datamodel/subtree_data_model.md`)

Holds a **batch of tx IDs + their merkle root** (NOT full tx data — nodes already hold the txs).

- Fields: `Height`, `Fees` (uint64), `SizeInBytes`, `FeeHash` (**unused**), `Nodes []SubtreeNode{hash,fee,size}`, `ConflictingNodes []hash`.
- Size = any **power of 2**; all subtrees in a block equal size (last may be smaller). Default `InitialMerkleItemsPerSubtree` = 1,048,576; dynamically adjusted (`UseDynamicSubtreeSize`).
- Storage ~48B/node; network transfer ~32B/node (hash only; fees/sizes reconstructed on receipt).

### Block (`docs/topics/datamodel/block_data_model.md`, `model/Block.go`)

Container of **subtree hashes**, NOT raw txs.

- `Header *BlockHeader`, `CoinbaseTx`, `TransactionCount`, `SizeInBytes`, `Subtrees []*hash`, `SubtreeSlices []*Subtree`, `Height`, `ID uint32` (storage ID from Blockchain; pre-allocatable via `GetNextBlockID()`).
- `BlockHeaderMeta`: `ID`, `Invalid` (inherited by children), `MinedSet`, `SubtreesSet`, `Height`, `TxCount`, `SizeInBytes`, `Miner`.

### Block Header (`docs/topics/datamodel/block_header_data_model.md`)

Standard Bitcoin **80-byte** header.

| Field | Size | Offset | Note |
|---|---|---|---|
| Version | 4B | 0 | LE |
| HashPrevBlock | 32B | 4 | chains blocks |
| HashMerkleRoot | 32B | 36 | from **subtree** roots (placeholder caveat §4) |
| Timestamp | 4B | 68 | MTP rule + now+2h |
| Bits (NBit) | 4B | 72 | compact target |
| Nonce | 4B | 76 | LE |

- Block hash = double-SHA256 of the 80 bytes.

### Blockchain store row (SQL) (`docs/topics/services/blockchain.md`)

`blocks` table: `id`, `parent_id`, `version`, `hash`, `previous_hash`, `merkle_root`, `block_time`, `n_bits`, `nonce`, `height`, **`chain_work`** (cumulative PoW — longest-chain selector), `tx_count`, `size_in_bytes`, `subtree_count`, `subtrees` (BYTEA), `coinbase_tx` (BYTEA), `invalid`, `peer_id`, `inserted_at`, `persisted_at`.

---

## 6. Stores

### UTXO Store — pluggable (`docs/topics/stores/utxo.md`)

Tracks the spendable UTXO set on the longest honest chain. One implementation per node
(set via the `utxostore` URL). Shared library used by Validator, Block Assembly, Block
Validation, Subtree Validation, Asset Server, Block Persister.

- **Backends:** **Aerospike** (production reference; ~1MB record cap → large txs externalized to blob), **SQL** (PostgreSQL = persistent/shared; SQLite = in-memory/dev), **Memory** (non-persistent), **Null** (testing). **[AMBIGUITY]** tech-stack doc implies Aerospike-only — see §10.
- Operations: `Get`/`GetMeta`, `Create`, `Spend`/`Unspend`, `Delete`, `SetMined`/`SetMinedMulti`, `SetLocked`, `Freeze`/`Unfreeze`/`ReAssignUTXO`, `SetBlockHeight`. Aerospike spend/mined logic in **Lua** (`teranode.lua`, `spend.lua`).
- **TxMeta cache** (`txmetacache`): in-mem UTXO-meta cache warmed via Kafka `txmeta`; heavy use by Subtree + Block Validation.
- **Pruning**: via the **Pruner service** using DAH; Aerospike needs a secondary index on `deleteAtHeight`.

### Blob Store ("Tx and Subtree Store") (`docs/topics/stores/blob.md`)

Generic key→blob store; holds **raw transactions** (Tx store) and **subtrees** (Subtree store).

- **Backends:** File (`file://`), S3 / S3-compatible (`s3://`, MinIO/SeaweedFS), HTTP, Memory, Null. Shared-storage option: Lustre FS (S3-backed).
- Interface: `Health/Exists/Get/GetIoReader/Set/SetFromReader/SetDAH/GetDAH/Del/Close/SetCurrentBlockHeight`. **DAH** = blockchain-height-based auto-expiry.

### Blockchain Store

SQL only (**PostgreSQL** production, SQLite test) — block headers, coinbase, subtrees,
chainwork, FSM/`State` key-value, mined/subtrees-set flags, invalid tracking. **[PERF]**
known catchup slowdown from recursive-CTE chain traversal + cache invalidation on each
`StoreBlock` — can drop to single-digit blocks/min on very long chains; workaround is
seeding from an exported UTXO set.

---

## 7. Communication & Messaging

| Transport | Used for | Examples |
|---|---|---|
| **gRPC** (sync) | Inter-service request/response, mining-critical paths | Validator→Block Assembly `Store()`; Blockchain `AddBlock`/FSM |
| **Kafka** (async) | Event streaming / decoupling / buffering | tx ingress, tx-meta, rejections, block & subtree propagation, finalized blocks |
| **HTTP/HTTPS + WebSocket** | External clients, peer data fetch, RPC, real-time feeds | Asset REST + Centrifuge WS; RPC JSON-RPC; Propagation `/tx` |
| **UDP multicast (IPv6)** | High-perf tx propagation (Propagation only) | raw tx dissemination |
| **libp2p (GossipSub + Kademlia DHT)** | P2P discovery + block/subtree/rejected-tx gossip; alert net | P2P, Alert |

**Main Kafka topics** (`docs/topics/kafka/kafka.md`):

| Topic (setting) | Producer → Consumer | Payload | Commit |
|---|---|---|---|
| `kafka_validatortxsConfig` | Propagation → Validator | new tx notifications | — |
| `kafka_txmetaConfig` | Validator → Subtree Validation | UTXO tx-meta (+ "delete" reversals) | auto-commit |
| `kafka_rejectedTxConfig` | Validator → P2P | rejected tx {id, reason} | auto-commit |
| `kafka_blocksConfig` | P2P (and Legacy) → Block Validation | block announcements | **manual** |
| `kafka_subtreesConfig` | P2P → Subtree Validation | subtree notifications | manual |
| `kafka_blocksFinalConfig` | Blockchain → Block Persister | finalized blocks | **manual** |
| `kafka_invalid_blocks` | Block Validation → subscribers | `{block_hash, reason}` | varies |
| `kafka_legacy_inv` | Legacy P2P | legacy inv messages | auto |

- Consumer concurrency = `partitions / consumer_ratio` (NOT the `kafkaWorkers` setting directly).
- Teranode **pauses** processing when Kafka is unhealthy and auto-resumes (safe-state design).

---

## 8. Key Design Decisions & Invariants

- **UTXO model.** State = the set of unspent outputs. UTXO hash = `H(prevTxid || index || lockingScript || satoshis)` (`util/utxo_hash.go`). Why: parallelizable, no global tx-graph state.
- **Subtrees + Merkle trees.** Continuously-broadcast power-of-2 batches of tx IDs let peers "pre-approve" txs, so block validation just confirms known subtrees instead of re-validating millions of txs. Block merkle root is computed over subtree roots (with the coinbase-placeholder substitution). Why: turns the 10-minute burst into continuous flow → unbounded block size becomes tractable.
- **No mempool.** Validated txs go straight into subtrees / block assembly. Why: removes a memory-bound bottleneck.
- **Two-phase commit (per-tx UTXO `locked` flag).** Phase 1: Validator creates outputs `locked=true`. Phase 2: `locked=false` after block-assembly add, or via `SetMinedMulti` when mined. `WithIgnoreLocked` lets block txs ignore the flag. Why: prevents race-condition double-spends in the in-flight window. (`docs/topics/services/validator.md`)
- **FSM / blockchain state machine** (`docs/topics/architecture/stateManagement.md`). States: **IDLE** (nothing runs), **LEGACYSYNCING** (full sync from legacy nodes; "speedy process blocks" only), **RUNNING** (full participation; **only state in which blocks are mined / subtrees created**), **CATCHINGBLOCKS** (catching up; processes but does not mine/create subtrees). Events: `LegacySync`, `Run`, `CatchupBlocks`, `Stop`. **On catchup error the FSM stays in CATCHINGBLOCKS** (no auto-revert). Most services block on "wait until FSM leaves IDLE" before starting. **[AMBIGUITY]** diagram vs prose — see §10.
- **Double-spend handling** (`docs/topics/architecture/understandingDoubleSpends.md`).
    - **First-seen rule**: first valid tx spending a UTXO is "original"; later ones "conflicting".
    - **During tx validation**: double-spends **rejected outright**, not propagated/stored.
    - **During block validation (PoW override)**: a double-spend inside a valid-PoW block is **stored and marked `conflicting`**. Subtree gets it in `ConflictingNodes`; the first non-conflicting parent records it in `conflictingChildren`. Conflicting records get a TTL/DAH.
    - **Poison-pill**: children of a conflicting tx are recursively marked conflicting.
    - **Reorg = five-phase commit**: (1) mark original+children conflicting; (2) unspend original+children, lock parent UTXOs; (3) process the double-spend, spending inputs even if locked; (4) mark double-spend non-conflicting; (5) unlock parents. **[AMBIGUITY]** §10.
- **Finality / maturity.** Coinbase outputs spendable only after **100 blocks** (`spendingHeight = coinbase_height + 100`). UTXO Persister stays ≥100 blocks behind tip. Catchup enforces coinbase-maturity (100) and secret-mining (10) thresholds. Longest chain chosen by **cumulative chainwork**.
- **Optimistic mining.** Add-before-validate for faster propagation, with background validate + `ReValidateBlock` rollback. Experimental; disabled during catchup.

---

## 9. Glossary

- **Subtree** — power-of-2 batch of tx IDs + their merkle root; the unit of continuous propagation and the building block of a block. Contains tx IDs (+ fee/size), not raw tx data.
- **SubtreeNode** — one entry in a subtree: `{tx hash, fee, size}`.
- **ConflictingNodes** — array on a subtree listing double-spend tx hashes.
- **Extended Transaction (EF / BIP-239)** — tx format with `0000000000EF` marker + per-input previous output satoshis+script, removing the UTXO lookup at validation.
- **UTXO Meta Data** — convenience struct (`Tx`, parent hashes, blockIDs, fee, size, isCoinbase, locktime) from the UTXO store.
- **TxInpoints** — parallel-array structure of which parent-tx outputs a tx consumes.
- **TxMeta cache (txmetacache)** — in-memory UTXO-meta cache warmed via Kafka `txmeta`.
- **FSM** — the Blockchain service's finite state machine (IDLE/LEGACYSYNCING/RUNNING/CATCHINGBLOCKS) gating node behavior.
- **Two-phase commit** — the UTXO `locked` flag protocol making outputs unspendable until safely in block assembly / mined.
- **DAH (Delete-At-Height)** — height after which a UTXO record / blob is pruned; enforced by the Pruner.
- **Mining candidate** — block template {subtrees, coinbase, header template, merkle proof} given to miners.
- **Coinbase placeholder** — `0xFF...` entry at position 0 of subtree 0, replaced by the real coinbase when computing the block merkle root.
- **Catchup** — Block Validation's chain-sync process when a parent block is missing.
- **Blaster (tx-blaster)** — **external load-generation tooling**, not a documented Teranode service. Generates and fires synthetic txs at Propagation during load tests. Absent from the architecture docs; treat as test harness, not part of the node.
- **Lustre FS** — shared parallel filesystem (S3-backed) used to share subtree/tx files between services without re-propagation.
- **Legacy Service** — bridge to traditional BSV (SV Node) network.

---

## 10. Debugging Anchors & Flagged Ambiguities

**High-value debugging anchors:**

- **Tx stuck "not mined":** check UTXO record `locked` (two-phase commit) and `unminedSince` (≠0 = unmined). Block Assembly reloads unmined txs on startup via `UnminedTxIterator`; loading blocks `SubmitMiningSolution`/`ResetBlockAssembly`/`GenerateBlocks`.
- **Tx rejected at validation:** likely double-spend (first-seen), `ErrTxMissingParent` (parent not yet validated / CPFP), policy (min fee), or frozen UTXO. Rejections → Kafka `rejectedTx`.
- **Block won't validate:** PoW/merkle/MTP/coinbase failures → `invalid=true` + Kafka `invalid_blocks`. Missing subtrees/timeouts are **recoverable** (NOT marked invalid) → revalidation queue (≤3 retries).
- **`SetMinedMulti` coverage failures:** `setMinedChan` retry with exp backoff (10 attempts), then logs `manual_intervention_required` + drops. Metrics `teranode_blockvalidation_setmined_retry_total` / `_drops_total`.
- **Node "doing nothing":** check FSM state — IDLE means no activity; must be driven to RUNNING (CLI `setfsmstate`, Asset `POST /api/v1/fsm/state`, or gRPC `Run`).
- **Stuck in CATCHINGBLOCKS:** by design the FSM does not auto-revert on catchup error.
- **Reorg not happening / chain split:** Block Assembly (not Block Validation/Blockchain) owns building on the strongest chain; deep reorgs (> coinbase maturity, height >1000) trigger a reset.
- **Frozen/reassigned UTXO unspendable:** alert system; 68B `0xFF` pattern; reassigned needs +1,000 blocks.
- **Slow catchup:** known blockchain-store recursive-CTE perf issue; seed instead.

**Flagged ambiguities (docs unclear/contradictory — verify against code before relying):**

1. **Block Persister trigger.** Described as both a **polling** service (`persisted_at IS NULL`) and Kafka `blocksFinal`-driven. Likely both (notify + poll fallback) but not reconciled in docs.
2. **Optimistic mining default.** `blockValidation.md` shows default `true` in a config block yet calls it "experimental and disabled by default." Verify the real default.
3. **Five- vs four-phase commit.** `understandingDoubleSpends.md` §1.3 lists 4 steps; §3 details 5. The 5-phase version is authoritative.
4. **UTXO store backends.** `technologyStack.md` frames Aerospike as *the* UTXO store; the store docs make clear it is **pluggable** (Aerospike / SQL / Memory / Null) and the **blockchain store is separate** SQL. Don't conflate.
5. **Subtree size "1 million".** Overview docs say "1 million tx per subtree"; data-model says **configurable power of 2** (default 1,048,576, dynamic). Treat 1M as default/illustrative.
6. **FSM diagram completeness.** `state-machine.diagram.md` omits `IDLE ↔ LEGACYSYNCING` edges documented in `stateManagement.md`. Trust `stateManagement.md` + `services/blockchain/fsm.go`.
7. **Quick validation / optimistic mining status.** Both described with experimental/disabled status in places — historical-block fast paths may not be active in a given build.
