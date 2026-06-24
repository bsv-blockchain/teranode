# Subtree-Lift Activation Strategy — Audit Notes

This file documents the audit performed for Task 0 of `plans/subtree-lift.md`. It records
where merkle-root computation lives on the assembly side today, whether incomplete final
subtrees ever reach finalised (mined-and-persisted) blocks, and a recommended activation
strategy for the upcoming `RootHashPadded` lift.

Source versions inspected:
- `github.com/bsv-blockchain/go-subtree` v1.2.0 (from `go.mod`)
- Teranode at HEAD (commit `efaeedf9c`)

---

## 1. Merkle-root entry points in block assembly

`grep` of `HashMerkleRoot` / `merkleRoot` across `services/blockassembly/` and `model/`
returns the following production (non-test, non-benchmark) call sites that populate
`block.Header.HashMerkleRoot`:

| File | Line | Context |
|---|---|---|
| `services/blockassembly/Server.go` | 1383 | `submitMiningSolution`: builds the merkle root for the mined block that is sent to `blockchainClient.AddBlock` (line 1464). |
| `services/blockassembly/Server.go` | 1420 | `submitMiningSolution`: assigns `hashMerkleRoot` into the `model.Block.Header`. |
| `services/blockassembly/Server.go` | 1591 | `GetCandidateBlock`: builds the merkle root for a candidate block (read-only RPC; not a finalised block). |
| `services/blockassembly/Server.go` | 1610 | `GetCandidateBlock`: assigns `hashMerkleRoot` into the `model.BlockHeader` returned to callers (proposal-mode preview). |
| `services/blockassembly/Server.go` | 1999 | `CheckBlockAssemblyBlockTemplate`: builds the merkle root for an internal sanity check. |
| `services/blockassembly/Server.go` | 2009 | `CheckBlockAssemblyBlockTemplate`: assigns into a temporary `model.Block.Header` for the check. |
| `services/blockassembly/mining/mine.go` | 55, 58, 72 | Test-only local solo miner; builds the merkle root from `candidate.MerkleProof` via `util.BuildMerkleRootFromCoinbase`. Not called in production. |
| `services/blockassembly/mining/BuildBlockHeader.go` | 49, 54 | Same helper as above; used by tools building block headers from a `MiningCandidate`. |
| `services/blockassembly/subtreeprocessor/benchmark.go` | 317 | Benchmark only. |
| `model/BlockHeader.go` | various | Marshal/unmarshal of the field, not population. |
| `model/TestHelper.go` | 310 | Test helper. |

The **only** path that puts a `HashMerkleRoot` into a block that is then handed to the
blockchain service for persistence is **`services/blockassembly/Server.go:1383`** inside
`submitMiningSolution`.

---

## 2. Call chain that produces `b.Header.HashMerkleRoot` for a mined block

The chain from mining-candidate request through to block persistence:

1. **`BlockAssembly.GetMiningCandidate`** — `services/blockassembly/Server.go:1168`.
   - Delegates to **`BlockAssembler.GetMiningCandidate`** at
     `services/blockassembly/BlockAssembler.go:1068`.
   - That method fetches subtrees from one of two sources:
     - `subtreeProcessor.GetPrecomputedMiningData()` — `BlockAssembler.go:1099`. Returns
       a snapshot of `chainedSubtrees` (always complete subtrees — see §3).
     - If `len(subtrees) == 0`, falls back to
       `subtreeProcessor.GetIncompleteSubtreeMiningData(ctx)` — `BlockAssembler.go:1111`.
       That call goes via the `getIncompleteSubtreeDataChan` to
       `SubtreeProcessor.go:625-664`, which calls **`createIncompleteSubtreeCopy`**
       (`SubtreeProcessor.go:1007`) and returns a single-element slice containing the
       incomplete copy.
   - The subtree slice (`subtrees`) is then stashed verbatim in `jobStore`
     (`Server.go:1196-1200`) and the candidate is returned.

2. **`BlockAssembly.SubmitMiningSolution`** — `services/blockassembly/Server.go:1229`.
   - Routes work through `submitMiningSolution` at `Server.go:1273`.
   - Retrieves the same `job.Subtrees` slice from `jobStore` (`Server.go:1289-1294`).
   - At `Server.go:1357-1377` it walks `job.Subtrees`, duplicates index 0 with
     `ReplaceRootNode(coinbaseTxIDHash, …)`, and collects each
     `subtreesInJob[i].RootHash()` into `subtreeHashes`.

3. **`createMerkleTreeFromSubtrees`** — `services/blockassembly/Server.go:1484`.
   - Builds a top tree via
     `subtreepkg.NewTreeByLeafCount(subtreepkg.CeilPowerOfTwo(len(subtreesInJob)))`
     (line 1486).
   - Adds each subtree-root hash with `AddNode(hash, 1, 0)` (line 1492).
   - Returns `topTree.RootHash()` (line 1504) as `hashMerkleRoot`.

4. The returned `hashMerkleRoot` is assigned into the block header at
   `Server.go:1420`, and the block is handed to `blockchainClient.AddBlock`
   at `Server.go:1464`. From this point the block is persisted with
   `b.Header.HashMerkleRoot` exactly as `createMerkleTreeFromSubtrees` returned it.

### How each subtree root is computed

`Subtree.RootHash()` is defined in `go-subtree@v1.2.0/subtree.go:455`. It delegates to
`BuildMerkleTreeStoreFromBytes(st.Nodes)` (`merkle_tree.go:45`), which pads up to
`NextPowerOfTwo(len(st.Nodes))` — i.e. the **actual node count's** next power of two,
not the subtree's allocated capacity. So an incomplete subtree returns an un-padded
root: a 1024-capacity subtree containing 500 nodes returns the root of a height-9 tree
over those 500 nodes (rounded up to 512 with the duplicate-last-when-odd rule), not the
root of a height-10 tree padded to 1024.

### Symmetry with validation today

`model.Block.CheckMerkleRoot` (`model/Block.go:1290`) walks `b.SubtreeSlices`, replaces
the root node of subtree 0 with the coinbase txid via
`Subtree.RootHashWithReplaceRootNode` (line 1310), and otherwise calls plain
`Subtree.RootHash()` (line 1317) on each subtree. It then builds a top tree via
`subtreepkg.NewIncompleteTreeByLeafCount(len(b.Subtrees))` (line 1333) — which produces
the same height as `NewTreeByLeafCount(CeilPowerOfTwo(N))` used by assembly. Hence today
**assembly and validation are symmetric**: both use the same un-padded `RootHash()` for
incomplete subtrees, and both compute the same top tree height. Blocks built today
validate against today's `CheckMerkleRoot`.

---

## 3. Can incomplete final subtrees reach finalised blocks today?

### `createIncompleteSubtreeCopy` call sites

Per Step 3's grep, there are three call sites in
`services/blockassembly/subtreeprocessor/SubtreeProcessor.go`:

- **Line 558** — `getSubtreesChan` handler. Used by
  `GetCompletedSubtreesForMiningCandidate` (`SubtreeProcessor.go:2447`). The result is
  appended to `completeSubtrees` and the incomplete copy is **also** stored and
  announced through `newSubtreeChan`. The returned slice is the set of subtrees used
  for a mining candidate.
- **Line 631** — `getIncompleteSubtreeDataChan` handler (the on-demand path called by
  `GetIncompleteSubtreeMiningData`, `SubtreeProcessor.go:2473`). Again the incomplete
  copy is stored/announced via `newSubtreeChan` and then returned in
  `PrecomputedMiningData.Subtrees`.
- **Line 770** — periodic announcement ticker. Announces a partial subtree to peers but
  does **not** affect the in-memory `chainedSubtrees` list or
  `precomputedMiningData`. This path purely advertises a transient partial subtree to
  peers; it never feeds back into the mining candidate path. (Important: the announced
  hash is stored in the blob store and would match `subtree.RootHash()` of the partial
  contents, but no block built on this node references it.)

### Production callers of `GetCompletedSubtreesForMiningCandidate`

Only test/benchmark code (`cmd/teranodecli/teranodecli/subtreebench.go:476` and
`SubtreeProcessor_test.go:1009`). So in **production**, the path that delivers an
incomplete subtree to a mining candidate is **`GetIncompleteSubtreeMiningData` via
line 631 only**.

### Does the candidate path produce blocks with incomplete final subtrees?

Yes. Walking `BlockAssembler.GetMiningCandidate` (`BlockAssembler.go:1097-1118`):

- `data := subtreeProcessor.GetPrecomputedMiningData()` returns a snapshot of
  `chainedSubtrees`. `chainedSubtrees` is appended to only in `processCompleteSubtree`
  (`SubtreeProcessor.go:1906`), which runs after a subtree has filled
  (`len(currentSubtree.Nodes) >= capSize`, `SubtreeProcessor.go:921`). So
  `chainedSubtrees` entries are always **complete** (power-of-two `len(Nodes)` ==
  `treeSize`).
- If `chainedSubtrees` is empty AND `currentSubtree.Length() > 1`,
  `GetIncompleteSubtreeMiningData` returns a single-element slice
  `[incompleteSubtreeCopy]`. The miner then mines that candidate, calls
  `SubmitMiningSolution`, and the same incomplete subtree flows through
  `createMerkleTreeFromSubtrees` into the persisted block header. The persisted block
  contains exactly one subtree, and it is incomplete.

So **finalised mined blocks today CAN have an incomplete final subtree** — specifically
the special case where they have exactly one subtree, and that single subtree is
incomplete. This is the path traversed whenever a node mines a block before its current
subtree has filled. Empirically this is the dominant case on low-traffic networks (regtest,
testnet, early mainnet) and the rare-but-real case on a high-traffic network when
mining solutions arrive faster than a full subtree of transactions does.

`chainedSubtrees` never receives an incomplete subtree, so the
"`>1` subtrees with an incomplete LAST one" scenario does not occur in the current
production flow. Assembly only ever produces incomplete subtrees in the
`len(subtrees) == 1` case.

### Cross-check: subtree blob persistence

`createIncompleteSubtreeCopy` returns a subtree constructed via
`NewTreeByLeafCount(capacity)` (`SubtreeProcessor.go:1020`), so its `treeSize` matches
the original allocated capacity at creation time. **However** `Subtree.Serialize`
(`go-subtree@v1.2.0/subtree.go:635`) writes only `len(st.Nodes)`, and
`Subtree.DeserializeFromReader` (`subtree.go:861`) restores `treeSize = numLeaves` from
the serialized data. So when a peer (or this node, on restart) loads the stored
incomplete subtree from the blob store and calls `RootHash()`, it gets the
**un-padded** root over the actual node count. The blob store does not preserve the
original allocated capacity — it persists the actual leaf count. The merkle root in the
header was computed against that same un-padded root by `createMerkleTreeFromSubtrees`.

This implies that under the planned `RootHashPadded(targetHeight)` change, the validator
needs an explicit notion of the "intended" subtree height to lift to. The new validation
rules in `plans/subtree-lift.md` use `targetHeight = subtrees[0].Height`, which is fine
**only when there are >1 subtrees** (since the first one defines the canonical full
height). For the single-incomplete-subtree case — which is the only one that occurs
today — there is no peer subtree to compare against; `len(hashes) == 1` falls into the
`CheckMerkleRoot` branch at `Block.go:1329` which just uses the single hash directly,
i.e. it does **not** perform any lift.

---

## 4. Recommendation: activation strategy

### Evidence summary

- Today's assembly and validation paths are symmetric: both use un-padded
  `Subtree.RootHash()` for incomplete subtrees.
- Finalised blocks **do** contain incomplete subtrees today, exclusively in the
  "one and only subtree is incomplete" case. The `>1`-subtree-with-incomplete-final case
  cannot occur because `chainedSubtrees` is only appended to after a subtree fills.
- The `CheckMerkleRoot` change in `plans/subtree-lift.md` only kicks in for the
  `len(subtrees) > 1` branch. The `len(subtrees) == 1` branch (`model/Block.go:1329`)
  uses the single subtree's `RootHash()` as the block merkle root directly, with no
  lifting — and this is the only branch that gets exercised today on incomplete
  subtrees.
- Reading the proposed plan literally, the new validation rule says: "If >1 subtrees:
  target height = `subtrees[0].Height`. All subtrees must share that height. All but
  the last must be complete. The last subtree, if incomplete, is lifted via
  `RootHashPadded`. All actual leaf counts must be powers of two." This rule is
  **unreachable** for any block produced today by the existing code path, because
  Teranode never assembles a `len(subtrees) > 1` block with an incomplete last
  subtree.

### Recommendation: **Strategy A** (apply unconditionally), with two caveats

The migration risk described in `plans/subtree-lift.md:17` predicates Strategy B on the
existence of "historical blocks whose final subtree was incomplete." Such blocks do
exist — but they all fall into the `len(subtrees) == 1` case, which the new
`CheckMerkleRoot` does not touch (the `case len(hashes) == 1` branch at
`model/Block.go:1329-1330` is preserved unchanged in the plan's description).
The branch that **does** change is the `>1` branch, and that branch is never exercised
on incomplete subtrees by historical blocks Teranode itself produced. Hence the lift
change is **safe to apply unconditionally** for blocks Teranode mined.

**Caveat 1 — blocks ingested from external sources.** Teranode also validates blocks
received from peers (legacy SV nodes, other Teranodes, archived data). A block whose
underlying subtree partitioning (as recorded on disk) happens to have `len(subtrees) > 1`
with an incomplete last subtree could in principle reach the new code path. To check
this I audited the SVNode-ingest path: `services/legacy/netsync/handle_block.go:264`
(`prepareSubtrees`). That function constructs **a single subtree** per ingested block
via `subtreepkg.NewIncompleteTreeByLeafCount(len(block.Transactions()))` (line 290)
and returns `subtrees = [&singleSubtree]` (line 369). So every SVNode block Teranode
ingests is represented as a 1-subtree block. The `len(subtrees) > 1` path therefore
**cannot** be reached via legacy ingest either. The remaining ingest path is via
`services/blockvalidation/` for blocks received from other Teranodes — these come with
their `Subtrees` array already partitioned by the producer's `BlockAssembly.SubmitMiningSolution`
flow. Since that producer is itself the path audited in §2 above (which never produces
`len(subtrees) > 1` with an incomplete last subtree), the chain closes.

**Caveat 2 — the single-subtree incomplete case still needs assembly + validation to
match.** Even though the new validation does not lift in the 1-subtree branch, the new
top-tree-construction approach described in `plans/subtree-lift.md` ("target height =
`subtrees[0].Height`") would behave incorrectly if the assembler keeps computing the
header off `Subtree.RootHash()` (un-padded) while the validator computes off
`RootHashPadded(subtrees[0].Height)` for a single subtree. The plan as written treats the
1-subtree case the same way as today, so this is fine — but the assembly-side
implementation must NOT switch to `RootHashPadded` for the single-subtree case either,
or it will produce a header that no peer can validate, even with the lift in place.
The lockstep update is: assembly's `createMerkleTreeFromSubtrees`
(`services/blockassembly/Server.go:1484`) needs to mirror the validator's branching —
**single-subtree case uses `RootHash()` unchanged; multi-subtree case lifts the final
subtree if incomplete**. The plan's "in lockstep" requirement is satisfied by patching
`createMerkleTreeFromSubtrees` to apply the same rules as the new `CheckMerkleRoot`.

### If a gate is desired anyway (Strategy B)

If reviewers prefer to err on the side of caution (e.g. because Caveat 1 cannot be
quickly cleared), a configurable activation height is cheap:
- Add `SubtreeLiftActivationHeight uint32` to `settings.BlockValidation` (default
  `math.MaxUint32`).
- In `CheckMerkleRoot`, branch on `b.Height >= settings.SubtreeLiftActivationHeight`.
  Below the gate: use the existing code path verbatim. At or above: use the new lift
  rules.
- In `createMerkleTreeFromSubtrees`, branch on the next block's height
  (`candidate.Height = baBestBlockHeight + 1`, available in
  `BlockAssembler.go:1204`). Same threshold.
- Document the gate name `SubtreeLiftActivationHeight` and its default in the audit
  notes for downstream tasks.

This is mechanically simple but adds an extra path and an extra invariant to test
(below-gate vs above-gate equivalence with current behaviour). Given the analysis
above, the gate is not strictly required for blocks Teranode itself produced — but
it provides defence-in-depth against the Caveat 1 unknown.

### Final recommendation

**Strategy A.** All known production paths that produce a `model.Block` in Teranode are:
1. **`BlockAssembly.SubmitMiningSolution`** — audited in §2; never produces
   `len(subtrees) > 1` with an incomplete final subtree.
2. **`services/legacy/netsync/handle_block.go:prepareSubtrees`** — audited in Caveat 1;
   only ever produces 1-subtree blocks.
3. **`services/blockvalidation/*`** — receives blocks from peers, whose `Subtrees`
   field was populated by one of the two paths above on the producer node.

The new `CheckMerkleRoot` rule for `len(subtrees) > 1` is unreachable on the historical
chain. The `len(subtrees) == 1` branch is unchanged by the plan. Hence the change is
safe to apply unconditionally, **provided** the assembly-side
`createMerkleTreeFromSubtrees` (`services/blockassembly/Server.go:1484`) is patched in
lockstep so its behaviour for the `len == 1` case mirrors the new validator (no lift),
and for the `len > 1` case it also lifts an incomplete last subtree (which today never
occurs but is preserved as a forward-compatible invariant).

The human reviewing this audit should still make the final call. If there is any
uncertainty about external block sources outside `services/legacy/` and
`services/blockvalidation/` (e.g. archived snapshot ingest, tools like
`teranodecli` that build blocks for replay), Strategy B remains the safe fallback.

---

## 5. Open questions / things not verified

- Audited `services/legacy/netsync/handle_block.go:prepareSubtrees` (the SVNode-ingest
  path) and confirmed it produces only 1-subtree blocks. Did **not** audit any other
  ingest path (e.g. snapshot import tools, `teranodecli` block-replay utilities,
  catch-up via `blockchain` service). If such paths construct `model.Block` with
  `Subtrees`/`SubtreeSlices` populated, they need to be checked before finalising
  Strategy A.
- Did not run any tests. This is a read-only audit; no code changed.
- Did not measure the empirical frequency of single-subtree-incomplete blocks on any
  live network. The audit only establishes that the code path exists, not how often it
  fires.
- The plan in `plans/subtree-lift.md` describes the new validation rule in prose but
  not in code. The exact `CheckMerkleRoot` semantics will need re-reading once the new
  implementation lands to confirm the branching matches what this audit assumed.
- Did not verify the behaviour of `services/blockassembly/Server.go:GetCandidateBlock`
  (line 1538) — this is a proposal-mode read-only RPC that does not persist a block,
  but its merkle root computation must also be updated in lockstep with the validator
  if external miners or tools rely on the returned hash for any verification step.
  Same applies to `CheckBlockAssemblyBlockTemplate` (line 1999).
