# The utxoset UTXO store

A plain-English tour of how Teranode keeps the set of spendable coins in PostgreSQL with the `utxoset` store, and how it differs from the aerospike and sql stores.

**Who is this for?** Each Teranode developer. You do not need to know PostgreSQL internals. If you read only one section, read section 3, "The big idea".

**Where is the code?** Everything is in `stores/utxo/utxoset`. Section 15 maps each topic to its file. The comments in `schema.go` give the reasoning behind each table in more detail than this page.

## Contents

1. What this is
2. UTXOs in 60 seconds
3. The big idea: a spend is a DELETE
4. How utxoset differs from the aerospike and sql stores
5. The tables
6. The life of a transaction
7. What happens during a spend
8. Reading a transaction
9. The stamp: giving a UTXO its block
10. The pruner: dropping partitions
11. Reorgs, unspends and conflicts
12. Crashes and repeats
13. Running it
14. Known limits
15. Where to read the code
16. Glossary

## 1. What this is

A node must know which coins exist and can be spent. That set is the **UTXO set**. Each new transaction is checked against it, millions of times, and the set changes with each block.

Teranode has several places to keep the UTXO set, and each implements the same Go interface, `utxo.Store`. The rest of the node does not know which one is in use. `utxoset` is one of them. It is a PostgreSQL store, and an operator selects it with a `utxoset://` store URL.

Why a new design? A store that records each spend as a new row must index every spend the chain ever made, and that set only grows. Measured on mainnet, such an index reached 392,884,647 rows and about 30 GB, against 16 to 20 GB of cache. When an index does not fit in memory, each lookup becomes a disk read, and sync slowed from 11,563 blocks a day to 2,529. utxoset indexes the other set, the outputs that still exist.

Two goals shape every decision:

- **Keep the indexed set small.** Index the coins that exist, not the coins that ever existed.
- **Never lose data.** These are real coins, so every write is durable. The store does not use unlogged tables or turn off fsync.

## 2. UTXOs in 60 seconds

A transaction has **inputs** (coins it spends) and **outputs** (new coins it makes). An output that no transaction has spent yet is an **unspent transaction output**, a UTXO. When a later transaction uses it as an input, it is spent and leaves the live set.

So the store answers a few questions, fast:

- Does this coin exist, and is it unspent? (validate an input)
- Spend this coin. (apply a transaction)
- Keep the new coins this transaction makes.
- Put a coin back, if a reorg (a switch to a different chain tip) or a double-spend decision says it was not spent after all.
- Forget what is no longer needed, so the database does not grow for ever.

## 3. The big idea: a spend is a DELETE

utxoset keeps **one row for each unspent output**, in the `utxo` table. A spend **deletes** that row. So the table and its index hold only the live UTXO set, which stays almost flat over time, at about 74 million rows on mainnet when it was measured.

That one `DELETE` does four jobs:

1. **It finds a double spend.** If the statement removes zero rows, the output is already gone: a different transaction spent it, or it never existed.
2. **It reads the parent's data.** `DELETE ... RETURNING` gives back the output's satoshis and locking script, so script validation does not parse the parent transaction.
3. **It reclaims the space.** No background job has to find and remove spent outputs later.
4. **It is the write.**

A deleted row cannot be put back unless something kept a copy. So the same statement also writes a copy into the **spend journal**, the undo record. This is SV Node's `rev*.dat` file, kept in a table. A UTXO is never removed without a record that can restore it.

**The cost.** PostgreSQL does not free a deleted row at once. Autovacuum reclaims it later, so autovacuum is the background job that must keep up. The store sets its autovacuum thresholds for that load. It also rebuilds a bloated UTXO index with `REINDEX CONCURRENTLY`, one partition at a time, from the pruner.

## 4. How utxoset differs from the aerospike and sql stores

| Question | aerospike and sql | utxoset |
|---|---|---|
| What is indexed? | One record for each transaction, with each output's spend state on it | One row for each **unspent** output; spent outputs are gone |
| What is a spend? | An update that marks the output spent | A `DELETE` of the UTXO row, plus an undo copy in the journal |
| Can a spend be partial? | Yes: each input is a separate write, and an undo runs when a later input fails | No, for `SpendAndCreate`: all inputs and the create are one PostgreSQL transaction. See section 7 for the multi-transaction path |
| A stored transaction validated again after a parent was locked | `ErrTxLocked` or `ErrTxConflicting` | Success: the same transaction already spent each input |
| How long is the transaction body kept? | While the record exists | 288 blocks after the pruner height, then only in the blob store, unless the transaction still waits to be mined |
| Does `Get` return the body? | Only when the caller names `fields.Tx` or `fields.Outputs` (sql) | The same rule. A nil `Tx` is a legal answer at any time |
| How does old data leave? | Delete-at-height marks, then a sweep or the aerospike TTL | Dropping whole partitions (`DROP TABLE`) |
| Which block is a UTXO in? | Kept on the record and updated | Written once: at create below the checkpoint, otherwise by the stamp when the block is deep enough that it cannot change |
| Is `meta.Data.Frozen` set by `Get`? | Depends on the store | No, the same as aerospike. The spend path refuses frozen outputs, and a metadata read gives the frozen marker for each frozen output |

The shared conformance tests in `stores/utxo/tests` run against all three stores. Where the stores differ on purpose, `SpendModel` says which model a store uses, and the test checks each store against its own model. `docs/topics/stores/utxo.md` explains the two spend models.

## 5. The tables

Three ideas hold for every table:

- **Partitions.** A big table is split into child tables. Most are split by a block-height range. Old data leaves with a `DROP TABLE` of a whole partition, which costs the same however many rows it holds and makes no work for autovacuum.
- **Leaves.** The `utxo` and `tx_ident` tables are split eight ways by `leaf = txid[0] & 7`, the low three bits of the first byte of the transaction id. All the outputs of one transaction are thus in one partition.
- **The full transaction id is checked in every statement.** The UTXO key is shorter than the id (see below), so the key finds candidates and the full 32-byte id decides.

### The data tables

| Table | One row is | Split by | How rows leave |
|---|---|---|---|
| `utxo` | One unspent output | Leaf (8) | Deleted by the spend |
| `spend_journal` | The undo copy of a spent UTXO | Spend height, 288 blocks a partition | The pruner drops a partition when it is older than the journal retention |
| `tx_ident` | A transaction above the checkpoint that the stamp has not yet processed | Leaf (8) | The stamp deletes it, when its block is deep enough |
| `tx_mined` | "Block B contains transaction T" (containment) | Mined height, 288 blocks a partition | The pruner drops a partition after the stamp completes it |
| `tx_body` | The raw bytes of a transaction | Created height, 48 blocks a partition | The pruner drops a partition when it is 288 blocks below the pruner height |

**`utxo`.** The row holds the output's satoshis and locking script, the heights at which it was created and can first be spent (coinbase maturity), the block that contains its transaction (`mined_height`, `block_id`), flags (frozen, locked, conflicting, coinbase), and `hash_override`, which is set only after an operator reassigns the output. `mined_height = 0` means "no block yet". No spendable output is at height 0, because genesis pays nobody, so 0 is a safe marker.

The key, `ukey`, is the first 12 bytes of the transaction id followed by the output number, 16 bytes in all. It is **not unique, on purpose**. Two transactions can share a 96-bit prefix. With a unique index, an attacker with about 2^48 work could make a valid output fail to be created. Because every statement also checks the full id, a collision costs one more row read and never gives a wrong answer. Because the prefix comes first, "all outputs of transaction T" is a range scan of the same index.

**`spend_journal`.** The undo copy. It holds every field needed to rebuild the UTXO row byte for byte, `hash_override` included, plus the spender's id (`spending_txid`) and input number (`spending_vin`). The spender's id is an **ownership token**: a restore must name the transaction that took the UTXO, so a stale undo record cannot bring back a coin that a different transaction now owns. The journal is also the last record of a fully spent old parent, which block validation asks about.

**`tx_ident`.** The identity row of a transaction above the checkpoint, from the mempool or from a block, until the stamp processes it: fee, size, its inputs (`tx_inpoints`), lock time, flags, and the `off_chain_since` marker. A set marker means the transaction is not on the main chain (it waits, or a reorg took its block away). A clear marker means the last word was "a main-chain block contains it", and the row waits for the stamp. Below the highest checkpoint, transactions arrive in blocks, so this table stays empty.

**`tx_mined`.** One row for each (transaction, block) pair. Competing blocks can each have a row for the same transaction, and no row says which block won. Rows are only inserted (`ON CONFLICT DO NOTHING`) and deleted, never changed, so any number of writers in any order leave the same contents. The stamp decides the winner from the chain.

**`tx_body`.** The bytes, and nothing else. This is the only part of a transaction whose life has a fixed horizon. Keeping the bytes for as long as an output is unspent would make the store a permanent transaction archive, which on mainnet measured at 136 GB.

### The side tables

| Table | What it is for |
|---|---|
| `tx_body_carry` | The body of a transaction that still waits to be mined when its `tx_body` partition drops. A waiting transaction can still be mined, and the block persister needs its bytes then |
| `preserved_parent` | A copy of the containment row of a parent that an old unmined child still needs, kept past the `tx_mined` partition that held it |
| `conflict_children` | "Child C lost a double spend on parent P". Keyed on the parent's id, because a contested parent is often mined and has no identity row |
| `conflict_intents` | A write-ahead log for conflict resolution: written before the first change, removed after the last, replayed at startup |
| `tx_mined_floor` | Three heights: the highest dropped `tx_mined` partition, and how far the stamp has started and finished |
| `tx_mined_stamped` | One row for each stamped partition, with the tip height at which its stamp completed |
| `applied_block`, `applied_chunk` | A ledger that makes a re-applied block idempotent |

## 6. The life of a transaction

Two paths bring a transaction into the store.

**The mempool path (above the highest checkpoint).** The validator accepts a transaction before any block contains it:

1. `SpendAndCreate` deletes its inputs' UTXO rows, writes their undo copies, inserts its identity row with the marker set, inserts its body, and inserts its outputs as UTXOs at "no block yet". All of this is one PostgreSQL transaction.
2. A block arrives that contains it. `SetMinedMulti` inserts a `tx_mined` row. When that block is on the main chain, "mark on longest chain" clears the identity row's marker.
3. When the block's 288-block partition is 288 blocks deep, the **stamp** writes the winning block onto the transaction's UTXOs and deletes its identity row (section 9).
4. Later, the pruner drops the `tx_mined` partition and the `tx_body` partition. The UTXOs stay as long as they are unspent. Each spend deletes one and leaves an undo copy, which the pruner drops after the retention.

**The block path.** Block and subtree validation write a block's transactions with `SpendAndCreateMulti`. What it writes depends on the height:

- **At or below the highest checkpoint**, the block is final by rule. Each UTXO is written with its block already set. There is no identity row and nothing to stamp. With `utxostore_skipTxBodyBelowCheckpoint` on, there is no body either, so during sync the store keeps only what consensus needs: the UTXOs and the containment rows.
- **Above the checkpoint**, the block can still lose a reorg. So a create from a block is written like a mempool create: UTXOs at "no block yet", an identity row, and a containment row for the block. The stamp gives the UTXOs their block later. Thus no UTXO above the checkpoint can carry a block that a reorg makes wrong.

A transaction can arrive both ways: first in the mempool, then in a block. The identity row's primary key makes the second arrival recognise the first, so its outputs are not created twice. A transaction that arrives only in a block, above the checkpoint, gets its identity row from that block.

## 7. What happens during a spend

### One transaction: `SpendAndCreate`

All inputs are deleted, their undo copies written, and the outputs created, in **one PostgreSQL transaction**. If one input cannot be spent (already spent, frozen, locked, immature), nothing commits. No other process can see a partial spend, so there is nothing to undo.

Under load, the store collects many callers' `SpendAndCreate` calls into one batch. A transaction in the batch that fails is taken out and the rest run again, so a batch rejects only the transaction with the error and commits the others.

If the same transaction is validated again, its inputs are already gone. The store sees that the same spender took each one (the undo copy names it) and answers success: the spend exists, and nothing changed.

### Many transactions from a block: `SpendAndCreateMulti`

Block and subtree validation write a whole list of transactions, many of which spend each other's outputs. Writing them one at a time costs one commit each. The multi write uses two steps instead:

1. **Spend.** It deletes every input whose parent is **outside** the list, in parallel chunks, and commits.
2. **Create.** It creates the transactions in dependency order, a group of levels at a time.

An output that a later transaction **in the same list** spends is never written as a UTXO at all. Its undo copy is written directly, naming the child. This is called **netting**, and it saves an insert, a delete and an index entry for each such output.

Each transaction's inputs are still spent all together or not at all. But between the two steps, a parent names a spender that has no record yet. Two cases follow:

- **Step 2 stops with an error, or the caller cancels.** The store unspends the step 1 spends of every transaction it did not write, so the coins can be spent again.
- **The process crashes between the steps.** No undo runs. A retry of the same block repeats the spends as the same spends and completes the write. If no retry comes, the spends stay. This gap is the same in every UTXO store: the aerospike and sql stores also spend a transaction's inputs before they create it, and a crash between the two keeps the spends. Issue 1976 tracks one correction for all three.

**A repeated spend in a later partition.** If a crashed write's spends sit in an older journal partition and the same transaction is later accepted at a higher height, the spend moves its undo row up to the newer partition, so the pruner does not drop it too early.

## 8. Reading a transaction

`Get`, `BatchDecorate` and the batched get all use one function, `lookupMany` (`lookup.go`), and one **read order**. The order matters for correctness, not only speed:

1. **The identity row and the recent containment rows**, read together. A transaction above the checkpoint has both until the stamp, and each gives half the answer.
2. **A live UTXO**, for a transaction with neither.
3. **An undo copy in the journal**, for a fully spent transaction.
4. **A second containment read** over every live partition, in one snapshot, for the few cases the first reads cannot explain.
5. **A preserved parent**, last.

A transaction in none of these is not found. That is the correct answer past the retention, and it is what the aerospike store gives too.

**The body is read only when asked for.** A caller that names `fields.Tx`, `fields.Inputs`, `fields.Outputs` (or a few other body fields) or names no field gets the body. Any other read leaves `tx_body` out, so a metadata read does not pay for the whole transaction. The sql store uses the same rule.

**A nil body is always a legal answer.** After 288 blocks, or below the checkpoint with `utxostore_skipTxBodyBelowCheckpoint` on, the store has no body. The record still exists and its UTXOs are still live, so the store returns the metadata with `Tx` nil and no error. A caller that needs the bytes must check, and fetch them from the blob store or the subtree data.

**Parent outputs for validation.** `ParentOutputsForValidation` gives the validator the value and script of each output a transaction spends. It takes them from the UTXO table while the output is unspent, and from the journal once it is spent. It does **not** parse parent bodies. Subtree validation asks for about 131,000 outputs at a time on mainnet, and reading whole bodies for that held gigabytes of memory and took mainnet past its limit on 2026-10-15. Only an output that is in neither table is looked for in the body, 16 parents at a time.

## 9. The stamp: giving a UTXO its block

A UTXO created by the mempool path starts with "no block yet". It gets its block later, and only once, from **the stamp**.

Why so late? Near the tip, the chain can change: a block can lose a reorg, and the same transaction can be in competing blocks. A block written onto a UTXO early would sometimes be the loser, and block validation would then reject a valid block. The stamp waits until the answer cannot change.

**When.** The stamp works on one 288-block `tx_mined` partition at a time, when the partition's last block is **288 blocks deep**. 288 is twice the coinbase maturity of 100, rounded up to a whole partition. The node refuses a chain switch deeper than maturity, so twice that leaves room for an operator invalidation.

**How.** The pruner service's stamp worker (`services/pruner/stamp_worker.go`) runs it through the store's `Stamper()`. The store itself makes no call to the blockchain service. The worker builds the chain's winner at each height by walking parent links back from the best tip (`util/chainancestry`), not by the main-chain flag, which can change during the walk. Then:

1. **Begin.** Delete the containment rows of blocks that lost. Raise the fence (`stamp_fence`) so that no new write can change the partition.
2. **Stamp pages.** 256 pages, one for each first byte of the transaction id. Each page writes the winning block onto the UTXOs at "no block yet", and deletes the identity rows, in one statement.
3. **Complete.** A sampled audit, then the completion record. The record holds the tip height plus one partition (288 blocks), the highest height at which an undo copy of the partition's UTXOs can still be written while the stamp completes.

Below the checkpoint the identity table is empty, so a partition costs one scan for losers and a probe for each page: about 1.3 seconds for each 288-block partition during sync.

The rewind tool (`cmd/rewindblockchain`) refuses to go below a stamped height, because a stamped block is permanent.

## 10. The pruner: dropping partitions

The pruner service calls the store's `Prune` once per block. Each step is a `DROP TABLE` of partitions that are old enough. There is no list of rows to work through, so there is no backlog that can grow. The steps do not wait for each other: a failed step is logged and counted, and the others still run.

1. **Bodies.** Drop `tx_body` partitions more than 288 blocks below the pruner height. With the default trigger, `pruner_block_trigger = OnBlockPersisted`, the pruner height is the block persister's archived height, so a body is never dropped before the persister has used it. With `OnBlockMined`, which is for a node without a block persister, it is the height of the block just mined, and bodies drop 288 blocks below the tip. Before the drop, the body of each transaction that still waits to be mined is copied to `tx_body_carry`. A carried body leaves when its transaction has been mined for 288 blocks.
2. **Undo copies.** Drop `spend_journal` and `conflict_children` partitions older than `utxostore_spendJournalRetentionBlocks` (default 288). Before the drop, the undo copies of transactions that wait to be mined, or that were mined recently, are **moved** to a newer partition. A shallow reorg can still take such a transaction's block away, and its coins must then come back.
3. **Containment.** Drop a `tx_mined` partition when all four hold: the stamp completed it; the longest an undo copy can live has passed since the height in its completion record, which is the journal retention plus one partition, 576 blocks at the default retention of 288; no undo partition from before the stamp is still attached; and no identity row still points into it.
4. **Index rebuild.** Rebuild the most bloated UTXO index partition, at most one per block.

The pruner also preserves the parents of old unmined transactions (`preserved_parent`) and expires those copies, through the same two calls the aerospike store uses.

The store refuses to start with `pruner_skipDuringCatchup=true`. On this store the pruner also drives the stamp and every reclaim, and without them the disk fills during catch-up.

## 11. Reorgs, unspends and conflicts

**A reorg.** When a block is taken away, `SetMinedMulti` with `UnsetMined` deletes that block's containment rows and sets the marker on each identity row again, so block assembly sees the transactions as unmined. Above the checkpoint every such transaction still has its identity row, because only the stamp deletes it and the stamp waits 288 blocks. A request to un-mine a transaction with no identity row is refused and counted: it is below the checkpoint, a coinbase, seeded or already stamped, and no reorg can reach it. The transaction's spends are undone with `Unspend` where a double-spend decision needs it.

**Unspend.** It restores each UTXO from its undo copy and **consumes** that copy in the same statement. The undo row is the authorisation, so a second `Unspend` of the same output finds nothing and does nothing. A restore must name the spender that took the UTXO, so a stale request cannot bring back a coin a different transaction now holds.

**Double spends.** When a block contains a transaction that conflicts with one in the mempool, the shared code in `stores/utxo/process_conflicting.go` makes the block's transaction the winner and marks the other one conflicting. utxoset gives that code two things: `SpendsMadeBy`, which lists the coins a transaction took from the journal rather than from its body, and `conflict_children`, which finds the losing children of a contested parent. The journal also names the spender of an output, for as long as it keeps the spend (see section 14).

**Crash safety for conflicts.** Each resolution writes a `conflict_intents` row first and removes it last. Block assembly replays any row left at startup.

## 12. Crashes and repeats

- **A repeated block.** The identity key and `ON CONFLICT DO NOTHING` on containment make a re-applied block write nothing new. A repeated spend by the same spender is recognised from its undo copy.
- **A recreated old partition.** `tx_mined_floor` remembers the highest dropped `tx_mined` partition, and the store refuses to create a partition at or below it. Otherwise a very late block could claim every transaction in it again and double its UTXOs.
- **A crash during the stamp.** Each page commits on its own and is safe to run again. The completion record is written last.
- **A crash during the pruner.** Each drop is one partition. An interrupted detach is finished on the next pass.
- **A crash in the middle of the multi write.** See section 7 and issue 1976.

## 13. Running it

| Setting | Default | What it does |
|---|---|---|
| `utxostore` URL `utxoset://...` | | Selects this store |
| `utxostore_skipTxBodyBelowCheckpoint` | off | Below the highest checkpoint, keep no body. Saves most of the write volume during sync |
| `utxostore_spendJournalRetentionBlocks` | 288 | How long undo copies stay. At most 1,440 |
| `utxostore_retainWindowsIndefinitely` | off | Keep every partition, for an archive node. The stamp still runs |

The store needs a **new database**. A schema check at startup refuses a database from an earlier layout, and there is no migration. Every connection uses `plan_cache_mode = force_custom_plan`, so PostgreSQL plans each statement with its real parameters; several statements depend on that to skip partitions.

## 14. Known limits

- **An old transaction with unspent outputs has no body here.** The aerospike and sql stores keep a transaction's body while any of its outputs is unspent. utxoset drops it 288 blocks after the pruner height, whatever the state of the outputs. For such a transaction, `GET /tx/:hash` must find it in the transaction blob store, or it gives an error. For a fully spent old transaction the three stores are the same: each deletes it after the retention.
- **"Who spent this output" has a shorter memory.** utxoset can name the transaction that spent an output while the journal keeps the spend: 288 blocks, or longer for a spender that waits or was mined recently. aerospike and sql can name it while the parent's record exists, which for a parent with other unspent outputs is much longer. This changes the asset API's spend lookup and the text of the error for an old double spend. Validation and reorgs do not depend on it.
- **`tx_ident` rows leave one by one.** The stamp deletes each at a different time, so a partition drop cannot reclaim them.
- **Autovacuum must keep up.** Each block makes about 33,000 dead rows in the UTXO table.
- **A crash between a spend and its create keeps the spends, in every UTXO store.** This is not specific to utxoset. Each store spends a transaction's inputs before it creates the transaction: the aerospike and sql stores for each transaction, and utxoset in the two steps of the multi write. A crash between the two keeps the spends, with no record of the spender, until the same block is written again. Issue 1976 tracks one correction for all three stores.

## 15. Where to read the code

| Topic | File |
|---|---|
| Tables and the reasoning behind each | `schema.go` |
| Opening the store, settings, connection pool | `store.go` |
| Spends, the undo copy, repeated spends | `spend.go`, `spend_batch.go` |
| One transaction, batched | `spend_and_create.go`, `spend_and_create_batch.go` |
| A block's list, netting, the give-back | `spend_and_create_multi.go` |
| Creates | `create.go`, `create_batch.go` |
| The read order | `lookup.go`, `get.go`, `get_batch.go`, `decorate_batch.go` |
| Parent outputs for validation | `parent_outputs.go` |
| Containment, mark on longest chain | `set_mined.go`, `tx_mined.go`, `mark_on_longest_chain.go` |
| The stamp and its fence | `stamp.go`, `fence.go`, `services/pruner/stamp_worker.go` |
| The pruner, the copy-forward, the carry | `pruner.go`, `spend_journal.go`, `tx_body.go` |
| Unspend | `unspend.go` |
| Conflicts | `conflicting.go`, `conflicting_reads.go`, `conflict_wal.go`, `spends_made_by.go` |
| Freeze and reassign | `freeze.go`, `reassign.go` |
| The shared contract and its tests | `stores/utxo/Interface.go`, `stores/utxo/tests`, `conformance_test.go` |

## 16. Glossary

- **UTXO**: an output that no transaction has spent yet.
- **Undo copy, spend journal**: the copy of a UTXO taken at the moment a spend deletes it, so it can be restored.
- **Containment row**: a `tx_mined` row that says a block contains a transaction.
- **Identity row**: a `tx_ident` row for a transaction above the checkpoint that the stamp has not yet processed.
- **Marker (`off_chain_since`)**: set when a transaction is not on the main chain.
- **Partition**: one child table of a split table. Most here cover a fixed range of block heights.
- **Leaf**: one of the eight partitions of `utxo` and `tx_ident`, chosen by `txid[0] & 7`.
- **The stamp**: the pass that writes a UTXO's winning block once the block is 288 deep.
- **The fence**: the height below which no write may change containment, because the stamp has started there.
- **Netting**: not writing an output as a UTXO when a later transaction in the same list spends it.
- **Carry**: keeping the body of a waiting transaction after its body partition drops.
- **Pruner height**: the height the pruner uses to decide what is old. With the default `pruner_block_trigger = OnBlockPersisted` it is the block persister's archived height; with `OnBlockMined` it is the height of the block just mined.
- **Highest checkpoint**: the last hard-coded block that the node trusts. Below it a reorg is impossible by rule.
- **Two-phase commit, locked**: a new transaction's outputs are locked until block assembly acknowledges it.
