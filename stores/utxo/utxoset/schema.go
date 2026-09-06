// Package utxoset is a delete-on-spend UTXO-set store.
//
// It inverts what the postgres store indexes. That store records a spend as the
// PRESENCE of a row in an append-only `spends` table and detects a double-spend as a
// unique-constraint violation, which means the indexed structure is the set of
// everything ever spent — measured at 392,884,647 rows and growing with every block.
// This store indexes only the live UNSPENT set — measured at 74,426,411 and roughly
// flat — and detects a double-spend by ABSENCE. A spend becomes a DELETE.
//
// That single DELETE does four jobs at once: it arbitrates the double-spend (zero rows
// affected means the outpoint is already gone), it is the decorate fetch (RETURNING
// hands back satoshis and locking script, so PreviousOutputsDecorate never parses a
// raw transaction), it is the reclaim (no sweep, no pruner, no pending_deletes), and it
// is the write. The DAH sweep, pruner, spent_bits fold, reconciler and stagnation
// monitor do not exist here — on the mainnet box those measured at 76.7% of all disk
// reads, 52% of statement WAL and 25-30% of CPU.
//
// "No background job that can fall behind" is NOT true, and the qualifier matters.
// Autovacuum is that job: this store makes roughly 33,000 dead tuples per block forever,
// each pass is a full scan of a partition index, and it is unmeasured at scale. What
// delete-on-spend removes is the RECLAIM backlog, whose watermark on the old store sat
// 5,567 to 6,227 blocks behind the tip. It does not remove vacuum, and the autovacuum
// threshold below is sized deliberately for that reason.
//
// Measured evidence behind the design (see docs/superpowers/specs/):
//   - index bloat plateaus at exactly 2.00x its bulk-build floor and fully reverses via
//     REINDEX CONCURRENTLY (3.1 s per 10M entries)
//   - a packed 16-byte key costs 63.07 B per index entry at churn equilibrium, against
//     81.3 B for a bytea32 key
//
// Two claims that used to sit here have been REMOVED rather than softened, because
// neither has a source anywhere in this repository:
//
//   - "UTXO index 8.4 GB at the projected tip, vs a 25 GB budget". That figure is the
//     post-REINDEX floor at an assumed UTXO count, not a steady state, and the count it
//     assumed is the least certain input in the whole design. The honest position is that
//     nobody has run gettxoutsetinfo on a synced SV Node, so the tip index size is unknown.
//   - "19x less WAL per spend and 23x fewer full-page images than append-only".
//     Reconstructed from the underlying figures the best case is nearer 11.5x and 12.5x,
//     and the ratio INVERTS at high UTXO counts, because delete-on-spend does not avoid
//     the index write, it defers it to vacuum. Quote no ratio here until one is measured.
//
// M1 scope: the UTXO table, the spend journal and the block/chunk ledger. The tx_meta
// window arrives in M3.
//
// The journal is undo insurance and nothing else. It once doubled as the prune engine --
// a retiring partition was the list of parents to re-examine -- which made it load-bearing
// from genesis even below the hardcoded checkpoint, where a reorg is impossible by rule.
// It no longer is: a mined transaction claims on tx_mined, its coins carry the height and
// block that made them, and retiring its identity is dropping the window it lives in. See
// the spend_journal DDL comment below for what that leaves the journal doing.
package utxoset

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NumLeaves is the partition count. Deliberately 8, matching the postgres store.
//
// Partitioning is BY LIST on leaf = txid[0] & 7 — derived from the TXID, not from the
// outpoint and not by hashing ukey. Every output of one transaction must co-locate in a
// single leaf, otherwise the BIP30 probe, SetLocked, freeze and reorg restore each fan
// out across all 8 partitions. That fan-out is the 21x by-hash penalty the postgres
// store's invariant 6 exists to prevent.
const NumLeaves = 8

// LeafFor returns the partition a txid routes to. Computed Go-side so both sides share
// one routing function and Go never has to reimplement PostgreSQL's hash_any on bytea.
func LeafFor(txid []byte) int16 {
	if len(txid) == 0 {
		return 0
	}

	return int16(txid[0] & (NumLeaves - 1))
}

// Pack builds the UTXO table's key: the first 12 bytes of the txid followed by the vout as
// big-endian uint32. Sixteen bytes, stored as a uuid.
//
// PostgreSQL's uuid is typlen 16 / typalign 'c', so it occupies exactly 16 bytes with no
// padding and compares bytewise — which is what lets prefix-first packing turn "every
// output of parent P" into a range scan rather than a separate index.
//
// The key is NON-UNIQUE by design and this is load-bearing. A 96-bit prefix can collide,
// so identity is established by the full 32-byte txid carried on the row and rechecked in
// every predicate; a collision then costs one extra heap visit and never a wrong answer.
// A UNIQUE 96-bit key would be a consensus bug: 2^48 of attacker work to make a
// legitimate output fail to create.
func Pack(txid []byte, vout uint32) [16]byte {
	var k [16]byte

	copy(k[:12], txid)
	k[12] = byte(vout >> 24)
	k[13] = byte(vout >> 16)
	k[14] = byte(vout >> 8)
	k[15] = byte(vout)

	return k
}

// Flag bits on utxo.flags.
const (
	FlagFrozen      int16 = 1 << 0
	FlagLocked      int16 = 1 << 1
	FlagConflicting int16 = 1 << 2
	FlagCoinbase    int16 = 1 << 3
)

// schemaSQL is the M1 schema: the UTXO table, the spend journal, and the ledger that makes
// block application idempotent. tx_meta is not here yet.
const schemaSQL = `
-- ---------------------------------------------------------------------------
-- THE UTXO TABLE. One row per spendable output. Inserted once, deleted once.
--
-- The coin carries the height and block of the transaction that created it, exactly as
-- SV Node's coin carries nHeight. mined_height = 0 is the unconfirmed sentinel because no
-- spendable coin exists at height 0 -- genesis pays nobody -- so a mempool create can use
-- it without colliding with a real block. block_id = 0 must stay legitimate rather than
-- becoming a second sentinel: the seed tool writes it (cmd/seeder/seeder.go:674-681) and
-- block validation treats it as the genesis block's id (model/Block.go:47).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS utxo (
    satoshis        BIGINT   NOT NULL,
    created_height  INTEGER  NOT NULL,   -- first sight; keys the body window; restored verbatim
    spendable_from  INTEGER  NOT NULL,   -- coinbase maturity and ReAssignUTXO delay
    mined_height    INTEGER  NOT NULL DEFAULT 0, -- height of the block containing the creating tx; 0 = unconfirmed
    block_id        INTEGER  NOT NULL DEFAULT 0, -- blockchain store id of that block; 0 is legitimate (genesis, seeder)
    leaf            SMALLINT NOT NULL,   -- txid[0] & 7; the partition key
    flags           SMALLINT NOT NULL DEFAULT 0,
    ukey            UUID     NOT NULL,   -- pack(txid, vout). NON-UNIQUE by design.
    txid            BYTEA    NOT NULL,   -- full 32 bytes: the authorising identity
    script          BYTEA    NOT NULL,   -- locking script; serves decorate directly
    hash_override   BYTEA                -- NULL except after ReAssignUTXO
) PARTITION BY LIST (leaf);

-- ---------------------------------------------------------------------------
-- THE LEDGER. Replay safety.
--
-- The UTXO table's key is non-unique, so there is no ON CONFLICT (txid, vout) to make
-- create idempotent. A block arriving twice, arriving out of order, or being
-- re-applied after a crash must therefore be rejected by ground truth: a durable
-- record written in the SAME transaction as the work it describes. This is what
-- keeps invariant 5 intact — nothing is authorised by a counter.
-- ---------------------------------------------------------------------------
-- ---------------------------------------------------------------------------
-- THE SPEND JOURNAL. svnode's rev*.dat, in a table.
--
-- A delete-on-spend store destroys the row, so a reorg or a conflict resolution has
-- nothing to restore FROM unless the payload is captured at the moment it is deleted.
-- Re-deriving it from the block is not an option: this node retains 696 KB of blocks
-- against 2,838 GB of chain, and the subtree data it does keep is not in extended
-- format, so it records WHICH outpoints a block consumed and none of their satoshis or
-- scripts.
--
-- The journal row is therefore written in the SAME STATEMENT as the delete, not merely
-- the same transaction -- see spendJournalSQL. It carries every field needed to
-- reconstruct the UTXO row byte-for-byte, including hash_override, because
-- ReAssignUTXO splices an operator-supplied utxo hash that is NOT derivable from
-- (txid, vout, satoshis, script); recomputing it on restore would silently reverse a
-- court-ordered reassignment.
--
-- spending_txid is an ownership token, deliberately NOT indexed: a restore must match
-- the spender that actually took the coin, so a stale reorg record whose output has
-- since been re-spent by a different transaction matches nothing and is a no-op rather
-- than resurrecting a coin someone else now owns.
--
-- RANGE partitioned by spent_height so retiring it is DROP TABLE -- O(1), no scan, no
-- vacuum, no background job that can fall behind. Age clusters by insert time by
-- definition, which is why partition-drop works here and did not for the rejected
-- epoch-slab design (there, garbage clustered by SPEND time, which is decorrelated
-- from creation).
--
-- IT IS UNDO INSURANCE, AND ONLY THAT.
--
-- It used to be the PRUNE ENGINE as well, and that is why it had no off-switch. A spend
-- deletes one coin row and signals nothing, so "that transaction's last output has now
-- gone" was a fact about absence recorded nowhere else, and the journal recorded it for
-- free: every spend writes a row, rows are grouped by height, and a retiring partition
-- therefore WAS the list of transactions to re-examine. Retiring an identity needed that
-- list, so the journal was load-bearing even below the hardcoded checkpoint where a reorg
-- is impossible by rule.
--
-- It is not any more. A mined transaction claims on tx_mined, in 288-block windows, and its
-- coins carry the height and block of the transaction that made them, so retiring its
-- identity is dropping a window and nothing has to be re-examined at all. What is left here
-- is a restore, by outpoint, of a spend inside the retention window. Measured cost of
-- keeping it: 354.8 bytes of WAL and 12.9 microseconds per spend, about 6% of the per-block
-- budget in the worst band. Cheap enough that it stays on below the checkpoint too, rather
-- than adding a second write path that only the tip exercises.
--
-- IT IS ALSO THE LAST RECORD OF A FULLY-SPENT OLD PARENT.
--
-- mined_height and block_id are copied off the coin at the moment the spend destroys it. The
-- rule the design started from was that nothing mutable goes into the journal payload, and
-- that rule is about the RESTORE, which must keep re-resolving block facts at restore time
-- because a reorg can move a still-live parent to a different block. It is not about the
-- READ. These two columns are settled facts by the time anything reads them: a coin carries
-- them only once the block path wrote them or window retirement stamped them, and the only
-- reader is the lookup step that runs when identity, membership, preservation and the coin
-- have ALL missed. That is exactly the fully-spent parent older than the membership
-- retention, whose block is at least 1440 deep and cannot change.
--
-- Without them that parent is in no table at all, and model/Block.go's checkParentTransactions
-- asks about it on most blocks above the highest checkpoint: 96 percent of a block's
-- out-of-block parents have no coin left by the time block validation asks. getParentTxMetaBlockIDs
-- turns not-found into BlockIncompleteError, which callers retry rather than persist, so the
-- block retries forever. Both the base branch and aerospike keep a fully-spent parent
-- answerable for a window AFTER the spend; this is how that property comes back.
--
-- The two retentions are counted from DIFFERENT clocks and the sets are not nested: membership
-- retires 1440 blocks after the parent was MINED, the journal 1440 blocks after the coin was
-- SPENT. A parent mined at 500,000 and spent at 900,001 lost its window at 501,440 and keeps
-- its journal row until 901,441.
--
-- DEFAULT 0 on both is the unconfirmed sentinel, and the read step filters mined_height > 0
-- for it: a mempool parent's spend journals no block, and reporting block id 0 for it would be
-- a lie block validation cannot tell from genesis, whose id really is 0.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS spend_journal (
    spent_height    INTEGER  NOT NULL,
    satoshis        BIGINT   NOT NULL,
    created_height  INTEGER  NOT NULL,
    spendable_from  INTEGER  NOT NULL,
    flags           SMALLINT NOT NULL,
    mined_height    INTEGER  NOT NULL DEFAULT 0,
    block_id        INTEGER  NOT NULL DEFAULT 0,
    ukey            UUID     NOT NULL,
    txid            BYTEA    NOT NULL,
    spending_txid   BYTEA    NOT NULL,
    script          BYTEA    NOT NULL,
    hash_override   BYTEA
) PARTITION BY RANGE (spent_height);

-- ---------------------------------------------------------------------------
-- CONFLICT BOOKKEEPING. One row per (contested parent, losing child).
--
-- A transaction that loses a double-spend race is kept rather than discarded, because
-- resolving the race later has to find it, and finding it means asking the PARENT whose coin
-- was contested. This table IS that route.
--
-- It is a side table rather than a column on tx_ident, and that is a correctness requirement
-- rather than normalisation for its own sake. A contested parent is very often MINED, and a
-- mined transaction has no identity row at all: the longest-chain stamp moved it into
-- tx_mined. A column on tx_ident therefore had nowhere to land the note, so the write was a
-- zero-row update and the route silently did not exist -- and the stamp had to REFUSE to move
-- any row carrying the column, pinning contested transactions in the mempool table forever.
-- Keyed on the txid alone, this answers for a parent in either table, or in neither.
--
-- RANGE partitioned on noted_height, in the SAME 48-block windows as the spend journal and
-- created by the same DDL statement, so a window exists whenever its journal leaf does. It is
-- dropped in the same pass and on the same cutoff, which is the honest retention: the journal
-- is what conflict resolution restores the losing spends FROM, so a note whose journal leaf
-- has gone can no longer be acted on.
--
-- noted_height is the store's CURRENT chain height at the moment of noting, not a property of
-- either transaction. It is bookkeeping about a race happening now.
--
-- The uniqueness is PER WINDOW, deliberately, and the reader deduplicates. A unique index on
-- a partitioned table must include the partition key, so a global UNIQUE (parent_txid,
-- child_txid) is not available without noted_height in it -- which would defeat the purpose,
-- since the same pair noted at two heights would then be two legal rows anyway. So each
-- window carries a plain UNIQUE (parent_txid, child_txid), the inserts are ON CONFLICT DO
-- NOTHING against it, and the read is SELECT DISTINCT: a repeated note inside one window
-- writes nothing, and the same pair noted in two windows reads back once.
--
-- That pair index is the window's ONLY index, and it serves the reads too, because
-- parent_txid leads it and every read here is by parent_txid. See ensureSpendJournalPartition
-- for the DDL and the measurement.
--
-- NO DEFAULT PARTITION, for the reason tx_body gives: a default partition makes
-- ALTER TABLE ... DETACH PARTITION ... CONCURRENTLY impossible on every other partition.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS conflict_children (
    noted_height    INTEGER NOT NULL,
    parent_txid     BYTEA   NOT NULL,
    child_txid      BYTEA   NOT NULL
) PARTITION BY RANGE (noted_height);

-- ---------------------------------------------------------------------------
-- THE CONFLICT-RESOLUTION WRITE-AHEAD LOG. One row per in-flight resolution.
--
-- ProcessConflicting and its reverse mutate several transactions' state in sequence, and a
-- process killed between two of those steps leaves the set half-moved with nothing recording
-- that it was ever asked for. The row goes in BEFORE the first mutation and comes out AFTER
-- the last, so anything still here at startup is work a crash interrupted, and block assembly
-- replays it (BlockAssembler.go:936).
--
-- intent_id is the caller's deterministic hash over kind, block, height and the SORTED tx
-- hashes (utxo.ConflictIntent.IntentID), which is what makes Begin idempotent: a crash-retry
-- of the same operation, in whatever hash order, writes the same key and ON CONFLICT DO
-- NOTHING absorbs it. The length CHECK is there because the id is the primary key and a short
-- one would silently make a different intent addressable.
--
-- tx_hashes is the operation's hash list concatenated, 32 bytes each, in the order the caller
-- gave. Not an array type: the read splits it once at startup and nothing ever queries INTO
-- it, so a bytea costs one column and no per-element overhead.
--
-- Unpartitioned and unindexed beyond the key. In-flight resolutions number in the ones, the
-- table is read exactly once per process start, and every write is by primary key.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS conflict_intents (
    intent_id    BYTEA   PRIMARY KEY CHECK (length(intent_id) = 32),
    kind         TEXT    NOT NULL,
    block_height INTEGER NOT NULL,
    block_hash   BYTEA   NOT NULL,
    tx_hashes    BYTEA   NOT NULL,
    started_at   BIGINT  NOT NULL
);

CREATE TABLE IF NOT EXISTS applied_block (
    height       INTEGER NOT NULL,
    block_hash   BYTEA   NOT NULL,
    chunk_size   INTEGER NOT NULL,   -- re-offering a block with a different chunk_size
                                     -- must fail loudly, never silently re-cut
    chunk_count  INTEGER NOT NULL,
    completed    BOOLEAN NOT NULL DEFAULT FALSE,
    applied_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (block_hash)
);
CREATE INDEX IF NOT EXISTS applied_block_height ON applied_block (height);

CREATE TABLE IF NOT EXISTS applied_chunk (
    block_hash   BYTEA   NOT NULL,
    chunk_idx    INTEGER NOT NULL,
    PRIMARY KEY (block_hash, chunk_idx)
);

-- ---------------------------------------------------------------------------
-- BLOCK MEMBERSHIP. One row per (transaction, block) that stamped it, for the reorg window.
--
-- This replaces the identity row for a MINED transaction. Below the checkpoint it is written
-- once at create and read by nothing on the consensus path; at the tip a mempool row moves
-- here when its block is stamped on the longest chain (stage 2). It is RANGE partitioned by
-- mined_height in 288-block windows and dropped whole once a window's upper bound is 1440
-- blocks below the pruner's height, so reclaim is a catalog operation with nothing to fall
-- behind on.
--
-- The primary key LEADS with txid. PostgreSQL requires the partition key inside a partitioned
-- table's primary key but not at its head, and a height-leading key cannot be probed by
-- transaction id: on 18 it becomes a skip scan over every distinct height in the partition.
-- Every by-id lookup here is one descent per live window, six at 288-block windows.
--
-- seq keeps insertion order, which the shared conformance suite requires of BlockIDs.
-- The payload columns are what a transaction needs if it is un-mined back into the mempool
-- (stage 2); below the checkpoint tx_inpoints is written NULL because nothing there can
-- un-mine.
--
-- fee is carried because tx_ident carries one and the tip's stamp MOVES that row here. A
-- transaction un-mined back into the mempool is handed to block assembly, which prices it,
-- so dropping the column would silently zero the fee of every reorged transaction. It is
-- nullable because the block path has no fee to carry: a create below the checkpoint writes
-- NULL, exactly as the identity claim does.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tx_mined (
    txid            BYTEA    NOT NULL,
    mined_height    INTEGER  NOT NULL,
    block_id        INTEGER  NOT NULL,
    subtree_idx     INTEGER  NOT NULL,
    seq             BIGINT   GENERATED ALWAYS AS IDENTITY,
    created_height  INTEGER  NOT NULL,
    size_in_bytes   INTEGER,
    fee             BIGINT,
    tx_inpoints     BYTEA,
    locktime        INTEGER,
    created_at      BIGINT,
    flags           SMALLINT NOT NULL DEFAULT 0,
    CONSTRAINT tx_mined_ck CHECK (length(txid) = 32),
    PRIMARY KEY (txid, mined_height, block_id)
) PARTITION BY RANGE (mined_height);

-- The highest membership window ever dropped, plus one. ensureTxMinedPartition refuses to
-- create a window at or below it: a block re-offered more than 1440 blocks after its first
-- application would otherwise recreate its window and claim every transaction in it afresh,
-- and the coin table has no uniqueness on the outpoint to stop the coins doubling.
CREATE TABLE IF NOT EXISTS tx_mined_floor (
    id      SMALLINT PRIMARY KEY CHECK (id = 0),
    floor   INTEGER  NOT NULL
);
INSERT INTO tx_mined_floor (id, floor) VALUES (0, 0) ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- THE PRESERVED PARENT. One row per transaction the pruner asked to keep alive past the
-- membership window that would otherwise have retired it.
--
-- It exists for one case, the lingering unmined child. A parent whose window has been dropped
-- and whose coins are all spent is GONE, and that is right: nothing on the consensus path can
-- ask about a transaction that is 1440 blocks buried and has no live output. The exception is
-- an unmined child still sitting in block assembly, which will be validated again the day it
-- is finally mined and needs its parent's facts to be validated against. The pruner names
-- exactly those parents each cycle -- see PreserveParentsOfOldUnminedTransactions -- and this
-- table is where the answer for them survives.
--
-- It is TINY, and by construction rather than by hope. Nothing writes here on the block path
-- or the validator's path; the only writer is the pruner's phase 1, and it names only the
-- parents of transactions that have been waiting longer than the retention window. An
-- ordinary mempool that keeps up puts nothing in it at all.
--
-- The payload is the FULL membership payload, not just the block, because this row has to
-- answer exactly what the tx_mined row it copies would have answered: a thinner row would
-- hand block assembly a parent with no fee, no size and no inputs, which is a different
-- record rather than a slower one. See readPreserved for the read and minedRow.toMeta for
-- the conversion the two share.
--
-- This is the store-side version of what aerospike does with its preserveUntil bin: same
-- pruner, same two calls, same expiry height, but expressed as a row that outlives a dropped
-- partition instead of a bin that suppresses a delete. preserve_until is that height, and
-- ProcessExpiredPreservations deletes every row that has reached it.
--
-- NOT partitioned, deliberately. It is keyed by transaction id alone and read by primary key,
-- its rows leave by an expiry height that has nothing to do with any window's bounds, and it
-- is small enough that a delete plus autovacuum costs nothing worth a catalog operation.
--
-- The primary key is its ONLY index. Every read is by transaction id, which the key serves,
-- and the expiry sweep is a once-per-block scan of a table this small; an index on
-- preserve_until would be write amplification for a plan that has nothing to gain.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS preserved_parent (
    txid            BYTEA    PRIMARY KEY CHECK (length(txid) = 32),
    mined_height    INTEGER  NOT NULL,
    block_id        INTEGER  NOT NULL,
    subtree_idx     INTEGER  NOT NULL,
    created_height  INTEGER  NOT NULL,
    fee             BIGINT,
    size_in_bytes   INTEGER,
    tx_inpoints     BYTEA,
    locktime        INTEGER,
    created_at      BIGINT,
    flags           SMALLINT NOT NULL DEFAULT 0,
    preserve_until  INTEGER  NOT NULL
);

-- ---------------------------------------------------------------------------
-- THE IDENTITY TABLE. One row per MEMPOOL transaction, from first sight until the block
-- that settles it.
--
-- "Settles" rather than "contains", because the two are not the same fact and the difference
-- decides whether the row leaves. A stamp naming a block ON THE LONGEST CHAIN moves the row
-- into tx_mined and deletes it here, but only when the row then names exactly ONE block. A row
-- naming two blocks stays, with its mempool marker cleared: the id-less mark-on-longest-chain
-- call cannot later say which of them is main, so the row waits for an un-mine or a further
-- stamp to reduce it to one. A stamp for a block NOT on the longest chain moves nothing at
-- all; it only appends the block.
--
-- Conflict bookkeeping used to pin a row here too, because it lived in a column of this table
-- and tx_mined had nowhere to put it. It is a side table now (see conflict_children above),
-- keyed on the txid alone, so a contested row settles like any other.
--
-- Partitioned BY LIST (leaf), the same eight-way split as utxo, and never by
-- created_height. Three reasons, worst first:
--
--   1. created_height is NOT a property of the transaction. A mempool create writes
--      tip+1, a guess; a block-application create of the SAME transaction writes the
--      block's height. One block of mempool residency makes them differ, so a key
--      including created_height admits the ordinary mempool-then-mined duplicate --
--      which is the "two outputs became four" failure this table exists to stop.
--   2. Fifteen Store methods take a txid and no height at all, and two of the
--      highest-volume ones have no height field in their argument type, so they could
--      not supply one even after an interface change.
--   3. Height only ever bought a partition drop, and a partition holds transactions whose
--      coins are still unspent, so it could never fire.
--
-- leaf is a REAL stored column. PARTITION BY LIST ((get_byte(txid,0) & 7)) is accepted
-- but then postgres bans every unique constraint on the table, and a GENERATED column
-- cannot be a partition key -- verified on 17.11 and 18.6, including 18's new VIRTUAL
-- generated columns.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tx_ident (
    leaf                 SMALLINT NOT NULL,   -- txid[0] & 7; the partition key
    txid                 BYTEA    NOT NULL,   -- full 32 bytes: THE identity
    created_height       INTEGER  NOT NULL,   -- first sight; immutable
    membership           BYTEA,               -- packed 12-byte triples: blockID, height, subtreeIdx
    off_chain_since      INTEGER,             -- see the comment on the index below
    fee                  BIGINT,
    size_in_bytes        INTEGER,
    tx_inpoints          BYTEA,
    locktime             INTEGER,
    created_at           BIGINT,
    flags                SMALLINT NOT NULL DEFAULT 0,

    -- LOAD-BEARING, and not defensive tidiness: this IS the global uniqueness rule.
    --
    -- Postgres enforces PRIMARY KEY (leaf, txid) only WITHIN a partition. Verified on
    -- 17.11 and 18.6: the same txid under leaf 0 and leaf 1 is ACCEPTED, two rows, and
    -- ON CONFLICT (leaf, txid) DO NOTHING reports the second as a fresh insert. Seven of
    -- the eight wrong values are in range, so no mistake fails safe.
    --
    -- The length test must come first and share the AND. get_byte on an empty bytea
    -- RAISES, where LeafFor returns 0 for an empty txid; the short-circuit turns that
    -- divergence into a clean constraint violation instead of an error from inside the
    -- expression.
    CONSTRAINT tx_ident_ck CHECK (length(txid) = 32 AND leaf = (get_byte(txid, 0) & 7)),
    CONSTRAINT tx_ident_membership_triples
        CHECK (membership IS NULL OR length(membership) % 12 = 0),
    PRIMARY KEY (leaf, txid)
) PARTITION BY LIST (leaf);

-- ---------------------------------------------------------------------------
-- THE BODY. Serialized transaction bytes, and nothing else.
--
-- This is the ONE part of a transaction whose life is bounded by a horizon rather
-- than by its coins. Everything on tx_ident is pinned while any output is unspent,
-- at any age, because the validator reads the parent's block ids and heights for
-- every input it spends. Keeping the bytes there too would make the transaction
-- archive permanent for that whole population: measured at 136 GB of out-of-line
-- storage today against roughly 95 GB free on the mainnet box, and past the disk
-- at the projected tip.
--
-- RANGE partitioned on created_height so reclaim is dropping a file. Measured on
-- 18.6: a partition drop returned 883 MB to the operating system for 228 KB of
-- crash-recovery journal, where the equivalent delete plus vacuum returned 128 KB
-- for 1.75 GB.
--
-- Uniqueness here is LOCAL to a partition, and that is fine rather than a
-- compromise: tx_ident already guarantees the txid appears once globally, and the
-- claim gates this insert, so two rows for one txid cannot be written.
--
-- NO DEFAULT PARTITION, ever. On 18.6 a default partition makes
-- ALTER TABLE ... DETACH PARTITION ... CONCURRENTLY impossible on every other
-- partition of the table, and the concurrent form is what keeps a drop from
-- blocking readers.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tx_body (
    created_height  INTEGER NOT NULL,
    txid            BYTEA   NOT NULL,
    raw_tx          BYTEA,
    PRIMARY KEY (created_height, txid)
) PARTITION BY RANGE (created_height);
`

// partitionSQL is applied per leaf.
//
// fillfactor is 90, not 100. The only routine UPDATE is the two-phase-commit SetLocked
// release; at fillfactor 100 that becomes a non-HOT update which must write into the
// UTXO index. The tuple never changes size (flags and spendable_from are fixed
// width), so page slack keeps it HOT with zero index writes. This costs disk, not index
// budget. The box has never run block assembly with a mempool, so there is no local
// evidence either way — this is measurement M4.
const partitionSQL = `
CREATE TABLE IF NOT EXISTS utxo_p%[1]d PARTITION OF utxo FOR VALUES IN (%[1]d)
  WITH (fillfactor = 90,
        autovacuum_vacuum_scale_factor  = 0,
        -- Sized to the index, not pinned to a round number. This store makes roughly
        -- 33,000 dead tuples per block forever, so autovacuum is the one background job
        -- it does have, and unlike a pruner it CAN fall behind. Each pass is a full scan
        -- of this partition's index. At fat-band density 200,000 fires every 19 blocks,
        -- about 1.13 GB/block of index scanning, capping the node near 2.65 blk/s on that
        -- term alone; 1,000,000 gives ~0.23 GB/block for ~1.6 GB of resident dead rows.
        -- scale_factor stays 0 because the dead-row rate is proportional to block
        -- production, not to table size.
        autovacuum_vacuum_threshold     = 1000000,
        autovacuum_vacuum_cost_delay    = 0,
        autovacuum_vacuum_cost_limit    = 10000,
        autovacuum_analyze_scale_factor = 0.02);

-- The ONE index. There is deliberately no index on txid: every by-txid access is a
-- ukey range scan with a full-txid heap recheck. A (leaf, txid) index would cost
-- roughly 385M x 63 B = 24 GB and take the budget past 1.4x the entire allowance. Any query filtering on
-- txid without a ukey range bound is a review failure.
CREATE INDEX IF NOT EXISTS utxo_p%[1]d_ukey ON utxo_p%[1]d (ukey);

-- The identity partitions take the coin table's autovacuum block, for the same reason. This is
-- the MEMPOOL table now, and the ordinary mempool row is deleted at row level the moment a
-- longest-chain stamp moves it to tx_mined, so the dead-row rate here tracks block production
-- exactly as the coin table's does. The rows that stay behind on a stamp -- the ones naming
-- two blocks, and the ones carrying conflicting children -- are the fork and conflict
-- residue, a rounding error against a block's worth of ordinary transactions, so they do not
-- change that rate. The shipped default (20 percent dead tuples,
-- default throttling) meant a pass every twenty minutes or so per partition with the primary
-- key drifting toward its 2x bloat plateau between passes. The ALTER makes an existing
-- database converge; SET is catalog-only.
CREATE TABLE IF NOT EXISTS tx_ident_l%[1]d PARTITION OF tx_ident FOR VALUES IN (%[1]d)
  WITH (autovacuum_vacuum_scale_factor  = 0,
        autovacuum_vacuum_threshold     = 1000000,
        autovacuum_vacuum_cost_delay    = 0,
        autovacuum_vacuum_cost_limit    = 10000,
        autovacuum_analyze_scale_factor = 0.02);
ALTER TABLE tx_ident_l%[1]d
  SET (autovacuum_vacuum_scale_factor  = 0,
       autovacuum_vacuum_threshold     = 1000000,
       autovacuum_vacuum_cost_delay    = 0,
       autovacuum_vacuum_cost_limit    = 10000,
       autovacuum_analyze_scale_factor = 0.02);
`

// txIdentIndexSQL is the ONE secondary index tx_ident should ever carry, plus the reducer
// the settled predicate needs.
//
// off_chain_since is a CACHED ANSWER TO A CHAIN QUESTION, not a timer, and it can never be
// derived here. An index only answers questions about columns on the row it indexes, and
// "is any block containing me on the main chain" is not one: the answer changes when a
// block is invalidated and no row changes. This store cannot see the chain at all -- it
// holds a pool, a pushed-in height and journal state -- so the answer has to be written
// down by the four paths that already learned it. NULL means the last thing we were told is
// that a MAIN-CHAIN block contains this transaction.
//
// The index is PARTIAL, covering only the mempool. Measured on PG 18.6 at 43,000,000 rows
// with a 43,100-transaction mempool: 524,288 bytes, which is 0.0122 bytes per table row and
// 0.016% of the primary key beside it. The reload it serves costs 3,484 page fetches with a
// fresh mempool against 978,145 for the sequential scan it would otherwise need.
//
// Do NOT add INCLUDE columns to make it index-only. Measured on 18.6: including
// tx_inpoints refuses any transaction with 82 or more inputs at INSERT time --
// "index row size 2720 exceeds btree version 4 maximum 2704" -- which is transaction
// intake down, not a slow path.
const txIdentIndexSQL = `
CREATE INDEX IF NOT EXISTS tx_ident_off_chain_idx ON tx_ident (off_chain_since)
    WHERE off_chain_since IS NOT NULL;

-- Highest block height named by a packed membership, NULL if there is none.
--
-- The casts MUST be bigint. In postgres 255::int << 24 wraps to -16777216, SILENTLY, so an
-- int4 version returns negative heights and every "<= cutoff" test comes back true, which
-- would settle every transaction in the store. Verified on 18.6.
--
-- The MAXIMUM is what makes the settled predicate sound: it is at least the main-chain
-- height, so if the maximum is 288 deep the main-chain block is too. Taking the first or
-- the most favourable height instead lets a child mined low on a fork and re-mined recently
-- read as stable, which is what the incumbent SQL pruner does.
CREATE OR REPLACE FUNCTION mh_max(m bytea) RETURNS bigint
    LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $fn$
  SELECT max( (get_byte(m, i*12+4)::bigint << 24)
            | (get_byte(m, i*12+5)::bigint << 16)
            | (get_byte(m, i*12+6)::bigint <<  8)
            |  get_byte(m, i*12+7)::bigint )
    FROM generate_series(0, octet_length(m)/12 - 1) i
$fn$;

-- mh_strip returns the membership with every triple naming one of ids removed, in insertion
-- order.
--
-- The casts MUST be bigint, for exactly the reason mh_max's must. In PostgreSQL 255::int << 24
-- wraps to a negative number, silently, so an int4 version would compare a negative value
-- against a positive block id and strip nothing at all.
--
-- EVERY matching triple goes, not the first. One block can be stamped against a transaction
-- twice under different subtree indexes, so a first-match removal would leave it still claiming
-- a block the caller asked it to forget.
--
-- Stripping the last triple yields NULL rather than an empty value, which is what a transaction
-- no block has ever named already carries, so the two spellings of "no block" stay one. STRICT
-- keeps a NULL membership NULL rather than turning it into a value.
CREATE OR REPLACE FUNCTION mh_strip(m bytea, ids bigint[]) RETURNS bytea
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $fn$
  SELECT string_agg(substring(m from i*12 + 1 for 12), ''::bytea ORDER BY i)
    FROM generate_series(0, octet_length(m)/12 - 1) i
   WHERE NOT ( ( (get_byte(m, i*12+0)::bigint << 24)
               | (get_byte(m, i*12+1)::bigint << 16)
               | (get_byte(m, i*12+2)::bigint <<  8)
               |  get_byte(m, i*12+3)::bigint ) = ANY (ids) )
$fn$;

-- mh_triple packs ONE membership entry: block id, height, subtree index, each a big-endian
-- uint32 in that order, which is the layout packMembership writes and mh_max and mh_strip
-- read.
--
-- It exists because the un-mine has to REBUILD a membership from tx_mined rows, and written
-- inline that is twelve nested set_byte calls inside a string_agg -- unreadable, and
-- impossible to check by eye against the Go packer. One function, one place to compare.
--
-- The casts MUST be bigint, for exactly the reason mh_max's must. In PostgreSQL
-- 255::int << 24 wraps to a negative number, silently, and set_byte then raises on a value
-- outside 0..255 instead of storing the byte the caller meant.
CREATE OR REPLACE FUNCTION mh_triple(block_id int, height int, subtree int) RETURNS bytea
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $fn$
  SELECT set_byte(set_byte(set_byte(set_byte(
         set_byte(set_byte(set_byte(set_byte(
         set_byte(set_byte(set_byte(set_byte(
             '\x000000000000000000000000'::bytea,
              0, ((block_id::bigint >> 24) & 255)::int),
              1, ((block_id::bigint >> 16) & 255)::int),
              2, ((block_id::bigint >>  8) & 255)::int),
              3, ( block_id::bigint        & 255)::int),
              4, ((height::bigint   >> 24) & 255)::int),
              5, ((height::bigint   >> 16) & 255)::int),
              6, ((height::bigint   >>  8) & 255)::int),
              7, ( height::bigint          & 255)::int),
              8, ((subtree::bigint  >> 24) & 255)::int),
              9, ((subtree::bigint  >> 16) & 255)::int),
             10, ((subtree::bigint  >>  8) & 255)::int),
             11, ( subtree::bigint         & 255)::int)
$fn$;
`

// CreateSchema installs the M1 schema. Idempotent.
func CreateSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		return err
	}

	for i := 0; i < NumLeaves; i++ {
		if _, err := pool.Exec(ctx, fmt.Sprintf(partitionSQL, i)); err != nil {
			return err
		}
	}

	// After the partitions: an index on a partitioned parent is created on every existing
	// partition, and postgres adds it to any created later.
	if _, err := pool.Exec(ctx, txIdentIndexSQL); err != nil {
		return err
	}

	return assertSchemaShape(ctx, pool)
}

// requiredColumns are the facts that distinguish this schema from the one before it. A
// database missing any of them cannot serve this binary.
//
// A table name with an empty column asserts the table itself. tx_mined did not exist at all in
// the previous shape, so a database old enough to be missing it has to be refused by name
// rather than by column: information_schema.columns simply returns nothing for a table that is
// not there, and "tx_mined.txid is missing" would send an operator looking for a column.
var requiredColumns = []struct{ table, column string }{
	{"utxo", "mined_height"},
	{"utxo", "block_id"},
	{"tx_mined", ""},
	{"spend_journal", "mined_height"},
	{"spend_journal", "block_id"},
	{"conflict_intents", ""},
}

// assertSchemaShape refuses to start against a database written by the previous schema.
//
// Fresh-sync-only is by design and there is no migration, but that design needs a gate. Every
// statement in CreateSchema is CREATE TABLE IF NOT EXISTS, so against a database from the
// previous shape it is a complete no-op on utxo -- the table is there, without mined_height
// and block_id -- creates tx_mined, tx_mined_floor, preserved_parent and conflict_children
// beside it, installs the functions, and reports success. New then returns a healthy store,
// and the first create to run fails with `column "mined_height" of relation "utxo" does not
// exist`. Every batch after it fails the same way, forever, and the columns the reshape
// removed are all nullable or defaulted, so nothing fails earlier or louder.
//
// One query converts that flood into a single startup refusal naming what is missing. It runs
// as part of CreateSchema rather than beside it so that no caller can install the schema and
// skip the check.
func assertSchemaShape(ctx context.Context, pool *pgxpool.Pool) error {
	for _, req := range requiredColumns {
		var found bool

		var err error

		if req.column == "" {
			err = pool.QueryRow(ctx, `
				SELECT EXISTS (SELECT 1 FROM information_schema.tables
				                WHERE table_schema = current_schema() AND table_name = $1)`,
				req.table).Scan(&found)
		} else {
			err = pool.QueryRow(ctx, `
				SELECT EXISTS (SELECT 1 FROM information_schema.columns
				                WHERE table_schema = current_schema() AND table_name = $1
				                  AND column_name = $2)`,
				req.table, req.column).Scan(&found)
		}

		if err != nil {
			return errors.NewStorageError("[utxoset] check schema shape for %s", req.table, err)
		}

		if found {
			continue
		}

		what := req.table
		if req.column != "" {
			what = req.table + "." + req.column
		}

		return errors.NewConfigurationError(
			"[utxoset] this database was written by an older schema: %s is missing. There is no migration by design -- the store needs a fresh database, and the chain has to be re-synced into it",
			what)
	}

	return nil
}
