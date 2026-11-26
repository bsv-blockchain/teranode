package seeder

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"io"
	"os"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxofactory "github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"golang.org/x/sync/errgroup"
)

// Lane positions for a seed, at utxostore_laneMigration shadow.
//
// The seeder gives every record it imports a lane position (util/lane) from one grant, and writes
// one map row per block for the records of that block. A .utxo-set file is sorted by txid, so a
// block's records are spread over the whole file. Positions are therefore handed out by height,
// not in file order: a first pass counts the records of each height, and each record then takes
// the next position of its height's stretch. Take a file of five records: A and C at height 7, B
// at height 9, coinbase D at height 7 and E at height 8, in that order, with a grant from 100. The
// stretches are height 7 at 100 to 101, height 8 at 102 and height 9 at 103, so A takes 100, B
// 103, C 101 and E 102. Coinbases come after every height, in file order: D takes 104. Each block
// then has one range row, 100 to 101 for height 7, and no row lists a coinbase, as no Phase 1 row
// does. A position follows from the grant and the file alone, so a seed run again over the same
// file gives each record the position it already has.

// The blockchain store state keys of one snapshot's seed, each the prefix and the first 14 bytes of
// the snapshot's tip hash in hex, under the state table's 32-character key limit. The grant key
// holds the grant the seed takes its positions from, so a seed run again reuses it; a seed of
// another snapshot leaves it alone. The rows key holds the tip hash once every seeded row of that
// seed is written, and is cleared when an import starts.
const (
	seederGrantKeyPrefix   = "sg:"
	seederRowsKeyPrefix    = "sr:"
	seederPendingKeyPrefix = "sp:"
	seederKeyHashBytes     = 14
)

func seederStateKey(prefix string, tip chainhash.Hash) string {
	return prefix + hex.EncodeToString(tip[:seederKeyHashBytes])
}

// seederDBTimeout bounds the database work of each blockchain store call the seed's lanes make, so
// a lock another transaction holds fails the seed instead of stalling it. On SQLite a call first
// takes an in-process mutex, which the bound does not cover.
var seederDBTimeout = 2 * time.Minute

// withDBTimeout runs fn with a context bounded by seederDBTimeout.
func withDBTimeout(ctx context.Context, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, seederDBTimeout)
	defer cancel()

	return fn(ctx)
}

// seederGrantRecordBytes is the size of the grant record: a version byte, the snapshot's tip hash,
// its record count, the lane, and the grant's first and last positions.
const seederGrantRecordBytes = 1 + 32 + 8 + 1 + 8 + 8

// seedLayout is where each height's records go in a seed's grant.
type seedLayout struct {
	// counts is how many records other than coinbases each height holds, and base where the
	// height's stretch starts, as an offset from the grant's first position.
	counts []uint32
	base   []uint64
	// nonCoinbase and coinbase count the file's records of each kind; coinbases take the
	// positions from offset nonCoinbase on.
	nonCoinbase uint64
	coinbase    uint64
}

// total is the number of records, and so of positions the seed takes.
func (l *seedLayout) total() uint64 {
	return l.nonCoinbase + l.coinbase
}

// seededLanes is what a seed at shadow hands from the UTXO pass to the rows it writes after both
// passes: the layout and the grant.
type seededLanes struct {
	layout *seedLayout
	grant  lane.Grant
}

// countSeedLayout reads every record of utxoFile and counts the records of each height, refusing a
// height above the file's tip or a file whose footer does not match what it read.
func countSeedLayout(ctx context.Context, utxoFile string) (*seedLayout, error) {
	// A pipe can be read once, and a seed at shadow reads the file to count it, again to check the
	// store, and again to import it.
	if info, statErr := os.Stat(utxoFile); statErr == nil && info.Mode()&os.ModeNamedPipe != 0 {
		return nil, errors.NewConfigurationError("%s is a named pipe, but a seed at utxostore_laneMigration shadow reads the snapshot more than once: unpack it to disk", utxoFile)
	}

	f, reader, _, tipHeight, err := openUTXOSetFile(utxoFile)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = f.Close()
	}()

	l := &seedLayout{counts: make([]uint32, uint64(tipHeight)+1)}

	var (
		scratch []byte
		utxos   uint64
		footer  []byte
	)

	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}

		frame, _, readErr := utxopersister.ReadUTXOWrapperFrame(reader, scratch)
		if readErr != nil {
			var boundary *utxopersister.ErrRecordBoundary
			if errors.As(readErr, &boundary) {
				footer = boundary.FooterBytes[:]

				break
			}

			if errors.Is(readErr, io.EOF) {
				break
			}

			return nil, errors.NewProcessingError("counting the records of %s", utxoFile, readErr)
		}

		scratch = frame
		utxos += uint64(utxopersister.FrameUTXOCount(frame))

		height, coinbase := frameHeight(frame)
		if height > tipHeight {
			return nil, errors.NewProcessingError("record %d of %s is at height %d, above the file's tip %d", l.total()+1, utxoFile, height, tipHeight)
		}

		if coinbase {
			l.coinbase++

			continue
		}

		l.counts[height]++
		l.nonCoinbase++
	}

	if err = checkCountedFooter(f, footer, l.total(), utxos); err != nil {
		return nil, errors.NewProcessingError("counting the records of %s", utxoFile, err)
	}

	l.base = make([]uint64, len(l.counts))

	var next uint64

	for h, n := range l.counts {
		l.base[h] = next
		next += uint64(n)
	}

	return l, nil
}

// checkCountedFooter compares the counts read with the file's footer: the bytes the records ended
// at, or, when they ended at a clean EOF, the footer read by seeking.
func checkCountedFooter(f *os.File, footer []byte, txs, utxos uint64) error {
	var (
		wantTxs, wantUTXOs uint64
		err                error
	)

	if footer != nil {
		wantTxs, wantUTXOs, err = utxopersister.DecodeFooter(footer)
	} else {
		wantTxs, wantUTXOs, err = utxopersister.GetFooter(f)
	}

	if err != nil {
		return errors.NewProcessingError("reading the footer", err)
	}

	if wantTxs != txs || wantUTXOs != utxos {
		return errors.NewProcessingError("the footer says %d transactions and %d UTXOs, the records hold %d and %d", wantTxs, wantUTXOs, txs, utxos)
	}

	return nil
}

// frameHeight reads a record's height and coinbase flag from its raw frame.
func frameHeight(frame []byte) (uint32, bool) {
	encoded := binary.LittleEndian.Uint32(frame[32:36])

	return encoded >> 1, encoded&1 == 1
}

// seedPositioner hands out one pass's positions: each pass reads every record of the file in the
// same order, so each pass's own positioner gives a record the same position.
type seedPositioner struct {
	layout       *seedLayout
	grant        lane.Grant
	seen         []uint32
	coinbaseSeen uint64
}

func (s *seededLanes) positioner() *seedPositioner {
	return &seedPositioner{layout: s.layout, grant: s.grant, seen: make([]uint32, len(s.layout.counts))}
}

// next returns the packed lane position of the record in frame, the next one read. A record the
// count did not see, as a file changed since, is refused rather than given a position outside its
// height's stretch.
func (p *seedPositioner) next(frame []byte) (uint64, error) {
	height, coinbase := frameHeight(frame)

	var offset uint64

	switch {
	case coinbase:
		if p.coinbaseSeen >= p.layout.coinbase {
			return lane.None, errors.NewProcessingError("the file holds more coinbases than the %d counted", p.layout.coinbase)
		}

		offset = p.layout.nonCoinbase + p.coinbaseSeen
		p.coinbaseSeen++
	case uint64(height) >= uint64(len(p.seen)) || p.seen[height] >= p.layout.counts[height]:
		return lane.None, errors.NewProcessingError("the file holds more records at height %d than were counted", height)
	default:
		offset = p.layout.base[height] + uint64(p.seen[height])
		p.seen[height]++
	}

	return lane.Pack(p.grant.Lane, p.grant.First+offset), nil
}

// prepareSeedLanes readies a seed at shadow: it checks the UTXO store can hold every lane column,
// counts the file's records by height, takes the grant, and refuses a store already holding this
// file's records at other positions.
func prepareSeedLanes(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, bcStore blockchain.Store,
	utxoStore utxo.Store, utxoFile string, tip chainhash.Hash) (*seededLanes, error) {
	if err := checkLaneSchema(ctx, utxoStore); err != nil {
		return nil, err
	}

	logger.Infof("[seeder lanes] counting the records of %s by height", utxoFile)

	layout, err := countSeedLayout(ctx, utxoFile)
	if err != nil {
		return nil, err
	}

	if layout.total() == 0 {
		return nil, errors.NewProcessingError("%s holds no record to give a lane position", utxoFile)
	}

	grant, err := takeSeederGrant(ctx, logger, bcStore, tSettings, tip, layout.total())
	if err != nil {
		return nil, err
	}

	lanes := &seededLanes{layout: layout, grant: grant}

	if err = checkExistingPositions(ctx, utxoStore, utxoFile, lanes, false); err != nil {
		return nil, err
	}

	// The import may change the records the rows describe, so the rows count as unwritten until
	// this run writes them.
	if err = setSeededRowsWritten(ctx, bcStore, tip, false); err != nil {
		return nil, err
	}

	return lanes, nil
}

// checkRecordedPositions opens the UTXO store and checks it holds the file's first records at the
// positions lanes gives them, for a run that relies on an earlier import instead of importing.
func checkRecordedPositions(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, utxoFile string, lanes *seededLanes) error {
	store, err := utxofactory.NewStore(ctx, logger, tSettings, "seeder", false)
	if err != nil {
		return errors.NewStorageError("failed to create utxostore", err)
	}

	defer func() {
		if closeErr := store.Close(ctx); closeErr != nil {
			logger.Warnf("[seeder lanes] closing the UTXO store opened to check positions: %v", closeErr)
		}
	}()

	return checkExistingPositions(ctx, store, utxoFile, lanes, true)
}

// checkSeededAtShadow refuses to call a completed seed done at shadow unless it was stamped. The
// blockchain store must record the grant of the seed of utxoFile's tip, and the seed is done when
// it also records every row written. Otherwise, as after a forced re-run that stopped before its
// rows over an earlier checkpoint, the UTXO store must hold the file's first records at the grant's
// positions: a grant alone proves nothing, because a seed refused after taking one leaves it
// recorded. It then returns the tip with the lanes, and the caller writes the rows and then this
// tip's checkpoint. A done seed returns nil.
func checkSeededAtShadow(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store blockchain.Store, utxoFile string) (*utxoSetTip, error) {
	tip, err := readUTXOSetTip(utxoFile)
	if err != nil {
		return nil, err
	}

	if _, ok, err := readSeederGrant(ctx, store, tip.hash); err != nil || !ok {
		if err == nil {
			err = errors.NewProcessingError("utxostore_laneMigration is shadow but the blockchain store records no lane grant for the seed of %s: either lastProcessed.dat is from another snapshot's seed, or this snapshot was imported at off and its records have no lane position. Only in the second case, wipe the UTXO store, and the blockchain database too if any process has run at shadow on it since, then re-run with -force", tip.hash.String())
		}

		return nil, err
	}

	// Once every row is written the seed is done. Its records may have been spent and pruned since,
	// so they are not looked up again.
	written, err := seededRowsWritten(ctx, store, tip.hash)
	if err != nil || written {
		return nil, err
	}

	if tip.lanes, err = recordedSeedLanes(ctx, store, utxoFile, tip.hash); err != nil {
		return nil, err
	}

	if err = checkRecordedPositions(ctx, logger, tSettings, utxoFile, tip.lanes); err != nil {
		return nil, err
	}

	logger.Warnf("[seeder lanes] the seed of %s is imported but its rows are not all written; writing them and its checkpoint", tip.hash.String())

	return tip, nil
}

// seederHolder names the seeder's lane lease.
func seederHolder() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown-host"
	}

	return "seeder@" + host
}

// takeSeederGrant returns the grant a seed of the snapshot at tip takes its positions from: the one
// an earlier run of this seed recorded, or a new one. A new grant is one lease's first grant, sized
// to every record of the file, so the seed needs no renewals; the lease is released at once, which
// leaves the grant the seeder's, since every later grant in the lane starts above it. It is
// recorded at height 0, because every record it numbers is mined below the snapshot's tip, so block
// assembly's restart, which reads grants at or above its target height less
// utxostore_unminedTxRetention, skips it once the tip is more than that retention above genesis.
func takeSeederGrant(ctx context.Context, logger ulogger.Logger, store blockchain.Store, tSettings *settings.Settings,
	tip chainhash.Hash, records uint64) (lane.Grant, error) {
	recorded, ok, err := readSeederGrant(ctx, store, tip)
	if err != nil {
		return lane.Grant{}, err
	}

	if ok && recorded.records == records {
		logger.Infof("[seeder lanes] reusing the grant of an earlier run of this seed: lane %d positions %d to %d", recorded.grant.Lane, recorded.grant.First, recorded.grant.Last)

		return recorded.grant, nil
	}

	if ok {
		return lane.Grant{}, errors.NewProcessingError("the blockchain store records a grant for %d records of the seed of %s, and this file of that tip holds %d: seed it into a blockchain database and UTXO store that are both empty", recorded.records, tip.String(), records)
	}

	// Rows are insert-only, so a block that already has rows, from another seed or from live
	// blocks, could never take this seed's. A database that ever handed out a position may hold
	// such rows, so a new grant needs one that never did.
	var earlier []lane.CapGrant

	if err = withDBTimeout(ctx, func(ctx context.Context) error {
		var readErr error

		earlier, readErr = store.LaneCapGrants(ctx, 0)

		return readErr
	}); err != nil {
		return lane.Grant{}, errors.NewStorageError("could not read the lane grants already handed out", err)
	}

	// A pending record says an earlier run of this seed found no grant and then opened a lease. The
	// one grant that lease took, recorded at height 0 like every seed's, holds no row. More grants, or
	// one at a later height, came from something else, such as a node run since.
	pending, err := readState(ctx, store, seederStateKey(seederPendingKeyPrefix, tip))
	if err != nil {
		return lane.Grant{}, err
	}

	abandoned := bytes.Equal(pending, tip[:]) && len(earlier) == 1 && earlier[0].Height == 0

	if len(earlier) > 0 && !abandoned {
		return lane.Grant{}, errors.NewProcessingError("the blockchain store has handed out lane positions before (lane %d from position %d), so its blocks may already hold lane rows this seed cannot replace: seed at shadow into a blockchain database and UTXO store that are both empty", earlier[0].Lane, earlier[0].First)
	}

	if err = withDBTimeout(ctx, func(ctx context.Context) error {
		return store.SetState(ctx, seederStateKey(seederPendingKeyPrefix, tip), tip[:])
	}); err != nil {
		return lane.Grant{}, errors.NewStorageError("could not record the seeder's pending lane grant", err)
	}

	var (
		id lane.LeaseID
		g  lane.Grant
	)

	if err = withDBTimeout(ctx, func(ctx context.Context) error {
		var openErr error

		id, g, _, openErr = store.OpenLaneLease(ctx, blockchainoptions.LaneLeaseOpen{
			Holder:     seederHolder(),
			Height:     0,
			GrantMin:   records,
			CloseAfter: tSettings.BlockChain.LaneLeaseCloseAfter,
		})

		return openErr
	}); err != nil {
		return lane.Grant{}, errors.NewServiceError("could not open the seeder's lane lease", err)
	}

	release := func(lastTaken uint64) {
		if relErr := withDBTimeout(ctx, func(ctx context.Context) error {
			_, err := store.ReleaseLaneLease(ctx, blockchainoptions.LaneLeaseRelease{Lease: id, Lane: g.Lane, LastTaken: lastTaken})
			return err
		}); relErr != nil {
			logger.Warnf("[seeder lanes] could not release the seeder's lane lease on lane %d, it closes as lapsed: %v", g.Lane, relErr)
		}
	}

	if g.Len() < records {
		release(0)

		return lane.Grant{}, errors.NewProcessingError("lane %d granted %d positions, the seed needs %d", g.Lane, g.Len(), records)
	}

	g.Last = g.First + records - 1

	if err = withDBTimeout(ctx, func(ctx context.Context) error {
		return store.SetState(ctx, seederStateKey(seederGrantKeyPrefix, tip), encodeSeederGrant(tip, records, g))
	}); err != nil {
		release(0)

		return lane.Grant{}, errors.NewStorageError("could not record the seeder's lane grant", err)
	}

	release(g.Last)

	// The grant is recorded, so the pending record has done its job; left set, it would let a later
	// seed of this tip past the check for earlier grants.
	if err = withDBTimeout(ctx, func(ctx context.Context) error {
		return store.SetState(ctx, seederStateKey(seederPendingKeyPrefix, tip), []byte{})
	}); err != nil {
		return lane.Grant{}, errors.NewStorageError("could not clear the seeder's pending lane grant", err)
	}

	logger.Infof("[seeder lanes] took lane %d positions %d to %d for %d records", g.Lane, g.First, g.Last, records)

	return g, nil
}

// seederGrant is the grant record an earlier run left.
type seederGrant struct {
	tip     chainhash.Hash
	records uint64
	grant   lane.Grant
}

func encodeSeederGrant(tip chainhash.Hash, records uint64, g lane.Grant) []byte {
	b := make([]byte, seederGrantRecordBytes)
	b[0] = 1
	copy(b[1:33], tip[:])
	binary.LittleEndian.PutUint64(b[33:41], records)
	b[41] = g.Lane
	binary.LittleEndian.PutUint64(b[42:50], g.First)
	binary.LittleEndian.PutUint64(b[50:58], g.Last)

	return b
}

// readState reads a blockchain store state key, nil when it holds nothing.
func readState(ctx context.Context, store blockchain.Store, key string) ([]byte, error) {
	var b []byte

	err := withDBTimeout(ctx, func(ctx context.Context) error {
		var getErr error

		b, getErr = store.GetState(ctx, key)

		return getErr
	})
	if err != nil {
		if isNoState(err) {
			return nil, nil
		}

		return nil, errors.NewStorageError("could not read the seeder's state %s", key, err)
	}

	return b, nil
}

// readSeederGrant returns the grant record an earlier run of the seed of tip left, if any.
func readSeederGrant(ctx context.Context, store blockchain.Store, tip chainhash.Hash) (seederGrant, bool, error) {
	b, err := readState(ctx, store, seederStateKey(seederGrantKeyPrefix, tip))
	if err != nil || len(b) == 0 {
		return seederGrant{}, false, err
	}

	if len(b) != seederGrantRecordBytes || b[0] != 1 {
		return seederGrant{}, false, errors.NewProcessingError("the seeder's lane grant record is %d bytes of version %d, not %d bytes of version 1", len(b), b[0], seederGrantRecordBytes)
	}

	var r seederGrant

	copy(r.tip[:], b[1:33])
	r.records = binary.LittleEndian.Uint64(b[33:41])
	r.grant = lane.Grant{Lane: b[41], First: binary.LittleEndian.Uint64(b[42:50]), Last: binary.LittleEndian.Uint64(b[50:58])}

	if !lane.Valid(lane.Pack(r.grant.Lane, r.grant.First)) || r.grant.Len() != r.records {
		return seederGrant{}, false, errors.NewProcessingError("the seeder's lane grant record holds lane %d positions %d to %d for %d records", r.grant.Lane, r.grant.First, r.grant.Last, r.records)
	}

	// Two tips that share the key's hash prefix are two different seeds.
	if r.tip != tip {
		return seederGrant{}, false, nil
	}

	return r, true, nil
}

// seededRowsWritten reports whether every seeded row of the seed of tip was written.
func seededRowsWritten(ctx context.Context, store blockchain.Store, tip chainhash.Hash) (bool, error) {
	b, err := readState(ctx, store, seederStateKey(seederRowsKeyPrefix, tip))
	if err != nil {
		return false, err
	}

	return bytes.Equal(b, tip[:]), nil
}

// setSeededRowsWritten records that the seeded rows of the seed of tip are all written, or, with
// done false, that an import has started and they may not be.
func setSeededRowsWritten(ctx context.Context, store blockchain.Store, tip chainhash.Hash, done bool) error {
	data := []byte{}
	if done {
		data = tip[:]
	}

	return withDBTimeout(ctx, func(ctx context.Context) error {
		if err := store.SetState(ctx, seederStateKey(seederRowsKeyPrefix, tip), data); err != nil {
			return errors.NewStorageError("could not record the seeded rows' state", err)
		}

		return nil
	})
}

// recordedSeedLanes rebuilds what a completed seed of the snapshot at tip used, for a run that does
// not import: it needs the grant an earlier run recorded and counts the file again.
func recordedSeedLanes(ctx context.Context, store blockchain.Store, utxoFile string, tip chainhash.Hash) (*seededLanes, error) {
	recorded, ok, err := readSeederGrant(ctx, store, tip)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, errors.NewProcessingError("utxostore_laneMigration is shadow but the blockchain store records no lane grant for the seed of %s, so that seed was imported at off and its records have no lane position: wipe the UTXO store, and the blockchain database too if any process has run at shadow on it since, then re-run with -force", tip.String())
	}

	layout, err := countSeedLayout(ctx, utxoFile)
	if err != nil {
		return nil, err
	}

	if layout.total() != recorded.records {
		return nil, errors.NewProcessingError("%s holds %d records, the recorded grant was taken for %d", utxoFile, layout.total(), recorded.records)
	}

	return &seededLanes{layout: layout, grant: recorded.grant}, nil
}

// probedRecords is how many of the file's first records checkExistingPositions looks up.
const probedRecords = 16

// checkExistingPositions refuses a seed over a UTXO store that already holds records of this file
// with other positions: an import made at off, or from a grant the blockchain store no longer
// records. A create of an existing record is skipped, so those records would keep their positions
// while the rows name the new ones. With requirePresent, a record missing from the store is refused
// too, for a run that relies on a completed import. It looks up the file's first records only.
func checkExistingPositions(ctx context.Context, store utxo.Store, utxoFile string, lanes *seededLanes, requirePresent bool) error {
	f, reader, _, _, err := openUTXOSetFile(utxoFile)
	if err != nil {
		return err
	}

	defer func() {
		_ = f.Close()
	}()

	p := lanes.positioner()

	for i := 0; i < probedRecords; i++ {
		frame, _, readErr := utxopersister.ReadUTXOWrapperFrame(reader, nil)
		if readErr != nil {
			// A file of fewer records ends at its footer, or at a clean end.
			var boundary *utxopersister.ErrRecordBoundary
			if errors.As(readErr, &boundary) || errors.Is(readErr, io.EOF) {
				return nil
			}

			return errors.NewProcessingError("reading %s", utxoFile, readErr)
		}

		want, posErr := p.next(frame)
		if posErr != nil {
			return posErr
		}

		var txid chainhash.Hash

		copy(txid[:], frame[:32])

		md, getErr := store.Get(ctx, &txid, fields.LanePosition)
		if getErr != nil {
			if errors.Is(getErr, errors.ErrTxNotFound) {
				if requirePresent {
					return errors.NewProcessingError("the UTXO store does not hold %s, which a completed import of this seed wrote: it was wiped or is another store. Re-run with -force", txid.String())
				}

				continue
			}

			return errors.NewStorageError("could not read the lane position of %s", txid.String(), getErr)
		}

		if md.LanePosition != want {
			wantLane, wantPos := lane.Unpack(want)
			haveLane, havePos := lane.Unpack(md.LanePosition)

			return errors.NewProcessingError("the UTXO store already holds %s at lane %d position %d, and this seed gives it lane %d position %d: the store holds an import made at off or from another grant, and a record keeps the position it has. Wipe the UTXO store, and the blockchain database too if any process has run at shadow on it since, then re-run with -force", txid.String(), haveLane, havePos, wantLane, wantPos)
		}
	}

	return nil
}

// checkExistingPosition refuses a record the store already holds at another position than the
// seed gives it, as an import made at off or from another grant leaves: the seeded rows would name
// a position the record never received. At off, with position lane.None, it reads nothing.
func checkExistingPosition(ctx context.Context, store utxo.Store, txid *chainhash.Hash, position uint64) error {
	if position == lane.None {
		return nil
	}

	md, err := store.Get(ctx, txid, fields.LanePosition)
	if err != nil {
		return errors.NewStorageError("could not read the lane position of %s, which the UTXO store already holds", txid.String(), err)
	}

	if md.LanePosition != position {
		haveLane, havePos := lane.Unpack(md.LanePosition)
		wantLane, wantPos := lane.Unpack(position)

		return errors.NewProcessingError("the UTXO store already holds %s at lane %d position %d, and this seed gives it lane %d position %d: the store holds an import made at off or from another grant. Wipe the UTXO store, and the blockchain database too if any process has run at shadow on it since, then re-run with -force", txid.String(), haveLane, havePos, wantLane, wantPos)
	}

	return nil
}

// checkLaneSchema refuses a seed at shadow into a store whose lane columns are not all installed,
// such as utxoset before teranode-cli prepare-lane-schema: its outputs and spend-journal rows would
// carry no position.
func checkLaneSchema(ctx context.Context, store utxo.Store) error {
	checker, ok := utxo.As[utxo.LaneSchemaChecker](store)
	if !ok {
		return nil
	}

	installed, err := checker.LaneSchemaInstalled(ctx)
	if err != nil {
		return errors.NewStorageError("could not check the UTXO store's lane columns", err)
	}

	if !installed {
		return errors.NewConfigurationError("utxostore_laneMigration is shadow but the UTXO store's lane columns are not all installed; run teranode-cli prepare-lane-schema first")
	}

	return nil
}

// seededRowsConcurrency bounds the row inserts in flight.
const seededRowsConcurrency = 16

// seededHeadersBatch is how many heights of headers one read fetches.
const seededHeadersBatch = 10_000

// recentSeededBlocks is how far below the tip writeSeededRows reports blocks with no row: the lane
// index loads that many blocks when a process starts (util/lanereader).
const recentSeededBlocks = 288

// writeSeededRows writes one RowTagSeededRange row for each block of the snapshot's main chain that
// holds a record other than a coinbase. Rows are insert-only and a retry with the same rows writes
// nothing, so it is safe to run again.
func writeSeededRows(ctx context.Context, logger ulogger.Logger, store blockchain.Store, lanes *seededLanes, tip *utxoSetTip) error {
	var (
		tipHeaders []*model.BlockHeader
		tipMetas   []*model.BlockHeaderMeta
	)

	err := withDBTimeout(ctx, func(ctx context.Context) error {
		var readErr error

		tipHeaders, tipMetas, readErr = store.GetBlockHeadersByHeight(ctx, tip.height, tip.height)

		return readErr
	})
	if err != nil {
		return errors.NewStorageError("could not read the main chain at the snapshot's tip height %d", tip.height, err)
	}

	if len(tipHeaders) != 1 || *tipHeaders[0].Hash() != tip.hash || tipMetas[0].Height != tip.height {
		return errors.NewProcessingError("the blockchain store's main chain does not hold the snapshot's tip %s at height %d, so the seeded rows cannot name its blocks", tip.hash.String(), tip.height)
	}

	var (
		rows, emptyRecent int
		heights           = uint32(len(lanes.layout.counts)) //nolint:gosec // one per height up to the tip
	)

	for lo := uint32(0); lo < heights; lo += seededHeadersBatch {
		hi := min(lo+seededHeadersBatch-1, heights-1)

		var metas []*model.BlockHeaderMeta

		if err = withDBTimeout(ctx, func(ctx context.Context) error {
			var readErr error

			_, metas, readErr = store.GetBlockHeadersByHeight(ctx, lo, hi)

			return readErr
		}); err != nil {
			return errors.NewStorageError("could not read the main chain at heights %d to %d", lo, hi, err)
		}

		byHeight := make(map[uint32]*blockchainMeta, len(metas))
		for _, m := range metas {
			byHeight[m.Height] = &blockchainMeta{id: m.ID, txCount: m.TxCount}
		}

		// Every block of the batch first, so no insert starts for a batch that cannot finish.
		for h := lo; h <= hi; h++ {
			if _, ok := byHeight[h]; !ok {
				return errors.NewProcessingError("the blockchain store's main chain has no block at height %d", h)
			}
		}

		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(seededRowsConcurrency)

		for h := lo; h <= hi; h++ {
			m := byHeight[h]
			n := lanes.layout.counts[h]
			if n == 0 {
				if m.txCount > 1 && h+recentSeededBlocks > tip.height {
					emptyRecent++
				}

				continue
			}

			row := lane.MapRow{
				Lane:  lanes.grant.Lane,
				First: lanes.grant.First + lanes.layout.base[h],
				Last:  lanes.grant.First + lanes.layout.base[h] + uint64(n) - 1,
				Tag:   lane.RowTagSeededRange,
			}
			id := m.id
			rows++

			g.Go(func() error {
				if err := withDBTimeout(gCtx, func(ctx context.Context) error {
					return store.InsertLaneMapRows(ctx, id, []lane.MapRow{row})
				}); err != nil {
					return errors.NewStorageError("could not write the seeded lane row of block %d", id, err)
				}

				return nil
			})
		}

		if err = g.Wait(); err != nil {
			return err
		}
	}

	if err = setSeededRowsWritten(ctx, store, tip.hash, true); err != nil {
		return err
	}

	logger.Infof("[seeder lanes] wrote %d seeded lane rows in lane %d", rows, lanes.grant.Lane)

	if emptyRecent > 0 {
		logger.Warnf("[seeder lanes] %d of the %d blocks below the tip hold transactions besides their coinbase but no unspent record, so they have no row; while the lane index holds them it answers no_rows for positions no row takes", emptyRecent, recentSeededBlocks)
	}

	return nil
}

// blockchainMeta is what writeSeededRows needs of a block.
type blockchainMeta struct {
	id      uint32
	txCount uint64
}
