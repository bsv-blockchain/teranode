package seeder

import (
	"context"
	"encoding/binary"
	"io"
	"os"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
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

// seederLaneGrantKey is the blockchain store state key holding the grant a seed takes its positions
// from, so a seed run again reuses it.
const seederLaneGrantKey = "seederLaneGrant"

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

	return lanes, nil
}

// checkRecordedPositions opens the UTXO store and checks it holds the file's first records at the
// positions lanes gives them, for a run that relies on an earlier import instead of importing.
func checkRecordedPositions(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, utxoFile string, lanes *seededLanes) error {
	store, err := utxofactory.NewStore(ctx, logger, tSettings, "seeder", false)
	if err != nil {
		return errors.NewStorageError("failed to create utxostore", err)
	}

	return checkExistingPositions(ctx, store, utxoFile, lanes, true)
}

// checkSeededAtShadow refuses to call a completed seed done at shadow unless it was stamped: the
// blockchain store records the grant of the seed of utxoFile's tip, and the UTXO store holds the
// file's first records at that grant's positions. A grant alone proves nothing, because a seed
// refused after taking one leaves it recorded.
func checkSeededAtShadow(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store blockchain.Store, utxoFile string) error {
	tip, err := readUTXOSetTip(utxoFile)
	if err != nil {
		return err
	}

	lanes, err := recordedSeedLanes(ctx, store, utxoFile, tip.hash)
	if err != nil {
		return err
	}

	return checkRecordedPositions(ctx, logger, tSettings, utxoFile, lanes)
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
// recorded at height 0, below any height block assembly's restart reads grants from, because every
// record it numbers is mined below the snapshot's tip.
func takeSeederGrant(ctx context.Context, logger ulogger.Logger, store blockchain.Store, tSettings *settings.Settings,
	tip chainhash.Hash, records uint64) (lane.Grant, error) {
	recorded, ok, err := readSeederGrant(ctx, store)
	if err != nil {
		return lane.Grant{}, err
	}

	if ok && recorded.tip == tip && recorded.records == records {
		logger.Infof("[seeder lanes] reusing the grant of an earlier run of this seed: lane %d positions %d to %d", recorded.grant.Lane, recorded.grant.First, recorded.grant.Last)

		return recorded.grant, nil
	}

	if ok {
		logger.Warnf("[seeder lanes] the blockchain store records a grant for the seed of %s with %d records; this seed of %s takes a new one", recorded.tip.String(), recorded.records, tip.String())
	}

	id, g, _, err := store.OpenLaneLease(ctx, blockchainoptions.LaneLeaseOpen{
		Holder:     seederHolder(),
		Height:     0,
		GrantMin:   records,
		CloseAfter: tSettings.BlockChain.LaneLeaseCloseAfter,
	})
	if err != nil {
		return lane.Grant{}, errors.NewServiceError("could not open the seeder's lane lease", err)
	}

	release := func(lastTaken uint64) {
		if _, relErr := store.ReleaseLaneLease(ctx, blockchainoptions.LaneLeaseRelease{Lease: id, Lane: g.Lane, LastTaken: lastTaken}); relErr != nil {
			logger.Warnf("[seeder lanes] could not release the seeder's lane lease on lane %d, it closes as lapsed: %v", g.Lane, relErr)
		}
	}

	if g.Len() < records {
		release(0)

		return lane.Grant{}, errors.NewProcessingError("lane %d granted %d positions, the seed needs %d", g.Lane, g.Len(), records)
	}

	g.Last = g.First + records - 1

	if err = store.SetState(ctx, seederLaneGrantKey, encodeSeederGrant(tip, records, g)); err != nil {
		release(0)

		return lane.Grant{}, errors.NewStorageError("could not record the seeder's lane grant", err)
	}

	release(g.Last)

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

// readSeederGrant returns the grant record an earlier run left, if any.
func readSeederGrant(ctx context.Context, store blockchain.Store) (seederGrant, bool, error) {
	b, err := store.GetState(ctx, seederLaneGrantKey)
	if err != nil {
		if isNoState(err) {
			return seederGrant{}, false, nil
		}

		return seederGrant{}, false, errors.NewStorageError("could not read the seeder's lane grant", err)
	}

	if len(b) == 0 {
		return seederGrant{}, false, nil
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

	return r, true, nil
}

// recordedSeedLanes rebuilds what a completed seed of the snapshot at tip used, for a run that does
// not import: it needs the grant an earlier run recorded and counts the file again.
func recordedSeedLanes(ctx context.Context, store blockchain.Store, utxoFile string, tip chainhash.Hash) (*seededLanes, error) {
	recorded, ok, err := readSeederGrant(ctx, store)
	if err != nil {
		return nil, err
	}

	if !ok || recorded.tip != tip {
		return nil, errors.NewProcessingError("utxostore_laneMigration is shadow but the blockchain store records no lane grant for the seed of %s, so that seed was imported at off and its records have no lane position: wipe the UTXO store and re-run with -force", tip.String())
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

			return errors.NewProcessingError("the UTXO store already holds %s at lane %d position %d, and this seed gives it lane %d position %d: the store holds an import made at off or from another grant, and a record keeps the position it has. Wipe the UTXO store and re-run with -force", txid.String(), haveLane, havePos, wantLane, wantPos)
		}
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
	tipHeaders, tipMetas, err := store.GetBlockHeadersByHeight(ctx, tip.height, tip.height)
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

		_, metas, err := store.GetBlockHeadersByHeight(ctx, lo, hi)
		if err != nil {
			return errors.NewStorageError("could not read the main chain at heights %d to %d", lo, hi, err)
		}

		byHeight := make(map[uint32]*blockchainMeta, len(metas))
		for _, m := range metas {
			byHeight[m.Height] = &blockchainMeta{id: m.ID, txCount: m.TxCount}
		}

		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(seededRowsConcurrency)

		for h := lo; h <= hi; h++ {
			m, ok := byHeight[h]
			if !ok {
				return errors.NewProcessingError("the blockchain store's main chain has no block at height %d", h)
			}

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
				if err := store.InsertLaneMapRows(gCtx, id, []lane.MapRow{row}); err != nil {
					return errors.NewStorageError("could not write the seeded lane row of block %d", id, err)
				}

				return nil
			})
		}

		if err = g.Wait(); err != nil {
			return err
		}
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
