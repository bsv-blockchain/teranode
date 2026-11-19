// Package backfill holds the maintenance formats and calls of the historical lane conversion: the
// deterministic order in which a frozen source's transactions are numbered, the append-only
// assignment manifest that records that numbering so a crash resumes it rather than recounting,
// and the import that turns an explicit frozen assignment into a block's map rows and proof file.
//
// It reads nothing live. It never decodes txmeta, takes a lease, allocates a position or reads a
// bitmap a running node wrote. Every position it produces comes from the assignment it is given.
// The manifest is maintenance data on disk beside the stores, not an index any hot path reads.
package backfill

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// The assignment manifest of one conversion segment is one append-only file:
//
//	[4] magic "LNAM", [1] version 1
//	records, each [1] type, [8] payload length uint64 LE, [4] CRC-32C of type and length,
//	payload, [4] CRC-32C of type, length and payload
//
// Records, in order:
//
//	'H' header, once: [4] segment, [8] first position, [4] source tip block ID, [32] tip hash
//	'B' block, per main-chain block in order: [4] seq, [4] block ID, [4] height, [8] first,
//	    [8] count, then count txids of 32 bytes in assignment order
//	'S' suffix, at most once: [8] first, [8] count, then count txids
//	'E' end, once, last: [8] prefix last, [8] last
//
// The transaction at index i of a block or suffix record holds position first+i of the backfill
// lane, and a block or suffix payload is exactly its fixed part plus count txids. Every record is
// written and synced before the next, so a crash can only leave a partial last record: a frame
// head shorter than its 13 bytes, a head whose own CRC is good with a payload that runs past the
// end of the file or, ending exactly there, fails its CRC, or zero bytes from the record's start to
// the end of the file, which a filesystem that grows a file before its data is durable can leave.
// ResumeManifest cuts such a record; OpenManifest refuses it; both refuse any other damage, a
// corrupt length field included, since its own CRC then fails. A tail of stale non-zero bytes is
// refused as damage too, so a manifest needs a filesystem that makes data durable before the size
// that covers it, as ext4 with data=ordered does; on another, a crash can leave a manifest the
// operator must cut by hand. The manifest digest is SHA-256 over the whole file once the end record
// is written.

const (
	manifestMagic   = "LNAM"
	manifestVersion = byte(1)

	recordHeader = 'H'
	recordBlock  = 'B'
	recordSuffix = 'S'
	recordEnd    = 'E'

	frameHead = 1 + 8 + 4
	frameTail = 4

	headerPayload = 4 + 8 + 4 + chainhash.HashSize
	blockFixed    = 4 + 4 + 4 + 8 + 8
	suffixFixed   = 8 + 8
	endPayload    = 8 + 8

	readRecordAt = "read manifest record at %d"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Header names a segment and its frozen source.
type Header struct {
	Segment          uint32
	First            uint64
	SourceTipBlockID uint32
	SourceTipHash    chainhash.Hash
}

// recordRef locates one record's txids in the file.
type recordRef struct {
	offset int64 // of the first txid
	count  uint64
}

// manifestFile is what a Manifest uses of its file. *os.File is the only one outside tests; a test
// substitutes one whose writes or syncs fail.
type manifestFile interface {
	io.ReaderAt
	io.WriterAt
	Sync() error
	Close() error
	Stat() (os.FileInfo, error)
	Truncate(size int64) error
}

// wrapManifestFile is the file a new manifest writes through: the temporary file itself, unless a
// test substitutes a faulty one.
var wrapManifestFile = func(f *os.File) manifestFile { return f }

// Manifest is an open assignment manifest: what it holds so far, and the writer that extends it.
type Manifest struct {
	path   string
	f      manifestFile
	digest hash.Hash

	header   Header
	blocks   []lane.ConversionBlock
	blockRef []recordRef
	suffix   *recordRef
	suffixAt uint64
	end      bool
	last     uint64
	next     uint64 // the next position to assign
	size     int64
	readOnly bool
	// failed is set when a write or sync of a record failed: what is on disk past the last good
	// record is unknown, so nothing more is appended until ResumeManifest reloads the file.
	failed bool
}

// CreateManifest starts a new manifest for a segment. The file must not exist. The magic and the
// header record are written to a temporary file this call creates exclusively, under a name of its
// own beside path, synced, and linked to path, so a crash during creation leaves either no
// manifest or a whole header, never one ResumeManifest refuses and CreateManifest cannot replace.
// It never opens, truncates or removes a file it did not create: a temporary file an earlier crash
// left stays as it is, and may be a second name of a manifest in use.
func CreateManifest(path string, h Header) (*Manifest, error) {
	if h.Segment == 0 || h.First < lane.BackfillFirstPosition || h.First > lane.MaxPosition {
		return nil, errors.NewProcessingError("manifest header: segment %d first %d is not a valid start", h.Segment, h.First)
	}

	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return nil, errors.NewStorageError("create a temporary file for manifest %s", path, err)
	}

	tmp := f.Name()

	m := &Manifest{path: path, f: wrapManifestFile(f), digest: sha256.New(), header: h, next: h.First}

	fail := func(err error) (*Manifest, error) {
		_ = f.Close()
		_ = os.Remove(tmp)

		return nil, err
	}

	if err = m.write(append([]byte(manifestMagic), manifestVersion)); err != nil {
		return fail(err)
	}

	payload := binary.LittleEndian.AppendUint32(nil, h.Segment)
	payload = binary.LittleEndian.AppendUint64(payload, h.First)
	payload = binary.LittleEndian.AppendUint32(payload, h.SourceTipBlockID)
	payload = append(payload, h.SourceTipHash[:]...)

	if err = m.appendRecord(recordHeader, payload); err != nil {
		return fail(err)
	}

	// A link, unlike a rename, refuses to replace a manifest another run created meanwhile.
	if err = os.Link(tmp, path); err != nil {
		return fail(errors.NewStorageError("create manifest %s", path, err))
	}

	// The manifest is in place; from here a failure must not remove it through fail.
	if err = os.Remove(tmp); err != nil {
		_ = f.Close()
		return nil, errors.NewStorageError("remove %s after creating manifest %s", tmp, path, err)
	}

	if err = syncDir(filepath.Dir(path)); err != nil {
		_ = f.Close()
		return nil, err
	}

	return m, nil
}

// syncDir syncs a directory, so a file linked or removed in it survives a crash.
func syncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // the manifest's own directory
	if err != nil {
		return errors.NewStorageError("open directory %s", dir, err)
	}

	defer func() { _ = d.Close() }()

	if err = d.Sync(); err != nil {
		return errors.NewStorageError("sync directory %s", dir, err)
	}

	return nil
}

// OpenManifest opens a manifest to read it. It changes nothing on disk, and refuses a manifest
// with a partial last record, which ResumeManifest cuts, or with any damage.
func OpenManifest(path string) (*Manifest, error) {
	return openManifest(path, false)
}

// ResumeManifest opens a manifest to extend it after a crash: a partial last record, the only
// thing a crash mid-write leaves, is cut off. Any other damage is an error.
func ResumeManifest(path string) (*Manifest, error) {
	return openManifest(path, true)
}

func openManifest(path string, resume bool) (*Manifest, error) {
	flag := os.O_RDONLY
	if resume {
		flag = os.O_RDWR
	}

	f, err := os.OpenFile(path, flag, 0) //nolint:gosec // an operator-supplied maintenance path
	if err != nil {
		return nil, errors.NewStorageError("open manifest %s", path, err)
	}

	m := &Manifest{path: path, f: f, digest: sha256.New(), readOnly: !resume}

	if err = m.load(resume); err != nil {
		_ = f.Close()
		return nil, err
	}

	return m, nil
}

func (m *Manifest) load(resume bool) error {
	info, err := m.f.Stat()
	if err != nil {
		return errors.NewStorageError("stat manifest", err)
	}

	total := info.Size()
	r := bufio.NewReaderSize(io.NewSectionReader(m.f, 0, total), 1<<20)

	magic := make([]byte, len(manifestMagic)+1)
	if _, err = io.ReadFull(r, magic); err != nil || string(magic[:4]) != manifestMagic || magic[4] != manifestVersion {
		return errors.NewProcessingError("%s is not a version %d lane assignment manifest", m.path, manifestVersion)
	}

	m.digest.Write(magic)
	pos := int64(len(magic))
	first := true
	torn := false

	for pos < total {
		typ, payloadLen, ok, err := readFrame(r, total-pos, m.path, pos)
		if err != nil {
			return err
		}

		if !ok {
			torn = true
			break
		}

		body, crcOK, err := m.readBody(r, typ, payloadLen, pos)
		if err != nil {
			return err
		}

		recordLen := frameHead + int64(payloadLen) + frameTail //nolint:gosec // bounded by the file size

		if !crcOK {
			if pos+recordLen == total {
				torn = true
				break
			}

			return errors.NewProcessingError("manifest %s: damaged record at offset %d", m.path, pos)
		}

		if err = m.apply(typ, payloadLen, body, pos, first); err != nil {
			return err
		}

		first = false

		if _, err = io.Copy(m.digest, io.NewSectionReader(m.f, pos, recordLen)); err != nil {
			return errors.NewStorageError("read manifest %s", m.path, err)
		}

		pos += recordLen
	}

	if first {
		return errors.NewProcessingError("manifest %s has no complete header", m.path)
	}

	if torn {
		if !resume {
			return errors.NewProcessingError("manifest %s ends in a partial record at offset %d; resume it to cut the record", m.path, pos)
		}

		if err = m.f.Truncate(pos); err != nil {
			return errors.NewStorageError("cut the partial last record of manifest %s", m.path, err)
		}
	}

	m.size = pos

	return nil
}

// readFrame reads a record's type and length and checks the head's own CRC. ok is false for a
// partial last record: fewer bytes left than a head, a good head whose payload and tail run past
// the end of the file, or a head that fails its CRC with it and every byte after it to the end of
// the file zero. Any other head that fails its CRC is damage.
func readFrame(r *bufio.Reader, remaining int64, path string, pos int64) (byte, uint64, bool, error) {
	if remaining < frameHead {
		return 0, 0, false, nil
	}

	var head [frameHead]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, 0, false, errors.NewStorageError("read manifest record", err)
	}

	if crc32.Checksum(head[:9], crcTable) != binary.LittleEndian.Uint32(head[9:]) {
		zero, err := zeroToEnd(head[:], r)
		if err != nil {
			return 0, 0, false, errors.NewStorageError("read manifest record", err)
		}

		if zero {
			return 0, 0, false, nil // a zero-filled tail: the last record was never written
		}

		return 0, 0, false, errors.NewProcessingError("manifest %s: damaged record head at offset %d", path, pos)
	}

	payloadLen := binary.LittleEndian.Uint64(head[1:9])
	if payloadLen > uint64(remaining-frameHead-frameTail) || remaining < frameHead+frameTail {
		return 0, 0, false, nil
	}

	return head[0], payloadLen, true, nil
}

// zeroToEnd reports whether head and every byte left in r are zero. No committed record is all
// zero: its type byte never is.
func zeroToEnd(head []byte, r io.Reader) (bool, error) {
	for _, b := range head {
		if b != 0 {
			return false, nil
		}
	}

	buf := make([]byte, 64<<10)

	for {
		n, err := r.Read(buf)
		for _, b := range buf[:n] {
			if b != 0 {
				return false, nil
			}
		}

		if err == io.EOF { //nolint:errorlint // bufio returns io.EOF itself, unwrapped
			return true, nil
		}

		if err != nil {
			return false, err
		}
	}
}

// readBody reads a record's payload, keeping only the fixed part of a block or suffix record in
// memory, and checks its CRC.
func (m *Manifest) readBody(r *bufio.Reader, typ byte, payloadLen uint64, pos int64) ([]byte, bool, error) {
	crc := crc32.New(crcTable)

	var head [9]byte
	head[0] = typ
	binary.LittleEndian.PutUint64(head[1:], payloadLen)
	crc.Write(head[:])

	keep := payloadLen

	switch typ {
	case recordBlock:
		keep = min(payloadLen, blockFixed)
	case recordSuffix:
		keep = min(payloadLen, suffixFixed)
	}

	body := make([]byte, keep)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, false, errors.NewStorageError(readRecordAt, pos, err)
	}

	crc.Write(body)

	if _, err := io.CopyN(crc, r, int64(payloadLen-keep)); err != nil { //nolint:gosec // bounded by the file size
		return nil, false, errors.NewStorageError(readRecordAt, pos, err)
	}

	var tail [frameTail]byte
	if _, err := io.ReadFull(r, tail[:]); err != nil {
		return nil, false, errors.NewStorageError(readRecordAt, pos, err)
	}

	return body, crc.Sum32() == binary.LittleEndian.Uint32(tail[:]), nil
}

func (m *Manifest) apply(typ byte, payloadLen uint64, body []byte, pos int64, first bool) error {
	if first != (typ == recordHeader) {
		return errors.NewProcessingError("manifest %s: record %q at %d is out of order", m.path, typ, pos)
	}

	if m.end {
		return errors.NewProcessingError("manifest %s: record %q after the end", m.path, typ)
	}

	switch typ {
	case recordHeader:
		if len(body) != headerPayload {
			return errors.NewProcessingError("manifest %s: header of %d bytes", m.path, len(body))
		}

		m.header = Header{
			Segment:          binary.LittleEndian.Uint32(body),
			First:            binary.LittleEndian.Uint64(body[4:]),
			SourceTipBlockID: binary.LittleEndian.Uint32(body[12:]),
		}
		copy(m.header.SourceTipHash[:], body[16:])
		m.next = m.header.First

		return nil

	case recordBlock:
		if len(body) != blockFixed {
			return errors.NewProcessingError("manifest %s: short block record at %d", m.path, pos)
		}

		b := lane.ConversionBlock{
			Seq:     binary.LittleEndian.Uint32(body),
			BlockID: binary.LittleEndian.Uint32(body[4:]),
			Height:  binary.LittleEndian.Uint32(body[8:]),
			First:   binary.LittleEndian.Uint64(body[12:]),
			Count:   binary.LittleEndian.Uint64(body[20:]),
		}

		if b.Count > (payloadLen-blockFixed)/chainhash.HashSize || payloadLen != blockFixed+b.Count*chainhash.HashSize {
			return errors.NewProcessingError("manifest %s: block %d claims %d txids in a %d-byte record", m.path, b.Seq, b.Count, payloadLen)
		}

		if err := m.checkBlock(b); err != nil {
			return err
		}

		m.addBlock(b, pos+frameHead+blockFixed)

		return nil

	case recordSuffix:
		if len(body) != suffixFixed || m.suffix != nil {
			return errors.NewProcessingError("manifest %s: unexpected suffix record at %d", m.path, pos)
		}

		first, count := binary.LittleEndian.Uint64(body), binary.LittleEndian.Uint64(body[8:])
		if first != m.next {
			return errors.NewProcessingError("manifest %s: suffix starts at %d, want %d", m.path, first, m.next)
		}

		if count > (payloadLen-suffixFixed)/chainhash.HashSize || payloadLen != suffixFixed+count*chainhash.HashSize {
			return errors.NewProcessingError("manifest %s: suffix claims %d txids in a %d-byte record", m.path, count, payloadLen)
		}

		if count > lane.MaxPosition-m.next+1 {
			return errors.NewProcessingError("manifest %s: the suffix overflows the backfill lane", m.path)
		}

		m.suffix = &recordRef{offset: pos + frameHead + suffixFixed, count: count}
		m.suffixAt = first
		m.next += count

		return nil

	case recordEnd:
		if len(body) != endPayload {
			return errors.NewProcessingError("manifest %s: end record of %d bytes", m.path, len(body))
		}

		return m.checkEnd(binary.LittleEndian.Uint64(body), binary.LittleEndian.Uint64(body[8:]))

	default:
		return errors.NewProcessingError("manifest %s: unknown record %q at %d", m.path, typ, pos)
	}
}

func (m *Manifest) checkBlock(b lane.ConversionBlock) error {
	if m.suffix != nil {
		return errors.NewProcessingError("manifest %s: block %d after the suffix", m.path, b.Seq)
	}

	if int(b.Seq) != len(m.blocks) || b.First != m.next {
		return errors.NewProcessingError("manifest %s: block %d at %d does not follow block %d at %d", m.path, b.Seq, b.First, len(m.blocks)-1, m.next)
	}

	if len(m.blocks) > 0 && b.Height <= m.blocks[len(m.blocks)-1].Height {
		return errors.NewProcessingError("manifest %s: block %d at height %d does not rise", m.path, b.Seq, b.Height)
	}

	if b.Count > lane.MaxPosition-m.next+1 {
		return errors.NewProcessingError("manifest %s: block %d overflows the backfill lane", m.path, b.Seq)
	}

	return nil
}

func (m *Manifest) addBlock(b lane.ConversionBlock, txidsAt int64) {
	m.blocks = append(m.blocks, b)
	m.blockRef = append(m.blockRef, recordRef{offset: txidsAt, count: b.Count})
	m.next += b.Count
}

func (m *Manifest) checkEnd(prefixLast, last uint64) error {
	wantPrefix := m.header.First - 1
	if len(m.blocks) > 0 {
		lb := m.blocks[len(m.blocks)-1]
		wantPrefix = lb.First + lb.Count - 1
	}

	if prefixLast != wantPrefix || last != m.next-1 {
		return errors.NewProcessingError("manifest %s: end says prefix %d last %d, records say %d and %d", m.path, prefixLast, last, wantPrefix, m.next-1)
	}

	m.end = true
	m.last = last

	return nil
}

func (m *Manifest) write(b []byte) error {
	if _, err := m.f.WriteAt(b, m.size); err != nil {
		return errors.NewStorageError("write manifest %s", m.path, err)
	}

	m.digest.Write(b)
	m.size += int64(len(b))

	return nil
}

// appendRecord writes one framed record and syncs it. When either fails the record may be partly
// on disk or wholly there, so the manifest is marked failed and refuses every later append, rather
// than write the record a second time after a copy that may already be there; ResumeManifest
// reloads the file and cuts a partial record.
func (m *Manifest) appendRecord(typ byte, payload []byte) error {
	if m.readOnly {
		return errors.NewProcessingError("manifest %s was opened to read; resume it to write", m.path)
	}

	if m.failed {
		return errors.NewProcessingError("manifest %s failed an earlier write; resume it before writing again", m.path)
	}

	frame := make([]byte, 0, frameHead+len(payload)+frameTail)
	frame = append(frame, typ)
	frame = binary.LittleEndian.AppendUint64(frame, uint64(len(payload)))
	frame = binary.LittleEndian.AppendUint32(frame, crc32.Checksum(frame, crcTable))
	frame = append(frame, payload...)

	body := crc32.New(crcTable)
	body.Write(frame[:9])
	body.Write(payload)
	frame = binary.LittleEndian.AppendUint32(frame, body.Sum32())

	if err := m.write(frame); err != nil {
		m.failed = true
		return err
	}

	if err := m.f.Sync(); err != nil {
		m.failed = true
		return errors.NewStorageError("sync manifest %s", m.path, err)
	}

	return nil
}

// AppendBlock records the next main-chain block's assignment: its transactions, in assignment
// order, take the next positions.
func (m *Manifest) AppendBlock(blockID, height uint32, txids []chainhash.Hash) (lane.ConversionBlock, error) {
	if m.end {
		return lane.ConversionBlock{}, errors.NewProcessingError("manifest %s is complete", m.path)
	}

	b := lane.ConversionBlock{Seq: uint32(len(m.blocks)), BlockID: blockID, Height: height, First: m.next, Count: uint64(len(txids))} //nolint:gosec // bounded by the file
	if err := m.checkBlock(b); err != nil {
		return lane.ConversionBlock{}, err
	}

	payload := make([]byte, 0, blockFixed+len(txids)*chainhash.HashSize)
	payload = binary.LittleEndian.AppendUint32(payload, b.Seq)
	payload = binary.LittleEndian.AppendUint32(payload, b.BlockID)
	payload = binary.LittleEndian.AppendUint32(payload, b.Height)
	payload = binary.LittleEndian.AppendUint64(payload, b.First)
	payload = binary.LittleEndian.AppendUint64(payload, b.Count)

	for i := range txids {
		payload = append(payload, txids[i][:]...)
	}

	at := m.size

	if err := m.appendRecord(recordBlock, payload); err != nil {
		return lane.ConversionBlock{}, err
	}

	m.addBlock(b, at+frameHead+blockFixed)

	return b, nil
}

// AppendSuffix records the segment's suffix: readable transactions no main-chain block of the
// source holds, which take the positions after the prefix.
func (m *Manifest) AppendSuffix(txids []chainhash.Hash) (uint64, error) {
	if m.end || m.suffix != nil {
		return 0, errors.NewProcessingError("manifest %s already has its suffix", m.path)
	}

	if uint64(len(txids)) > lane.MaxPosition-m.next+1 {
		return 0, errors.NewProcessingError("manifest %s: the suffix overflows the backfill lane", m.path)
	}

	payload := make([]byte, 0, suffixFixed+len(txids)*chainhash.HashSize)
	payload = binary.LittleEndian.AppendUint64(payload, m.next)
	payload = binary.LittleEndian.AppendUint64(payload, uint64(len(txids)))

	for i := range txids {
		payload = append(payload, txids[i][:]...)
	}

	at := m.size

	if err := m.appendRecord(recordSuffix, payload); err != nil {
		return 0, err
	}

	first := m.next
	m.suffix = &recordRef{offset: at + frameHead + suffixFixed, count: uint64(len(txids))}
	m.suffixAt = first
	m.next += uint64(len(txids))

	return first, nil
}

// Finish writes the end record and returns the segment the manifest describes, with its digest.
// Nothing can be appended after it. A manifest that assigns no position, or whose segment is not
// valid, is refused before the end record is written, so nothing durable marks it finished.
func (m *Manifest) Finish() (lane.ConversionSegment, error) {
	if m.end {
		return m.Segment()
	}

	seg := m.pendingSegment()

	// Check refuses an empty segment too: Last below First.
	if err := seg.Check(); err != nil {
		return lane.ConversionSegment{}, errors.NewProcessingError("manifest %s", m.path, err)
	}

	payload := binary.LittleEndian.AppendUint64(nil, seg.PrefixLast)
	payload = binary.LittleEndian.AppendUint64(payload, seg.Last)

	if err := m.appendRecord(recordEnd, payload); err != nil {
		return lane.ConversionSegment{}, err
	}

	m.end = true
	m.last = seg.Last

	return m.Segment()
}

func (m *Manifest) pendingSegment() lane.ConversionSegment {
	prefixLast := m.header.First - 1
	if len(m.blocks) > 0 {
		lb := m.blocks[len(m.blocks)-1]
		prefixLast = lb.First + lb.Count - 1
	}

	return lane.ConversionSegment{
		ID:               m.header.Segment,
		First:            m.header.First,
		PrefixLast:       prefixLast,
		Last:             m.next - 1,
		SourceTipBlockID: m.header.SourceTipBlockID,
		SourceTipHash:    m.header.SourceTipHash,
	}
}

// Segment returns the finished manifest's segment and digest. It fails until Finish.
func (m *Manifest) Segment() (lane.ConversionSegment, error) {
	if !m.end {
		return lane.ConversionSegment{}, errors.NewProcessingError("manifest %s is not finished", m.path)
	}

	seg := m.pendingSegment()
	copy(seg.ManifestDigest[:], m.digest.Sum(nil))

	if seg.Last < seg.First {
		return lane.ConversionSegment{}, errors.NewProcessingError("manifest %s assigns no position", m.path)
	}

	return seg, seg.Check()
}

// Header returns the manifest's header.
func (m *Manifest) Header() Header { return m.header }

// Blocks returns the block records written so far.
func (m *Manifest) Blocks() []lane.ConversionBlock {
	return append([]lane.ConversionBlock(nil), m.blocks...)
}

// HasSuffix reports whether the suffix record is written.
func (m *Manifest) HasSuffix() bool { return m.suffix != nil }

// Finished reports whether the end record is written.
func (m *Manifest) Finished() bool { return m.end }

// BlockTxIDs reads a block record's transactions in assignment order.
func (m *Manifest) BlockTxIDs(seq uint32) ([]chainhash.Hash, error) {
	if int(seq) >= len(m.blockRef) {
		return nil, errors.NewProcessingError("manifest %s has no block %d", m.path, seq)
	}

	return m.readTxIDs(m.blockRef[seq])
}

// SuffixTxIDs reads the suffix record's transactions and its first position.
func (m *Manifest) SuffixTxIDs() ([]chainhash.Hash, uint64, error) {
	if m.suffix == nil {
		return nil, 0, errors.NewProcessingError("manifest %s has no suffix", m.path)
	}

	txids, err := m.readTxIDs(*m.suffix)

	return txids, m.suffixAt, err
}

func (m *Manifest) readTxIDs(ref recordRef) ([]chainhash.Hash, error) {
	buf := make([]byte, ref.count*chainhash.HashSize)
	if _, err := m.f.ReadAt(buf, ref.offset); err != nil {
		return nil, errors.NewStorageError("read manifest %s txids", m.path, err)
	}

	out := make([]chainhash.Hash, ref.count)
	for i := range out {
		copy(out[i][:], buf[i*chainhash.HashSize:])
	}

	return out, nil
}

// Close closes the file.
func (m *Manifest) Close() error {
	return m.f.Close()
}
