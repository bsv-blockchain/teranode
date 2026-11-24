package protocol

import (
	"encoding/hex"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// peers.json — the Teranode snapshot of the address tables.
//
// This is a FRESH format, deliberately. It shares only its filename with
// services/legacy's addrmgr snapshot (services/legacy/addrmgr/addrmanager.go,
// its savePeers writer) and reads NONE of that file's layout. A node switching
// from services/legacy to svp2p therefore cold-starts its address table once.
// That was accepted by the plan owner (OPEN QUESTION 3: fresh format, same
// filename). Do NOT "fix" this into a migration — the legacy format carries
// bsvd's knownAddress/triedBucket layout, which does not describe the SVNode
// tables this file reconstructs.
//
// The snapshot mirrors what CAddrMan::Serialize (addrman.h:308) records and
// nothing more:
//   - nKey, so the buckets a reloaded entry hashes into are the SAME ones it
//     occupied before the restart;
//   - the per-entry fields CAddrInfo::SerializationOp writes (addrman.h:58):
//     the CAddress (nTime, nServices, ip, port), the source, nLastSuccess and
//     nAttempts — and NOT the members C++ marks "memory only" (nLastTry,
//     nLastCountAttempt, nRefCount, fInTried, nRandomPos);
//   - the occupancy of vvNew as a bucket -> entry-index list. vvTried and the
//     bucket POSITIONS are not stored: like C++, they are recomputed from
//     nKey, which is why the round trip is exact.
//
// Persistence is behind legacy_savePeers, which defaults to FALSE
// ("by default we do not save the peers", settings/settings.go:674). An empty
// AddrManOptions.Path is that disabled state and is the common case: nothing
// is read, nothing is written, no goroutine starts, nothing is logged.

// peersFileVersion is bumped only on a breaking layout change. Load rejects an
// unknown version rather than guessing.
const peersFileVersion = 1

// peersFile is the on-disk document.
type peersFile struct {
	Version int    `json:"version"`
	NKey    string `json:"nKey"`

	// New and Tried carry the entries in the two tables, in the same order
	// CAddrMan::Serialize emits them (new first, then tried).
	New   []persistedAddrInfo `json:"new"`
	Tried []persistedAddrInfo `json:"tried"`

	// NewBucketCount records ADDRMAN_NEW_BUCKET_COUNT at write time. If the
	// geometry ever changes, NewBuckets is discarded and every new entry is
	// re-bucketed from its own source, exactly as
	// CAddrMan::Unserialize does when nUBuckets != ADDRMAN_NEW_BUCKET_COUNT
	// (addrman.h:400).
	NewBucketCount int `json:"newBucketCount"`

	// NewBuckets is sparse: only buckets that hold at least one entry appear.
	// Entries are indices into New.
	NewBuckets []persistedBucket `json:"newBuckets"`
}

type persistedBucket struct {
	Bucket  int   `json:"bucket"`
	Entries []int `json:"entries"`
}

// persistedAddrInfo is the field-for-field image of
// CAddrInfo::SerializationOp (addrman.h:58).
type persistedAddrInfo struct {
	IP          string `json:"ip"`
	Port        uint16 `json:"port"`
	Services    uint64 `json:"services"`
	Time        int64  `json:"time"`
	Source      string `json:"source"`
	LastSuccess int64  `json:"lastSuccess"`
	Attempts    int    `json:"attempts"`
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------
//
// LIFECYCLE OWNER of the periodic snapshot goroutine.
//
//   - The AddrMan instance owns it. Nothing else holds the channel or the
//     WaitGroup, and no other function on this type touches them.
//   - Stop() is the ONLY thing that stops it, via a sync.Once, so a second
//     Stop is a no-op rather than a double close.
//   - Stop() is reachable on every shutdown path because it needs no
//     preconditions: it is safe when StartPersistence was never called, safe
//     when persistence is disabled (Path == ""), safe when it has already
//     run, and it never returns early before releasing the goroutine — the
//     final Save happens AFTER the join, and a Save error is returned without
//     skipping anything.
//   - No path that merely reconfigures or rotates anything calls Stop().
//     AddrMan has no rotation concept and no shared cleanup helper that a
//     reconfigure path could reach; Clear() re-initialises the tables and
//     deliberately does NOT touch the goroutine.

// StartPersistence begins the periodic peers.json snapshot.
//
// It is a no-op when persistence is disabled (Path == "" — the default, since
// legacy_savePeers is false). It is also a no-op on a second call.
func (a *AddrMan) StartPersistence() {
	if a.path == "" {
		return
	}

	// lifecycleMu serialises the whole start/stop transition. Without it the
	// window between the stopped check and wg.Add lets a concurrent Stop close
	// quit, observe a zero counter and return — a wg.Add after a completed
	// Wait, which is documented misuse — and lets this CAS land after Stop's
	// running.Store(false), stranding running at true forever. Real callers
	// invoke these sequentially, so the window is theoretical; lifecycle
	// ownership is not the place to rely on that.
	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()

	// Once Stop has run, quit is closed for good: starting here would spawn a
	// goroutine that returns immediately and leave running=true lying about
	// it.
	if a.stopped.Load() {
		return
	}

	if !a.running.CompareAndSwap(false, true) {
		return
	}

	a.wg.Add(1)

	go func() {
		defer a.wg.Done()

		ticker := time.NewTicker(a.saveInterval)
		defer ticker.Stop()

		for {
			select {
			case <-a.quit:
				return
			case <-ticker.C:
				if err := a.Save(); err != nil {
					a.logger.Errorf("[svp2p] failed to write peers.json: %v", err)
				}
			}
		}
	}()
}

// Stop stops the periodic snapshot, joins its goroutine, and writes the final
// peers.json. It is idempotent and safe to call when persistence is disabled
// or when StartPersistence was never called.
func (a *AddrMan) Stop() error {
	var err error

	a.stopOnce.Do(func() {
		a.lifecycleMu.Lock()

		a.stopped.Store(true)

		close(a.quit)
		a.wg.Wait()
		a.running.Store(false)

		// lifecycleMu is released before the final save: it guards the
		// start/stop transition only, and Save writes to disk.
		//
		// One honest caveat, since the rule this follows is "no blocking call
		// under a lock": the wg.Wait() above IS under lifecycleMu, and it can
		// wait out a save the ticker goroutine already started — so the lock
		// is held for a disk-write duration on that path. No deadlock: the
		// goroutine needs nothing this lock holds. The only contender for the
		// lock is a concurrent StartPersistence, which real lifecycles never
		// issue, so the practical cost is nil. Do not add a contender without
		// revisiting this.
		a.lifecycleMu.Unlock()

		// The final snapshot runs AFTER the goroutine has joined, so the two
		// writers can never overlap on the file.
		err = a.Save()
	})

	return err
}

// ---------------------------------------------------------------------------
// Save
// ---------------------------------------------------------------------------

// Save writes the current tables to peers.json. It is a no-op, with no error
// and no log line, when persistence is disabled.
//
// The file write happens AFTER cs is released: cs guards the tables only, and
// a disk write is a blocking call that must not run inside a critical section
// other goroutines are waiting on.
func (a *AddrMan) Save() error {
	if a.path == "" {
		return nil
	}

	// Never write over a snapshot we could not read. After a failed Load the
	// in-memory tables are empty, so saving would destroy the operator's
	// address table on a transient EACCES, or overwrite the corrupt file they
	// wanted to inspect. This gate covers BOTH writers — the periodic ticker
	// and Stop's final save — because both go through here. Recovery is to fix
	// or remove peers.json and restart; Load logs that once.
	if a.loadFailed.Load() {
		return nil
	}

	doc := a.snapshot()

	data, err := json.Marshal(doc)
	if err != nil {
		return errors.New(errors.ERR_STORAGE_ERROR, "svp2p: cannot encode peers.json", err)
	}

	if err := writeFileAtomic(a.path, data); err != nil {
		return err
	}

	a.saves.Add(1)

	return nil
}

// snapshot builds the on-disk document under cs, so the file write below runs
// outside the lock. It mirrors CAddrMan::Serialize (addrman.h:308).
func (a *AddrMan) snapshot() *peersFile {
	a.cs.Lock()
	defer a.cs.Unlock()

	doc := &peersFile{
		Version:        peersFileVersion,
		NKey:           hex.EncodeToString(a.nKey[:]),
		NewBucketCount: addrmanNewBucketCount,
	}

	// mapUnkIds maps an nId to its index in doc.New, the way Serialize's
	// std::map<int, int> mapUnkIds does (addrman.h:320).
	mapUnkIds := make(map[int]int)

	// mapInfo is a std::map in C++, so Serialize walks it in ascending nId
	// order. Go maps have no order, so the ids are sorted to keep the output
	// byte-stable across runs of the same table.
	ids := sortedKeys(a.mapInfo)

	for _, nID := range ids {
		info := a.mapInfo[nID]
		if info.nRefCount != 0 {
			mapUnkIds[nID] = len(doc.New)
			doc.New = append(doc.New, persistInfo(info))
		}
	}

	for _, nID := range ids {
		info := a.mapInfo[nID]
		if info.fInTried {
			doc.Tried = append(doc.Tried, persistInfo(info))
		}
	}

	for bucket := 0; bucket < addrmanNewBucketCount; bucket++ {
		var entries []int

		for i := 0; i < addrmanBucketSize; i++ {
			if a.vvNew[bucket][i] == emptyBucketSlot {
				continue
			}

			// C++ needs no guard here at all: it populates mapUnkIds for
			// EVERY mapInfo entry (addrman.h:324:
			// `mapUnkIds[(*it).first] = nIds;`) BEFORE the
			// `if (info.nRefCount)` test at addrman.h:326 — the
			// `const CAddrInfo &info = (*it).second;` binding sits between
			// the two — so its
			// bucket-position loop can index mapUnkIds unconditionally
			// (addrman.h:353: `int nIndex = mapUnkIds[vvNew[bucket][i]];`).
			// The map built above holds only the REFERENCED entries, so the
			// lookup is guarded instead. The guard is unreachable — an id
			// sitting in vvNew always has nRefCount > 0 — but a missing id
			// would be written as index 0 and reload as a DIFFERENT address,
			// so it is skipped rather than trusted.
			index, ok := mapUnkIds[a.vvNew[bucket][i]]
			if !ok {
				continue
			}

			entries = append(entries, index)
		}

		if len(entries) > 0 {
			doc.NewBuckets = append(doc.NewBuckets, persistedBucket{Bucket: bucket, Entries: entries})
		}
	}

	return doc
}

// sortedKeys returns the nIds in ascending order, reproducing the
// std::map<int, CAddrInfo> iteration order CAddrMan::Serialize relies on. A
// full table holds up to 81,920 entries, so the sort has to be O(n log n).
func sortedKeys(m map[int]*addrInfo) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	slices.Sort(out)

	return out
}

func persistInfo(info *addrInfo) persistedAddrInfo {
	return persistedAddrInfo{
		IP:          info.addr.netIP().String(),
		Port:        info.port,
		Services:    uint64(info.nServices),
		Time:        info.nTime,
		Source:      info.source.netIP().String(),
		LastSuccess: info.nLastSuccess,
		Attempts:    info.nAttempts,
	}
}

// writeFileAtomic writes via a sibling temp file and a rename, so a crash
// mid-write leaves the previous snapshot intact rather than a truncated file
// that Load would reject.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)

	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp*")
	if err != nil {
		return errors.New(errors.ERR_STORAGE_ERROR, "svp2p: cannot create peers.json temp file", err)
	}

	tmpName := tmp.Name()

	// Every error path below removes the temp file, so a failed write leaves
	// no peers.json.tmpNNNN sibling behind to accumulate in WorkingDir across
	// restarts. renamed is set once the temp file no longer exists under its
	// own name. A process hard-killed between CreateTemp and Rename still
	// leaves one — no in-process cleanup can cover that.
	renamed := false

	defer func() {
		if !renamed {
			_ = os.Remove(tmpName)
		}
	}()

	// os.CreateTemp already creates the file with mode 0600, so there is no
	// Chmod here.
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()

		return errors.New(errors.ERR_STORAGE_ERROR, "svp2p: cannot write peers.json temp file", err)
	}

	// Sync before Close: without it the Rename below is atomic with respect to
	// a concurrent reader but not durable, so a power loss can leave the file
	// present and empty.
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()

		return errors.New(errors.ERR_STORAGE_ERROR, "svp2p: cannot flush peers.json temp file", err)
	}

	if err := tmp.Close(); err != nil {
		return errors.New(errors.ERR_STORAGE_ERROR, "svp2p: cannot close peers.json temp file", err)
	}

	if err := os.Rename(tmpName, path); err != nil {
		return errors.New(errors.ERR_STORAGE_ERROR, "svp2p: cannot replace peers.json", err)
	}

	renamed = true

	// The directory itself is deliberately NOT fsynced after the Rename. The
	// rename is already visible to every reader, and a failure to fsync the
	// directory is not something this code could act on — it would either be
	// swallowed or turn a successful write into a reported failure. peers.json
	// is a cache: losing the rename to a power cut costs one cold start, which
	// is the same cost as the first svp2p run.
	return nil
}

// ---------------------------------------------------------------------------
// Load
// ---------------------------------------------------------------------------

// Load replaces the tables with the peers.json snapshot. A missing file is not
// an error: a cold start on the first svp2p run is expected and accepted
// (OPEN QUESTION 3). Persistence disabled means nothing is read at all.
func (a *AddrMan) Load() error {
	if a.path == "" {
		return nil
	}

	nLostUnk, nLost, err := a.load()
	if err != nil {
		a.loadFailed.Store(true)
		a.logger.Warnf("[svp2p] peers.json could not be loaded and will NOT be overwritten; fix or remove %s and restart to re-enable peer persistence: %v", a.path, err)

		return err
	}

	// Logged after cs is released: a log write is a blocking call and cs
	// guards the tables only.
	if nLost+nLostUnk > 0 {
		a.logger.Infof("[svp2p] addrman lost %d new and %d tried addresses due to collisions", nLostUnk, nLost)
	}

	return nil
}

// load does the work of Load, returning the two collision-loss counts for the
// caller to log outside the critical section.
//
// ATOMICITY. load is all-or-nothing: either the whole snapshot is published or
// the live tables are left exactly as they were. This is not cosmetic. C++
// guarantees it structurally — CAddrMan::Unserialize throws out of a Clear()ed
// object, and Check_ asserts the invariant it protects
// (addrman.cpp:415: `if (vRandom.size() != nTried + nNew) return -7;`). A Go
// port that clear()ed and then failed partway through would publish
// len(vRandom) > 0 with nNew == 0 and nTried == 0, and from that state
// Select's new-node walk (addrman.cpp:388-397) can never find an occupied slot:
// an unbounded spin holding cs, which wedges every other method.
//
// The guarantee is enforced by structure, not by care: EVERY fallible step runs
// before cs is taken. restoreInfo is the only operation below the parse that can
// fail, so all of them are hoisted into the two pre-restore loops. Nothing after
// the `a.cs.Lock()` line returns an error, and nothing that can return an error
// may be added there.
func (a *AddrMan) load() (nLostUnk, nLost int, err error) {
	data, err := os.ReadFile(a.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil
		}

		return 0, 0, errors.New(errors.ERR_STORAGE_ERROR, "svp2p: cannot read peers.json", err)
	}

	var doc peersFile
	if err := json.Unmarshal(data, &doc); err != nil {
		return 0, 0, errors.New(errors.ERR_STORAGE_ERROR, "svp2p: cannot parse peers.json", err)
	}

	if doc.Version != peersFileVersion {
		return 0, 0, errors.New(errors.ERR_INVALID_ARGUMENT,
			"svp2p: unsupported peers.json version %d", doc.Version)
	}

	nKeyBytes, err := hex.DecodeString(doc.NKey)
	if err != nil || len(nKeyBytes) != 32 {
		return 0, 0, errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: peers.json has a malformed nKey")
	}

	// The C++ limits from Unserialize (addrman.h:382, :387).
	if len(doc.New) > addrmanNewBucketCount*addrmanBucketSize {
		return 0, 0, errors.New(errors.ERR_INVALID_ARGUMENT,
			"svp2p: corrupt peers.json, nNew exceeds limit")
	}

	if len(doc.Tried) > addrmanTriedBucketCount*addrmanBucketSize {
		return 0, 0, errors.New(errors.ERR_INVALID_ARGUMENT,
			"svp2p: corrupt peers.json, nTried exceeds limit")
	}

	// --- every fallible step, hoisted above the critical section ---

	newInfos := make([]*addrInfo, 0, len(doc.New))

	for _, rec := range doc.New {
		info, err := restoreInfo(rec)
		if err != nil {
			return 0, 0, err
		}

		newInfos = append(newInfos, info)
	}

	triedInfos := make([]*addrInfo, 0, len(doc.Tried))

	for _, rec := range doc.Tried {
		info, err := restoreInfo(rec)
		if err != nil {
			return 0, 0, err
		}

		triedInfos = append(triedInfos, info)
	}

	// --- from here on nothing can fail, so publishing is safe ---

	a.cs.Lock()
	defer a.cs.Unlock()

	a.clear()
	copy(a.nKey[:], nKeyBytes)

	// Deserialize entries from the new table (addrman.h:393).
	for n, info := range newInfos {
		a.mapInfo[n] = info
		a.mapAddr[info.addr] = n
		info.nRandomPos = len(a.vRandom)
		a.vRandom = append(a.vRandom, n)

		if doc.NewBucketCount != addrmanNewBucketCount {
			// The bucket data cannot be used, so give the entry a reference
			// based on its primary source address instead.
			nUBucket := info.getNewBucketFromSource(a.nKey)
			nUBucketPos := info.getBucketPosition(a.nKey, true, nUBucket)

			if a.vvNew[nUBucket][nUBucketPos] == emptyBucketSlot {
				a.vvNew[nUBucket][nUBucketPos] = n
				info.nRefCount++
			}
		}
	}

	a.nNew = len(newInfos)
	a.nIdCount = a.nNew

	// Deserialize entries from the tried table (addrman.h:418).
	nTried := len(triedInfos)

	for _, info := range triedInfos {
		nKBucket := info.getTriedBucket(a.nKey)
		nKBucketPos := info.getBucketPosition(a.nKey, false, nKBucket)

		if a.vvTried[nKBucket][nKBucketPos] == emptyBucketSlot {
			info.nRandomPos = len(a.vRandom)
			info.fInTried = true
			a.vRandom = append(a.vRandom, a.nIdCount)
			a.mapInfo[a.nIdCount] = info
			a.mapAddr[info.addr] = a.nIdCount
			a.vvTried[nKBucket][nKBucketPos] = a.nIdCount
			a.nIdCount++
		} else {
			nLost++
		}
	}

	a.nTried = nTried - nLost

	// Deserialize positions in the new table (addrman.h:441).
	if doc.NewBucketCount == addrmanNewBucketCount {
		for _, pb := range doc.NewBuckets {
			if pb.Bucket < 0 || pb.Bucket >= addrmanNewBucketCount {
				continue
			}

			for _, nIndex := range pb.Entries {
				if nIndex < 0 || nIndex >= a.nNew {
					continue
				}

				info := a.mapInfo[nIndex]
				nUBucketPos := info.getBucketPosition(a.nKey, true, pb.Bucket)

				if a.vvNew[pb.Bucket][nUBucketPos] == emptyBucketSlot &&
					info.nRefCount < addrmanNewBucketsPerAddress {
					info.nRefCount++
					a.vvNew[pb.Bucket][nUBucketPos] = nIndex
				}
			}
		}
	}

	// Prune new entries with refcount 0 (as a result of collisions).
	for _, nID := range sortedKeys(a.mapInfo) {
		info := a.mapInfo[nID]
		if !info.fInTried && info.nRefCount == 0 {
			a.delete(nID)
			nLostUnk++
		}
	}

	return nLostUnk, nLost, nil
}

func restoreInfo(rec persistedAddrInfo) (*addrInfo, error) {
	ip := net.ParseIP(rec.IP)
	if ip == nil {
		return nil, errors.New(errors.ERR_INVALID_ARGUMENT,
			"svp2p: peers.json holds an unparseable address %q", rec.IP)
	}

	source := net.ParseIP(rec.Source)
	if source == nil {
		return nil, errors.New(errors.ERR_INVALID_ARGUMENT,
			"svp2p: peers.json holds an unparseable source %q", rec.Source)
	}

	info := newAddrInfo(NewAddressAtTime(ip, rec.Port, wire.ServiceFlag(rec.Services), rec.Time), source)

	info.nLastSuccess = rec.LastSuccess
	info.nAttempts = rec.Attempts

	return info, nil
}
