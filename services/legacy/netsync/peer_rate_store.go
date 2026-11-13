package netsync

import (
	"encoding/json"
	"errors"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// peerRatesFile keeps each peer address's last measured download rate across a restart, beside
// the address manager's peers.json. The schedule then starts known peers at their speed
// instead of handing near blocks to a peer that turns out to be slow.
const peerRatesFile = "legacy-peer-rates.json"

// peerRateRecord is one address's rate and when it was measured.
type peerRateRecord struct {
	BPS float64   `json:"bps"`
	At  time.Time `json:"at"`
}

const (
	// peerRateHalfLife is how fast a remembered rate loses weight with age. A peer that was fast
	// before a restart and is slow or gone now would otherwise be ranked at its old speed, and
	// handed the near blocks at full depth, until decay caught up with it.
	peerRateHalfLife = time.Hour

	// peerRateMaxAge drops a remembered rate this old: at 24 half-lives it is worth nothing.
	peerRateMaxAge = 24 * time.Hour

	// peerRatesMaxEntries bounds the rates file. The newest entries are kept.
	peerRatesMaxEntries = 1000
)

// discountedRate is a remembered rate weighed down by its age at now. A rate with no time, or
// one from the future, is taken as it is.
func (rec peerRateRecord) discountedRate(now time.Time) float64 {
	age := now.Sub(rec.At)
	if rec.At.IsZero() || age <= 0 {
		return rec.BPS
	}

	return rec.BPS * math.Exp2(-age.Hours()/peerRateHalfLife.Hours())
}

// prunePeerRates drops entries with no rate or older than peerRateMaxAge, and keeps the
// peerRatesMaxEntries newest of the rest.
func prunePeerRates(rates map[string]peerRateRecord, now time.Time) map[string]peerRateRecord {
	addrs := make([]string, 0, len(rates))

	for addr, rec := range rates {
		if rec.BPS > 0 && now.Sub(rec.At) <= peerRateMaxAge {
			addrs = append(addrs, addr)
		}
	}

	sort.Slice(addrs, func(i, j int) bool {
		return rates[addrs[i]].At.After(rates[addrs[j]].At)
	})

	if len(addrs) > peerRatesMaxEntries {
		addrs = addrs[:peerRatesMaxEntries]
	}

	out := make(map[string]peerRateRecord, len(addrs))
	for _, addr := range addrs {
		out[addr] = rates[addr]
	}

	return out
}

// loadPeerRates reads the rates file. No file is an empty memory, not an error. A file in the
// earlier format, a bare rate per address, is read with the file's modification time as the time
// of every rate.
func loadPeerRates(path string) (map[string]peerRateRecord, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]peerRateRecord{}, nil
	}

	if err != nil {
		return nil, err
	}

	rates := map[string]peerRateRecord{}
	if err := json.Unmarshal(b, &rates); err == nil {
		return rates, nil
	}

	bare := map[string]float64{}
	if err := json.Unmarshal(b, &bare); err != nil {
		return nil, err
	}

	at := time.Now()
	if info, err := os.Stat(path); err == nil {
		at = info.ModTime()
	}

	rates = make(map[string]peerRateRecord, len(bare))
	for addr, bps := range bare {
		rates[addr] = peerRateRecord{BPS: bps, At: at}
	}

	return rates, nil
}

// savePeerRates writes through a temporary file and a rename, so a crash leaves the previous file.
func savePeerRates(path string, rates map[string]peerRateRecord) error {
	b, err := json.Marshal(rates)
	if err != nil {
		return err
	}

	// A temporary file of its own for each save: the stop and the 10-minute save can run at the
	// same time, and with one shared name one save renamed the other's half-written file.
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}

	tmp := f.Name()

	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)

		return err
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)

		return err
	}

	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)

		return err
	}

	return nil
}

// peerRatesSaveEvery is how many race ticks (raceCheckInterval, 5 s) go between two writes of the
// rates file: 10 minutes.
const peerRatesSaveEvery = 120

// savePeerRates writes each peer address's rate to the rates file, when the node keeps one.
func (sm *SyncManager) savePeerRates() {
	if sm.peerRatesPath == "" || sm.streams == nil {
		return
	}

	if err := savePeerRates(sm.peerRatesPath, sm.streams.rememberedRates(time.Now())); err != nil {
		sm.logger.Warnf("[legacy] could not write %s: %v", sm.peerRatesPath, err)
	}
}
