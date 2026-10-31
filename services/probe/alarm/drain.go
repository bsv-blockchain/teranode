package alarm

import (
	"math"
	"sync"
	"time"
)

const (
	drainMinSpan       = 30 * time.Second
	drainFlatItems     = 0.5
	drainMinCapacity   = 4
	drainCapResolution = time.Second
)

type drainObservation struct {
	t       time.Time
	backlog float64
}

type Drain struct {
	mu       sync.Mutex
	window   time.Duration
	capacity int
	obs      []drainObservation
	slot     time.Time
}

func NewDrain(window time.Duration) *Drain {
	capacity := int(2 * window / drainCapResolution)
	if capacity < drainMinCapacity {
		capacity = drainMinCapacity
	}

	return &Drain{window: window, capacity: capacity}
}

func (d *Drain) Observe(t time.Time, backlog float64) {
	if math.IsNaN(backlog) || math.IsInf(backlog, 0) || backlog < 0 {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if n := len(d.obs); n > 0 && !t.After(d.obs[n-1].t) {
		return
	}

	if len(d.obs) > 0 && t.Sub(d.slot) < drainCapResolution {
		d.obs[len(d.obs)-1] = drainObservation{t: t, backlog: backlog}

		return
	}

	d.slot = t
	d.obs = append(d.obs, drainObservation{t: t, backlog: backlog})

	cutoff := t.Add(-d.window)
	drop := 0

	for drop < len(d.obs) && d.obs[drop].t.Before(cutoff) {
		drop++
	}

	if over := len(d.obs) - drop - d.capacity; over > 0 {
		drop += over
	}

	if drop > 0 {
		d.obs = append(d.obs[:0], d.obs[drop:]...)
	}
}

func (d *Drain) Seconds() (secs float64, growing bool, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	n := len(d.obs)
	if n < 2 {
		return 0, false, false
	}

	first := d.obs[0].t
	newest := d.obs[n-1]

	if newest.t.Sub(first) < drainMinSpan {
		return 0, false, false
	}

	if newest.backlog == 0 {
		return 0, false, true
	}

	var sumX, sumY float64

	for _, o := range d.obs {
		sumX += o.t.Sub(first).Seconds()
		sumY += o.backlog
	}

	meanX := sumX / float64(n)
	meanY := sumY / float64(n)

	var sxy, sxx float64

	for _, o := range d.obs {
		dx := o.t.Sub(first).Seconds() - meanX
		sxy += dx * (o.backlog - meanY)
		sxx += dx * dx
	}

	if sxx == 0 {
		return 0, false, false
	}

	slope := sxy / sxx

	if math.Abs(slope)*d.window.Seconds() < drainFlatItems {
		return 0, false, false
	}

	if slope > 0 {
		return 0, true, false
	}

	return newest.backlog / -slope, false, true
}
