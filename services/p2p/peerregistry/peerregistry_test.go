package peerregistry

import (
	"testing"
	"time"
)

func TestMinTTLIsDerivedFromTheP2PCadence(t *testing.T) {
	if MinTTL != time.Duration(MinTTLMultiplier)*RegistryReassertTTL {
		t.Errorf("MinTTL must equal %dx the batcher reassert window, got %s", MinTTLMultiplier, MinTTL)
	}

	if MinTTL < 30*NodeStatusPublishInterval {
		t.Errorf("MinTTL %s must cover at least 30 publish intervals (%s) so a live peer cannot be expired between refreshes",
			MinTTL, 30*NodeStatusPublishInterval)
	}

	// The shipped numbers the floor's justification quotes: if one of these
	// changes, this test forces a fresh look at MinTTL rather than letting the
	// floor silently become wrong arithmetic.
	if NodeStatusPublishInterval != 10*time.Second || RegistryReassertTTL != time.Minute || MinTTL != 5*time.Minute {
		t.Errorf("registry cadence constants changed: publish=%s reassert=%s minTTL=%s - re-verify the floor's justification",
			NodeStatusPublishInterval, RegistryReassertTTL, MinTTL)
	}
}
