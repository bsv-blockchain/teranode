package p2p

import "time"

type PeerManagerOptions func(p *PeerManager)

func WithBatchDuration(batchDuration time.Duration) PeerManagerOptions {
	return func(pm *PeerManager) {
		pm.batchDelay = batchDuration
	}
}

func WithExcessiveBlockSize(ebs int64) PeerManagerOptions {
	return func(p *PeerManager) {
		p.ebs = ebs
	}
}
