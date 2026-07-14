package netsync

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/legacy/diskblocks"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// diskSyncProgressInterval is how many blocks between progress log lines.
const diskSyncProgressInterval = 1000

// blocksToFeed returns the slice of chain refs strictly above bestHeight, up to
// and including stopHeight (0 = chain tip). chain must be ordered genesis->tip.
func blocksToFeed(chain []*diskblocks.BlockRef, bestHeight, stopHeight uint32) []*diskblocks.BlockRef {
	out := make([]*diskblocks.BlockRef, 0, len(chain))
	for _, ref := range chain {
		if ref.Height <= bestHeight {
			continue
		}
		// stopHeight is also applied in ReadChain/selectChain; this is a
		// belt-and-suspenders cap so the feed is bounded even if the chain
		// passed in was not pre-truncated. Keep the two in agreement.
		if stopHeight > 0 && ref.Height > stopHeight {
			break
		}
		out = append(out, ref)
	}
	return out
}

// RunDiskSync ingests blocks from a stopped SV Node datadir through the existing
// HandleBlockDirect pipeline, bypassing P2P. It is the entry point for disk-IBD
// mode and blocks until the feed completes, the context is cancelled, or a
// read error (dirty frontier / truncation) stops it cleanly.
func (sm *SyncManager) RunDiskSync(ctx context.Context, datadir string, stopHeight uint32) error {
	idx, err := diskblocks.OpenIndex(datadir)
	if err != nil {
		return err
	}
	defer idx.Close()

	chain, err := idx.ReadChain(stopHeight)
	if err != nil {
		return err
	}
	if len(chain) == 0 {
		return errors.NewProcessingError("[DiskSync] block index produced an empty chain")
	}
	sm.logger.Infof("[DiskSync] block index exposes %d blocks (tip height %d)", len(chain), chain[len(chain)-1].Height)

	_, meta, err := sm.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return errors.NewProcessingError("[DiskSync] failed to get best block header", err)
	}
	feed := blocksToFeed(chain, meta.Height, stopHeight)
	if len(feed) == 0 {
		sm.logger.Infof("[DiskSync] nothing to do: best height %d already at or above target", meta.Height)
		return nil
	}
	sm.logger.Infof("[DiskSync] feeding %d blocks from height %d to %d", len(feed), feed[0].Height, feed[len(feed)-1].Height)

	if err = sm.blockchainClient.LegacySync(ctx); err != nil {
		return errors.NewProcessingError("[DiskSync] failed to enter LEGACYSYNCING", err)
	}

	reader := diskblocks.NewBlockReader(datadir, sm.chainParams.Net)
	diskPeer := peer.NewInboundPeer(sm.logger, sm.settings, &peer.Config{ChainParams: sm.chainParams})

	start := time.Now()
	var blocks, txs, bytesRead uint64
	lastHeight := meta.Height

	for _, ref := range feed {
		if ctx.Err() != nil {
			// FSM remains in LEGACYSYNCING; the caller that cancelled owns cleanup.
			return ctx.Err()
		}

		var msgBlock *wire.MsgBlock
		var n uint64
		msgBlock, n, err = reader.ReadBlock(ref)
		if err != nil {
			// Dirty frontier or truncated final block: stop cleanly.
			sm.logger.Warnf("[DiskSync] stopping at height %d: %v", ref.Height, err)
			break
		}

		if err = sm.HandleBlockDirect(ctx, diskPeer, ref.Hash, msgBlock); err != nil {
			return errors.NewProcessingError("[DiskSync] HandleBlockDirect failed at height %d (%s)", ref.Height, ref.Hash, err)
		}

		blocks++
		txs += uint64(len(msgBlock.Transactions))
		bytesRead += n
		lastHeight = ref.Height

		if blocks%diskSyncProgressInterval == 0 {
			elapsed := time.Since(start).Seconds()
			sm.logger.Infof("[DiskSync] height %d | %d blocks | %.1f blocks/s | %.1f MB/s",
				ref.Height, blocks, float64(blocks)/elapsed, float64(bytesRead)/1e6/elapsed)
		}
	}

	elapsed := time.Since(start)
	var blocksPerSec float64
	if secs := elapsed.Seconds(); secs > 0 {
		blocksPerSec = float64(blocks) / secs
	}
	sm.logger.Infof("[DiskSync] done: %d blocks, %d txs, %.2f GB in %s (height %d) | %.1f blocks/s",
		blocks, txs, float64(bytesRead)/1e9, elapsed.Round(time.Second), lastHeight, blocksPerSec)

	if err = sm.blockchainClient.Run(ctx, "legacy/netsync/disksync"); err != nil {
		return errors.NewProcessingError("[DiskSync] failed to transition to RUNNING", err)
	}
	return nil
}
