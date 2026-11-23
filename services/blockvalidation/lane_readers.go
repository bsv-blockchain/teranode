package blockvalidation

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/lanereader"
)

// The lanes shadow of block validation's two mined-state checks, at utxostore_laneMigration
// shadow: the parent check and the check that a block re-includes no transaction already mined on
// its chain. Each is answered from the process's lane index beside today's answer and counted
// (util/lanereader); neither changes a verdict. Loading the chain's rows and the duplicate
// comparison run in the background; once the chain is in, each parent's lookup, about 100 ns,
// runs on the parent check's own goroutine. At off there is no lane index and both do nothing.

// duplicatesTimeout bounds loading a block's ancestry for the duplicate check's shadow.
const duplicatesTimeout = 10 * time.Second

// laneParentsContext is ctx carrying the shadow of block's parent check, judged on the chain
// ending at its parent; ctx itself at off.
func laneParentsContext(ctx context.Context, block *model.Block) context.Context {
	r := lanereader.Current()
	if r == nil || block == nil || block.Header == nil || block.Header.HashPrevBlock == nil {
		return ctx
	}

	return model.ContextWithLaneParentObserver(ctx, r.BlockParents(ctx, *block.Header.HashPrevBlock))
}

// shadowLaneDuplicates compares, in the background, the lane index's answer to the duplicate check
// with setMined's verdict err for block, stored as blockID, and returns err unchanged. A verdict
// that is neither a pass nor ErrBlockInvalid, and an unset, are not compared.
func shadowLaneDuplicates(ctx context.Context, block *model.Block, blockID uint32, unsetMined []bool, err error) error {
	r := lanereader.Current()
	if r == nil || (len(unsetMined) > 0 && unsetMined[0]) {
		return err
	}

	duplicate := errors.Is(err, errors.ErrBlockInvalid)
	if err != nil && !duplicate {
		return err
	}

	hash := *block.Hash()

	go func() {
		prepareCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), duplicatesTimeout)
		defer cancel()

		r.ShadowDuplicates(prepareCtx, hash, blockID, duplicate)
	}()

	return err
}
