//go:build bvgaps

package model

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestKnownDefect_FutureDatedHeader_IsNotClassifiedInvalid checks the class the contextual header
// check gives a timestamp more than two hours ahead of the local clock.
//
// Spec: BV-HEADER-006 and the CLOCK-DEFERRED row of BV-VERDICT-001 (tracked as GAP-BV-050).
//
// Scenario: Given a header stamped three hours after now. When CheckHeaderContextual runs, Then it
// is rejected ("two hours in the future"), but not as ERR_BLOCK_INVALID: the outcome depends on the
// local clock, so it is not a verdict on the block and the block may be accepted once the clock
// catches up.
//
// Today it returns ERR_BLOCK_INVALID (model/Block.go). TestBlock_CheckHeaderContextual's
// "timestamp more than two hours in the future" subtest used to assert that class.
func TestKnownDefect_FutureDatedHeader_IsNotClassifiedInvalid(t *testing.T) {
	const aheadOfClock = 3 * time.Hour

	tSettings := test.CreateBaseTestSettings(t)

	header := minedHeaderOnParent(t, 4, &chainhash.Hash{0x01}, tSettings.ChainCfgParams.GenesisHash)
	header.Timestamp = uint32(time.Now().Add(aheadOfClock).Unix()) //nolint:gosec
	remineHeader(t, header)

	block := &Block{Header: header, Height: 1}

	err := block.CheckHeaderContextual([]*BlockHeader{}, tSettings, ulogger.TestLogger{})

	require.Error(t, err)
	require.ErrorContains(t, err, "two hours in the future")
	require.False(t, errors.Is(err, errors.ErrBlockInvalid),
		"BV-HEADER-006: a block ahead of the local clock is CLOCK-DEFERRED, not a verdict on the block, got: %v", err)
}
