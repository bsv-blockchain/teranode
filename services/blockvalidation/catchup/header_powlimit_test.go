package catchup

import (
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// A header whose hash meets the target it declares says nothing about whether that
// target is permitted on the network: nBits 0x207fffff is met by about every second
// nonce. ValidateHeaderProofOfWork must bound the declared target by the network's
// proof-of-work limit before it trusts the hash check.

func TestValidateHeaderProofOfWork_NetworkLimit(t *testing.T) {
	easy := createValidPoWHeader(t, "207fffff")

	t.Run("RejectsEasierThanMainnetLimit", func(t *testing.T) {
		err := ValidateHeaderProofOfWork(easy, &chaincfg.MainNetParams)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNetworkInvalidResponse), "got %v", err)
		require.Contains(t, err.Error(), "easier than the network")
		// the limit diagnostic is text, not the cause: the peer lied about its own header, which is
		// not a verdict on the announced block, so catch-up must go on to try other peers
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "got %v", err)
	})

	t.Run("AcceptsTheNetworkMinimum", func(t *testing.T) {
		require.NoError(t, ValidateHeaderProofOfWork(easy, &chaincfg.RegressionNetParams))
	})

	t.Run("NilParamsEnforceNoLimit", func(t *testing.T) {
		require.NoError(t, ValidateHeaderProofOfWork(easy, nil))
	})

	t.Run("MalformedBitsStillFail", func(t *testing.T) {
		bad := createValidPoWHeader(t, "207fffff")
		bits, err := model.NewNBitFromString("20800000") // sign bit set: CalculateTarget returns 0
		require.NoError(t, err)
		bad.Bits = *bits

		require.Error(t, ValidateHeaderProofOfWork(bad, &chaincfg.MainNetParams))
		require.Error(t, ValidateHeaderProofOfWork(bad, nil))
	})
}

// ParseBlockHeaders is the live entry for headers fetched from a peer: it must carry
// the limit through to the check.
func TestParseBlockHeaders_NetworkLimit(t *testing.T) {
	easy := createValidPoWHeader(t, "207fffff")
	raw := easy.Bytes()

	_, err := ParseBlockHeaders(raw, &chaincfg.MainNetParams)
	require.Error(t, err, "a header declaring an easier-than-mainnet target must not parse on mainnet")

	got, err := ParseBlockHeaders(raw, &chaincfg.RegressionNetParams)
	require.NoError(t, err)
	require.Len(t, got, 1)
}
