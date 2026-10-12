package seedcheckpoint

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/stretchr/testify/require"
)

// testNetMagic and otherNetMagic are arbitrary distinct network magics used to
// exercise the domain-separation binding.
const (
	testNetMagic  uint32 = 0xe8f3e1e3
	otherNetMagic uint32 = 0xf4e5f3f4
)

func sampleCheckpoint() Checkpoint {
	var blockHash, setHash [32]byte
	for i := range blockHash {
		blockHash[i] = byte(i)
		setHash[i] = byte(i + 100)
	}

	return Checkpoint{
		CommitmentVersion: 1,
		Height:            800000,
		BlockHash:         chainhash.Hash(blockHash),
		SetHash:           setHash,
	}
}

func TestSignVerifyRoundTrip(t *testing.T) {
	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	sc, err := Sign(priv, sampleCheckpoint(), testNetMagic)
	require.NoError(t, err)

	require.NoError(t, sc.Verify(testNetMagic))
	require.NoError(t, sc.VerifyWithKey(priv.PubKey().Compressed(), testNetMagic))
}

func TestVerifyRejectsTamperedHeight(t *testing.T) {
	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	sc, err := Sign(priv, sampleCheckpoint(), testNetMagic)
	require.NoError(t, err)

	sc.Checkpoint.Height++

	require.Error(t, sc.Verify(testNetMagic))
}

func TestVerifyRejectsTamperedSetHash(t *testing.T) {
	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	sc, err := Sign(priv, sampleCheckpoint(), testNetMagic)
	require.NoError(t, err)

	sc.Checkpoint.SetHash[0] ^= 0xff

	require.Error(t, sc.Verify(testNetMagic))
}

func TestVerifyWithKeyRejectsUntrustedSigner(t *testing.T) {
	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	other, err := bec.NewPrivateKey()
	require.NoError(t, err)

	sc, err := Sign(priv, sampleCheckpoint(), testNetMagic)
	require.NoError(t, err)

	require.Error(t, sc.VerifyWithKey(other.PubKey().Compressed(), testNetMagic),
		"a valid signature from the wrong key must be rejected")
}

func TestSignedCheckpointRoundTrip(t *testing.T) {
	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	sc, err := Sign(priv, sampleCheckpoint(), testNetMagic)
	require.NoError(t, err)

	got, err := ParseSignedCheckpoint(sc.Serialize())
	require.NoError(t, err)

	require.Equal(t, sc.Checkpoint, got.Checkpoint)
	require.Equal(t, sc.PubKey, got.PubKey)
	require.Equal(t, sc.Sig, got.Sig)
	require.NoError(t, got.Verify(testNetMagic))
}

func TestParseSignedCheckpointRejectsTruncated(t *testing.T) {
	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	sc, err := Sign(priv, sampleCheckpoint(), testNetMagic)
	require.NoError(t, err)

	b := sc.Serialize()

	_, err = ParseSignedCheckpoint(b[:len(b)-1])
	require.Error(t, err)

	_, err = ParseSignedCheckpoint(b[:5])
	require.Error(t, err)
}

func TestVerifyRejectsTamperedCommitmentVersion(t *testing.T) {
	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	sc, err := Sign(priv, sampleCheckpoint(), testNetMagic)
	require.NoError(t, err)

	sc.Checkpoint.CommitmentVersion++

	require.Error(t, sc.Verify(testNetMagic),
		"the commitment version is part of the signed digest and must not be alterable")
}

func TestVerifyRejectsWrongNetwork(t *testing.T) {
	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	sc, err := Sign(priv, sampleCheckpoint(), testNetMagic)
	require.NoError(t, err)

	// Same key, same checkpoint, different network: must not verify.
	require.Error(t, sc.Verify(otherNetMagic),
		"a checkpoint signed for one network must not verify on another")
	require.Error(t, sc.VerifyWithKey(priv.PubKey().Compressed(), otherNetMagic),
		"trusted key is irrelevant if the network magic differs")

	// And it still verifies under its own network.
	require.NoError(t, sc.Verify(testNetMagic))
}

func TestSignDeterministic(t *testing.T) {
	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	c := sampleCheckpoint()

	a, err := Sign(priv, c, testNetMagic)
	require.NoError(t, err)

	b, err := Sign(priv, c, testNetMagic)
	require.NoError(t, err)

	require.Equal(t, a.Sig, b.Sig, "RFC6979 signatures must be deterministic")
}
