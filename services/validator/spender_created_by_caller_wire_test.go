package validator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The legacy block path can run against a remote validator. The flag that
// switches the missing-parent bless off has to survive both transports, or a
// remote validator blesses a replay of a fully pruned chain on the strength of
// the record the block path just wrote.
func TestOptionsFromValidateRequest_SpenderCreatedByCallerRoundTrip(t *testing.T) {
	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 620000, &Options{SpenderCreatedByCaller: true})
	got, err := optionsFromValidateRequest(req)
	require.NoError(t, err)
	require.True(t, got.SpenderCreatedByCaller, "SpenderCreatedByCaller must survive the gRPC round-trip")

	req = buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 620000, &Options{SkipPolicyChecks: true})
	got, err = optionsFromValidateRequest(req)
	require.NoError(t, err)
	require.False(t, got.SpenderCreatedByCaller, "SpenderCreatedByCaller must default to false")
}

// IgnoreLocked became load-bearing on the legacy catchup path this round: the
// create phase writes every transaction of a block locked, so a child spending
// an in-block parent must be allowed past that lock. Without the field a remote
// validator answers TX_LOCKED and catchup wedges on any block with an
// intra-block parent chain.
func TestOptionsFromValidateRequest_IgnoreLockedRoundTrip(t *testing.T) {
	req := buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 620000, &Options{IgnoreLocked: true})
	got, err := optionsFromValidateRequest(req)
	require.NoError(t, err)
	require.True(t, got.IgnoreLocked, "IgnoreLocked must survive the gRPC round-trip")

	req = buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 620000, &Options{SkipPolicyChecks: true})
	got, err = optionsFromValidateRequest(req)
	require.NoError(t, err)
	require.False(t, got.IgnoreLocked, "IgnoreLocked must default to false: a mempool submitter must never bypass a lock")
}

// The HTTP fallback carries transaction bytes only, so a block-path request that
// sets either flag must be refused before anything is sent. Stripping the flag
// would be worse than failing: without SpenderCreatedByCaller a remote validator
// blesses a replay on the strength of the record the block path just wrote.
func TestValidateTransactionViaHTTP_RefusesBlockPathFlags(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*Options)
	}{
		{"spenderCreatedByCaller", func(o *Options) { o.SpenderCreatedByCaller = true }},
		{"ignoreLocked", func(o *Options) { o.IgnoreLocked = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, calls, _ := countingValidatorStub(t)
			client := &Client{validatorHTTPAddr: addr, logger: &testLogger{t: t}}

			opts := NewDefaultOptions()
			tc.set(opts)

			err := client.validateTransactionViaHTTP(context.Background(), createTestTransaction(t), 0, opts)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.name, "the error must name the field that cannot be carried")
			require.Equal(t, int64(0), calls.Load(), "the refusal must happen before the request is sent")
		})
	}
}
