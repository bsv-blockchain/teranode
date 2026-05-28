package validator

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestSelectFinalityComparisonTime pins the per-context wiring documented in
// selectFinalityComparisonTime's doc-comment: policy / next-tip uses tip MTP
// in all eras; pre-CSV consensus uses Options.CandidateBlockTime when supplied
// and skips otherwise; the at-CSV boundary skips; post-CSV consensus uses tip
// MTP as a placeholder until candidate-parent MTP plumbing lands.
func TestSelectFinalityComparisonTime(t *testing.T) {
	const csvHeight uint32 = 1000

	type want struct {
		comparisonTime uint32
		skipFinality   bool
		err            bool
	}
	tests := []struct {
		name        string
		opts        *Options
		blockHeight uint32
		blockState  utxo.BlockState
		want        want
	}{
		// --- Policy mode: tip MTP in all eras ---
		{
			name:        "policy_uses_tip_mtp_pre_csv",
			opts:        &Options{SkipPolicyChecks: false},
			blockHeight: csvHeight - 100,
			blockState:  utxo.BlockState{MedianTime: 1700000000},
			want:        want{comparisonTime: 1700000000, skipFinality: false},
		},
		{
			name:        "policy_uses_tip_mtp_post_csv",
			opts:        &Options{SkipPolicyChecks: false},
			blockHeight: csvHeight + 100,
			blockState:  utxo.BlockState{MedianTime: 1700000000},
			want:        want{comparisonTime: 1700000000, skipFinality: false},
		},
		{
			name:        "policy_fails_when_tip_mtp_zero",
			opts:        &Options{SkipPolicyChecks: false},
			blockHeight: csvHeight - 100,
			blockState:  utxo.BlockState{MedianTime: 0},
			want:        want{err: true},
		},
		// --- Pre-CSV consensus: caller-supplied candidate block time ---
		{
			name:        "preCSV_consensus_uses_candidate_block_time",
			opts:        &Options{SkipPolicyChecks: true, CandidateBlockTime: 1234567890},
			blockHeight: csvHeight - 1,
			blockState:  utxo.BlockState{MedianTime: 1700000000}, // must NOT be used
			want:        want{comparisonTime: 1234567890, skipFinality: false},
		},
		{
			name:        "preCSV_consensus_skips_when_candidate_block_time_zero",
			opts:        &Options{SkipPolicyChecks: true, CandidateBlockTime: 0},
			blockHeight: csvHeight - 1,
			blockState:  utxo.BlockState{MedianTime: 1700000000},
			want:        want{skipFinality: true},
		},
		// --- At-CSV consensus: skip (BIP113 boundary, candidate-parent MTP not wired) ---
		{
			name:        "atCSV_consensus_skips_even_when_candidate_block_time_set",
			opts:        &Options{SkipPolicyChecks: true, CandidateBlockTime: 1234567890},
			blockHeight: csvHeight,
			blockState:  utxo.BlockState{MedianTime: 1700000000},
			want:        want{skipFinality: true},
		},
		// --- Post-CSV consensus: tip MTP (placeholder for candidate-parent MTP) ---
		{
			name:        "postCSV_consensus_uses_tip_mtp_placeholder",
			opts:        &Options{SkipPolicyChecks: true, CandidateBlockTime: 1234567890},
			blockHeight: csvHeight + 1,
			blockState:  utxo.BlockState{MedianTime: 1700000000},
			want:        want{comparisonTime: 1700000000, skipFinality: false},
		},
		{
			name:        "postCSV_consensus_fails_when_tip_mtp_zero",
			opts:        &Options{SkipPolicyChecks: true},
			blockHeight: csvHeight + 1,
			blockState:  utxo.BlockState{MedianTime: 0},
			want:        want{err: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct, skip, err := selectFinalityComparisonTime(tt.opts, tt.blockHeight, csvHeight, tt.blockState)
			if tt.want.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want.skipFinality, skip)
			require.Equal(t, tt.want.comparisonTime, ct)
		})
	}
}
