package validator

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestSelectFinalityComparisonTime pins the per-context wiring documented in
// selectFinalityComparisonTime's doc-comment: policy / next-tip uses tip MTP
// in all eras; pre-CSV consensus uses Options.CandidateBlockTime when supplied
// and skips otherwise; post-CSV consensus (blockHeight >= CSVHeight) uses
// Options.CandidateParentMedianTime when supplied. Block-validation callers
// are required to always populate it; the tip-MTP fall-back below it is
// reserved for callers with no parent-chain context at all (peer-facing
// CheckSubtree). The fall-back cases in this table cover that
// CheckSubtree-like contract — they are NOT a sanctioned path for the
// mainline block-validation flow.
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
		// --- At-CSV consensus (BIP113 activation height) ---
		// CandidateBlockTime is the pre-CSV-only field, so even when it is set
		// (e.g. caller hasn't migrated) the post-CSV switch arm ignores it and
		// soft-falls to tip MTP via blockState.MedianTime.
		{
			name:        "atCSV_consensus_ignores_pre_csv_field_softfalls_to_tip_mtp",
			opts:        &Options{SkipPolicyChecks: true, CandidateBlockTime: 1234567890},
			blockHeight: csvHeight,
			blockState:  utxo.BlockState{MedianTime: 1700000000},
			want:        want{comparisonTime: 1700000000, skipFinality: false},
		},
		{
			name:        "atCSV_consensus_uses_candidate_parent_median_time_when_supplied",
			opts:        &Options{SkipPolicyChecks: true, CandidateParentMedianTime: 1699999000},
			blockHeight: csvHeight,
			blockState:  utxo.BlockState{MedianTime: 1700000000}, // ignored when parent MTP supplied
			want:        want{comparisonTime: 1699999000, skipFinality: false},
		},
		// --- Post-CSV consensus: candidate-parent MTP preferred, soft-fall to tip MTP ---
		{
			name:        "postCSV_consensus_uses_candidate_parent_median_time_when_supplied",
			opts:        &Options{SkipPolicyChecks: true, CandidateParentMedianTime: 1699999000},
			blockHeight: csvHeight + 1,
			blockState:  utxo.BlockState{MedianTime: 1700000000}, // ignored when parent MTP supplied
			want:        want{comparisonTime: 1699999000, skipFinality: false},
		},
		{
			name:        "postCSV_consensus_softfalls_to_tip_mtp_when_parent_mtp_zero",
			opts:        &Options{SkipPolicyChecks: true},
			blockHeight: csvHeight + 1,
			blockState:  utxo.BlockState{MedianTime: 1700000000},
			want:        want{comparisonTime: 1700000000, skipFinality: false},
		},
		{
			name:        "postCSV_consensus_fails_when_both_parent_mtp_and_tip_mtp_zero",
			opts:        &Options{SkipPolicyChecks: true},
			blockHeight: csvHeight + 1,
			blockState:  utxo.BlockState{MedianTime: 0},
			want:        want{err: true},
		},
		{
			name:        "postCSV_consensus_pre_csv_field_does_not_affect_post_csv_path",
			opts:        &Options{SkipPolicyChecks: true, CandidateBlockTime: 1234567890},
			blockHeight: csvHeight + 1,
			blockState:  utxo.BlockState{MedianTime: 1700000000},
			want:        want{comparisonTime: 1700000000, skipFinality: false},
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
