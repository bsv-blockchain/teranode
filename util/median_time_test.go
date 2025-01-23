package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalcFourMedianTimes(t *testing.T) {
	timestamps := []int64{
		1231470988,
		1231470173,
		1231469744,
		1231469665,
		1231006505,
	}

	expected := int64(1231469744)

	medianTime, err := CalcPastMedianTime(timestamps)
	require.NoError(t, err)

	assert.Equal(t, expected, medianTime)
}

func TestCalcElevenMedianTime(t *testing.T) {
	timestamps := []int64{
		1686609209,
		1686608789,
		1686608129,
		1686606869,
		1686606449,
		1686603569,
		1686603509,
		1686603449,
		1686603089,
		1686601469,
		1686600089,
	}

	expected := int64(1686603569)

	medianTime, err := CalcPastMedianTime(timestamps)
	require.NoError(t, err)

	assert.Equal(t, expected, medianTime)
}

func TestTwelveMedianTime(t *testing.T) {
	timestamps := []int64{
		1686609209,
		1686608789,
		1686608129,
		1686606869,
		1686606449,
		1686603569,
		1686603509,
		1686603449,
		1686603089,
		1686601469,
		1686600089,
		1686600000,
	}

	_, err := CalcPastMedianTime(timestamps)
	require.Error(t, err, "too many timestamps for median time calculation")
}
