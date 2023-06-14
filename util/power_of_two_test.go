package util

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsPowerOf2(t *testing.T) {
	// Testing the function
	numbers := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 1048576, 70368744177664}
	for _, num := range numbers {
		assert.True(t, IsPowerOfTwo(num), fmt.Sprintf("%d should be a power of 2", num))
	}

	numbers = []int{-1, 0, 41, 13}
	for _, num := range numbers {
		assert.False(t, IsPowerOfTwo(num), fmt.Sprintf("%d should be a power of 2", num))
	}

}
