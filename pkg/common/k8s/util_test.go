package k8s

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// Test The TruncateLabelValue() Functionality
func TestTruncateLabelValue(t *testing.T) {

	// Define The TestCase Struct
	type TestCase struct {
		Value  string
		Result string
	}

	// Create The TestCases
	testCases := []TestCase{
		{Value: "", Result: ""},
		{Value: "a", Result: "a"},
		{Value: "123456789012345678901234567890123456789012345678901234567890ab", Result: "123456789012345678901234567890123456789012345678901234567890ab"},    // 62 Characters
		{Value: "123456789012345678901234567890123456789012345678901234567890abc", Result: "123456789012345678901234567890123456789012345678901234567890abc"},  // 63 Characters
		{Value: "123456789012345678901234567890123456789012345678901234567890abcd", Result: "123456789012345678901234567890123456789012345678901234567890abc"}, // 64 Characters
	}

	// Run The TestCases
	for _, testCase := range testCases {
		result := TruncateLabelValue(testCase.Value)
		assert.Equal(t, result, testCase.Result)
	}
}
