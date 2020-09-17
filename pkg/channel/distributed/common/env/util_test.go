package env

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	logtesting "knative.dev/pkg/logging/testing"
)

const (
	TestBoolEnvKey       = "TEST_BOOL_KEY"
	TestBoolDefaultValue = "true"
	TestBoolNewValue     = "false"
	TestBoolInvalidValue = "not_bool"
	TestBoolEnvName      = "TestBoolKey"

	TestInt64EnvKey       = "TEST_INT64_KEY"
	TestInt64DefaultValue = "1234567890123"
	TestInt64NewValue     = "2345678901234"
	TestInt64InvalidValue = "not_int64"
	TestInt64EnvName      = "TestInt64Key"

	TestInt32EnvKey       = "TEST_INT32_KEY"
	TestInt32NewValue     = "32"
	TestInt32InvalidValue = "not_int32"
	TestInt32EnvName      = "TestInt32Key"

	TestInt16EnvKey       = "TEST_INT16_KEY"
	TestInt16NewValue     = "16"
	TestInt16InvalidValue = "not_int16"
	TestInt16EnvName      = "TestInt16Key"

	TestIntEnvKey       = "TEST_INT_KEY"
	TestIntNewValue     = "2345678"
	TestIntInvalidValue = "not_int"
	TestIntEnvName      = "TestIntKey"

	TestQuantityEnvKey       = "TEST_QUANTITY_KEY"
	TestQuantityNewValue     = "100m"
	TestQuantityInvalidValue = "not_quantity"
)

func assertEqualNoErr(t *testing.T, err error, expected, actual interface{}) {
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}

func assertErr(t *testing.T, expected string, err error) {
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), expected)
}

func TestGetOptionalConfigBool(t *testing.T) {
	logger := logtesting.TestLogger(t).Desugar()

	// Should return the default value for an empty variable
	os.Clearenv()
	result, err := GetOptionalConfigBool(logger, TestBoolEnvKey, TestBoolDefaultValue, TestBoolEnvName)
	assertEqualNoErr(t, err, strconv.FormatBool(result), TestBoolDefaultValue)

	// Should obtain the value from the environment
	_ = os.Setenv(TestBoolEnvKey, TestBoolNewValue)
	result, err = GetOptionalConfigBool(logger, TestBoolEnvKey, TestBoolDefaultValue, TestBoolEnvName)
	assertEqualNoErr(t, err, strconv.FormatBool(result), TestBoolNewValue)

	// Should return an error for an invalid value
	_ = os.Setenv(TestBoolEnvKey, TestBoolInvalidValue)
	result, err = GetOptionalConfigBool(logger, TestBoolEnvKey, TestBoolDefaultValue, TestBoolEnvName)
	assertErr(t, fmt.Sprintf("invalid (non boolean) value '%v' for environment variable '%v'", TestBoolInvalidValue, TestBoolEnvKey), err)
}

func TestGetConfigBool(t *testing.T) {
	logger := logtesting.TestLogger(t).Desugar()

	// Should return an error for an empty value
	os.Clearenv()
	result, exists, err := GetRequiredConfigBool(logger, TestBoolEnvKey, TestBoolEnvName)
	assertErr(t, fmt.Sprintf("missing required environment variable '%s'", TestBoolEnvKey), err)
	assert.False(t, exists)

	// Should obtain the value from the environment
	_ = os.Setenv(TestBoolEnvKey, TestBoolNewValue)
	result, exists, err = GetRequiredConfigBool(logger, TestBoolEnvKey, TestBoolEnvName)
	assertEqualNoErr(t, err, strconv.FormatBool(result), TestBoolNewValue)
	assert.True(t, exists)

	// Should return an error for an invalid value
	_ = os.Setenv(TestBoolEnvKey, TestBoolInvalidValue)
	result, exists, err = GetRequiredConfigBool(logger, TestBoolEnvKey, TestBoolEnvName)
	assertErr(t, fmt.Sprintf("invalid (non boolean) value '%v' for environment variable '%v'", TestBoolInvalidValue, TestBoolEnvKey), err)
	assert.False(t, exists)
}

func TestGetConfigInt(t *testing.T) {
	logger := logtesting.TestLogger(t).Desugar()

	// Should return an error for an empty value
	os.Clearenv()
	result, err := GetRequiredConfigInt(logger, TestIntEnvKey, TestIntEnvName)
	assertErr(t, fmt.Sprintf("missing required environment variable '%s'", TestIntEnvKey), err)

	// Should obtain the value from the environment
	_ = os.Setenv(TestIntEnvKey, TestIntNewValue)
	result, err = GetRequiredConfigInt(logger, TestIntEnvKey, TestIntEnvName)
	assertEqualNoErr(t, err, strconv.Itoa(result), TestIntNewValue)

	// Should return an error for an invalid value
	_ = os.Setenv(TestIntEnvKey, TestIntInvalidValue)
	result, err = GetRequiredConfigInt(logger, TestIntEnvKey, TestIntEnvName)
	assertErr(t, fmt.Sprintf("invalid (non int) value '%v' for environment variable '%v'", TestIntInvalidValue, TestIntEnvKey), err)
}

func TestGetConfigInt64(t *testing.T) {
	logger := logtesting.TestLogger(t).Desugar()

	// Should return an error for an empty value
	os.Clearenv()
	result, err := GetRequiredConfigInt64(logger, TestInt64EnvKey, TestInt64EnvName)
	assertErr(t, fmt.Sprintf("missing required environment variable '%s'", TestInt64EnvKey), err)

	// Should obtain the value from the environment
	_ = os.Setenv(TestInt64EnvKey, TestInt64NewValue)
	result, err = GetRequiredConfigInt64(logger, TestInt64EnvKey, TestInt64EnvName)
	assertEqualNoErr(t, err, strconv.FormatInt(result, 10), TestInt64NewValue)

	// Should return an error for an invalid value
	_ = os.Setenv(TestInt64EnvKey, TestInt64InvalidValue)
	result, err = GetRequiredConfigInt64(logger, TestInt64EnvKey, TestInt64EnvName)
	assertErr(t, fmt.Sprintf("invalid (non int64) value '%v' for environment variable '%v'", TestInt64InvalidValue, TestInt64EnvKey), err)
}

func TestGetConfigInt32(t *testing.T) {
	logger := logtesting.TestLogger(t).Desugar()

	// Should return an error for an empty value
	os.Clearenv()
	result, err := GetRequiredConfigInt32(logger, TestInt32EnvKey, TestInt32EnvName)
	assertErr(t, fmt.Sprintf("missing required environment variable '%s'", TestInt32EnvKey), err)

	// Should obtain the value from the environment
	_ = os.Setenv(TestInt32EnvKey, TestInt32NewValue)
	result, err = GetRequiredConfigInt32(logger, TestInt32EnvKey, TestInt32EnvName)
	assertEqualNoErr(t, err, strconv.Itoa(int(result)), TestInt32NewValue)

	// Should return an error for an invalid value
	_ = os.Setenv(TestInt32EnvKey, TestInt32InvalidValue)
	result, err = GetRequiredConfigInt32(logger, TestInt32EnvKey, TestInt32EnvName)
	assertErr(t, fmt.Sprintf("invalid (non int32) value '%v' for environment variable '%v'", TestInt32InvalidValue, TestInt32EnvKey), err)
}

func TestGetConfigInt16(t *testing.T) {
	logger := logtesting.TestLogger(t).Desugar()

	// Should return an error for an empty value
	os.Clearenv()
	result, err := GetRequiredConfigInt16(logger, TestInt16EnvKey, TestInt16EnvName)
	assertErr(t, fmt.Sprintf("missing required environment variable '%s'", TestInt16EnvKey), err)

	// Should obtain the value from the environment
	_ = os.Setenv(TestInt16EnvKey, TestInt16NewValue)
	result, err = GetRequiredConfigInt16(logger, TestInt16EnvKey, TestInt16EnvName)
	assertEqualNoErr(t, err, strconv.Itoa(int(result)), TestInt16NewValue)

	// Should return an error for an invalid value
	_ = os.Setenv(TestInt16EnvKey, TestInt16InvalidValue)
	result, err = GetRequiredConfigInt16(logger, TestInt16EnvKey, TestInt16EnvName)
	assertErr(t, fmt.Sprintf("invalid (non int16) value '%v' for environment variable '%v'", TestInt16InvalidValue, TestInt16EnvKey), err)
}

func TestGetOptionalConfigInt64(t *testing.T) {
	logger := logtesting.TestLogger(t).Desugar()

	// Should return the default value for an empty variable
	os.Clearenv()
	result, err := GetOptionalConfigInt64(logger, TestInt64EnvKey, TestInt64DefaultValue, TestInt64EnvName)
	assertEqualNoErr(t, err, strconv.FormatInt(result, 10), TestInt64DefaultValue)

	// Should obtain the value from the environment
	_ = os.Setenv(TestInt64EnvKey, TestInt64NewValue)
	result, err = GetOptionalConfigInt64(logger, TestInt64EnvKey, TestInt64DefaultValue, TestInt64EnvName)
	assertEqualNoErr(t, err, strconv.FormatInt(result, 10), TestInt64NewValue)

	// Should return an error for an invalid value
	_ = os.Setenv(TestInt64EnvKey, TestInt64InvalidValue)
	result, err = GetOptionalConfigInt64(logger, TestInt64EnvKey, TestInt64DefaultValue, TestInt64EnvName)
	assertErr(t, fmt.Sprintf("invalid (non int64) value '%v' for environment variable '%v'", TestInt64InvalidValue, TestInt64EnvKey), err)
}

func TestGetRequiredQuantityConfigValue(t *testing.T) {
	logger := logtesting.TestLogger(t).Desugar()

	// Should return an error for an empty value
	os.Clearenv()
	result, err := GetRequiredQuantityConfigValue(logger, TestQuantityEnvKey)
	assertErr(t, fmt.Sprintf("missing required environment variable '%s'", TestQuantityEnvKey), err)

	// Should obtain the value from the environment
	_ = os.Setenv(TestQuantityEnvKey, TestQuantityNewValue)
	result, err = GetRequiredQuantityConfigValue(logger, TestQuantityEnvKey)
	quantity, _ := resource.ParseQuantity(TestQuantityNewValue)
	assertEqualNoErr(t, err, result, &quantity)

	// Should return an error for an invalid value
	_ = os.Setenv(TestQuantityEnvKey, TestQuantityInvalidValue)
	result, err = GetRequiredQuantityConfigValue(logger, TestQuantityEnvKey)
	assertErr(t, fmt.Sprintf("invalid (non quantity) value '%v' for environment variable '%v'", TestQuantityInvalidValue, TestQuantityEnvKey), err)
}
