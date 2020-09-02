package custom

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Test The Custom TopicDetail's ToSaramaTopicDetail() Functionality
func TestToSaramaTopicDetail(t *testing.T) {

	// Test Data
	const numPartitions = int32(123)
	const replicationFactor = int16(2)
	replicaAssignment := make(map[int32][]int32)
	configEntries := make(map[string]*string)

	// Create The Custom TopicDetail To Test (Populated)
	customTopicDetail := NewTopicDetail(numPartitions, replicationFactor, replicaAssignment, configEntries)

	// Perform The Test
	saramaTopicDetail := customTopicDetail.ToSaramaTopicDetail()

	// Verify The Results
	assert.NotNil(t, saramaTopicDetail)
	assert.Equal(t, customTopicDetail.NumPartitions, saramaTopicDetail.NumPartitions)
	assert.Equal(t, customTopicDetail.ReplicationFactor, saramaTopicDetail.ReplicationFactor)
	assert.Equal(t, customTopicDetail.ReplicaAssignment, saramaTopicDetail.ReplicaAssignment)
	assert.Equal(t, customTopicDetail.ConfigEntries, saramaTopicDetail.ConfigEntries)
}

// Test The Custom TopicDetail's FromSaramaTopicDetail() Functionality
func TestFromSaramaTopicDetail(t *testing.T) {

	// Test Data
	const numPartitions = int32(123)
	const replicationFactor = int16(2)
	replicaAssignment := make(map[int32][]int32)
	configEntries := make(map[string]*string)

	// The Sarama TopicDetail To Convert
	saramaTopicDetail := &sarama.TopicDetail{
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
		ReplicaAssignment: replicaAssignment,
		ConfigEntries:     configEntries,
	}

	// Create The Custom TopicDetail To Test (Empty)
	customTopicDetail := &TopicDetail{}

	// Perform The Test
	customTopicDetail.FromSaramaTopicDetail(saramaTopicDetail)

	// Verify The Results
	assert.Equal(t, saramaTopicDetail.NumPartitions, customTopicDetail.NumPartitions)
	assert.Equal(t, saramaTopicDetail.ReplicationFactor, customTopicDetail.ReplicationFactor)
	assert.Equal(t, saramaTopicDetail.ReplicaAssignment, customTopicDetail.ReplicaAssignment)
	assert.Equal(t, saramaTopicDetail.ConfigEntries, customTopicDetail.ConfigEntries)
}

// Test The Custom TopicDetail's FromSaramaTopicDetail() Functionality - Nil Use Case
func TestFromSaramaTopicDetailNil(t *testing.T) {

	// Test Data
	var nilReplicaAssignment map[int32][]int32
	var nilConfigEntries map[string]*string

	// Create The Custom TopicDetail To Test (Empty)
	customTopicDetail := &TopicDetail{}

	// Perform The Test
	customTopicDetail.FromSaramaTopicDetail(nil)
	// Verify The Results
	assert.Equal(t, int32(0), customTopicDetail.NumPartitions)
	assert.Equal(t, int16(0), customTopicDetail.ReplicationFactor)
	assert.Equal(t, nilReplicaAssignment, customTopicDetail.ReplicaAssignment)
	assert.Equal(t, nilConfigEntries, customTopicDetail.ConfigEntries)
}

// Test The JSON Advice Marshal/Unmarshal Of The TopicDetail Struct
func TestJsonMarshall(t *testing.T) {

	// Test Data
	const numPartitions = int32(123)
	const replicationFactor = int16(2)
	configEntries := make(map[string]*string)

	// Create The Custom TopicDetail To Test (Populated)
	customTopicDetail := NewTopicDetail(numPartitions, replicationFactor, nil, configEntries)

	// Perform The Tests
	actualResult, err := json.Marshal(customTopicDetail)

	// Create The Expected JSON String
	expectedResult := fmt.Sprintf("{\"numPartitions\":%d,\"replicationFactor\":%d}", numPartitions, replicationFactor)

	// Verify The JSON Marshal Results
	assert.NotNil(t, actualResult)
	assert.Nil(t, err)
	assert.Equal(t, expectedResult, string(actualResult))

	// Unmarshal The Json Bytes Back Into A TopicDetail
	customTopicDetail = &TopicDetail{}
	err = json.Unmarshal(actualResult, customTopicDetail)

	// Verify The JSON Unmarshal Results
	assert.Nil(t, err)
	assert.Equal(t, numPartitions, customTopicDetail.NumPartitions)
	assert.Equal(t, replicationFactor, customTopicDetail.ReplicationFactor)
	assert.Nil(t, customTopicDetail.ReplicaAssignment)
	assert.Nil(t, customTopicDetail.ConfigEntries)
}
