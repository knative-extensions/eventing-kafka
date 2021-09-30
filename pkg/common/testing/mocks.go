package testing

import "github.com/Shopify/sarama"

var _ sarama.ClusterAdmin = (*MockClusterAdmin)(nil)

type MockClusterAdmin struct {
	MockCreateTopicFunc        func(topic string, detail *sarama.TopicDetail, validateOnly bool) error
	MockDeleteTopicFunc        func(topic string) error
	MockListConsumerGroupsFunc func() (map[string]string, error)
}

func (ca *MockClusterAdmin) AlterPartitionReassignments(topic string, assignment [][]int32) error {
	return nil
}

func (ca *MockClusterAdmin) ListPartitionReassignments(topics string, partitions []int32) (topicStatus map[string]map[int32]*sarama.PartitionReplicaReassignmentsStatus, err error) {
	return nil, nil
}

func (ca *MockClusterAdmin) DescribeLogDirs(brokers []int32) (map[int32][]sarama.DescribeLogDirsResponseDirMetadata, error) {
	return nil, nil
}

func (ca *MockClusterAdmin) DescribeUserScramCredentials(users []string) ([]*sarama.DescribeUserScramCredentialsResult, error) {
	return nil, nil
}

func (ca *MockClusterAdmin) DeleteUserScramCredentials(delete []sarama.AlterUserScramCredentialsDelete) ([]*sarama.AlterUserScramCredentialsResult, error) {
	return nil, nil
}

func (ca *MockClusterAdmin) UpsertUserScramCredentials(upsert []sarama.AlterUserScramCredentialsUpsert) ([]*sarama.AlterUserScramCredentialsResult, error) {
	return nil, nil
}

func (ca *MockClusterAdmin) CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
	if ca.MockCreateTopicFunc != nil {
		return ca.MockCreateTopicFunc(topic, detail, validateOnly)
	}
	return nil
}

func (ca *MockClusterAdmin) Close() error {
	return nil
}

func (ca *MockClusterAdmin) DeleteTopic(topic string) error {
	if ca.MockDeleteTopicFunc != nil {
		return ca.MockDeleteTopicFunc(topic)
	}
	return nil
}

func (ca *MockClusterAdmin) DescribeTopics(topics []string) (metadata []*sarama.TopicMetadata, err error) {
	return nil, nil
}

func (ca *MockClusterAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	return nil, nil
}

func (ca *MockClusterAdmin) CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error {
	return nil
}

func (ca *MockClusterAdmin) DeleteRecords(topic string, partitionOffsets map[int32]int64) error {
	return nil
}

func (ca *MockClusterAdmin) DescribeConfig(resource sarama.ConfigResource) ([]sarama.ConfigEntry, error) {
	return nil, nil
}

func (ca *MockClusterAdmin) AlterConfig(resourceType sarama.ConfigResourceType, name string, entries map[string]*string, validateOnly bool) error {
	return nil
}

func (ca *MockClusterAdmin) CreateACL(resource sarama.Resource, acl sarama.Acl) error {
	return nil
}

func (ca *MockClusterAdmin) ListAcls(filter sarama.AclFilter) ([]sarama.ResourceAcls, error) {
	return nil, nil
}

func (ca *MockClusterAdmin) DeleteACL(filter sarama.AclFilter, validateOnly bool) ([]sarama.MatchingAcl, error) {
	return nil, nil
}

func (ca *MockClusterAdmin) ListConsumerGroups() (map[string]string, error) {
	if ca.MockListConsumerGroupsFunc != nil {
		return ca.MockListConsumerGroupsFunc()
	}
	return nil, nil
}

func (ca *MockClusterAdmin) DescribeConsumerGroups(groups []string) ([]*sarama.GroupDescription, error) {
	return nil, nil
}

func (ca *MockClusterAdmin) ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	return &sarama.OffsetFetchResponse{}, nil
}

func (ca *MockClusterAdmin) DescribeCluster() (brokers []*sarama.Broker, controllerID int32, err error) {
	return nil, 0, nil
}

// Delete a consumer group.
func (ca *MockClusterAdmin) DeleteConsumerGroup(group string) error {
	return nil
}
