/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package testing

import "github.com/Shopify/sarama"

//
// Mock Sarama SyncProducer Implementation
//

var _ sarama.SyncProducer = &MockSyncProducer{}

type MockSyncProducer struct {
	producerMessages chan sarama.ProducerMessage
	offset           int64
	closed           bool
}

func NewMockSyncProducer() *MockSyncProducer {
	return &MockSyncProducer{
		producerMessages: make(chan sarama.ProducerMessage, 1),
		closed:           false,
	}
}

func (p *MockSyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	p.producerMessages <- *msg
	p.offset = p.offset + 1
	return 1, p.offset, nil
}

func (p *MockSyncProducer) SendMessages(_ []*sarama.ProducerMessage) error {
	// Not Currently In Use - No Need To Mock
	return nil
}

func (p *MockSyncProducer) GetMessage() sarama.ProducerMessage {
	return <-p.producerMessages
}

func (p *MockSyncProducer) Close() error {
	p.closed = true
	close(p.producerMessages)
	return nil
}

func (p *MockSyncProducer) Closed() bool {
	return p.closed
}
