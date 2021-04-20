package kafka

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ctrl "knative.dev/control-protocol/pkg"
	ctrlmessage "knative.dev/control-protocol/pkg/message"
	ctrlnetwork "knative.dev/control-protocol/pkg/network"
	consumertesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/consumer/testing"
	"knative.dev/eventing-kafka/pkg/common/consumer"
	kafkasourcecontrol "knative.dev/eventing-kafka/pkg/source/control"
)

func TestControlledAdapter_Start_And_Send_Contract(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var invokedConsume sync.WaitGroup
	invokedConsume.Add(1)
	mockConsumerGroupInstance := &mockConsumerGroup{
		MockConsumerGroup: consumertesting.NewMockConsumerGroup(),
		invokedConsume:    &invokedConsume,
	}

	a := NewControlledAdapter(ctx, NewEnvConfig(), nil, nil).(*ControlledAdapter)
	a.newConsumerGroupFactory = func(addrs []string, config *sarama.Config) consumer.KafkaConsumerGroupFactory {
		return &kafkaConsumerGroupFactory{
			mockConsumerGroup: mockConsumerGroupInstance,
		}
	}
	a.controlServerOptions = []ctrlnetwork.ControlServerOption{ctrlnetwork.WithPort(10000)}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, a.Start(ctx))
	}()

	// Create the control service emulating the reconciler
	svc, err := ctrlnetwork.StartControlClient(ctx, &net.Dialer{}, "localhost:10000")
	require.NoError(t, err)

	// Record incoming messages
	var receivedMsgWg sync.WaitGroup
	receivedMsgWg.Add(1)
	var receivedMsgs []ctrl.ServiceMessage
	svc.MessageHandler(ctrl.MessageHandlerFunc(func(ctx context.Context, message ctrl.ServiceMessage) {
		receivedMsgs = append(receivedMsgs, message)
		message.Ack()

		// Let the ack propagate.
		// If it doesn't, it logs an error because context is cancelled before (although this is not a problem)
		time.Sleep(100 * time.Millisecond)
		receivedMsgWg.Done()
	}))

	// Send the contract
	expectedConsumerGroup := "myconsumergroup"
	require.NoError(t, svc.SendAndWaitForAck(kafkasourcecontrol.SetContractCommand, kafkasourcecontrol.KafkaSourceContract{
		Generation:       1,
		BootstrapServers: []string{"localhost"},
		Topics:           []string{"mytopic"},
		ConsumerGroup:    expectedConsumerGroup,
		KeyType:          "",
	}))

	// Wait for the async notification
	receivedMsgWg.Wait()
	require.Len(t, receivedMsgs, 1)
	receivedMessage := receivedMsgs[0]
	require.Equal(t, uint8(kafkasourcecontrol.NotifyContractUpdated), receivedMessage.Headers().OpCode())
	res, err := ctrlmessage.ParseAsyncCommandResult(receivedMessage.Payload())
	require.NoError(t, err)
	require.Empty(t, res.(ctrlmessage.AsyncCommandResult).Error)
	require.Equal(t, ctrlmessage.Int64CommandId(1), res.(ctrlmessage.AsyncCommandResult).CommandId)

	// Consume should have been invoked at this point
	invokedConsume.Wait()

	// Check consumer group started with the proper consumer group name
	require.Equal(t, expectedConsumerGroup, a.GetConsumerGroup())

	cancel()
	wg.Wait()

	require.True(t, mockConsumerGroupInstance.Closed)
}

func TestControlledAdapter_Start_And_Send_Contract_Twice(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var invokedConsume sync.WaitGroup
	invokedConsume.Add(1)
	mockConsumerGroupInstance := &mockConsumerGroup{
		MockConsumerGroup: consumertesting.NewMockConsumerGroup(),
		invokedConsume:    &invokedConsume,
	}

	a := NewControlledAdapter(ctx, NewEnvConfig(), nil, nil).(*ControlledAdapter)
	a.newConsumerGroupFactory = func(addrs []string, config *sarama.Config) consumer.KafkaConsumerGroupFactory {
		return &kafkaConsumerGroupFactory{
			mockConsumerGroup: mockConsumerGroupInstance,
		}
	}
	a.controlServerOptions = []ctrlnetwork.ControlServerOption{ctrlnetwork.WithPort(10000)}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, a.Start(ctx))
	}()

	// Create the control service emulating the reconciler
	svc, err := ctrlnetwork.StartControlClient(ctx, &net.Dialer{}, "localhost:10000")
	require.NoError(t, err)

	// Record incoming messages
	var receivedMsgWg sync.WaitGroup
	receivedMsgWg.Add(2)
	var receivedMsgs []ctrl.ServiceMessage
	svc.MessageHandler(ctrl.MessageHandlerFunc(func(ctx context.Context, message ctrl.ServiceMessage) {
		receivedMsgs = append(receivedMsgs, message)
		message.Ack()

		// Let the ack propagate.
		// If it doesn't, it logs an error because context is cancelled before (although this is not a problem)
		time.Sleep(100 * time.Millisecond)
		receivedMsgWg.Done()
	}))

	// Send the contract twice
	expectedConsumerGroup := "myconsumergroup"
	require.NoError(t, svc.SendAndWaitForAck(kafkasourcecontrol.SetContractCommand, kafkasourcecontrol.KafkaSourceContract{
		Generation:       1,
		BootstrapServers: []string{"localhost"},
		Topics:           []string{"mytopic"},
		ConsumerGroup:    expectedConsumerGroup,
		KeyType:          "",
	}))
	require.NoError(t, svc.SendAndWaitForAck(kafkasourcecontrol.SetContractCommand, kafkasourcecontrol.KafkaSourceContract{
		Generation:       1,
		BootstrapServers: []string{"localhost"},
		Topics:           []string{"mytopic"},
		ConsumerGroup:    expectedConsumerGroup,
		KeyType:          "",
	}))

	// Wait for the async notification
	receivedMsgWg.Wait()
	require.Len(t, receivedMsgs, 2)
	require.Equal(t, receivedMsgs[0].Headers().OpCode(), receivedMsgs[1].Headers().OpCode())
	require.Equal(t, receivedMsgs[0].Payload(), receivedMsgs[1].Payload())

	receivedMessage := receivedMsgs[0]
	require.Equal(t, uint8(kafkasourcecontrol.NotifyContractUpdated), receivedMessage.Headers().OpCode())
	res, err := ctrlmessage.ParseAsyncCommandResult(receivedMessage.Payload())
	require.NoError(t, err)
	require.Empty(t, res.(ctrlmessage.AsyncCommandResult).Error)
	require.Equal(t, ctrlmessage.Int64CommandId(1), res.(ctrlmessage.AsyncCommandResult).CommandId)

	// Consume should have been invoked at this point
	invokedConsume.Wait()

	// Check consumer group started with the proper consumer group name
	require.Equal(t, expectedConsumerGroup, a.GetConsumerGroup())

	cancel()
	wg.Wait()

	require.True(t, mockConsumerGroupInstance.Closed)
}

func TestControlledAdapter_Start_Then_Update(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var invokedConsume sync.WaitGroup
	invokedConsume.Add(2)
	mockConsumerGroupInstance := &mockConsumerGroup{
		invokedConsume: &invokedConsume,
	}

	a := NewControlledAdapter(ctx, NewEnvConfig(), nil, nil).(*ControlledAdapter)
	a.newConsumerGroupFactory = func(addrs []string, config *sarama.Config) consumer.KafkaConsumerGroupFactory {
		return &kafkaConsumerGroupFactory{
			mockConsumerGroup: mockConsumerGroupInstance,
		}
	}
	a.controlServerOptions = []ctrlnetwork.ControlServerOption{ctrlnetwork.WithPort(10000)}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, a.Start(ctx))
	}()

	// Create the control service emulating the reconciler
	svc, err := ctrlnetwork.StartControlClient(ctx, &net.Dialer{}, "localhost:10000")
	require.NoError(t, err)

	// Record incoming messages
	var receivedMsgWg sync.WaitGroup
	receivedMsgWg.Add(2)
	var receivedMsgs []ctrl.ServiceMessage
	svc.MessageHandler(ctrl.MessageHandlerFunc(func(ctx context.Context, message ctrl.ServiceMessage) {
		receivedMsgs = append(receivedMsgs, message)
		message.Ack()

		// Let the ack propagate.
		// If it doesn't, it logs an error because context is cancelled before (although this is not a problem)
		time.Sleep(100 * time.Millisecond)
		receivedMsgWg.Done()
	}))

	// Send the contracts
	require.NoError(t, svc.SendAndWaitForAck(kafkasourcecontrol.SetContractCommand, kafkasourcecontrol.KafkaSourceContract{
		Generation:       1,
		BootstrapServers: []string{"localhost"},
		Topics:           []string{"mytopic"},
		ConsumerGroup:    "myconsumergroup",
		KeyType:          "",
	}))
	expectedConsumerGroup := "anotherconsumergroup"
	require.NoError(t, svc.SendAndWaitForAck(kafkasourcecontrol.SetContractCommand, kafkasourcecontrol.KafkaSourceContract{
		Generation:       2,
		BootstrapServers: []string{"localhost"},
		Topics:           []string{"anothertopic"},
		ConsumerGroup:    expectedConsumerGroup,
		KeyType:          "",
	}))

	// Wait for the async notification
	receivedMsgWg.Wait()
	require.Len(t, receivedMsgs, 2)

	firstContractUpdatedMessage := receivedMsgs[0]
	require.Equal(t, uint8(kafkasourcecontrol.NotifyContractUpdated), firstContractUpdatedMessage.Headers().OpCode())
	res, err := ctrlmessage.ParseAsyncCommandResult(firstContractUpdatedMessage.Payload())
	require.NoError(t, err)
	require.Empty(t, res.(ctrlmessage.AsyncCommandResult).Error)
	require.Equal(t, ctrlmessage.Int64CommandId(1), res.(ctrlmessage.AsyncCommandResult).CommandId)

	secondContractUpdatedMessage := receivedMsgs[1]
	require.Equal(t, uint8(kafkasourcecontrol.NotifyContractUpdated), secondContractUpdatedMessage.Headers().OpCode())
	res, err = ctrlmessage.ParseAsyncCommandResult(secondContractUpdatedMessage.Payload())
	require.NoError(t, err)
	require.Empty(t, res.(ctrlmessage.AsyncCommandResult).Error)
	require.Equal(t, ctrlmessage.Int64CommandId(2), res.(ctrlmessage.AsyncCommandResult).CommandId)

	// Consume should have been invoked at this point
	invokedConsume.Wait()

	// Check consumer group started with the proper consumer group name
	require.Equal(t, expectedConsumerGroup, a.GetConsumerGroup())

	cancel()
	wg.Wait()

	require.True(t, mockConsumerGroupInstance.MockConsumerGroup.Closed)
}
