package kafka_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/codingDr/watermill-kafka/pkg/kafka"
)

func TestDefaultMarshaler_MarshalUnmarshal(t *testing.T) {
	m := kafka.DefaultMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := m.Marshal("topic", msg)
	require.NoError(t, err)

	unmarshaledMsg, err := m.Unmarshal(marshaled) // m.Unmarshal(producerToConsumerMessage(marshaled))
	require.NoError(t, err)
	t.Log("msg1: ", msg)
	t.Log("msg2: ", unmarshaledMsg)
	assert.True(t, msg.Equals(unmarshaledMsg))
}

func BenchmarkDefaultMarshaler_Marshal(b *testing.B) {
	m := kafka.DefaultMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	for i := 0; i < b.N; i++ {
		m.Marshal("foo", msg)
	}
}

func BenchmarkDefaultMarshaler_Unmarshal(b *testing.B) {
	m := kafka.DefaultMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := m.Marshal("foo", msg)
	if err != nil {
		b.Fatal(err)
	}

	consumedMsg := marshaled // producerToConsumerMessage(marshaled)

	for i := 0; i < b.N; i++ {
		m.Unmarshal(consumedMsg)
	}
}

// func TestWithPartitioningMarshaler_MarshalUnmarshal(t *testing.T) {
// 	m := kafka.NewWithPartitioningMarshaler(func(topic string, msg *message.Message) (string, error) {
// 		return msg.Metadata.Get("partition"), nil
// 	})

// 	partitionKey := "1"
// 	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
// 	msg.Metadata.Set("partition", partitionKey)

// 	producerMsg, err := m.Marshal("topic", msg)
// 	require.NoError(t, err)

// 	unmarshaledMsg, err := m.Unmarshal(producerToConsumerMessage(producerMsg))
// 	require.NoError(t, err)

// 	assert.True(t, msg.Equals(unmarshaledMsg))

// 	assert.NoError(t, err)

// 	producerKey := producerMsg.Key
// 	require.NoError(t, err)

// 	assert.Equal(t, string(producerKey), partitionKey)
// }

// func producerToConsumerMessage(producerMessage *kafka.Message) *kafka.Message {
// 	return producerMessage
// }
