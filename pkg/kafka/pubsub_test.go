package kafka_test

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/codingDr/watermill-kafka/pkg/kafka"
	"github.com/stretchr/testify/require"
)


func kafkaBrokers() []string {
	brokers := os.Getenv("WATERMILL_TEST_KAFKA_BROKERS")
	if brokers != "" {
		return strings.Split(brokers, ",")
	}
	return []string{"localhost:9091", "localhost:9092", "localhost:9093", "localhost:9094", "localhost:9095"}
}

func newPubSub(t *testing.T, marshaler kafka.MarshalerUnmarshaler, consumerGroup string) (*kafka.Publisher, *kafka.Subscriber) {

	logger := watermill.NewStdLogger(true, true)

	var err error
	var publisher *kafka.Publisher
	retriesLeft := 5
	for {
		publisher, err = kafka.NewPublisher(kafka.PublisherConfig{
			Brokers:   kafkaBrokers(),
			Marshaler: marshaler,
		}, logger)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create kafka Publisher: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}
	require.NoError(t, err)

	var subscriber *kafka.Subscriber

	retriesLeft = 5
	for {
		subscriber, err = kafka.NewSubscriber(
			kafka.SubscriberConfig{
				Brokers:       kafkaBrokers(),
				Unmarshaler:   marshaler,
				ConsumerGroup: consumerGroup,
			},
			logger,
		)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create kafka Subscriber: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}

	require.NoError(t, err)

	return publisher, subscriber
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, kafka.DefaultMarshaler{}, consumerGroup)
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPubSubWithConsumerGroup(t, "test")
}

func createNoGroupPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newPubSub(t, kafka.DefaultMarshaler{}, "")
}

func TestPublishSubscribe(t *testing.T) {

	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     false,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}

func TestNoGroupSubscriber(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long tests")
	}

	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:                   false,
			ExactlyOnceDelivery:              false,
			GuaranteedOrder:                  false,
			Persistent:                       true,
			NewSubscriberReceivesOldMessages: true,
		},
		createNoGroupPubSub,
		nil,
	)
}

