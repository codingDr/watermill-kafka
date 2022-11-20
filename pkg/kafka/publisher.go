package kafka

import (
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Publisher struct {
	config   PublisherConfig
	producer *kafka.Producer
	logger   watermill.LoggerAdapter

	closed bool
}

type PublisherConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Marshaler is used to marshal messages from Watermill format into Kafka format.
	Marshaler Marshaler

	// Kafka ConfigMap
	KafkaConfig *kafka.ConfigMap
}

func (c *PublisherConfig) setBrokers() (err error) {
	var brokerString string
	if brokerString, err = buildBrokersString(c.Brokers); err != nil {
		return
	}
	c.KafkaConfig.Set("bootstrap.servers=" + brokerString)

	return nil
}

func buildBrokersString(brokers []string) (string, error) {
	builder := strings.Builder{}

	for i, broker := range brokers {
		if i == 0 {
			if _, err := builder.WriteString(broker); err != nil {
				return "", err
			}
			continue
		}
		// prefix broker with comma after first broker
		if _, err := builder.WriteString("," + broker); err != nil {
			return "", err
		}
	}

	return builder.String(), nil
}

// NewPublisher creates a new Kafka Publisher.
func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	if len(config.Brokers) == 0 {
		return nil, errors.New("missing brokers")
	}

	if config.Marshaler == nil {
		config.Marshaler = DefaultMarshaler{}
	}

	if config.KafkaConfig == nil {
		config.KafkaConfig = DefaultKafkaConfig()
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	logger = logger.With(watermill.LogFields{
		"publisher_uuid": watermill.NewShortUUID(),
	})

	err := config.setBrokers()
	if err != nil {
		return nil, err
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	producer, err := kafka.NewProducer(config.KafkaConfig)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create Kafka producer")
	}

	return &Publisher{
		config:   config,
		producer: producer,
		logger:   logger,
	}, nil
}

// Publish publishes message to Kafka.
//
// Publish is blocking and wait for ack from Kafka.
// When one of messages delivery fails - function is interrupted.
func (p *Publisher) Publish(topic string, msgs ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	logFields := make(watermill.LogFields, 4)
	logFields["topic"] = topic

	for _, msg := range msgs {
		logFields["message_uuid"] = msg.UUID
		p.logger.Trace("Sending message to Kafka", logFields)

		kafkaMsg, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		deliveryChan := make(chan kafka.Event)

		err = p.producer.Produce(kafkaMsg, deliveryChan)
		if err != nil {
			return errors.Wrapf(err, "cannot produce message %s", msg.UUID)
		}

		e := <-deliveryChan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			return m.TopicPartition.Error
		}

		logFields["kafka_partition"] = fmt.Sprint(m.TopicPartition.Partition)
		logFields["kafka_partition_offset"] = m.TopicPartition.Offset.String()

		p.logger.Trace("Message sent to Kafka", logFields)
	}

	return nil
}

func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	p.producer.Close()
	return nil
}
