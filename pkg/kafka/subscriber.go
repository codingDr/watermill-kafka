package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Message = kafka.Message

type Subscriber struct {
	config  SubscriberConfig
	logger  watermill.LoggerAdapter
	closing chan struct{}
	closed  bool

	subscribersWg sync.WaitGroup
	consumerWg    sync.WaitGroup
}

type SubscriberConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Unmarshaler is used to unmarshal messages from Kafka format into Watermill format.
	Unmarshaler Unmarshaler

	// Kafka ConfigMap
	KafkaConfig *kafka.ConfigMap

	// ConsumerGroup is the group.id set for kafka.Consumer
	ConsumerGroup string
}

func (c *SubscriberConfig) setBrokers() (err error) {
	var brokerString string
	if brokerString, err = buildBrokersString(c.Brokers); err != nil {
		return
	}

	err = c.KafkaConfig.Set("bootstrap.servers= " + brokerString)
	return
}

// NewSubscriber creates a new Kafka Subscriber.
func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	subscriber_uuid := watermill.NewShortUUID()

	if len(config.Brokers) == 0 {
		return nil, errors.New("missing brokers")
	}

	if config.Unmarshaler == nil {
		config.Unmarshaler = DefaultMarshaler{}
	}

	if config.KafkaConfig == nil {
		config.KafkaConfig = DefaultKafkaConfig()
	}

	if config.ConsumerGroup != "" {
		config.KafkaConfig.Set("group.id=" + config.ConsumerGroup)
	} else {
		config.KafkaConfig.Set("group.id=" + subscriber_uuid)
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	logger = logger.With(watermill.LogFields{
		"subscriber_uuid": subscriber_uuid,
	})

	err := config.setBrokers()
	if err != nil {
		return nil, err
	}

	return &Subscriber{
		config:  config,
		logger:  logger,
		closing: make(chan struct{}),
	}, nil
}

// DefaultKafkaSubscriberConfig creates default kafka.ConfigMap used by Publishers / Subscribers.
//
// Custom config can be passed to NewSubscriber and NewPublisher.
//
//	kafkaConfig := DefaultKafkaSubscriberConfig()
//	kafkaConfig.Set("session.timeout.ms=90000")
//
//	subscriberConfig.KafkaConfig = kafkaConfig
//
//	subscriber, err := NewSubscriber(subscriberConfig, logger)
//	// ...
func DefaultKafkaConfig() *kafka.ConfigMap {
	return &kafka.ConfigMap{"enable.auto.commit": "false", "auto.offset.reset": "earliest"}
}

// Subscribe creates the kafka.Consumer, subscribes, and begins polling for messages.
func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	s.subscribersWg.Add(1)

	output := make(chan *message.Message)

	logFields := watermill.LogFields{
		"provider":            "kafka",
		"topic":               topic,
		"consumer_group":      s.config.ConsumerGroup,
		"kafka_consumer_uuid": watermill.NewShortUUID(),
	}

	go s.consumeMessages(ctx, topic, output, logFields)

	go func() {
		select {
		case <-s.closing:
			s.logger.Debug("Closing subscriber, waiting for consumeMessages", logFields)
			s.consumerWg.Wait()
			close(output)
			s.subscribersWg.Done()
		case <-ctx.Done():
			s.logger.Debug("Subscriber ctx cancelled, waiting for consumeMessages", logFields)
			s.consumerWg.Wait()
			close(output)
			s.subscribersWg.Done()
			// avoid goroutine leak
		}
	}()

	return output, nil
}

func (s *Subscriber) consumeMessages(
	ctx context.Context,
	topic string,
	output chan *message.Message,
	logFields watermill.LogFields,
) {

	consumer, err := s.initializeConsumer(topic)
	if err != nil {
		s.logger.Error("unable to initialize consumer", err, logFields)
		return
	}

	s.consumerWg.Add(1)

	messageHandler := s.createMessagesHandler(output)

	defer func() {
		if err := consumer.Close(); err != nil {
			s.logger.Error("failed to close consumer", err, logFields)
		}
		s.consumerWg.Done()

		s.logger.Debug("consumeMessages stopped", logFields)
	}()

	s.logger.Info("Starting Consumer", logFields)

ConsumeLoop:
	for {
		select {
		case <-s.closing:
			s.logger.Trace("Stopping consumeMessages, subscriber closing", logFields)
			return
		case <-ctx.Done():
			s.logger.Trace("Stopping consumeMessages, ctx cancelled", logFields)
			return
		default:
			kafkaMsg, err := consumer.ReadMessage(time.Second * 5)
			if err == nil {
				var kafkaMsgKey string
				if kafkaMsg.Key == nil {
					kafkaMsgKey = ""
				} else {
					kafkaMsgKey = string(kafkaMsg.Key)
				}

				receivedMsgLogFields := logFields.Add(watermill.LogFields{
					"Partition":   kafkaMsg.TopicPartition.Partition,
					"Offset":      kafkaMsg.TopicPartition.Offset.String(),
					"Message Key": kafkaMsgKey,
				})
				s.logger.Info("Consumed message", receivedMsgLogFields)

				if err := messageHandler.processMessage(ctx, kafkaMsg, receivedMsgLogFields); err != nil {
					s.logger.Error("error processing message", err, receivedMsgLogFields)
					continue ConsumeLoop // how to handle error returned from processMessage?
				}

				_, err = consumer.CommitMessage(kafkaMsg)
				if err != nil {
					s.logger.Error("error committing kafkaMsg", err, receivedMsgLogFields)
					// how to handle error committing kafkaMsg?
				}

				s.logger.Info("committed kafkaMsg", receivedMsgLogFields)

			} else {
				s.logger.Info("Error reading message: "+err.Error(), logFields)
			}
		}

	}
}

func (s *Subscriber) initializeConsumer(topic string) (consumer *kafka.Consumer, err error) {

	s.logger.Info("Creating consumer", nil)
	consumer, err = kafka.NewConsumer(s.config.KafkaConfig)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create new Kafka consumer")
	}

	s.logger.Info("Subscribing to Kafka topic", nil)
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		if err2 := consumer.Close(); err2 != nil {
			err = errors.Wrap(err, err2.Error())
		}
		return nil, errors.Wrap(err, "cannot subscribe to topic")
	}

	return consumer, nil
}

func (s *Subscriber) Close() (err error) {
	if s.closed {
		return
	}

	s.closed = true
	close(s.closing)
	s.subscribersWg.Wait()
	s.logger.Debug("Kafka subscriber closed", nil)

	return
}

type messageHandler struct {
	outputChannel chan<- *message.Message
	unmarshaler   Unmarshaler
	logger        watermill.LoggerAdapter
	closing       chan struct{}
}

func (s *Subscriber) createMessagesHandler(output chan *message.Message) messageHandler {
	return messageHandler{
		outputChannel: output,
		unmarshaler:   s.config.Unmarshaler,
		logger:        s.logger,
		closing:       s.closing,
	}
}

func (h *messageHandler) processMessage(
	ctx context.Context,
	kafkaMsg *kafka.Message,
	messageLogFields watermill.LogFields,
) error {

	h.logger.Trace("Received message from Kafka", messageLogFields)

	ctx = setPartitionToCtx(ctx, kafkaMsg.TopicPartition.Partition)
	ctx = setPartitionOffsetToCtx(ctx, int64(kafkaMsg.TopicPartition.Offset))
	ctx = setMessageTimestampToCtx(ctx, kafkaMsg.Timestamp)

	msg, err := h.unmarshaler.Unmarshal(kafkaMsg)
	if err != nil {
		return errors.Wrap(err, "message unmarshal failed")
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	messageLogFields = messageLogFields.Add(watermill.LogFields{
		"message_uuid": msg.UUID,
	})

ResendLoop:
	for {
		select {
		case h.outputChannel <- msg:
			h.logger.Trace("Message sent to outputChannel", messageLogFields)
		case <-h.closing:
			h.logger.Trace("Closing, message discarded", messageLogFields)
			return errors.New("handler closed before message sent to outputChannel")
		case <-ctx.Done():
			h.logger.Trace("ctx cancelled before message sent to outputChannel", messageLogFields)
			return errors.New("ctx cancelled before message sent to outputChannel")
		}

		select {
		case <-msg.Acked():
			h.logger.Trace("Message Acked", messageLogFields)
			break ResendLoop
		// TODO: How to handle msg.Nacked()? Possibly implement cenkalti/backoff for retry logic?
		case <-msg.Nacked():
			h.logger.Trace("Message Nacked", messageLogFields)
			// reset acks, etc.
			msg = msg.Copy()
			continue ResendLoop
		case <-h.closing:
			h.logger.Trace("Closing, message discarded before ack", messageLogFields)
			return errors.New("handler closed before message sent to outputChannel")
		case <-ctx.Done():
			h.logger.Trace("Closing, ctx cancelled before ack", messageLogFields)
			return errors.New("ctx cancelled before message sent to outputChannel")
		}
	}

	return nil
}

// type PartitionOffset map[int32]int64

// func (s *Subscriber) PartitionOffset(topic string) (PartitionOffset, error) {

// 	var topics []string
// 	topics, err := s.consumer.Subscription()
// 	if err != nil {
// 		return nil, err
// 	}
// 	subscribedTopic := topics[0]
// 	kafkaMetadata, err := s.consumer.GetMetadata(&topics[0], false, 5000)
// 	if err != nil {
// 		return nil, err
// 	}

// 	topicMetadata := kafkaMetadata.Topics[subscribedTopic]
// 	partitionOffset := make(PartitionOffset, len(topicMetadata.Partitions))
// 	for _, partitionMetadata := range topicMetadata.Partitions {
// 		_, highOffset, err := s.consumer.QueryWatermarkOffsets(subscribedTopic, partitionMetadata.ID, 5000)
// 		if err != nil {
// 			return nil, err
// 		}

// 		partitionOffset[partitionMetadata.ID] = highOffset
// 	}

// 	return partitionOffset, nil
// }
