package gobus

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	goeh "github.com/hetacode/go-eh"
)

// KafkaServiceBus implementation of service bus
type KafkaServiceBus struct {
	mode         ServiceBusMode
	logger       ServiceBusLogger
	eventsMapper *goeh.EventsMapper
	producer     *kafka.Producer
	topic        string
	deliveryChan chan kafka.Event

	options *KafkaServiceBusOptions
}

// KafkaServiceBusOptions configuration struct for kafka service bus
type KafkaServiceBusOptions struct {
	Servers string
	Topic   string
	Retry   *RetryOptions

	// For consumer
	GroupName               string
	IsGroupNameAutoGenerate bool
}

// NewKafkaServiceBus instance
// eventsMapper is using only in consumer mode
func NewKafkaServiceBus(mode ServiceBusMode, eventsMapper *goeh.EventsMapper, options *KafkaServiceBusOptions, logger ServiceBusLogger) ServiceBus {
	if options == nil {
		panic(fmt.Errorf("options parameter cannot be empty"))
	}

	bus := &KafkaServiceBus{
		options:      options,
		eventsMapper: eventsMapper,
		logger:       logger,
		mode:         mode,
	}
	
	bus.topic = options.Topic
	if mode == PublisherServiceBusMode {
		pr, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": options.Servers,
		})
		if err != nil {
			panic(err)
		}
		bus.producer = pr

		deliveryChan := make(chan kafka.Event)
		bus.deliveryChan = deliveryChan

		go func(b *KafkaServiceBus) {
			defer close(deliveryChan)

			for e := range deliveryChan {
				logger.Infof("event %s has been delivered ", e.String())
			}
		}(bus)
	}

	return bus
}

// Consume events from kafka partition
func (s *KafkaServiceBus) Consume() (<-chan goeh.Event, <-chan error) {
	if s.mode != ConsumerServiceBusMode {
		panic(fmt.Sprintf("Consume failed - Kafka client is created in '%s' mode", ServiceBusModeNameMapping[s.mode]))
	}
	rand.Seed(time.Now().UTC().UnixNano())
	eCh := make(chan goeh.Event)
	errCh := make(chan error)

	groupName := s.options.GroupName
	if s.options.IsGroupNameAutoGenerate {
		groupName = fmt.Sprintf("%s-%d", groupName, rand.Int63n(10000000))
	}

	go func(eventCh chan goeh.Event, errCh chan error) {
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": s.options.Servers,
			"group.id":          groupName,
			"auto.offset.reset": "earliest",
		})

		if err != nil {
			errCh <- err
			return
		}

		defer consumer.Close()

		err = consumer.SubscribeTopics([]string{s.topic}, nil)
		if err != nil {
			errCh <- err
			return
		}

		for {
			ev := consumer.Poll(0)
			switch e := ev.(type) {
			case *kafka.Message:
				rawValue := string(e.Value)
				event, err := s.eventsMapper.Resolve(rawValue)
				if err != nil {
					s.logger.Errorf("cannot resolve event: ", rawValue)
					continue
				}
				eventCh <- event
			case kafka.Error:
				s.logger.Errorf("kafka error: %v", e)
				errCh <- e
			}
		}
	}(eCh, errCh)

	return eCh, errCh
}

// Publish event to kafka topic
// Event ID should represent kafka message key - it means that can be same for multiple events which should were put on the same partition
func (s *KafkaServiceBus) Publish(event goeh.Event) error {
	if s.mode != PublisherServiceBusMode {
		panic(fmt.Sprintf("Publish failed - Kafka client is created in '%s' mode", ServiceBusModeNameMapping[s.mode]))
	}
	return publish(s.logger, event, s.options.Retry, func(ev goeh.Event) error {
		msg := kafka.Message{
			Key:            []byte(ev.GetID()),
			TopicPartition: kafka.TopicPartition{Topic: &s.topic, Partition: kafka.PartitionAny},
			Value:          []byte(ev.GetPayload()),
		}

		err := s.producer.Produce(&msg, s.deliveryChan)
		return err
	})
}

// PublishWithRouting - routing is only for RabbitMQ implementation so Kafka version should behave like Publish
func (s *KafkaServiceBus) PublishWithRouting(key string, event goeh.Event) error {
	return s.Publish(event)
}
