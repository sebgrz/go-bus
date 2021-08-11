package gobus

import (
	"fmt"
	"strings"

	goeh "github.com/hetacode/go-eh"
	"github.com/streadway/amqp"
)

// RabbitMQServiceBus implementation of service bus
type RabbitMQServiceBus struct {
	mode         ServiceBusMode
	logger       ServiceBusLogger
	channel      *amqp.Channel
	options      *RabbitMQServiceBusOptions
	eventsMapper *goeh.EventsMapper
}

// RabbitMQServiceBusOptions struct with configuration for rabbitmq service bus
type RabbitMQServiceBusOptions struct {
	Server string
	Queue  string
	// Exchange - queue can be bind to multiple exchanges ex1|ex2...
	Exchange string
	// RoutingKey - queue can be bind to exchange with multiple routing keys rk1|rk2...
	RoutingKey string
	Kind       *string
	Retry      *RetryOptions
}

const (
	// RabbitMQServiceBusOptionsTopicKind topic kind of rabbitmq
	RabbitMQServiceBusOptionsTopicKind string = "topic"
	// RabbitMQServiceBusOptionsFanOutKind fanout kind of rabbitmq
	RabbitMQServiceBusOptionsFanOutKind string = "fanout"
)

// NewRabbitMQServiceBus new instance of queue
func NewRabbitMQServiceBus(mode ServiceBusMode, eventsMapper *goeh.EventsMapper, logger ServiceBusLogger, options *RabbitMQServiceBusOptions) ServiceBus {
	if options == nil {
		panic("Options struct is not initialized")
	}

	if eventsMapper == nil {
		panic("EventsMapper isn't initialized")
	}

	if options.Kind == nil {
		t := RabbitMQServiceBusOptionsFanOutKind
		options.Kind = &t
	}

	conn, err := amqp.Dial(options.Server)
	if err != nil {
		panic(err)
	}

	channel, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	if mode == ConsumerServiceBusMode {
		exLen := len(strings.Split(options.Exchange, "|"))
		rkLen := len(strings.Split(options.RoutingKey, "|"))

		if exLen == 0 && rkLen > 0 {
			panic("no exchange to attach queue with routing key")
		}

		if exLen > 1 && rkLen > 1 {
			panic(fmt.Sprintf("exchanges count: %d routing count: %d | to multiple exchanges can be attach at most one routing key OR multiple routing keys can be attach to only one exchange", exLen, rkLen))
		}
	}

	if mode == PublisherServiceBusMode {
		// decalare exchange for publisher
		if err := channel.ExchangeDeclare(
			options.Exchange,
			*options.Kind,
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			panic(err)
		}
	}
	bus := &RabbitMQServiceBus{
		channel:      channel,
		options:      options,
		eventsMapper: eventsMapper,
		logger:       logger,
		mode:         mode,
	}

	return bus
}

// Consume events
func (b *RabbitMQServiceBus) Consume() (<-chan goeh.Event, <-chan error) {
	if b.mode != ConsumerServiceBusMode {
		panic(fmt.Sprintf("Consume failed - RabbitMQ client is created in '%s' mode", ServiceBusModeNameMapping[b.mode]))
	}

	evChan := make(chan goeh.Event)
	errChan := make(chan error)

	go func(b *RabbitMQServiceBus) {
		exchanges := strings.Split(b.options.Exchange, "|")
		ch := b.channel

		for _, ex := range exchanges {
			if err := ch.ExchangeDeclare(
				ex,
				*b.options.Kind,
				true,
				false,
				false,
				false,
				nil,
			); err != nil {
				errChan <- err
				return
			}
		}
		defer ch.Close()

		q, err := ch.QueueDeclare(
			b.options.Queue,
			false,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			errChan <- err
			return
		}

		for _, ex := range exchanges {
			keys := strings.Split(b.options.RoutingKey, "|")
			for _, rk := range keys {
				err := ch.QueueBind(
					q.Name,
					rk,
					ex,
					false,
					nil,
				)
				if err != nil {
					errChan <- err
					return
				}
			}
		}
		msgs, err := ch.Consume(
			q.Name,
			"",
			false,
			false,
			false,
			true,
			nil,
		)
		if err != nil {
			errChan <- err
			return
		}
		errChan <- nil

		forever := make(<-chan bool)

		for msg := range msgs {
			m := string(msg.Body)
			b.logger.Infof("message: %s | %s", m, msg.Type)
			e, err := b.eventsMapper.Resolve(m)
			if err != nil {
				b.logger.Errorf("cannot resolve event %s | err: %v", msg.Type, err)
				msg.Ack(true)
				continue
			}
			evChan <- e
			msg.Ack(true)
		}
		<-forever
	}(b)

	return evChan, errChan
}

// Publish message
func (b *RabbitMQServiceBus) Publish(event goeh.Event) error {
	if b.mode != PublisherServiceBusMode {
		panic(fmt.Sprintf("Publish failed - RabbitMQ client is created in '%s' mode", ServiceBusModeNameMapping[b.mode]))
	}

	return publish(b.logger, event, b.options.Retry, func(ev goeh.Event) error {
		err := b.channel.Publish(b.options.Exchange, b.options.RoutingKey, false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(ev.GetPayload()),
		})
		return err
	})
}

// PublishWithRouting - send message with specific routing key
func (b *RabbitMQServiceBus) PublishWithRouting(key string, event goeh.Event) error {
	return publish(b.logger, event, b.options.Retry, func(ev goeh.Event) error {
		err := b.channel.Publish(b.options.Exchange, key, false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(ev.GetPayload()),
		})
		return err
	})
}
