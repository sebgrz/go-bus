package gobus

import (
	"log"
	"strings"

	goeh "github.com/hetacode/go-eh"
	"github.com/streadway/amqp"
)

// RabbitMQServiceBus implementation of service bus
type RabbitMQServiceBus struct {
	channelConsumer  *amqp.Channel
	channelPublisher *amqp.Channel
	options          *RabbitMQServiceBusOptions
	eventsMapper     *goeh.EventsMapper
}

// RabbitMQServiceBusOptions struct with configuration for rabbitmq service bus
type RabbitMQServiceBusOptions struct {
	Server     string
	Queue      string
	Exchanage  string
	RoutingKey string
	Kind       *string
}

const (
	// RabbitMQServiceBusOptionsTopicKind topic kind of rabbitmq
	RabbitMQServiceBusOptionsTopicKind string = "topic"
	// RabbitMQServiceBusOptionsFanOutKind fanout kind of rabbitmq
	RabbitMQServiceBusOptionsFanOutKind string = "fanout"
)

// NewRabbitMQServiceBus new instance of queue
func NewRabbitMQServiceBus(eventsMapper *goeh.EventsMapper, options *RabbitMQServiceBusOptions) ServiceBus {
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

	consumerConn, err := amqp.Dial(options.Server)
	publisherConn, err := amqp.Dial(options.Server)

	if err != nil {
		panic(err)
	}
	channelConsumer, err := consumerConn.Channel()
	channelPublisher, err := publisherConn.Channel()
	if err != nil {
		panic(err)
	}

	// decalare exchange for publisher
	if err := channelPublisher.ExchangeDeclare(
		options.Exchanage,
		*options.Kind,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		panic(err)
	}

	bus := &RabbitMQServiceBus{
		channelConsumer:  channelConsumer,
		channelPublisher: channelPublisher,
		options:          options,
		eventsMapper:     eventsMapper,
	}

	return bus
}

// Consume events
func (b *RabbitMQServiceBus) Consume() (<-chan goeh.Event, <-chan error) {
	evChan := make(chan goeh.Event)
	errChan := make(chan error)

	go func(b *RabbitMQServiceBus) {
		exchanges := strings.Split(b.options.Exchanage, "|")
		ch := b.channelConsumer

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
			if err := ch.QueueBind(
				q.Name,
				b.options.RoutingKey,
				ex,
				false,
				nil,
			); err != nil {
				errChan <- err
				return
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
			log.Printf("Message: %s | %s", m, msg.Type)
			e, err := b.eventsMapper.Resolve(m)
			if err != nil {
				log.Print("Cannot resolve event: ", msg.Type)
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
	if err := event.SavePayload(event); err != nil {
		return err
	}

	if err := b.channelPublisher.Publish(b.options.Exchanage, b.options.RoutingKey, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        []byte(event.GetPayload()),
	}); err != nil {
		return err
	}
	log.Printf("Event: %s has been sent", event.GetType())
	return nil
}
