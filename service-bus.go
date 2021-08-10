package gobus

import (
	"fmt"
	"time"

	goeh "github.com/hetacode/go-eh"
)

type ServiceBusMode int8

const (
	PublisherServiceBusMode ServiceBusMode = 1
	ConsumerServiceBusMode  ServiceBusMode = 2
)

var ServiceBusModeNameMapping map[ServiceBusMode]string = map[ServiceBusMode]string{
	PublisherServiceBusMode: "publisher",
	ConsumerServiceBusMode:  "consumer",
}

// ServiceBus general abstraction for bus
type ServiceBus interface {
	Consume() (<-chan goeh.Event, <-chan error)
	Publish(message goeh.Event) error
	PublishWithRouting(key string, message goeh.Event) error
}

type ServiceBusLogger interface {
	Infof(message string, args ...interface{})
	Errorf(message string, args ...interface{})
}

func publish(log ServiceBusLogger, event goeh.Event, retryOptions *RetryOptions, sendFunc func(ev goeh.Event) error) error {
	if err := event.SavePayload(event); err != nil {
		return err
	}

	retryAttempt := 0
	for {
		err := sendFunc(event)
		if err != nil {
			if retryOptions == nil {
				return err
			} else {
				if retryAttempt >= retryOptions.Attempts {
					return fmt.Errorf("cannot send event after %d attempts | err: %s", retryAttempt, err)
				}
				retryAttempt++
				time.Sleep(retryOptions.Delay)
				continue
			}
		}

		break
	}
	log.Infof("Event: %s has been sent after %d attempts", event.GetType(), retryAttempt)
	return nil
}
