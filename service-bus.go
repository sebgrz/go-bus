package gobus

import (
	"fmt"
	"log"
	"time"

	goeh "github.com/hetacode/go-eh"
)

// ServiceBus general abstraction for bus
type ServiceBus interface {
	Consume() (<-chan goeh.Event, <-chan error)
	Publish(message goeh.Event) error
}

func publish(event goeh.Event, retryOptions *RetryOptions, sendFunc func(ev goeh.Event) error) error {
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
	log.Printf("Event: %s has been sent", event.GetType())
	return nil
}
