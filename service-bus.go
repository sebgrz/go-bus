package gobus

import goeh "github.com/hetacode/go-eh"

// ServiceBus general abstraction for bus
type ServiceBus interface {
	Consume() (<-chan goeh.Event, <-chan error)
	Publish(message goeh.Event) error
}
