package gobus

import goeh "github.com/hetacode/go-eh"

type BusMiddleware interface {
	Before(ev goeh.Event, err error)
	After(ev goeh.Event, err error)
}

type MiddlewareOptions struct {
	ConsumerMiddlewares  []BusMiddleware
	PublisherMiddlewares []BusMiddleware
}
