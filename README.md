# go-bus
Universal pub-sub library for rabbitmq and kafka in Go  
**Dev version - only for tests**

## Installation
`go get github.com/hetacode/go-bus`

## Consumer implementation

### General implementation

```golang

// fakeLogger is only for tests - you should use own implementation with own logger
type fakeLogger struct{}

func (l *fakeLogger) Infof(message string, args ...interface{}) {
	log.Printf(message, args...)
}
func (l *fakeLogger) Errorf(message string, args ...interface{}) {
	log.Printf(message, args...)
}

func main() {
	eventsMapper := new(goeh.EventsMapper)
	eventsMapper.Register(new(TestEvent))

	done := make(<-chan os.Signal)

	kind := gobus.RabbitMQServiceBusOptionsFanOutKind
	bus := gobus.NewRabbitMQServiceBus(eventsMapper,  new(fakeLogger), &gobus.RabbitMQServiceBusOptions{
		Kind:      &kind,
		Exchanage: "test-ex",
		Queue:     "test-queue",
		Server:    "amqp://rabbit:5672",
	})
	go func() {
		msgCh, errCh := bus.Consume()
		for {
			select {
			case msg := <-msgCh:
				log.Printf("Do something with received event: %+v", msg)
			case err := <-errCh:
				if err != nil {
					panic(err)
				}
			}
		}
	}()

	<-done
	log.Printf("the end")
}
```

### Bind queue to multiple exchanges
Consumer queue can be bind to multiple exchanges.
> Important! In this case can be only zero or one routing key attach between **exchange** <-> **queue**

```golang
kind := gobus.RabbitMQServiceBusOptionsFanOutKind
bus := gobus.NewRabbitMQServiceBus(eventsMapper,  new(fakeLogger), &gobus.RabbitMQServiceBusOptions{
		Kind:      &kind,
	Exchange: "test-ex1|test-ex2",
	Queue:     "test-queue",
	RoutingKey: "" // or "routing-key"
	Server:    "amqp://rabbit:5672",
})
```
### Bind queue to exchange with multiple routing keys
Consumer queue can be bind to **only one** exchange with multiple routing keys.

```golang
kind := gobus.RabbitMQServiceBusOptionsFanOutKind
bus := gobus.NewRabbitMQServiceBus(eventsMapper,  new(fakeLogger), &gobus.RabbitMQServiceBusOptions{
		Kind:      &kind,
	Exchange: "test-ex", // one exchange
	Queue:     "test-queue",
	RoutingKey: "routing-key1|routing-key2"
	Server:    "amqp://rabbit:5672",
})
```

## Producer implementation
```golang
// fakeLogger is only for tests - you should use own implementation with own logger
type fakeLogger struct{}

func (l *fakeLogger) Infof(message string, args ...interface{}) {
	log.Printf(message, args...)
}
func (l *fakeLogger) Errorf(message string, args ...interface{}) {
	log.Printf(message, args...)
}

func main() {
	eventsMapper := new(goeh.EventsMapper)
	eventsMapper.Register(new(TestEvent))

	kind := gobus.RabbitMQServiceBusOptionsFanOutKind
	bus := gobus.NewRabbitMQServiceBus(eventsMapper, new(fakeLogger), &gobus.RabbitMQServiceBusOptions{
		Kind:      &kind,
		Exchanage: "test-ex",
		Server:    "amqp://rabbit:5672",
	})
    
    n := 1
	for n < 10 {
		bus.Publish(&TestEvent{
			EventData: &goeh.EventData{ID: strconv.Itoa(n)},
			FullName:  fmt.Sprintf("Janusz %d", n),
		})
    }
    
	log.Printf("the end")
}
```

## Retry option
It's a retry mechanism that can be add to both kafka and rabbit implementation of service bus. When send event function return any error a library try send the event again after some delay.  
```golang
type RetryOptions struct {
	Attempts int
	Delay    time.Duration
}
```

The RetryOption can be add to the both `RabbitMQServiceBusOptions` or `KafkaServiceBusOptions`