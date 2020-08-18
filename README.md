# go-bus
Universal pub-sub library for rabbitmq and kafka in Go  
**Dev version - only for tests**

## Installation
`go get github.com/hetacode/go-bus`

## Consumer implementation

```golang
func main() {
	eventsMapper := new(goeh.EventsMapper)
	eventsMapper.Register(new(TestEvent))

	done := make(<-chan os.Signal)

	kind := gobus.RabbitMQServiceBusOptionsFanOutKind
	bus := gobus.NewRabbitMQServiceBus(eventsMapper, &gobus.RabbitMQServiceBusOptions{
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

## Producer implementation
```golang
func main() {
	eventsMapper := new(goeh.EventsMapper)
	eventsMapper.Register(new(TestEvent))

	kind := gobus.RabbitMQServiceBusOptionsFanOutKind
	bus := gobus.NewRabbitMQServiceBus(eventsMapper, &gobus.RabbitMQServiceBusOptions{
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