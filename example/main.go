package main

import (
	"log"
	"os"

	gobus "github.com/hetacode/go-bus"
	goeh "github.com/hetacode/go-eh"
)

func main() {
	eventsMapper := new(goeh.EventsMapper)
	eventsMapper.Register(new(TestEvent))

	done := make(<-chan os.Signal)

	kind := gobus.RabbitMQServiceBusOptionsFanOutKind
	bus := gobus.NewRabbitMQServiceBus(eventsMapper, &gobus.RabbitMQServiceBusOptions{
		Kind:      &kind,
		Exchanage: "test-ex", // can be test-ex|test-ex2|... for multiple exchanges
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
