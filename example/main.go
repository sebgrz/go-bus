package main

import (
	"log"
	"os"

	gobus "github.com/hetacode/go-bus"
	goeh "github.com/hetacode/go-eh"
)

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
	bus := gobus.NewRabbitMQServiceBus(eventsMapper, new(fakeLogger), &gobus.RabbitMQServiceBusOptions{
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
