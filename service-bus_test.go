package gobus

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	mock_gobus "github.com/hetacode/go-bus/mocks"
	goeh "github.com/hetacode/go-eh"
)

func TestPublishMessageWithoutErrorShouldCreateZeroRetryAttempts(t *testing.T) {
	// Prepare
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockLogger := mock_gobus.NewMockServiceBusLogger(ctrl)
	mockLogger.EXPECT().
		Infof(gomock.Eq("Event: %s has been sent after %d attempts"), gomock.Eq("TestEvent"), gomock.Eq(0)).
		Times(1)

	// Init
	err := publish(mockLogger, new(TestEvent), &RetryOptions{Attempts: 3, Delay: time.Second}, func(ev goeh.Event) error {
		return nil
	})

	if err != nil {
		t.Error("error should be nil")
	}
}

func TestPublishMessageWithOneErrorShouldCreateOneRetryAttempts(t *testing.T) {
	// Prepare
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockLogger := mock_gobus.NewMockServiceBusLogger(ctrl)
	mockLogger.EXPECT().
		Infof(gomock.Eq("Event: %s has been sent after %d attempts"), gomock.Eq("TestEvent"), gomock.Eq(1)).
		Times(1)

	// Init
	counter := 0
	err := publish(mockLogger, new(TestEvent), &RetryOptions{Attempts: 3, Delay: time.Second}, func(ev goeh.Event) error {
		if counter == 0 {
			counter++
			return fmt.Errorf("mock error - first attempt")
		} else {
			return nil
		}
	})

	if err != nil {
		t.Error("error should be nil")
	}
}

func TestPublishMessageWithErrorsOverAttemptsShouldReturnError(t *testing.T) {
	// Prepare
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockLogger := mock_gobus.NewMockServiceBusLogger(ctrl)
	mockLogger.EXPECT().
		Infof(gomock.Any(), gomock.Any(), gomock.Any()).
		Times(0)

	// Init
	err := publish(mockLogger, new(TestEvent), &RetryOptions{Attempts: 3, Delay: time.Second}, func(ev goeh.Event) error {
		return fmt.Errorf("mock error")
	})

	if err == nil {
		t.Error("error is expected")
	}

	if err.Error() != "cannot send event after 3 attempts | err: mock error" {
		t.Errorf("error message is unexpected: %s", err)
	}
}

type TestEvent struct {
	goeh.EventData
}

func (e *TestEvent) GetType() string {
	return "TestEvent"
}
