package gobus

import "time"

type RetryOptions struct {
	Attempts int
	Delay    time.Duration
}
