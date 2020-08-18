package main

import goeh "github.com/hetacode/go-eh"

// TestEvent ...
type TestEvent struct {
	*goeh.EventData
	FullName string `json:"full_name"`
}

// GetType ...
func (e *TestEvent) GetType() string {
	return "TestEvent"
}
