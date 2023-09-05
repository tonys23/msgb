package tests

import (
	"time"
)

type (
	SimpleEvent struct {
		SomeValue string
		CreatedAt time.Time
	}
	ComplexEvent struct {
		Parent       *ComplexEvent
		SimpleEvents []SimpleEvent
		CreatedAt    time.Time
	}
)
