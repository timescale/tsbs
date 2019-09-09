package common

import (
	"time"
)

// Generator is a single entity which generates data from its respective measurements.
type Generator interface {
	Measurements() []SimulatedMeasurement
	Tags() []Tag
	TickAll(d time.Duration)
}

// Tag is a key-value pair of information which is used to tag a generator
type Tag struct {
	Key   []byte
	Value interface{}
}
