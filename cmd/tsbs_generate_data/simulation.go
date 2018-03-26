package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/cmd/tsbs_generate_data/serialize"
)

type simulatorConfig interface {
	ToSimulator(time.Duration) Simulator
}

// Simulator simulates a use case.
type Simulator interface {
	Finished() bool
	Next(*serialize.Point) bool
	Fields() map[string][][]byte
}

// SimulatedMeasurement simulates one measurement (e.g. Redis for DevOps).
type SimulatedMeasurement interface {
	Tick(time.Duration)
	ToPoint(*serialize.Point)
}

// MakeUsablePoint allocates a new Point ready for use by a Simulator.
func MakeUsablePoint() *serialize.Point {
	return &serialize.Point{
		MeasurementName: nil,
		TagKeys:         make([][]byte, 0),
		TagValues:       make([][]byte, 0),
		FieldKeys:       make([][]byte, 0),
		FieldValues:     make([]interface{}, 0),
		Timestamp:       &time.Time{},
	}
}
