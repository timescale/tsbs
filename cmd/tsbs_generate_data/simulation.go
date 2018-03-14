package main

import "time"

type simulatorConfig interface {
	ToSimulator(time.Duration) Simulator
}

// Simulator simulates a use case.
type Simulator interface {
	Finished() bool
	Next(*Point) bool
	Fields() map[string][][]byte
}

// SimulatedMeasurement simulates one measurement (e.g. Redis for DevOps).
type SimulatedMeasurement interface {
	Tick(time.Duration)
	ToPoint(*Point)
}

// MakeUsablePoint allocates a new Point ready for use by a Simulator.
func MakeUsablePoint() *Point {
	return &Point{
		MeasurementName: nil,
		TagKeys:         make([][]byte, 0),
		TagValues:       make([][]byte, 0),
		FieldKeys:       make([][]byte, 0),
		FieldValues:     make([]interface{}, 0),
		Timestamp:       &time.Time{},
	}
}
