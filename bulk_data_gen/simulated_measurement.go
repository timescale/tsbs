package main

import "time"

type SimulatedMeasurement interface {
	Tick(time.Duration)
	ToPoint(*Point)
}
