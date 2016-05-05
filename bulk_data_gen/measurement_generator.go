package main

// MeasurementGenerator creates data for populating a given measurement.
type MeasurementGenerator interface {
	Total() int64
	Seen() int64
	Finished() bool
	Next(*Point)
        MakeUsablePoint() *Point
}
