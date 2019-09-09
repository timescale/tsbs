package iot

import (
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

func testGenerator(s time.Time) []common.SimulatedMeasurement {
	return []common.SimulatedMeasurement{
		&testMeasurement{ticks: 0},
	}
}

type testMeasurement struct {
	ticks int
}

func (m *testMeasurement) Tick(_ time.Duration)       { m.ticks++ }
func (m *testMeasurement) ToPoint(_ *serialize.Point) {}

func TestNewTruckMeasurements(t *testing.T) {
	start := time.Now()

	measurements := newTruckMeasurements(start)

	if got := len(measurements); got != 2 {
		t.Errorf("incorrect number of measurements: got %d want %d", got, 2)
	}

	// Cast each measurement to its type; will panic if wrong types
	readings := measurements[0].(*ReadingsMeasurement)
	if got := readings.Timestamp; got != start {
		t.Errorf("incorrect readings measurement timestamp: got %v want %v", got, start)
	}

	diagnostics := measurements[1].(*DiagnosticsMeasurement)
	if got := diagnostics.Timestamp; got != start {
		t.Errorf("incorrect diagnostics measurement timestamp: got %v want %v", got, start)
	}
}

func TestNewTruck(t *testing.T) {
	start := time.Now()
	generator := NewTruck(1, start)

	truck := generator.(*Truck)

	if got := len(truck.Measurements()); got != 2 {
		t.Errorf("incorrect truck measurement count: got %v want %v", got, 2)
	}

	if got := len(truck.Tags()); got != 8 {
		t.Errorf("incorrect truck tag count: got %v want %v", got, 8)
	}
}

func TestTruckTickAll(t *testing.T) {
	now := time.Now()
	truck := newTruckWithMeasurementGenerator(0, now, testGenerator)
	if got := truck.simulatedMeasurements[0].(*testMeasurement).ticks; got != 0 {
		t.Errorf("ticks not equal to 0 to start: got %d", got)
	}
	truck.TickAll(time.Second)
	if got := truck.simulatedMeasurements[0].(*testMeasurement).ticks; got != 1 {
		t.Errorf("ticks incorrect: got %d want %d", got, 1)
	}
	truck.simulatedMeasurements = append(truck.simulatedMeasurements, &testMeasurement{})
	truck.TickAll(time.Second)
	if got := truck.simulatedMeasurements[0].(*testMeasurement).ticks; got != 2 {
		t.Errorf("ticks incorrect after 2nd tick: got %d want %d", got, 2)
	}
	if got := truck.simulatedMeasurements[1].(*testMeasurement).ticks; got != 1 {
		t.Errorf("ticks incorrect after 2nd tick: got %d want %d", got, 1)
	}
}
