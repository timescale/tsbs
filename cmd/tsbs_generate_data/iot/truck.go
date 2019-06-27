package iot

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/internal/usecase"
)

const (
	truckNameFmt = "truck_%d"
)

type model struct {
	Name            []byte
	LoadCapacity    []byte
	FuelCapacity    []byte
	FuelConsumption []byte
}

var (
	driverChoices = [][]byte{
		[]byte("Derek"),
		[]byte("Rodney"),
		[]byte("Albert"),
		[]byte("Andy"),
		[]byte("Seth"),
		[]byte("Trish"),
	}

	modelChoices = []model{
		{
			Name:            []byte("F-150"),
			LoadCapacity:    []byte("2000"),
			FuelCapacity:    []byte("200"),
			FuelConsumption: []byte("15"),
		},
		{
			Name:            []byte("G-2000"),
			LoadCapacity:    []byte("5000"),
			FuelCapacity:    []byte("300"),
			FuelConsumption: []byte("19"),
		},
		{
			Name:            []byte("H-2"),
			LoadCapacity:    []byte("1500"),
			FuelCapacity:    []byte("150"),
			FuelConsumption: []byte("12"),
		},
	}

	deviceVersionChoices = [][]byte{
		[]byte("v1.0"),
		[]byte("v1.5"),
		[]byte("v2.0"),
		[]byte("v2.3"),
	}
)

// Truck models a truck outfitted with an IoT device which sends back measurements.
type Truck struct {
	simulatedMeasurements []common.SimulatedMeasurement
	tags                  []common.Tag
}

// TickAll advances all Distributions of a Truck.
func (t *Truck) TickAll(d time.Duration) {
	for i := range t.simulatedMeasurements {
		t.simulatedMeasurements[i].Tick(d)
	}
}

// Measurements returns the trucks measurements.
func (t Truck) Measurements() []common.SimulatedMeasurement {
	return t.simulatedMeasurements
}

// Tags returns the truck tags.
func (t Truck) Tags() []common.Tag {
	return t.tags
}

func newTruckMeasurements(start time.Time) []common.SimulatedMeasurement {
	return []common.SimulatedMeasurement{
		NewReadingsMeasurement(start),
		NewDiagnosticsMeasurement(start),
	}
}

// NewTruck creates a new truck in a simulated iot use case
func NewTruck(i int, start time.Time) common.Generator {
	truck := newTruckWithMeasurementGenerator(i, start, newTruckMeasurements)
	return &truck
}

func newTruckWithMeasurementGenerator(i int, start time.Time, generator func(time.Time) []common.SimulatedMeasurement) Truck {
	sm := generator(start)

	m := modelChoices[rand.Intn(len(modelChoices))]

	h := Truck{
		tags: []common.Tag{
			{Key: []byte("name"), Value: []byte(fmt.Sprintf(truckNameFmt, i))},
			{Key: []byte("fleet"), Value: common.RandomByteStringSliceChoice(usecase.FleetChoices)},
			{Key: []byte("driver"), Value: common.RandomByteStringSliceChoice(driverChoices)},
			{Key: []byte("model"), Value: m.Name},
			{Key: []byte("device_version"), Value: common.RandomByteStringSliceChoice(deviceVersionChoices)},
			{Key: []byte("load_capacity"), Value: m.LoadCapacity},
			{Key: []byte("fuel_capacity"), Value: m.FuelCapacity},
			{Key: []byte("nominal_fuel_consumption"), Value: m.FuelConsumption},
		},
		simulatedMeasurements: sm,
	}

	return h
}
