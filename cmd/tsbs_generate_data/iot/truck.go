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
	Name            string
	LoadCapacity    float32
	FuelCapacity    float32
	FuelConsumption float32
}

var (
	driverChoices = []string{
		"Derek",
		"Rodney",
		"Albert",
		"Andy",
		"Seth",
		"Trish",
	}

	modelChoices = []model{
		{
			Name:            "F-150",
			LoadCapacity:    2000,
			FuelCapacity:    200,
			FuelConsumption: 15,
		},
		{
			Name:            "G-2000",
			LoadCapacity:    5000,
			FuelCapacity:    300,
			FuelConsumption: 19,
		},
		{
			Name:            "H-2",
			LoadCapacity:    1500,
			FuelCapacity:    150,
			FuelConsumption: 12,
		},
	}

	deviceVersionChoices = []string{
		"v1.0",
		"v1.5",
		"v2.0",
		"v2.3",
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
			{Key: []byte("name"), Value: fmt.Sprintf(truckNameFmt, i)},
			{Key: []byte("fleet"), Value: common.RandomStringSliceChoice(usecase.FleetChoices)},
			{Key: []byte("driver"), Value: common.RandomStringSliceChoice(driverChoices)},
			{Key: []byte("model"), Value: m.Name},
			{Key: []byte("device_version"), Value: common.RandomStringSliceChoice(deviceVersionChoices)},
			{Key: []byte("load_capacity"), Value: m.LoadCapacity},
			{Key: []byte("fuel_capacity"), Value: m.FuelCapacity},
			{Key: []byte("nominal_fuel_consumption"), Value: m.FuelConsumption},
		},
		simulatedMeasurements: sm,
	}

	return h
}
