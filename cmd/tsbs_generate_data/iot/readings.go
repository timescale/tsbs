package iot

import (
	"math/rand"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

const (
	maxLatitude        = 90.0
	maxLongitude       = 180.0
	maxElevation       = 5000.0
	maxVelocity        = 100
	maxHeading         = 360.0
	maxGrade           = 100.0
	maxFuelConsumption = 50
)

var (
	labelReadings        = []byte("readings")
	labelLatitude        = []byte("latitude")
	labelLongitude       = []byte("longitude")
	labelElevation       = []byte("elevation")
	labelVelocity        = []byte("velocity")
	labelHeading         = []byte("heading")
	labelGrade           = []byte("grade")
	labelFuelConsumption = []byte("fuel_consumption")
	geoStepUD            = common.UD(-0.005, 0.005)

	bigUD   = common.UD(-10, 10)
	smallUD = common.UD(-5, 5)

	readingsFields = []common.LabeledDistributionMaker{
		{
			Label: labelLatitude,
			DistributionMaker: func() common.Distribution {
				return common.FP(
					common.CWD(geoStepUD, -90.0, 90.0, rand.Float64()*maxLatitude),
					5,
				)
			},
		},
		{
			Label: labelLongitude,
			DistributionMaker: func() common.Distribution {
				return common.FP(
					common.CWD(geoStepUD, -180, 180, rand.Float64()*maxLongitude),
					5,
				)
			},
		},
		{
			Label: labelElevation,
			DistributionMaker: func() common.Distribution {
				return common.FP(
					common.CWD(bigUD, 0, maxElevation, rand.Float64()*500),
					0,
				)
			},
		},
		{
			Label: labelVelocity,
			DistributionMaker: func() common.Distribution {
				return common.FP(
					common.CWD(bigUD, 0, maxVelocity, 0),
					0,
				)
			},
		},
		{
			Label: labelHeading,
			DistributionMaker: func() common.Distribution {
				return common.FP(
					common.CWD(smallUD, 0, maxHeading, rand.Float64()*maxHeading),
					0,
				)
			},
		},
		{
			Label: labelGrade,
			DistributionMaker: func() common.Distribution {
				return common.FP(
					common.CWD(smallUD, 0, maxGrade, 0),
					0,
				)
			},
		},
		{
			Label: labelFuelConsumption,
			DistributionMaker: func() common.Distribution {
				return common.FP(
					common.CWD(smallUD, 0, maxFuelConsumption, maxFuelConsumption/2),
					1,
				)
			},
		},
	}
)

// ReadingsMeasurement represents a subset of truck measurement readings.
type ReadingsMeasurement struct {
	*common.SubsystemMeasurement
}

// ToPoint serializes ReadingsMeasurement to serialize.Point.
func (m *ReadingsMeasurement) ToPoint(p *serialize.Point) {
	p.SetMeasurementName(labelReadings)
	copy := m.Timestamp
	p.SetTimestamp(&copy)

	for i, d := range m.Distributions {
		p.AppendField(readingsFields[i].Label, float64(d.Get()))
	}
}

// NewReadingsMeasurement creates a new ReadingsMeasurement with start time.
func NewReadingsMeasurement(start time.Time) *ReadingsMeasurement {
	sub := common.NewSubsystemMeasurementWithDistributionMakers(start, readingsFields)

	return &ReadingsMeasurement{
		SubsystemMeasurement: sub,
	}
}
