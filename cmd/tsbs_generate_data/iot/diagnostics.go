package iot

import (
	"math/rand"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

const (
	maxFuel = 1.0
	maxLoad = 5000.0
)

var (
	labelDiagnostics = []byte("diagnostics")
	labelFuelState   = []byte("fuel_state")
	labelCurrentLoad = []byte("current_load")
	labelStatus      = []byte("status")
	fuelUD           = common.UD(-0.001, 0)
	loadND           = common.ND(0, 1)
	statusND         = common.ND(0, 1)

	diagnosticsFields = []common.LabeledDistributionMaker{
		{
			Label:             labelFuelState,
			DistributionMaker: func() common.Distribution { return &customFuelDistribution{common.CWD(fuelUD, 0, maxFuel, maxFuel)} },
		},
		{
			Label:             labelCurrentLoad,
			DistributionMaker: func() common.Distribution { return common.CWD(loadND, 0, maxLoad, rand.Float64()*maxLoad) },
		},
		{
			Label:             labelStatus,
			DistributionMaker: func() common.Distribution { return common.CWD(statusND, 0, 5, 0) },
		},
	}
)

type customFuelDistribution struct {
	*common.ClampedRandomWalkDistribution
}

// Advance computes the next value of this distribution and stores it.
// Its custom behavior is to refuel the truck once it gets to the min value.
func (d *customFuelDistribution) Advance() {
	d.ClampedRandomWalkDistribution.Advance()
	if d.State == d.Min {
		d.State = d.Max
	}
}

// DiagnosticsMeasurement represents a diagnostics subset of measurements.
type DiagnosticsMeasurement struct {
	*common.SubsystemMeasurement
}

// ToPoint serializes DiagnosticsMeasurement to serialize.Point.
func (m *DiagnosticsMeasurement) ToPoint(p *serialize.Point) {
	p.SetMeasurementName(labelDiagnostics)
	copy := m.Timestamp
	p.SetTimestamp(&copy)

	p.AppendField(diagnosticsFields[0].Label, float64(m.Distributions[0].Get()))
	p.AppendField(diagnosticsFields[1].Label, float64(m.Distributions[1].Get()))
	p.AppendField(diagnosticsFields[2].Label, int64(m.Distributions[2].Get()))
}

// NewDiagnosticsMeasurement creates a DiagnosticsMeasurement with start time.
func NewDiagnosticsMeasurement(start time.Time) *DiagnosticsMeasurement {
	sub := common.NewSubsystemMeasurementWithDistributionMakers(start, diagnosticsFields)

	return &DiagnosticsMeasurement{
		SubsystemMeasurement: sub,
	}
}
