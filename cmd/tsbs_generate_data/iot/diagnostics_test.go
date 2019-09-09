package iot

import (
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

func TestDiagnosticsMeasurementToPoint(t *testing.T) {
	now := time.Now()
	m := NewDiagnosticsMeasurement(now)
	duration := time.Second
	m.Tick(duration)

	p := serialize.NewPoint()
	m.ToPoint(p)
	if got := string(p.MeasurementName()); got != string(labelDiagnostics) {
		t.Errorf("incorrect measurement name: got %s want %s", got, labelReadings)
	}

	for _, ldm := range diagnosticsFields {
		if got := p.GetFieldValue(ldm.Label); got == nil {
			t.Errorf("field %s returned a nil value unexpectedly", ldm.Label)
		}
	}
}

func TestCustomFuelDistribution(t *testing.T) {
	testCount := 5
	fuelMin, fuelMax := 10.0, 100.0
	fuelStep := &common.ConstantDistribution{State: -1}

	clampedDist := common.CWD(fuelStep, fuelMin, fuelMax, fuelMax)
	clampedCopy := *clampedDist

	fuelDist := &customFuelDistribution{&clampedCopy}

	for i := 0; i < testCount; i++ {
		for clampedDist.Get() > fuelMin {
			clampedDist.Advance()
			fuelDist.Advance()

			if clampedDist.Get() != fuelDist.Get() {

				if clampedDist.Get() == fuelMin {
					if fuelDist.Get() != fuelMax {
						t.Fatalf("expected fuel to be refilled when state hits minimum")
					}
					break
				}

				t.Fatalf("distributions don't match when they are supposed to")
			}
		}

		// Resetting the distribution and running another test.
		clampedDist = common.CWD(fuelStep, fuelMin, fuelMax, fuelMax)
	}
}
