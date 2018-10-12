package devops

import (
	"math/rand"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

func TestRandMeasurementTick(t *testing.T) {
	now := time.Now()
	m := NewRandMeasurement(now)
	duration := time.Second
	oldVals := map[string]float64{}
	fields := ldmToFieldLabels(randFields)
	for i, ldm := range randFields {
		oldVals[string(ldm.label)] = m.distributions[i].Get()
	}

	rand.Seed(123)
	m.Tick(duration)
	err := testDistributionsAreDifferent(oldVals, m.subsystemMeasurement, fields)
	if err != nil {
		t.Errorf(err.Error())
	}
	m.Tick(duration)
	err = testDistributionsAreDifferent(oldVals, m.subsystemMeasurement, fields)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestRandMeasurementToPoint(t *testing.T) {
	now := time.Now()
	m := NewRandMeasurement(now)
	duration := time.Second
	m.Tick(duration)

	p := serialize.NewPoint()
	m.ToPoint(p)
	if got := string(p.MeasurementName()); got != string(labelRand) {
		t.Errorf("incorrect measurement name: got %s want %s", got, labelRand)
	}

	for _, ldm := range randFields {
		if got := p.GetFieldValue(ldm.label); got == nil {
			t.Errorf("field %s returned a nil value unexpectedly", ldm.label)
		}
	}
}
