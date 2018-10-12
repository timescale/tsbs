package devops

import (
	"math/rand"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

func TestCPUMeasurementTick(t *testing.T) {
	now := time.Now()
	m := NewCPUMeasurement(now)
	duration := time.Second
	oldVals := map[string]float64{}
	fields := ldmToFieldLabels(cpuFields)
	for i, ldm := range cpuFields {
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

func TestCPUMeasurementToPoint(t *testing.T) {
	now := time.Now()
	m := NewCPUMeasurement(now)
	duration := time.Second
	m.Tick(duration)

	p := serialize.NewPoint()
	m.ToPoint(p)
	if got := string(p.MeasurementName()); got != string(labelCPU) {
		t.Errorf("incorrect measurement name: got %s want %s", got, labelCPU)
	}

	for _, ldm := range cpuFields {
		if got := p.GetFieldValue(ldm.label); got == nil {
			t.Errorf("field %s returned a nil value unexpectedly", ldm.label)
		}
	}
}

func TestSingleCPUMeasurementTick(t *testing.T) {
	now := time.Now()
	m := newSingleCPUMeasurement(now)
	duration := time.Second
	oldVals := map[string]float64{}
	fields := ldmToFieldLabels(cpuFields[:1]) // only the first field in this use case
	if got := len(m.distributions); got != 1 {
		t.Errorf("single cpu has more than 1 distribution: got %d", got)
	}
	for i, f := range fields {
		oldVals[string(f)] = m.distributions[i].Get()
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

func TestSingleCPUMeasurementToPoint(t *testing.T) {
	now := time.Now()
	m := newSingleCPUMeasurement(now)
	duration := time.Second
	fields := cpuFields[:1] // only the first field in this use case
	m.Tick(duration)

	p := serialize.NewPoint()
	m.ToPoint(p)
	if got := string(p.MeasurementName()); got != string(labelCPU) {
		t.Errorf("incorrect measurement name: got %s want %s", got, labelCPU)
	}

	if got := len(p.FieldKeys()); got != 1 {
		t.Errorf("point has more than 1 field for single cpu: got %d", got)
	}

	for _, ldm := range fields {
		if got := p.GetFieldValue(ldm.label); got == nil {
			t.Errorf("field %s returned a nil value unexpectedly", ldm.label)
		}
	}
}
