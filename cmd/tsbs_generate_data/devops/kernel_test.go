package devops

import (
	"math/rand"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

func TestKernelMeasurementTick(t *testing.T) {
	now := time.Now()
	m := NewKernelMeasurement(now)
	duration := time.Second
	bootTime := m.bootTime
	oldVals := map[string]float64{}
	fields := ldmToFieldLabels(kernelFields)
	for i, ldm := range kernelFields {
		oldVals[string(ldm.Label)] = m.Distributions[i].Get()
	}

	rand.Seed(123)
	m.Tick(duration)
	err := testDistributionsAreDifferent(oldVals, m.SubsystemMeasurement, fields)
	if err != nil {
		t.Errorf(err.Error())
	}
	if got := m.bootTime; got != bootTime {
		t.Errorf("boot time changed unexpectedly: got %d", got)
	}
	m.Tick(duration)
	err = testDistributionsAreDifferent(oldVals, m.SubsystemMeasurement, fields)
	if err != nil {
		t.Errorf(err.Error())
	}
	if got := m.bootTime; got != bootTime {
		t.Errorf("boot time changed unexpectedly: got %d", got)
	}
}

func TestKernelMeasurementToPoint(t *testing.T) {
	now := time.Now()
	m := NewKernelMeasurement(now)
	duration := time.Second
	bootTime := m.bootTime
	m.Tick(duration)

	p := serialize.NewPoint()
	m.ToPoint(p)
	if got := string(p.MeasurementName()); got != string(labelKernel) {
		t.Errorf("incorrect measurement name: got %s want %s", got, labelKernel)
	}

	if got := p.GetFieldValue(labelKernelBootTime).(int64); got != bootTime {
		t.Errorf("boot time changed unexpectedly: got %d want %d", got, bootTime)
	}

	for _, ldm := range kernelFields {
		if got := p.GetFieldValue(ldm.Label); got == nil {
			t.Errorf("field %s returned a nil value unexpectedly", ldm.Label)
		}
	}
}
