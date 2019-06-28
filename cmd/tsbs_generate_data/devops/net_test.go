package devops

import (
	"math/rand"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

func TestNetMeasurementTick(t *testing.T) {
	now := time.Now()
	m := NewNetMeasurement(now)
	origName := string(m.interfaceName)
	duration := time.Second
	oldVals := map[string]float64{}
	fields := ldmToFieldLabels(netFields)
	for i, ldm := range netFields {
		oldVals[string(ldm.Label)] = m.Distributions[i].Get()
	}

	rand.Seed(123)
	m.Tick(duration)
	err := testDistributionsAreDifferent(oldVals, m.SubsystemMeasurement, fields)
	if err != nil {
		t.Errorf(err.Error())
	}
	if got := string(m.interfaceName); got != origName {
		t.Errorf("server name updated unexpectedly: got %s want %s", got, origName)
	}
	m.Tick(duration)
	err = testDistributionsAreDifferent(oldVals, m.SubsystemMeasurement, fields)
	if err != nil {
		t.Errorf(err.Error())
	}
	if got := string(m.interfaceName); got != origName {
		t.Errorf("server name updated unexpectedly: got %s want %s", got, origName)
	}
}

func TestNetMeasurementToPoint(t *testing.T) {
	now := time.Now()
	m := NewNetMeasurement(now)
	origName := m.interfaceName
	duration := time.Second
	m.Tick(duration)

	p := serialize.NewPoint()
	m.ToPoint(p)
	if got := string(p.MeasurementName()); got != string(labelNet) {
		t.Errorf("incorrect measurement name: got %s want %s", got, labelNet)
	}

	if got := p.GetTagValue(labelNetTagInterface); got.(string) != origName {
		t.Errorf("incorrect tag value for server name: got %s want %s", got, origName)
	}

	for _, ldm := range netFields {
		if got := p.GetFieldValue(ldm.Label); got == nil {
			t.Errorf("field %s returned a nil value unexpectedly", ldm.Label)
		}
	}
}
