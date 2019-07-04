package devops

import (
	"math/rand"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

func TestDiskIOMeasurementTick(t *testing.T) {
	now := time.Now()
	m := NewDiskIOMeasurement(now)
	origSerial := string(m.serial)
	duration := time.Second
	oldVals := map[string]float64{}
	fields := ldmToFieldLabels(diskIOFields)
	for i, ldm := range diskIOFields {
		oldVals[string(ldm.Label)] = m.Distributions[i].Get()
	}

	rand.Seed(123)
	m.Tick(duration)
	err := testDistributionsAreDifferent(oldVals, m.SubsystemMeasurement, fields)
	if err != nil {
		t.Errorf(err.Error())
	}
	if got := string(m.serial); got != origSerial {
		t.Errorf("server name updated unexpectedly: got %s want %s", got, origSerial)
	}
	m.Tick(duration)
	err = testDistributionsAreDifferent(oldVals, m.SubsystemMeasurement, fields)
	if err != nil {
		t.Errorf(err.Error())
	}
	if got := string(m.serial); got != origSerial {
		t.Errorf("server name updated unexpectedly: got %s want %s", got, origSerial)
	}
}

func TestDiskIOMeasurementToPoint(t *testing.T) {
	now := time.Now()
	m := NewDiskIOMeasurement(now)
	origSerial := string(m.serial)
	duration := time.Second
	m.Tick(duration)

	p := serialize.NewPoint()
	m.ToPoint(p)
	if got := string(p.MeasurementName()); got != string(labelDiskIO) {
		t.Errorf("incorrect measurement name: got %s want %s", got, labelDiskIO)
	}

	if got := p.GetTagValue(labelDiskIOSerial).(string); got != origSerial {
		t.Errorf("incorrect tag value for server name: got %s want %s", got, origSerial)
	}

	for _, ldm := range diskIOFields {
		if got := p.GetFieldValue(ldm.Label); got == nil {
			t.Errorf("field %s returned a nil value unexpectedly", ldm.Label)
		}
	}
}
