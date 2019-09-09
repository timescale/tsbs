package devops

import (
	"math/rand"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

func TestPostgresqlMeasurementTick(t *testing.T) {
	now := time.Now()
	m := NewPostgresqlMeasurement(now)
	duration := time.Second
	oldVals := map[string]float64{}
	fields := ldmToFieldLabels(postgresqlFields)
	for i, ldm := range postgresqlFields {
		oldVals[string(ldm.Label)] = m.Distributions[i].Get()
	}

	rand.Seed(123)
	m.Tick(duration)
	err := testDistributionsAreDifferent(oldVals, m.SubsystemMeasurement, fields)
	if err != nil {
		t.Errorf(err.Error())
	}
	m.Tick(duration)
	err = testDistributionsAreDifferent(oldVals, m.SubsystemMeasurement, fields)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestPostgresqlMeasurementToPoint(t *testing.T) {
	now := time.Now()
	m := NewPostgresqlMeasurement(now)
	duration := time.Second
	m.Tick(duration)

	p := serialize.NewPoint()
	m.ToPoint(p)
	if got := string(p.MeasurementName()); got != string(labelPostgresql) {
		t.Errorf("incorrect measurement name: got %s want %s", got, labelPostgresql)
	}

	for _, ldm := range postgresqlFields {
		if got := p.GetFieldValue(ldm.Label); got == nil {
			t.Errorf("field %s returned a nil value unexpectedly", ldm.Label)
		}
	}
}
