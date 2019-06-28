package devops

import (
	"math/rand"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

func TestNginxMeasurementTick(t *testing.T) {
	now := time.Now()
	m := NewNginxMeasurement(now)
	origName := string(m.serverName)
	origPort := string(m.port)
	duration := time.Second
	oldVals := map[string]float64{}
	fields := ldmToFieldLabels(nginxFields)
	for i, ldm := range nginxFields {
		oldVals[string(ldm.Label)] = m.Distributions[i].Get()
	}

	rand.Seed(123)
	m.Tick(duration)
	err := testDistributionsAreDifferent(oldVals, m.SubsystemMeasurement, fields)
	if err != nil {
		t.Errorf(err.Error())
	}
	if got := string(m.serverName); got != origName {
		t.Errorf("server name updated unexpectedly: got %s want %s", got, origName)
	}
	if got := string(m.port); got != origPort {
		t.Errorf("port updated unexpectedly: got %s want %s", got, origPort)
	}
	m.Tick(duration)
	err = testDistributionsAreDifferent(oldVals, m.SubsystemMeasurement, fields)
	if err != nil {
		t.Errorf(err.Error())
	}
	if got := string(m.serverName); got != origName {
		t.Errorf("server name updated unexpectedly: got %s want %s", got, origName)
	}
	if got := string(m.port); got != origPort {
		t.Errorf("port updated unexpectedly: got %s want %s", got, origPort)
	}
}

func TestNginxMeasurementToPoint(t *testing.T) {
	now := time.Now()
	m := NewNginxMeasurement(now)
	origName := m.serverName
	origPort := m.port
	duration := time.Second
	m.Tick(duration)

	p := serialize.NewPoint()
	m.ToPoint(p)
	if got := string(p.MeasurementName()); got != string(labelNginx) {
		t.Errorf("incorrect measurement name: got %s want %s", got, labelNginx)
	}

	if got := p.GetTagValue(labelNginxTagServer); got.(string) != origName {
		t.Errorf("incorrect tag value for server name: got %s want %s", got, origName)
	}

	if got := p.GetTagValue(labelNginxTagPort); got.(string) != origPort {
		t.Errorf("incorrect tag value for port: got %s want %s", got, origPort)
	}

	for _, ldm := range nginxFields {
		if got := p.GetFieldValue(ldm.Label); got == nil {
			t.Errorf("field %s returned a nil value unexpectedly", ldm.Label)
		}
	}
}
