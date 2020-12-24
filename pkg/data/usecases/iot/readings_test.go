package iot

import (
	"github.com/timescale/tsbs/pkg/data"
	"testing"
	"time"
)

func TestReadingsMeasurementToPoint(t *testing.T) {
	now := time.Now()
	m := NewReadingsMeasurement(now)
	duration := time.Second
	m.Tick(duration)

	p := data.NewPoint()
	m.ToPoint(p)
	if got := string(p.MeasurementName()); got != string(labelReadings) {
		t.Errorf("incorrect measurement name: got %s want %s", got, labelReadings)
	}

	for _, ldm := range readingsFields {
		if got := p.GetFieldValue(ldm.Label); got == nil {
			t.Errorf("field %s returned a nil value unexpectedly", ldm.Label)
		}
	}
}
