package devops

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

func ldmToFieldLabels(ldm []common.LabeledDistributionMaker) [][]byte {
	ret := make([][]byte, 0)
	for _, l := range ldm {
		ret = append(ret, l.Label)
	}
	return ret
}

// testDistributionsAreDifferent is used to check that the field values for a
// measurement have changed after a call to Tick.
func testDistributionsAreDifferent(oldVals map[string]float64, m *common.SubsystemMeasurement, fields [][]byte) error {
	for i, f := range fields {
		k := string(f)
		curr := m.Distributions[i].Get()
		if oldVals[k] == curr {
			return fmt.Errorf("value for %s unexpectedly the same: got %f", k, curr)
		}
		oldVals[k] = curr
	}
	return nil
}

func TestCPUMeasurementTick(t *testing.T) {
	now := time.Now()
	m := NewCPUMeasurement(now)
	duration := time.Second
	oldVals := map[string]float64{}
	fields := ldmToFieldLabels(cpuFields)
	for i, ldm := range cpuFields {
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
		if got := p.GetFieldValue(ldm.Label); got == nil {
			t.Errorf("field %s returned a nil value unexpectedly", ldm.Label)
		}
	}
}

func TestSingleCPUMeasurementTick(t *testing.T) {
	now := time.Now()
	m := newSingleCPUMeasurement(now)
	duration := time.Second
	oldVals := map[string]float64{}
	fields := ldmToFieldLabels(cpuFields[:1]) // only the first field in this use case
	if got := len(m.Distributions); got != 1 {
		t.Errorf("single cpu has more than 1 distribution: got %d", got)
	}
	for i, f := range fields {
		oldVals[string(f)] = m.Distributions[i].Get()
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
		if got := p.GetFieldValue(ldm.Label); got == nil {
			t.Errorf("field %s returned a nil value unexpectedly", ldm.Label)
		}
	}
}
