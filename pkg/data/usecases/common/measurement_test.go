package common

import (
	"github.com/timescale/tsbs/pkg/data"
	"math"
	"testing"
	"time"
)

func ldmToFieldLabels(ldm []LabeledDistributionMaker) [][]byte {
	ret := make([][]byte, 0)
	for _, l := range ldm {
		ret = append(ret, l.Label)
	}
	return ret
}

// monotonicDistribution simply increases the state by 1 every time Advance is
// called. This is a useful distribution for easy testing.
type monotonicDistribution struct {
	state float64
}

func (d *monotonicDistribution) Advance() {
	d.state++
}

func (d *monotonicDistribution) Get() float64 {
	return d.state
}

func TestNewSubsystemMeasurement(t *testing.T) {
	cases := []struct {
		desc       string
		numDistros int
	}{
		{
			desc:       "no distros",
			numDistros: 0,
		},
		{
			desc:       "one distro",
			numDistros: 1,
		},
		{
			desc:       "three distros",
			numDistros: 3,
		},
	}

	for _, c := range cases {
		now := time.Now()
		m := NewSubsystemMeasurement(now, c.numDistros)
		if !m.Timestamp.Equal(now) {
			t.Errorf("%s: incorrect timestamp set: got %v want %v", c.desc, m.Timestamp, now)
		}
		if got := len(m.Distributions); got != c.numDistros {
			t.Errorf("%s: incorrect number of distros: got %d want %d", c.desc, got, c.numDistros)
		}
	}
}

func TestNewSubsystemMeasurementWithDistributionMakers(t *testing.T) {
	makers := []LabeledDistributionMaker{
		{[]byte("foo"), func() Distribution { return &monotonicDistribution{state: 0.0} }},
		{[]byte("bar"), func() Distribution { return &monotonicDistribution{state: 1.0} }},
	}
	now := time.Now()
	m := NewSubsystemMeasurementWithDistributionMakers(now, makers)
	if !m.Timestamp.Equal(now) {
		t.Errorf("incorrect timestamp set: got %v want %v", m.Timestamp, now)
	}

	if got := len(m.Distributions); got != len(makers) {
		t.Errorf("incorrect number of distros: got %d want %d", got, len(makers))
	}

	for i := 0; i < 2; i++ {
		md := m.Distributions[i].(*monotonicDistribution)
		if got := md.state; got != float64(i) {
			t.Errorf("distribution %d has wrong state: got %f want %f", i, got, float64(i))
		}
	}
}

func TestSubsytemMeasurementTick(t *testing.T) {
	now := time.Now()
	numDistros := 3
	m := NewSubsystemMeasurement(now, numDistros)
	for i := 0; i < numDistros; i++ {
		m.Distributions[i] = &monotonicDistribution{state: float64(i)}
	}
	m.Tick(time.Nanosecond)
	if got := m.Timestamp.UnixNano(); got != now.UnixNano()+1 {
		t.Errorf("tick did not increase timestamp correct: got %d want %d", got, now.UnixNano()+1)
	}
	for i := 0; i < numDistros; i++ {
		if got := m.Distributions[i].Get(); got != float64(i+1) {
			t.Errorf("tick did not advance distro %d: got %f want %f", i, got, float64(i+1))
		}
	}
}

const (
	toPointState      = 0.5
	toPointLabel      = "foo"
	toPointFieldLabel = "foo1"
)

func TestToPoint(t *testing.T) {
	now := time.Now()
	m, makers := setupToPoint(now)
	p := data.NewPoint()
	m.ToPoint(p, []byte(toPointLabel), makers)
	testCommonToPoint(t, p, toPointState+1.0)
}

func TestToPointAllInt64(t *testing.T) {
	now := time.Now()
	m, makers := setupToPoint(now)
	p := data.NewPoint()
	m.ToPointAllInt64(p, []byte(toPointLabel), makers)
	testCommonToPoint(t, p, math.Floor(toPointState+1.0))
}

func setupToPoint(start time.Time) (*SubsystemMeasurement, []LabeledDistributionMaker) {
	makers := []LabeledDistributionMaker{
		{[]byte(toPointFieldLabel), func() Distribution { return &monotonicDistribution{state: toPointState} }},
	}
	m := NewSubsystemMeasurementWithDistributionMakers(start, makers)
	m.Tick(time.Nanosecond)
	return m, makers
}

func testCommonToPoint(t *testing.T, p *data.Point, fieldVal float64) {
	if got := string(p.MeasurementName()); got != toPointLabel {
		t.Errorf("measurement name incorrect: got %s want %s", got, toPointLabel)
	}

	for _, pointFieldVal := range p.FieldValues() {
		switch pointFieldVal.(type) {
		case int64:
			if fieldVal != float64(pointFieldVal.(int64)) {
				t.Errorf("incorrect field value: got %f want %f", pointFieldVal, fieldVal)
			}
		case float64:
			if fieldVal != pointFieldVal.(float64) {
				t.Errorf("incorrect field value: got %f want %f", pointFieldVal, fieldVal)
			}
		default:
			t.Errorf("wrong point field val sent, unexpected type")
		}
	}
	for _, pointFieldLabel := range p.FieldKeys() {
		if toPointFieldLabel != string(pointFieldLabel) {
			t.Errorf("incorrect field label: got %s want %s", pointFieldLabel, toPointFieldLabel)
		}
	}
}
