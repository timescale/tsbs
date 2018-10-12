package devops

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

func ldmToFieldLabels(ldm []labeledDistributionMaker) [][]byte {
	ret := make([][]byte, 0)
	for _, l := range ldm {
		ret = append(ret, l.label)
	}
	return ret
}

// testDistributionsAreDifferent is used to check that the field values for a
// measurement have changed after a call to Tick.
func testDistributionsAreDifferent(oldVals map[string]float64, m *subsystemMeasurement, fields [][]byte) error {
	for i, f := range fields {
		k := string(f)
		curr := m.distributions[i].Get()
		if oldVals[k] == curr {
			return fmt.Errorf("value for %s unexpectedly the same: got %f", k, curr)
		}
		oldVals[k] = curr
	}
	return nil
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
		m := newSubsystemMeasurement(now, c.numDistros)
		if !m.timestamp.Equal(now) {
			t.Errorf("%s: incorrect timestamp set: got %v want %v", c.desc, m.timestamp, now)
		}
		if got := len(m.distributions); got != c.numDistros {
			t.Errorf("%s: incorrect number of distros: got %d want %d", c.desc, got, c.numDistros)
		}
	}
}

func TestNewSubsystemMeasurementWithDistributionMakers(t *testing.T) {
	makers := []labeledDistributionMaker{
		{[]byte("foo"), func() common.Distribution { return &monotonicDistribution{state: 0.0} }},
		{[]byte("bar"), func() common.Distribution { return &monotonicDistribution{state: 1.0} }},
	}
	now := time.Now()
	m := newSubsystemMeasurementWithDistributionMakers(now, makers)
	if !m.timestamp.Equal(now) {
		t.Errorf("incorrect timestamp set: got %v want %v", m.timestamp, now)
	}

	if got := len(m.distributions); got != len(makers) {
		t.Errorf("incorrect number of distros: got %d want %d", got, len(makers))
	}

	for i := 0; i < 2; i++ {
		md := m.distributions[i].(*monotonicDistribution)
		if got := md.state; got != float64(i) {
			t.Errorf("distribution %d has wrong state: got %f want %f", i, got, float64(i))
		}
	}
}

func TestSubsytemMeasurementTick(t *testing.T) {
	now := time.Now()
	numDistros := 3
	m := newSubsystemMeasurement(now, numDistros)
	for i := 0; i < numDistros; i++ {
		m.distributions[i] = &monotonicDistribution{state: float64(i)}
	}
	m.Tick(time.Nanosecond)
	if got := m.timestamp.UnixNano(); got != now.UnixNano()+1 {
		t.Errorf("tick did not increase timestamp correct: got %d want %d", got, now.UnixNano()+1)
	}
	for i := 0; i < numDistros; i++ {
		if got := m.distributions[i].Get(); got != float64(i+1) {
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
	p := serialize.NewPoint()
	m.toPoint(p, []byte(toPointLabel), makers)
	testCommonToPoint(t, p, toPointState+1.0)
}

func TestToPointAllInt64(t *testing.T) {
	now := time.Now()
	m, makers := setupToPoint(now)
	p := serialize.NewPoint()
	m.toPointAllInt64(p, []byte(toPointLabel), makers)
	testCommonToPoint(t, p, math.Floor(toPointState+1.0))
}

func setupToPoint(start time.Time) (*subsystemMeasurement, []labeledDistributionMaker) {
	makers := []labeledDistributionMaker{
		{[]byte(toPointFieldLabel), func() common.Distribution { return &monotonicDistribution{state: toPointState} }},
	}
	m := newSubsystemMeasurementWithDistributionMakers(start, makers)
	m.Tick(time.Nanosecond)
	return m, makers
}

func testCommonToPoint(t *testing.T, p *serialize.Point, fieldVal float64) {
	// serialize the point to check output
	b := new(bytes.Buffer)
	serializer := &serialize.InfluxSerializer{}
	serializer.Serialize(p, b)

	if got := string(p.MeasurementName()); got != toPointLabel {
		t.Errorf("measurement name incorrect: got %s want %s", got, toPointLabel)
	}

	output := b.String()

	args := strings.Split(output, " ")
	fieldArgs := strings.Split(args[1], "=")
	fieldArgs[1] = strings.Replace(fieldArgs[1], "i", "", -1)
	if got := fieldArgs[0]; got != toPointFieldLabel {
		t.Errorf("incorrect field label: got %s want %s", got, toPointFieldLabel)
	}
	if got, err := strconv.ParseFloat(fieldArgs[1], 64); err != nil || got != fieldVal {
		if err != nil {
			t.Errorf("could not parse field value as float64: %v", err)
		} else {
			t.Errorf("incorrect field value: got %f want %f", got, fieldVal)
		}
	}
}
