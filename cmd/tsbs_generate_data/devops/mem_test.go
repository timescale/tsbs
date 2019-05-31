package devops

import (
	"math/rand"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

func testIfInInt64Slice(t *testing.T, arr []int64, choice int64) {
	for _, x := range arr {
		if x == choice {
			return
		}
	}
	t.Errorf("could not find choice in array: %d", choice)
}

func TestMemMeasurementTick(t *testing.T) {
	now := time.Now()
	m := NewMemMeasurement(now)
	duration := time.Second
	oldVals := map[string]float64{}
	oldTotal := m.bytesTotal
	testIfInInt64Slice(t, memoryTotalChoices, oldTotal)
	fields := [][]byte{[]byte("used"), []byte("cached"), []byte("buffered")}
	for i, f := range fields {
		oldVals[string(f)] = m.Distributions[i].Get()
	}

	rand.Seed(123)
	m.Tick(duration)
	err := testDistributionsAreDifferent(oldVals, m.SubsystemMeasurement, fields)
	if err != nil {
		t.Errorf(err.Error())
	}
	if got := m.bytesTotal; got != oldTotal {
		t.Errorf("total bytes unexpectedly changed: got %d want %d", got, oldTotal)
	}
	m.Tick(duration)
	err = testDistributionsAreDifferent(oldVals, m.SubsystemMeasurement, fields)
	if err != nil {
		t.Errorf(err.Error())
	}
	if got := m.bytesTotal; got != oldTotal {
		t.Errorf("total bytes unexpectedly changed: got %d want %d", got, oldTotal)
	}
}

func TestMemMeasurementToPoint(t *testing.T) {
	now := time.Now()
	m := NewMemMeasurement(now)
	duration := time.Second
	m.Tick(duration)

	p := serialize.NewPoint()
	m.ToPoint(p)
	if got := string(p.MeasurementName()); got != string(labelMem) {
		t.Errorf("incorrect measurement name: got %s want %s", got, labelMem)
	}
	totalKey := []byte("total")
	if got := p.GetFieldValue(totalKey); got != m.bytesTotal {
		t.Errorf("incorrect total: got %d want %d", got, m.bytesTotal)
	}

	usedKey := []byte("used")
	cachedKey := []byte("cached")
	bufferedKey := []byte("buffered")
	availableKey := []byte("available")

	used := p.GetFieldValue(usedKey).(int64)
	if used < 0 {
		t.Errorf("used data semantics incorrect: %d is less than 0", used)
	}
	want := int64(m.Distributions[0].Get())
	if got := used; got != want {
		t.Errorf("used data out of sync with distribution: got %d want %d", got, want)
	}

	cached := p.GetFieldValue(cachedKey).(int64)
	if cached < 0 {
		t.Errorf("cached data semantics incorrect: %d is less than 0", cached)
	}
	want = int64(m.Distributions[1].Get())
	if got := cached; got != want {
		t.Errorf("cached data out of sync with distribution: got %d want %d", got, want)
	}

	buffered := p.GetFieldValue(bufferedKey).(int64)
	if buffered < 0 {
		t.Errorf("buffered data semantics incorrect: %d is less than 0", buffered)
	}
	want = int64(m.Distributions[2].Get())
	if got := buffered; got != want {
		t.Errorf("buffered data out of sync with distribution: got %d want %d", got, want)
	}

	total := p.GetFieldValue(totalKey).(int64)
	if total < 0 {
		t.Errorf("total data semantics incorrect: %d is less than 0", total)
	}
	available := p.GetFieldValue(availableKey).(int64)
	if available < 0 {
		t.Errorf("available data semantics incorrect: %d is less than 0", available)
	}

	if total-int64(used) != int64(available) {
		t.Errorf("memory semantics do not make sense: %d - %d != %d", total, used, available)
	}

	usedPerc := 100.0 * float64(used) / float64(total)
	if got := p.GetFieldValue([]byte("used_percent")); got != usedPerc {
		t.Errorf("memory semantics do not make sense (used perc): got %f want %f", got, usedPerc)
	}

	availablePerc := 100.0 * float64(available) / float64(total)
	if got := p.GetFieldValue([]byte("available_percent")); got != availablePerc {
		t.Errorf("memory semantics do not make sense (available perc): got %f want %f", got, availablePerc)
	}

	bufferedPerc := 100.0 * float64(buffered) / float64(total)
	if got := p.GetFieldValue([]byte("buffered_percent")); got != bufferedPerc {
		t.Errorf("memory semantics do not make sense (buffered perc): got %f want %f", got, bufferedPerc)
	}

	for _, f := range memoryFieldKeys {
		if got := p.GetFieldValue(f); got == nil {
			t.Errorf("field %s returned a nil value unexpectedly", f)
		}
	}
}
