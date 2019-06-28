package devops

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

func testIfInByteStringSlice(t *testing.T, arr [][]byte, choice []byte) {
	for _, x := range arr {
		if bytes.Equal(x, choice) {
			return
		}
	}
	t.Errorf("could not find choice in array: %s", choice)
}

func testIfInStringSlice(t *testing.T, arr []string, choice string) {
	for _, x := range arr {
		if x == choice {
			return
		}
	}
	t.Errorf("could not find choice in array: %s", choice)
}

func TestDiskMeasurementTick(t *testing.T) {
	now := time.Now()
	m := NewDiskMeasurement(now)
	origPath := string(m.path)
	origFS := string(m.fsType)
	duration := time.Second
	oldVals := map[string]float64{}
	fields := [][]byte{[]byte("free")}
	for i, f := range fields {
		oldVals[string(f)] = m.Distributions[i].Get()
	}

	rand.Seed(123)
	m.Tick(duration)
	err := testDistributionsAreDifferent(oldVals, m.SubsystemMeasurement, fields)
	if err != nil {
		t.Errorf(err.Error())
	}
	if got := string(m.path); got != origPath {
		t.Errorf("disk path tag is incorrect: got %s want %s", got, origPath)
	}
	if got := string(m.fsType); got != origFS {
		t.Errorf("disk FS type is incorrect: got %s want %s", got, origFS)
	}

	m.Tick(duration)
	err = testDistributionsAreDifferent(oldVals, m.SubsystemMeasurement, fields)
	if err != nil {
		t.Errorf(err.Error())
	}
	if got := string(m.path); got != origPath {
		t.Errorf("disk path tag is incorrect: got %s want %s", got, origPath)
	}
	if got := string(m.fsType); got != origFS {
		t.Errorf("disk FS type is incorrect: got %s want %s", got, origFS)
	}
}

func TestDiskMeasurementToPoint(t *testing.T) {
	now := time.Now()
	m := NewDiskMeasurement(now)
	origPath := m.path
	origFS := m.fsType
	testIfInStringSlice(t, diskFSTypeChoices, m.fsType)
	duration := time.Second
	m.Tick(duration)

	p := serialize.NewPoint()
	m.ToPoint(p)
	if got := string(p.MeasurementName()); got != string(labelDisk) {
		t.Errorf("incorrect measurement name: got %s want %s", got, labelDisk)
	}
	if got := p.GetTagValue(labelDiskPath); got.(string) != origPath {
		t.Errorf("disk path tag is incorrect: got %s want %s", got, origPath)
	}
	if got := p.GetTagValue(labelDiskFSType); got.(string) != origFS {
		t.Errorf("disk FS type is incorrect: got %s want %s", got, origFS)
	}

	free := int64(m.Distributions[0].Get())
	if got := p.GetFieldValue(labelDiskFree); got != free {
		t.Errorf("free data out of sync with distribution: got %d want %d", got, free)
	}

	total := p.GetFieldValue(labelDiskTotal).(int64)
	if got := p.GetFieldValue(labelDiskTotal).(int64); got != oneTerabyte {
		t.Errorf("total data is not 1TB: got %d", got)
	}
	used := p.GetFieldValue(labelDiskUsed).(int64)
	if total-used != free {
		t.Errorf("disk semantics do not make sense: %d - %d != %d", total, used, free)
	}

	totalInodes := p.GetFieldValue(labelDiskINodesTotal).(int64)
	usedINodes := p.GetFieldValue(labelDiskINodesUsed).(int64)
	freeInodes := p.GetFieldValue(labelDiskINodesFree).(int64)
	if totalInodes-usedINodes != freeInodes {
		t.Errorf("disk semantics for inodes do not make sense: %d - %d != %d", total, used, free)
	}

	for _, f := range diskFields {
		if got := p.GetFieldValue(f); got == nil {
			t.Errorf("field %s returned a nil value unexpectedly", f)
		}
	}
}
