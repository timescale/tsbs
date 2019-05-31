package devops

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

func TestNewHostMeasurements(t *testing.T) {
	start := time.Now()
	measurements := newHostMeasurements(start)
	if got := len(measurements); got != 9 {
		t.Errorf("incorrect number of measurements: got %d want %d", got, 9)
	}
	// Cast each measurement to its type; will panic if wrong types
	cpu := measurements[0].(*CPUMeasurement)
	if got := cpu.Timestamp; got != start {
		t.Errorf("incorrect CPU measurement timestamp: got %v want %v", got, start)
	}
	if got := len(cpu.Distributions); got <= 1 {
		t.Errorf("too few CPU measurements: got %d", got)
	}
	diskio := measurements[1].(*DiskIOMeasurement)
	if got := diskio.Timestamp; got != start {
		t.Errorf("incorrect diskio measurement timestamp: got %v want %v", got, start)
	}
	disk := measurements[2].(*DiskMeasurement)
	if got := disk.Timestamp; got != start {
		t.Errorf("incorrect disk measurement timestamp: got %v want %v", got, start)
	}
	kernel := measurements[3].(*KernelMeasurement)
	if got := kernel.Timestamp; got != start {
		t.Errorf("incorrect kernel measurement timestamp: got %v want %v", got, start)
	}
	mem := measurements[4].(*MemMeasurement)
	if got := mem.Timestamp; got != start {
		t.Errorf("incorrect mem measurement timestamp: got %v want %v", got, start)
	}
	net := measurements[5].(*NetMeasurement)
	if got := net.Timestamp; got != start {
		t.Errorf("incorrect net measurement timestamp: got %v want %v", got, start)
	}
	nginx := measurements[6].(*NginxMeasurement)
	if got := nginx.Timestamp; got != start {
		t.Errorf("incorrect nginx measurement timestamp: got %v want %v", got, start)
	}
	postgresql := measurements[7].(*PostgresqlMeasurement)
	if got := postgresql.Timestamp; got != start {
		t.Errorf("incorrect postgresql measurement timestamp: got %v want %v", got, start)
	}
	redis := measurements[8].(*RedisMeasurement)
	if got := redis.Timestamp; got != start {
		t.Errorf("incorrect redis measurement timestamp: got %v want %v", got, start)
	}
}

func TestNewCPUOnlyHostMeasurements(t *testing.T) {
	start := time.Now()
	measurements := newCPUOnlyHostMeasurements(start)
	if got := len(measurements); got != 1 {
		t.Errorf("incorrect number of measurements: got %d want %d", got, 9)
	}
	// Cast each measurement to its type; will panic if wrong types
	cpu := measurements[0].(*CPUMeasurement)
	if got := cpu.Timestamp; got != start {
		t.Errorf("incorrect CPU measurement timestamp: got %v want %v", got, start)
	}
	if got := len(cpu.Distributions); got <= 1 {
		t.Errorf("too few CPU measurements: got %d", got)
	}
}

func TestNewCPUSingleHostMeasurements(t *testing.T) {
	start := time.Now()
	measurements := newCPUSingleHostMeasurements(start)
	if got := len(measurements); got != 1 {
		t.Errorf("incorrect number of measurements: got %d want %d", got, 9)
	}
	// Cast each measurement to its type; will panic if wrong types
	cpu := measurements[0].(*CPUMeasurement)
	if got := cpu.Timestamp; got != start {
		t.Errorf("incorrect CPU measurement timestamp: got %v want %v", got, start)
	}
	if got := len(cpu.Distributions); got != 1 {
		t.Errorf("CPU measurements not equal to 1: got %d", got)
	}
}

func TestNewHost(t *testing.T) {
	now := time.Now()
	// test 1000 times to get diversity of results
	for i := 0; i < 1000; i++ {
		h := NewHost(i, now)
		if got := len(h.SimulatedMeasurements); got != 9 {
			t.Errorf("incorrect number of measurements: got %d want %d", got, 9)
		}
		wantName := fmt.Sprintf(hostFmt, i)
		if got := string(h.Name); got != wantName {
			t.Errorf("incorrect host name format: got %s want %s", got, wantName)
		}

		cpu := h.SimulatedMeasurements[0].(*CPUMeasurement)
		if got := cpu.Timestamp; got != now {
			t.Errorf("incorrect CPU measurement timestamp: got %v want %v", got, now)
		}
		if got := len(cpu.Distributions); got <= 1 {
			t.Errorf("too few CPU measurements: got %d", got)
		}
	}
}

func TestNewHostCPUOnly(t *testing.T) {
	now := time.Now()
	// test 1000 times to get diversity of results
	for i := 0; i < 1000; i++ {
		h := NewHostCPUOnly(i, now)
		if got := len(h.SimulatedMeasurements); got != 1 {
			t.Errorf("incorrect number of measurements: got %d want %d", got, 9)
		}
		wantName := fmt.Sprintf(hostFmt, i)
		if got := string(h.Name); got != wantName {
			t.Errorf("incorrect host name format: got %s want %s", got, wantName)
		}

		cpu := h.SimulatedMeasurements[0].(*CPUMeasurement)
		if got := cpu.Timestamp; got != now {
			t.Errorf("incorrect CPU measurement timestamp: got %v want %v", got, now)
		}
		if got := len(cpu.Distributions); got <= 1 {
			t.Errorf("too few CPU measurements: got %d", got)
		}
	}
}

func TestNewHostCPUSingle(t *testing.T) {
	now := time.Now()
	// test 1000 times to get diversity of results
	for i := 0; i < 1000; i++ {
		h := NewHostCPUSingle(i, now)
		if got := len(h.SimulatedMeasurements); got != 1 {
			t.Errorf("incorrect number of measurements: got %d want %d", got, 9)
		}
		wantName := fmt.Sprintf(hostFmt, i)
		if got := string(h.Name); got != wantName {
			t.Errorf("incorrect host name format: got %s want %s", got, wantName)
		}

		cpu := h.SimulatedMeasurements[0].(*CPUMeasurement)
		if got := cpu.Timestamp; got != now {
			t.Errorf("incorrect CPU measurement timestamp: got %v want %v", got, now)
		}
		if got := len(cpu.Distributions); got != 1 {
			t.Errorf("CPU measurements not equal to 1: got %d", got)
		}
	}
}

func testGenerator(s time.Time) []common.SimulatedMeasurement {
	return []common.SimulatedMeasurement{
		&testMeasurement{ticks: 0},
	}
}

func findRegionDatacenters(name []byte) [][]byte {
	for _, r := range regions {
		if bytes.Equal(r.Name, name) {
			return r.Datacenters
		}
	}
	panic(fmt.Errorf("unknown region %s", name))
}

func testStringNumberIsValid(t *testing.T, limit int64, s []byte) {
	n, err := strconv.ParseInt(string(s), 10, 0)
	if err != nil {
		t.Errorf("string number conversion error: %v", err)
	}
	if n < 0 || n >= int64(limit) {
		t.Errorf("string number %s out of range 0 to %d: got %d", s, limit, n)
	}
}

func TestNewHostWithMeasurementGenerator(t *testing.T) {
	now := time.Now()
	// test 1000 times to get diversity of results
	for i := 0; i < 1000; i++ {
		h := newHostWithMeasurementGenerator(i, now, testGenerator)
		wantName := fmt.Sprintf(hostFmt, i)
		if got := string(h.Name); got != wantName {
			t.Errorf("incorrect host name format: got %s want %s", got, wantName)
		}
		dcs := findRegionDatacenters(h.Region)
		testIfInByteStringSlice(t, dcs, h.Datacenter)
		testStringNumberIsValid(t, machineRackChoicesPerDatacenter, h.Rack)
		testIfInByteStringSlice(t, MachineArchChoices, h.Arch)
		testIfInByteStringSlice(t, MachineOSChoices, h.OS)
		testStringNumberIsValid(t, machineServiceChoices, h.Service)
		testStringNumberIsValid(t, machineServiceVersionChoices, h.ServiceVersion)
		testIfInByteStringSlice(t, MachineServiceEnvironmentChoices, h.ServiceEnvironment)
		testIfInByteStringSlice(t, MachineTeamChoices, h.Team)

		if got := len(h.SimulatedMeasurements); got != 1 {
			t.Errorf("simulated measurements incorrect len: got %d", got)
		}
	}
}

type testMeasurement struct {
	ticks int
}

func (m *testMeasurement) Tick(_ time.Duration)       { m.ticks++ }
func (m *testMeasurement) ToPoint(_ *serialize.Point) {}

func TestHostTickAll(t *testing.T) {
	now := time.Now()
	h := newHostWithMeasurementGenerator(0, now, testGenerator)
	if got := h.SimulatedMeasurements[0].(*testMeasurement).ticks; got != 0 {
		t.Errorf("ticks not equal to 0 to start: got %d", got)
	}
	h.TickAll(time.Second)
	if got := h.SimulatedMeasurements[0].(*testMeasurement).ticks; got != 1 {
		t.Errorf("ticks incorrect: got %d want %d", got, 1)
	}
	h.SimulatedMeasurements = append(h.SimulatedMeasurements, &testMeasurement{})
	h.TickAll(time.Second)
	if got := h.SimulatedMeasurements[0].(*testMeasurement).ticks; got != 2 {
		t.Errorf("ticks incorrect after 2nd tick: got %d want %d", got, 2)
	}
	if got := h.SimulatedMeasurements[1].(*testMeasurement).ticks; got != 1 {
		t.Errorf("ticks incorrect after 2nd tick: got %d want %d", got, 1)
	}
}

func TestGetByteStringRandomInt(t *testing.T) {
	limit := int64(100)
	for i := 0; i < 1000000; i++ {
		s := getByteStringRandomInt(limit)
		testStringNumberIsValid(t, limit, s)
	}
}

func testIfInRegionSlice(t *testing.T, arr []region, choice *region) {
	for _, x := range arr {
		if bytes.Equal(x.Name, choice.Name) {
			return
		}
	}
	t.Errorf("could not find choice in array: %v", choice)
}

func TestRandomRegionSliceChoice(t *testing.T) {
	for i := 0; i < 1000000; i++ {
		r := randomRegionSliceChoice(regions)
		testIfInRegionSlice(t, regions, r)
	}
}
