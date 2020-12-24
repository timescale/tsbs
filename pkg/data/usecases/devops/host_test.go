package devops

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"strconv"
	"testing"
	"time"
)

func TestNewHostMeasurements(t *testing.T) {
	start := time.Now()
	measurements := newHostMeasurements(NewHostCtxTime(start))
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
	measurements := newCPUOnlyHostMeasurements(NewHostCtxTime(start))
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
	measurements := newCPUSingleHostMeasurements(NewHostCtxTime(start))
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
		h := NewHost(NewHostCtx(i, now))
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
		h := NewHostCPUOnly(NewHostCtx(i, now))
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
		h := NewHostCPUSingle(NewHostCtx(i, now))
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

func TestNewHostGenericMeasurments(t *testing.T) {
	now := time.Now()
	metricCount := uint64(100)
	resetGenericMetricFields()
	initGenericMetricFields(metricCount)
	// test 1000 times to get diversity of results
	for i := 0; i < 1000; i++ {
		h := NewHostGenericMetrics(&HostContext{i, now, metricCount, 0})
		if got := len(h.SimulatedMeasurements); got != 1 {
			t.Errorf("incorrect number of measurements: got %d want %d", got, 1)
		}
		wantName := fmt.Sprintf(hostFmt, i)
		if got := string(h.Name); got != wantName {
			t.Errorf("incorrect host name format: got %s want %s", got, wantName)
		}

		genericMeasurements := h.SimulatedMeasurements[0].(*GenericMeasurements)
		if got := genericMeasurements.Timestamp; got != now {
			t.Errorf("incorrect CPU measurement timestamp: got %v want %v", got, now)
		}
		if got := len(genericMeasurements.Distributions); got != 100 {
			t.Errorf("incorrect number of generic measurements: got %d want %d", got, 100)
		}
	}
}

func testGenerator(ctx *HostContext) []common.SimulatedMeasurement {
	return []common.SimulatedMeasurement{
		&testMeasurement{ticks: 0},
	}
}

func findRegionDatacenters(name string) []string {
	for _, r := range regions {
		if r.Name == name {
			return r.Datacenters
		}
	}
	panic(fmt.Errorf("unknown region %s", name))
}

func testStringNumberIsValid(t *testing.T, limit int64, s string) {
	n, err := strconv.ParseInt(s, 10, 0)
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
		h := newHostWithMeasurementGenerator(testGenerator, NewHostCtx(i, now))
		wantName := fmt.Sprintf(hostFmt, i)
		if got := string(h.Name); got != wantName {
			t.Errorf("incorrect host name format: got %s want %s", got, wantName)
		}
		dcs := findRegionDatacenters(h.Region)
		testIfInStringSlice(t, dcs, h.Datacenter)
		testStringNumberIsValid(t, machineRackChoicesPerDatacenter, h.Rack)
		testIfInStringSlice(t, MachineArchChoices, h.Arch)
		testIfInStringSlice(t, MachineOSChoices, h.OS)
		testStringNumberIsValid(t, machineServiceChoices, h.Service)
		testStringNumberIsValid(t, machineServiceVersionChoices, h.ServiceVersion)
		testIfInStringSlice(t, MachineServiceEnvironmentChoices, h.ServiceEnvironment)
		testIfInStringSlice(t, MachineTeamChoices, h.Team)

		if got := len(h.SimulatedMeasurements); got != 1 {
			t.Errorf("simulated measurements incorrect len: got %d", got)
		}
	}
}

type testMeasurement struct {
	ticks int
}

func (m *testMeasurement) Tick(_ time.Duration)  { m.ticks++ }
func (m *testMeasurement) ToPoint(_ *data.Point) {}

func TestHostTickAll(t *testing.T) {
	now := time.Now()
	h := newHostWithMeasurementGenerator(testGenerator, NewHostCtxTime(now))
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

func TestGetStringRandomInt(t *testing.T) {
	limit := int64(100)
	for i := 0; i < 1000000; i++ {
		s := getStringRandomInt(limit)
		testStringNumberIsValid(t, limit, s)
	}
}

func testIfInRegionSlice(t *testing.T, arr []region, choice *region) {
	for _, x := range arr {
		if x.Name == choice.Name {
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
