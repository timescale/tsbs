package devops

import (
	"fmt"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

func TestCommonDevopsSimulatorFinished(t *testing.T) {
	cases := []struct {
		desc       string
		madePoints uint64
		maxPoints  uint64
		want       bool
	}{
		{
			desc:       "made << max",
			madePoints: 0,
			maxPoints:  10,
			want:       false,
		},
		{
			desc:       "made < max",
			madePoints: 9,
			maxPoints:  10,
			want:       false,
		},
		{
			desc:       "made == max",
			madePoints: 10,
			maxPoints:  10,
			want:       true,
		},
		{
			desc:       "made > max",
			madePoints: 11,
			maxPoints:  10,
			want:       true,
		},
		{
			desc:       "made >> max",
			madePoints: 100,
			maxPoints:  10,
			want:       true,
		},
	}

	s := &commonDevopsSimulator{}
	for _, c := range cases {
		s.madePoints = c.madePoints
		s.maxPoints = c.maxPoints
		if got := s.Finished(); got != c.want {
			t.Errorf("%s: incorrect result: got %v want %v", c.desc, got, c.want)
		}
	}
}

func TestCommonDevopsSimulatorFields(t *testing.T) {
	s := &commonDevopsSimulator{}
	host := Host{}
	host.SimulatedMeasurements = []common.SimulatedMeasurement{NewCPUMeasurement(time.Now())}
	s.hosts = append(s.hosts, host)
	fields := s.Fields()
	if got := len(fields); got != 1 {
		t.Errorf("fields length does not equal 1: got %d", got)
	}
	if got, ok := fields[string(labelCPU)]; ok {
		if got2 := len(got); got2 <= 0 {
			t.Errorf("number of fields is non-positive: got %d", got2)
		}
	} else {
		t.Errorf("CPU was not one of the labels")
	}

	// Add a host with different measurement. This should not affect the results
	// because we assume each Host has the same set of simulated measurements.
	// TODO - Examine whether this assumption should be refined.
	host = Host{}
	host.SimulatedMeasurements = []common.SimulatedMeasurement{NewMemMeasurement(time.Now())}
	s.hosts = append(s.hosts, host)
	fields = s.Fields()
	if got := len(fields); got != 1 {
		t.Errorf("fields length does not equal 1: got %d", got)
	}

	// Add new measurement, this should change the result.
	host = s.hosts[0]
	host.SimulatedMeasurements = append(host.SimulatedMeasurements, NewMemMeasurement(time.Now()))
	s.hosts[0] = host
	fields = s.Fields()
	if got := len(fields); got != 2 {
		t.Errorf("fields length does not equal 2: got %d", got)
	}

	// Test panic condition
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("did not panic when should")
			}
		}()
		s.hosts = s.hosts[:0]
		_ = s.Fields()
	}()
}

func bprintf(format string, args ...interface{}) []byte {
	return []byte(fmt.Sprintf(format, args...))
}

var prefix = []string{"host", "region", "datacenter", "rack", "os", "arch", "team", "service", "service_version", "service_env"}

func TestCommonDevopsSimulatorPopulatePoint(t *testing.T) {
	s := &commonDevopsSimulator{}
	numHosts := uint64(2)
	for i := uint64(0); i < numHosts; i++ {
		host := Host{
			Name:               bprintf("%s%d", prefix[0], i),
			Region:             bprintf("%s%d", prefix[1], i),
			Datacenter:         bprintf("%s%d", prefix[2], i),
			Rack:               bprintf("%s%d", prefix[3], i),
			OS:                 bprintf("%s%d", prefix[4], i),
			Arch:               bprintf("%s%d", prefix[5], i),
			Team:               bprintf("%s%d", prefix[6], i),
			Service:            bprintf("%s%d", prefix[7], i),
			ServiceVersion:     bprintf("%s%d", prefix[8], i),
			ServiceEnvironment: bprintf("%s%d", prefix[9], i),
		}
		host.SimulatedMeasurements = []common.SimulatedMeasurement{NewCPUMeasurement(time.Now())}
		s.hosts = append(s.hosts, host)
	}
	s.hostIndex = 0
	s.epochHosts = numHosts - 1
	p := serialize.NewPoint()

	use := s.populatePoint(p, 0)
	if !use {
		t.Errorf("populatePoint returned false when it should be true")
	}
	for i := range prefix {
		want := prefix[i] + "0"
		if got := string(p.GetTagValue(MachineTagKeys[i])); got != want {
			t.Errorf("incorrect tag for idx %d: got %s want %s", i, got, want)
		}
	}
	if got := s.madePoints; got != 1 {
		t.Errorf("made points is not 1: got %d", got)
	}
	if got := s.hostIndex; got != 1 {
		t.Errorf("host index not incremented to 1: got %d", s.hostIndex)
	}

	// Second time should not want to write the point
	use = s.populatePoint(p, 0)
	if use {
		t.Errorf("populatePoint returned true when it should be false")
	}
	for i := range prefix {
		want := prefix[i] + "0"
		if got := string(p.GetTagValue(MachineTagKeys[i])); got != want {
			t.Errorf("incorrect tag for idx %d: got %s want %s", i, got, want)
		}
	}
	if got := s.madePoints; got != 2 {
		t.Errorf("made points is not 2: got %d", got)
	}
	if got := s.hostIndex; got != 2 {
		t.Errorf("host index not incremented to 2: got %d", s.hostIndex)
	}
}

func TestAdjustNumHostsForEpoch(t *testing.T) {
	totalHosts := 100
	cases := []struct {
		desc           string
		initHosts      uint64
		epochs         uint64
		wantEpochHosts []uint64
	}{
		{
			desc:           "no change",
			initHosts:      uint64(totalHosts),
			epochs:         5,
			wantEpochHosts: []uint64{100, 100, 100, 100, 100},
		},
		{
			desc:           "linear change from 0, non-integer",
			initHosts:      0,
			epochs:         9,
			wantEpochHosts: []uint64{0, 12, 25, 37, 50, 62, 75, 87, 100},
		},
		{
			desc:           "linear change from 0, integer",
			initHosts:      0,
			epochs:         5,
			wantEpochHosts: []uint64{0, 25, 50, 75, 100},
		},
		{
			desc:           "linear change from non-0, non-integer",
			initHosts:      50,
			epochs:         5,
			wantEpochHosts: []uint64{50, 62, 75, 87, 100}, // should be 12.5 per epoch, so alternates rounding down and then up
		},
		{
			desc:           "linear change from non-0, integer",
			initHosts:      60,
			epochs:         5,
			wantEpochHosts: []uint64{60, 70, 80, 90, 100},
		},
	}

	for _, c := range cases {
		s := &commonDevopsSimulator{}
		for i := 0; i < totalHosts; i++ {
			s.hosts = append(s.hosts, Host{})
		}
		s.initHosts = c.initHosts
		s.epochHosts = c.initHosts
		s.epochs = c.epochs
		s.epoch = 0
		for i := 0; i < int(c.epochs); i++ {
			want := c.wantEpochHosts[i]
			if got := s.epochHosts; got != want {
				t.Errorf("%s: incorrect number of hosts in epoch %d: got %d want %d", c.desc, i, got, want)
			}
			s.adjustNumHostsForEpoch()
		}
	}
}
