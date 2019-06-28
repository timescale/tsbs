package devops

import (
	"fmt"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

const testLayout = "2006-01-02"

func TestCalculateEpochs(t *testing.T) {
	cases := []struct {
		desc     string
		start    string
		end      string
		interval time.Duration
		want     uint64
	}{
		{
			desc:     "start and end are equal",
			start:    "2017-01-01",
			end:      "2017-01-01",
			interval: time.Second,
			want:     0,
		},
		{
			desc:     "start and end are under 1 interval apart",
			start:    "2017-01-01",
			end:      "2017-01-02",
			interval: 36 * time.Hour,
			want:     0,
		},
		{
			desc:     "start and end are 1 interval apart",
			start:    "2017-01-01",
			end:      "2017-01-02",
			interval: 24 * time.Hour,
			want:     1,
		},
		{
			desc:     "start and end are over 1 interval apart",
			start:    "2017-01-01",
			end:      "2017-01-02",
			interval: 18 * time.Hour,
			want:     1,
		},
		{
			desc:     "start and end are 2 intervals apart",
			start:    "2017-01-01",
			end:      "2017-01-02",
			interval: 12 * time.Hour,
			want:     2,
		},
	}

	for _, c := range cases {
		start, err := time.Parse(testLayout, c.start)
		if err != nil {
			t.Fatalf("could not parse start: %s", c.start)
		}
		end, err := time.Parse(testLayout, c.end)
		if err != nil {
			t.Fatalf("could not parse end: %s", c.end)
		}
		conf := commonDevopsSimulatorConfig{
			Start: start,
			End:   end,
		}
		if got := calculateEpochs(conf, c.interval); got != c.want {
			t.Errorf("%s: incorrect epochs: got %d want %d", c.desc, got, c.want)
		}
	}
}

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

func sprintf(format string, args ...interface{}) string {
	return fmt.Sprintf(format, args...)
}

var prefix = []string{"host", "region", "datacenter", "rack", "os", "arch", "team", "service", "service_version", "service_env"}

func TestCommonDevopsSimulatorPopulatePoint(t *testing.T) {
	s := &commonDevopsSimulator{}
	numHosts := uint64(2)
	for i := uint64(0); i < numHosts; i++ {
		host := Host{
			Name:               sprintf("%s%d", prefix[0], i),
			Region:             sprintf("%s%d", prefix[1], i),
			Datacenter:         sprintf("%s%d", prefix[2], i),
			Rack:               sprintf("%s%d", prefix[3], i),
			OS:                 sprintf("%s%d", prefix[4], i),
			Arch:               sprintf("%s%d", prefix[5], i),
			Team:               sprintf("%s%d", prefix[6], i),
			Service:            sprintf("%s%d", prefix[7], i),
			ServiceVersion:     sprintf("%s%d", prefix[8], i),
			ServiceEnvironment: sprintf("%s%d", prefix[9], i),
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
		if got := p.GetTagValue(MachineTagKeys[i]); got.(string) != want {
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
		if got := p.GetTagValue(MachineTagKeys[i]); got.(string) != want {
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
