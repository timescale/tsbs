package devops

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/timescale/tsbs/internal/utils"
)

func TestNewCore(t *testing.T) {
	s := time.Now()
	e := time.Now()
	c, err := NewCore(s, e, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := c.Interval.Start().UnixNano(); got != s.UnixNano() {
		t.Errorf("NewCore does not have right start time: got %d want %d", got, s.UnixNano())
	}
	if got := c.Interval.EndUnixNano(); got != e.UnixNano() {
		t.Errorf("NewCore does not have right end time: got %d want %d", got, e.UnixNano())
	}
	if got := c.Scale; got != 10 {
		t.Errorf("NewCore does not have right scale: got %d want %d", got, 10)
	}
}

func TestNewCoreEndBeforeStart(t *testing.T) {
	e := time.Now()
	s := e.Add(time.Second)
	_, err := NewCore(s, e, 10)
	if got := err.Error(); got != utils.ErrEndBeforeStart {
		t.Errorf("NewCore did not error correctly:\ngot\n%s\nwant\n%s", got, utils.ErrEndBeforeStart)
	}
}

func TestCoreGetRandomHosts(t *testing.T) {
	s := time.Now()
	e := time.Now()

	n := 5
	scale := 10

	c, err := NewCore(s, e, scale)
	if err != nil {
		t.Fatalf("unexpected error for NewCore: %v", err)
	}

	rand.Seed(100) // Resetting seed to get a deterministic output.
	hosts, err := c.GetRandomHosts(n)
	if err != nil {
		t.Fatalf("unexpected error for GetRandomHosts: %v", err)
	}
	coreHosts := strings.Join(hosts, ",")

	rand.Seed(100) // Resetting seed to get a deterministic output.
	hosts, err = getRandomHosts(n, scale)
	if err != nil {
		t.Fatalf("unexpected error for getRandomHosts: %v", err)
	}
	randomHosts := strings.Join(hosts, ",")

	if coreHosts != randomHosts {
		t.Errorf("incorrect output:\ngot\n%s\nwant\n%s", coreHosts, randomHosts)
	}
}

func TestGetCPUMetricsSlice(t *testing.T) {
	cases := []struct {
		desc      string
		nMetrics  int
		want      string
		shouldErr bool
		errMsg    string
	}{
		{
			desc:     "get 1 metric",
			nMetrics: 1,
			want:     cpuMetrics[0],
		},
		{
			desc:     "get 5 metrics",
			nMetrics: 5,
			want:     strings.Join(cpuMetrics[:5], ","),
		},
		{
			desc:      "0 metrics should error",
			nMetrics:  0,
			shouldErr: true,
			errMsg:    errNoMetrics,
		},
		{
			desc:      "-1 metrics should error",
			nMetrics:  -1,
			shouldErr: true,
			errMsg:    errNoMetrics,
		},
		{
			desc:      "too many metrics should error",
			nMetrics:  100,
			shouldErr: true,
			errMsg:    errTooManyMetrics,
		},
	}

	for _, c := range cases {
		if c.shouldErr {
			metrics, err := GetCPUMetricsSlice(c.nMetrics)
			if metrics != nil {
				t.Errorf("%s: errored but with non-nil return: %v", c.desc, metrics)
			}
			if got := err.Error(); got != c.errMsg {
				t.Errorf("%s: incorrect error:\ngot\n%s\nwant\n%s", c.desc, got, c.errMsg)
			}
		} else {
			metrics, err := GetCPUMetricsSlice(c.nMetrics)
			if err != nil {
				t.Fatalf("%s: unexpected error: got %v", c.desc, err)
			}
			if len(metrics) != c.nMetrics {
				t.Errorf("%s: incorrect len returned: got %d want %d", c.desc, len(metrics), c.nMetrics)
			}
			if got := strings.Join(metrics, ","); got != c.want {
				t.Errorf("%s: incorrect output:\ngot\n%s\nwant\n%s", c.desc, got, c.want)
			}
		}
	}
}

func TestGetAllCPUMetrics(t *testing.T) {
	result := strings.Join(GetAllCPUMetrics(), ",")
	want := strings.Join(cpuMetrics, ",")

	if result != want {
		t.Errorf("incorrect output:\ngot\n%s\nwant\n%s", result, want)
	}
}

func TestGetCPUMetricsLen(t *testing.T) {
	result := GetCPUMetricsLen()
	want := len(cpuMetrics)

	if result != want {
		t.Errorf("incorrect output: got %d want %d", result, want)
	}
}

func TestGetRandomHosts(t *testing.T) {
	cases := []struct {
		desc      string
		scale     int
		nHosts    int
		want      string
		shouldErr bool
		errMsg    string
	}{
		{
			desc:      "-1 host out of 100",
			scale:     100,
			nHosts:    -1,
			shouldErr: true,
			errMsg:    "number of hosts cannot be < 0; got -1.",
		},
		{
			desc:   "1 host out of 100",
			scale:  100,
			nHosts: 1,
			want:   "host_83",
		},
		{
			desc:   "5 host out of 100",
			scale:  100,
			nHosts: 5,
			want:   "host_83,host_68,host_80,host_60,host_62",
		},
		{
			desc:      "5 host out of 1",
			scale:     1,
			nHosts:    5,
			shouldErr: true,
			errMsg:    "number of hosts (5) larger than total hosts. See --scale (1)",
		},
	}

	for _, c := range cases {
		rand.Seed(100) // always reset the random number generator
		if c.shouldErr {
			hosts, err := getRandomHosts(c.nHosts, c.scale)
			if hosts != nil {
				t.Errorf("%s: errored but with non-nil return: %v", c.desc, hosts)
			}
			if got := err.Error(); got != c.errMsg {
				t.Errorf("%s: incorrect error:\ngot\n%s\nwant\n%s", c.desc, got, c.errMsg)
			}
		} else {
			hosts, err := getRandomHosts(c.nHosts, c.scale)
			if err != nil {
				t.Fatalf("%s: unexpected error: got %v", c.desc, err)
			} else if got := strings.Join(hosts, ","); got != c.want {
				t.Errorf("%s: incorrect output: got %s want %s", c.desc, got, c.want)
			}
		}
	}
}

func TestGetDoubleGroupByLabel(t *testing.T) {
	want := fmt.Sprintf("Foo mean of 10 metrics, all hosts, random %s by 1h", DoubleGroupByDuration)
	got := GetDoubleGroupByLabel("Foo", 10)
	if got != want {
		t.Errorf("incorrect output:\ngot\n%s\nwant\n%s", got, want)
	}
}

func TestGetHighCPULabel(t *testing.T) {
	cases := []struct {
		desc      string
		nHosts    int
		want      string
		shouldErr bool
	}{
		{
			desc:      "nHosts < 0",
			nHosts:    -1,
			shouldErr: true,
		},
		{
			desc:   "nHosts = 0",
			nHosts: 0,
			want:   fmt.Sprintf("Foo CPU over threshold, %s", allHosts),
		},
		{
			desc:   "nHosts > 0",
			nHosts: 1,
			want:   fmt.Sprintf("Foo CPU over threshold, %d host(s)", 1),
		},
	}
	for _, c := range cases {
		if c.shouldErr {
			_, err := GetHighCPULabel("Foo", c.nHosts)
			if got := err.Error(); got != errNHostsCannotNegative {
				t.Errorf("%s: incorrect error: got %s want %s", c.desc, got, errNHostsCannotNegative)
			}
		} else {
			got, err := GetHighCPULabel("Foo", c.nHosts)
			if err != nil {
				t.Fatalf("%s: unexpected error: got %v", c.desc, err)
			} else if got != c.want {
				t.Errorf("%s: incorrect output:\ngot\n%s\nwant\n%s", c.desc, got, c.want)
			}
		}
	}
}

func TestGetMaxAllLabel(t *testing.T) {
	want := fmt.Sprintf("Foo max of all CPU metrics, random  100 hosts, random %s by 1h", MaxAllDuration)
	got := GetMaxAllLabel("Foo", 100)
	if got != want {
		t.Errorf("incorrect output: got %s want %s", got, want)
	}
}
