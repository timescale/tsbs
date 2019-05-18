package devops

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestNewCore(t *testing.T) {
	s := time.Now()
	e := time.Now()
	c := NewCore(s, e, 10)
	if got := c.Interval.Start.UnixNano(); got != s.UnixNano() {
		t.Errorf("NewCore does not have right start time: got %d want %d", got, s.UnixNano())
	}
	if got := c.Interval.End.UnixNano(); got != e.UnixNano() {
		t.Errorf("NewCore does not have right end time: got %d want %d", got, e.UnixNano())
	}
	if got := c.Scale; got != 10 {
		t.Errorf("NewCore does not have right scale: got %d want %d", got, 10)
	}
}

func TestNewCoreEndBeforeStart(t *testing.T) {
	e := time.Now()
	s := time.Now()
	errMsg := ""
	fatal = func(format string, args ...interface{}) {
		errMsg = fmt.Sprintf(format, args...)
	}
	_ = NewCore(s, e, 10)
	if errMsg != errBadTimeOrder {
		t.Errorf("NewCore did not error correctly")
	}
}

func TestCoreGetRandomHosts(t *testing.T) {
	s := time.Now()
	e := time.Now()

	n := 5
	scale := 10

	c := NewCore(s, e, scale)

	rand.Seed(100) // Resetting seed to get a deterministic output.
	coreHosts := strings.Join(c.GetRandomHosts(n), ",")

	rand.Seed(100) // Resetting seed to get a deterministic output.
	randomHosts := strings.Join(getRandomHosts(n, scale), ",")

	if coreHosts != randomHosts {
		t.Errorf("incorrect output:\ngot\n%s\nwant\n%s", coreHosts, randomHosts)
	}
}

func TestGetCPUMetricsSlice(t *testing.T) {
	cases := []struct {
		desc        string
		nMetrics    int
		want        string
		shouldFatal bool
		wantFatal   string
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
			desc:        "0 metrics should error",
			nMetrics:    0,
			shouldFatal: true,
			wantFatal:   errNoMetrics,
		},
		{
			desc:        "-1 metrics should error",
			nMetrics:    -1,
			shouldFatal: true,
			wantFatal:   errNoMetrics,
		},
		{
			desc:        "too many metrics should error",
			nMetrics:    100,
			shouldFatal: true,
			wantFatal:   errTooManyMetrics,
		},
	}

	for _, c := range cases {
		if c.shouldFatal {
			errMsg := ""
			fatal = func(format string, args ...interface{}) {
				errMsg = fmt.Sprintf(format, args...)
			}
			metrics := GetCPUMetricsSlice(c.nMetrics)
			if metrics != nil {
				t.Errorf("%s: fatal'd but with non-nil return: %v", c.desc, metrics)
			}
			if errMsg != c.wantFatal {
				t.Errorf("%s: incorrect output:\ngot\n%s\nwant\n%s", c.desc, errMsg, c.wantFatal)
			}
		} else {
			metrics := GetCPUMetricsSlice(c.nMetrics)
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
		desc        string
		scale       int
		nHosts      int
		want        string
		shouldFatal bool
		wantFatal   string
	}{
		{
			desc:        "-1 host out of 100",
			scale:       100,
			nHosts:      -1,
			shouldFatal: true,
			wantFatal:   "number of hosts cannot be < 1; got -1",
		},
		{
			desc:        "0 host out of 100",
			scale:       100,
			nHosts:      0,
			shouldFatal: true,
			wantFatal:   "number of hosts cannot be < 1; got 0",
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
			desc:        "5 host out of 1",
			scale:       1,
			nHosts:      5,
			shouldFatal: true,
			wantFatal:   "number of hosts (5) larger than total hosts. See --scale (1)",
		},
	}

	for _, c := range cases {
		rand.Seed(100) // always reset the random number generator
		if c.shouldFatal {
			errMsg := ""
			fatal = func(format string, args ...interface{}) {
				errMsg = fmt.Sprintf(format, args...)
			}
			hosts := getRandomHosts(c.nHosts, c.scale)
			if hosts != nil {
				t.Errorf("%s: fatal'd but with non-nil return: %v", c.desc, hosts)
			}
			if errMsg != c.wantFatal {
				t.Errorf("%s: incorrect fatal msg:\ngot\n%s\nwant\n%s", c.desc, errMsg, c.wantFatal)
			}
		} else {
			hosts := getRandomHosts(c.nHosts, c.scale)
			if got := strings.Join(hosts, ","); got != c.want {
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
		desc        string
		nHosts      int
		want        string
		shouldFatal bool
	}{
		{
			desc:        "nHosts < 0",
			nHosts:      -1,
			shouldFatal: true,
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
		if c.shouldFatal {
			errMsg := ""
			fatal = func(format string, args ...interface{}) {
				errMsg = fmt.Sprintf(format, args...)
			}
			_ = GetHighCPULabel("Foo", c.nHosts)
			if errMsg != errNHostsCannotNegative {
				t.Errorf("%s: incorrect error: got %s want %s", c.desc, errMsg, errNHostsCannotNegative)
			}
		} else {
			if got := GetHighCPULabel("Foo", c.nHosts); got != c.want {
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

func TestGetRandomSubsetPerm(t *testing.T) {
	cases := []struct {
		scale  int
		nItems int
	}{
		{scale: 10, nItems: 0},
		{scale: 10, nItems: 1},
		{scale: 10, nItems: 5},
		{scale: 10, nItems: 10},
		{scale: 1000, nItems: 1000},
	}

	for _, c := range cases {
		ret := getRandomSubsetPerm(c.nItems, c.scale)
		if len(ret) != c.nItems {
			t.Errorf("return list not long enough: got %d want %d (scale %d)", len(ret), c.nItems, c.scale)
		}
		sort.Ints(ret)
		prev := -1
		for _, x := range ret {
			if x == prev {
				t.Errorf("duplicate int found in sorted result (scale %d nItems %d)", c.scale, c.nItems)
			}
			prev = x
		}
	}
}

func TestGetRandomSubsetPermError(t *testing.T) {
	errMsg := ""
	fatal = func(format string, args ...interface{}) {
		errMsg = fmt.Sprintf(format, args...)
	}
	ret := getRandomSubsetPerm(11, 10)
	if ret != nil {
		t.Errorf("return was non-nil: %v", ret)
	}
	if errMsg != errMoreItemsThanScale {
		t.Errorf("incorrect output: got %s", errMsg)
	}
}
