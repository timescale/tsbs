package hyprcubd

import (
	"math/rand"
	"net/url"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/query"
)

func TestDevopsGetHostWhereString(t *testing.T) {
	cases := []struct {
		desc   string
		nHosts int
		want   string
	}{
		{
			desc:   "single host",
			nHosts: 1,
			want:   "(hostname = 'host_1')",
		},
		{
			desc:   "multi host (2)",
			nHosts: 2,
			want:   "(hostname = 'host_7' or hostname = 'host_9')",
		},
		{
			desc:   "multi host (3)",
			nHosts: 3,
			want:   "(hostname = 'host_1' or hostname = 'host_8' or hostname = 'host_5')",
		},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			b := BaseGenerator{}
			dq, err := b.NewDevops(time.Now(), time.Now(), 10)
			if err != nil {
				t.Fatalf("Error while creating devops generator")
			}
			d := dq.(*Devops)
			if got := d.getHostsExpression(c.nHosts); got != c.want {
				t.Errorf("incorrect output:\ngot\n'%s'\nwant\n'%s'", got, c.want)
			}
		})
	}

}

func TestDevopsGroupByTime(t *testing.T) {
	expectedHumanLabel := "Hyprcubd 1 cpu metric(s), random    1 hosts, random 1s by 1m"
	expectedHumanDesc := "Hyprcubd 1 cpu metric(s), random    1 hosts, random 1s by 1m: 1970-01-01T00:05:58Z"
	expectedQuery := "SELECT max(usage_user) from cpu " +
		"where (hostname = 'host_9') and " +
		"time >= '1970-01-01T00:05:58Z' and time < '1970-01-01T00:05:59Z' " +
		"group by time(1m)"

	v := url.Values{}
	v.Set("q", expectedQuery)

	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(time.Hour)
	b := BaseGenerator{}
	dq, err := b.NewDevops(s, e, 10)
	if err != nil {
		t.Fatalf("Error while creating devops generator")
	}
	d := dq.(*Devops)

	metrics := 1
	nHosts := 1
	duration := time.Second

	q := d.GenerateEmptyQuery()
	d.GroupByTime(q, nHosts, metrics, duration)

	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc)
}

func TestDevopsGroupByOrderByLimit(t *testing.T) {
	expectedHumanLabel := "Hyprcubd max cpu over last 5 min-intervals (random end)"
	expectedHumanDesc := "Hyprcubd max cpu over last 5 min-intervals (random end): 1970-01-01T01:16:22Z"
	expectedQuery := "SELECT max(usage_user) from cpu " +
		"WHERE time < '1970-01-01T01:16:22Z' group by time(1m) limit 5"

	v := url.Values{}
	v.Set("q", expectedQuery)

	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(2 * time.Hour)
	b := BaseGenerator{}
	dq, err := b.NewDevops(s, e, 10)
	if err != nil {
		t.Fatalf("Error while creating devops generator")
	}
	d := dq.(*Devops)

	q := d.GenerateEmptyQuery()
	d.GroupByOrderByLimit(q)

	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc)
}

func TestDevopsGroupByTimeAndPrimaryTag(t *testing.T) {
	cases := []testCase{
		{
			desc:    "zero metrics",
			input:   0,
			fail:    true,
			failMsg: "cannot get 0 metrics",
		},
		{
			desc:               "1 metric",
			input:              1,
			expectedHumanLabel: "Hyprcubd mean of 1 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "Hyprcubd mean of 1 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:16:22Z",
			expectedQuery: "SELECT mean(usage_user) from cpu " +
				"where time >= '1970-01-01T00:16:22Z' and time < '1970-01-01T12:16:22Z' " +
				"group by time(1h),hostname",
		},
		{
			desc:               "5 metrics",
			input:              5,
			expectedHumanLabel: "Hyprcubd mean of 5 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "Hyprcubd mean of 5 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:54:10Z",
			expectedQuery: "SELECT mean(usage_user), mean(usage_system), mean(usage_idle), mean(usage_nice), mean(usage_iowait) " +
				"from cpu " +
				"where time >= '1970-01-01T00:54:10Z' and time < '1970-01-01T12:54:10Z' " +
				"group by time(1h),hostname",
		},
	}

	testFunc := func(d *Devops, c testCase) query.Query {
		q := d.GenerateEmptyQuery()
		d.GroupByTimeAndPrimaryTag(q, c.input)
		return q
	}

	start := time.Unix(0, 0)
	end := start.Add(devops.DoubleGroupByDuration).Add(time.Hour)

	runTestCases(t, testFunc, start, end, cases)
}

func TestMaxAllCPU(t *testing.T) {
	cases := []testCase{
		{
			desc:    "zero hosts",
			input:   0,
			fail:    true,
			failMsg: "number of hosts cannot be < 1; got 0",
		},
		{
			desc:               "1 host",
			input:              1,
			expectedHumanLabel: "Hyprcubd max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h",
			expectedHumanDesc:  "Hyprcubd max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h: 1970-01-01T00:16:22Z",
			expectedQuery: "SELECT max(usage_user),max(usage_system),max(usage_idle),max(usage_nice),max(usage_iowait)," +
				"max(usage_irq),max(usage_softirq),max(usage_steal),max(usage_guest),max(usage_guest_nice) " +
				"from cpu " +
				"where (hostname = 'host_3') and " +
				"time >= '1970-01-01T00:54:10Z' and time < '1970-01-01T08:54:10Z' " +
				"group by time(1h)",
		},
		{
			desc:               "5 hosts",
			input:              5,
			expectedHumanLabel: "Hyprcubd max of all CPU metrics, random    5 hosts, random 8h0m0s by 1h",
			expectedHumanDesc:  "Hyprcubd max of all CPU metrics, random    5 hosts, random 8h0m0s by 1h: 1970-01-01T00:47:30Z",
			expectedQuery: "SELECT max(usage_user),max(usage_system),max(usage_idle),max(usage_nice),max(usage_iowait)," +
				"max(usage_irq),max(usage_softirq),max(usage_steal),max(usage_guest),max(usage_guest_nice) " +
				"from cpu " +
				"where (hostname = 'host_9' or hostname = 'host_5' or hostname = 'host_1' or hostname = 'host_7' or hostname = 'host_2') " +
				"and time >= '1970-01-01T00:37:12Z' and time < '1970-01-01T08:37:12Z' " +
				"group by time(1h)",
		},
	}

	testFunc := func(d *Devops, c testCase) query.Query {
		q := d.GenerateEmptyQuery()
		d.MaxAllCPU(q, c.input)
		return q
	}

	start := time.Unix(0, 0)
	end := start.Add(devops.MaxAllDuration).Add(time.Hour)

	runTestCases(t, testFunc, start, end, cases)
}

func TestLastPointPerHost(t *testing.T) {
	expectedHumanLabel := "Hyprcubd last row per host"
	expectedHumanDesc := "Hyprcubd last row per host: cpu"
	expectedQuery := `SELECT * from cpu group by "hostname" order by time desc limit 1`

	v := url.Values{}
	v.Set("q", expectedQuery)

	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(2 * time.Hour)
	b := BaseGenerator{}
	dq, err := b.NewDevops(s, e, 10)
	if err != nil {
		t.Fatalf("Error while creating devops generator")
	}
	d := dq.(*Devops)

	q := d.GenerateEmptyQuery()
	d.LastPointPerHost(q)

	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc)
}

func TestHighCPUForHosts(t *testing.T) {
	cases := []testCase{
		{
			desc:    "negative hosts",
			input:   -1,
			fail:    true,
			failMsg: "number of hosts cannot be < 1; got -1",
		},
		{
			desc:               "zero hosts",
			input:              0,
			expectedHumanLabel: "Hyprcubd CPU over threshold, all hosts",
			expectedHumanDesc:  "Hyprcubd CPU over threshold, all hosts: 1970-01-01T00:16:22Z",
			expectedQuery: "SELECT * from cpu " +
				"where usage_user > 90.0  and " +
				"time >= '1970-01-01T00:16:22Z' and time < '1970-01-01T12:16:22Z'",
		},
		{
			desc:               "1 host",
			input:              1,
			expectedHumanLabel: "Hyprcubd CPU over threshold, 1 host(s)",
			expectedHumanDesc:  "Hyprcubd CPU over threshold, 1 host(s): 1970-01-01T00:47:30Z",
			expectedQuery: "SELECT * from cpu " +
				"where usage_user > 90.0 and (hostname = 'host_5') and " +
				"time >= '1970-01-01T00:47:30Z' and time < '1970-01-01T12:47:30Z'",
		},
		{
			desc:               "5 hosts",
			input:              5,
			expectedHumanLabel: "Hyprcubd CPU over threshold, 5 host(s)",
			expectedHumanDesc:  "Hyprcubd CPU over threshold, 5 host(s): 1970-01-01T00:08:59Z",
			expectedQuery: "SELECT * from cpu " +
				"where usage_user > 90.0 and " +
				"(hostname = 'host_9' or hostname = 'host_5' or hostname = 'host_1' or hostname = 'host_7' or hostname = 'host_2') and " +
				"time >= '1970-01-01T00:08:59Z' and time < '1970-01-01T12:08:59Z'",
		},
	}

	testFunc := func(d *Devops, c testCase) query.Query {
		q := d.GenerateEmptyQuery()
		d.HighCPUForHosts(q, c.input)
		return q
	}

	start := time.Unix(0, 0)
	end := start.Add(devops.HighCPUDuration).Add(time.Hour)

	runTestCases(t, testFunc, start, end, cases)
}

func TestDevopsFillInQuery(t *testing.T) {
	humanLabel := "this is my label"
	humanDesc := "and now my description"
	sql := "SELECT time, foo from cpu where usage_user > 90.0 and time < '2017-01-01'"
	b := BaseGenerator{}
	dq, err := b.NewDevops(time.Now(), time.Now(), 10)
	if err != nil {
		t.Fatalf("Error while creating devops generator")
	}
	d := dq.(*Devops)
	qi := d.GenerateEmptyQuery()
	q := qi.(*query.HTTP)
	if len(q.HumanLabel) != 0 {
		t.Errorf("empty query has non-zero length human label")
	}
	if len(q.HumanDescription) != 0 {
		t.Errorf("empty query has non-zero length human desc")
	}
	if len(q.Method) != 0 {
		t.Errorf("empty query has non-zero length method")
	}
	if len(q.Path) != 0 {
		t.Errorf("empty query has non-zero length path")
	}

	d.fillInQuery(q, humanLabel, humanDesc, sql)
	if got := string(q.HumanLabel); got != humanLabel {
		t.Errorf("filled query mislabeled: got %s want %s", got, humanLabel)
	}
	if got := string(q.HumanDescription); got != humanDesc {
		t.Errorf("filled query mis-described: got %s want %s", got, humanDesc)
	}
	if got := string(q.Method); got != "POST" {
		t.Errorf("filled query has wrong method: got %s want POST", got)
	}
}

type testCase struct {
	desc               string
	input              int
	fail               bool
	failMsg            string
	expectedHumanLabel string
	expectedHumanDesc  string
	expectedQuery      string
}

func runTestCases(t *testing.T, testFunc func(*Devops, testCase) query.Query, s time.Time, e time.Time, cases []testCase) {
	rand.Seed(123) // Setting seed for testing purposes.

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			b := BaseGenerator{}
			dq, err := b.NewDevops(s, e, 10)
			if err != nil {
				t.Fatalf("Error while creating devops generator")
			}
			d := dq.(*Devops)

			if c.fail {
				func() {
					defer func() {
						r := recover()
						if r == nil {
							t.Errorf("did not panic when should")
						}

						if r != c.failMsg {
							t.Fatalf("incorrect fail message: got %s, want %s", r, c.failMsg)
						}
					}()

					testFunc(d, c)
				}()
			} else {
				q := testFunc(d, c)

				v := url.Values{}
				v.Set("q", c.expectedQuery)
				verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc)
			}

		})
	}
}

func verifyQuery(t *testing.T, qi query.Query, humanLabel, humanDesc string) {
	q, ok := qi.(*query.HTTP)

	if !ok {
		t.Fatal("Filled query is not *query.HTTP type")
	}

	if got := string(q.HumanLabel); got != humanLabel {
		t.Errorf("incorrect human label:\ngot\n%s\nwant\n%s", got, humanLabel)
	}

	if got := string(q.HumanDescription); got != humanDesc {
		t.Errorf("incorrect human description:\ngot\n'%s'\nwant\n'%s'", got, humanDesc)
	}

	if got := string(q.Method); got != "POST" {
		t.Errorf("incorrect method:\ngot\n%s\nwant POST", got)
	}

	if q.Body != nil {
		t.Errorf("body not nil, got %+v", q.Body)
	}
}
