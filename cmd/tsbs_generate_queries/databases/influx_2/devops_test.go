package influx_2

import (
	"fmt"
	"math/rand"
	"net/url"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/pkg/query"
)

func TestDevopsGetHostFilterClauseWithHostnames(t *testing.T) {
	cases := []struct {
		desc      string
		hostnames []string
		want      string
	}{
		{
			desc:      "single host",
			hostnames: []string{"foo1"},
			want:      "(r.hostname == 'foo1')",
		},
		{
			desc:      "multi host (2)",
			hostnames: []string{"foo1", "foo2"},
			want:      "(r.hostname == 'foo1' or r.hostname == 'foo2')",
		},
		{
			desc:      "multi host (3)",
			hostnames: []string{"foo1", "foo2", "foo3"},
			want:      "(r.hostname == 'foo1' or r.hostname == 'foo2' or r.hostname == 'foo3')",
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

			if got := d.getHostFilterClauseWithHostnames(c.hostnames); got != c.want {
				t.Errorf("incorrect output: got %s want %s", got, c.want)
			}
		})
	}
}

func TestDevopsGetHostFilterClause(t *testing.T) {
	cases := []struct {
		desc   string
		nHosts int
		want   string
	}{
		{
			desc:   "single host",
			nHosts: 1,
			want:   "(r.hostname == 'host_1')",
		},
		{
			desc:   "multi host (2)",
			nHosts: 2,
			want:   "(r.hostname == 'host_7' or r.hostname == 'host_9')",
		},
		{
			desc:   "multi host (3)",
			nHosts: 3,
			want:   "(r.hostname == 'host_1' or r.hostname == 'host_8' or r.hostname == 'host_5')",
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

			if got := d.getHostFilterClause(c.nHosts); got != c.want {
				t.Errorf("incorrect output:\ngot\n%s\nwant\n%s", got, c.want)
			}
		})
	}

}

func TestDevopsGroupByTime(t *testing.T) {
	expectedHumanLabel := "Influx 2.x 1 cpu metric(s), random    1 hosts, random 1s by 1m"
	expectedHumanDesc := "Influx 2.x 1 cpu metric(s), random    1 hosts, random 1s by 1m: 1970-01-01T00:05:58Z"
	expectedQuery := `
	from(bucket: "benchmark") 
	|> range(start: 1970-01-01T00:05:58Z, stop: 1970-01-01T00:05:59Z)
	|> filter(fn: (r) => r._measurement == "cpu" and (r._field == 'usage_user') and (r.hostname == 'host_9'))
	|> aggregateWindow(every: 1m, fn: max, createEmpty: false)
	|> yield()
	`

	v := url.Values{}
	v.Set("q", expectedQuery)
	expectedPath := fmt.Sprintf("/api/v2/query")

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

	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc, expectedPath, expectedQuery)
}

func TestDevopsGroupByOrderByLimit(t *testing.T) {
	expectedHumanLabel := "Influx 2.x max cpu over last 5 min-intervals (random end)"
	expectedHumanDesc := "Influx 2.x max cpu over last 5 min-intervals (random end): 1970-01-01T00:16:22Z"
	expectedQuery := `
	from(bucket: "benchmark")
	|> range(start: 0, stop: 1970-01-01T01:16:22Z)
	|> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user")
	|> aggregateWindow(every: 1m, fn: max)
	|> sort(columns: ["_time"], desc: true)
	|> limit(n: 5)
	`

	v := url.Values{}
	v.Set("q", expectedQuery)
	expectedPath := fmt.Sprintf("/api/v2/query")

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

	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc, expectedPath, expectedQuery)
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
			expectedHumanLabel: "Influx 2.x mean of 1 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "Influx 2.x mean of 1 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:16:22Z",
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 1970-01-01T00:16:22Z, stop: 1970-01-01T12:16:22Z)
	|> filter(fn: (r) => r._measurement == "cpu" and (r._field == 'usage_user'))
	|> group(columns: ["_time", "hostname"])
	|> aggregateWindow(every: 1h, fn: mean)
	|> yield(name: "mean")
	`,
		},
		{
			desc:               "5 metrics",
			input:              5,
			expectedHumanLabel: "Influx 2.x mean of 5 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "Influx 2.x mean of 5 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:54:10Z",
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 1970-01-01T00:54:10Z, stop: 1970-01-01T12:54:10Z)
	|> filter(fn: (r) => r._measurement == "cpu" and (r._field == 'usage_user' or r._field == 'usage_system' or r._field == 'usage_idle' or r._field == 'usage_nice' or r._field == 'usage_iowait'))
	|> group(columns: ["_time", "hostname"])
	|> aggregateWindow(every: 1h, fn: mean)
	|> yield(name: "mean")
	`,
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
			expectedHumanLabel: "Influx 2.x max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h",
			expectedHumanDesc:  "Influx 2.x max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h: 1970-01-01T00:54:10Z",
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 1970-01-01T00:54:10Z, stop: 1970-01-01T08:54:10Z)
	|> filter(fn: (r) => r._measurement == "cpu" and (r._field == 'usage_user' or r._field == 'usage_system' or r._field == 'usage_idle' or r._field == 'usage_nice' or r._field == 'usage_iowait' or r._field == 'usage_irq' or r._field == 'usage_softirq' or r._field == 'usage_steal' or r._field == 'usage_guest' or r._field == 'usage_guest_nice') and (r.hostname == 'host_3'))
	|> aggregateWindow(every: 1h, fn: max)
	|> yield(name: "max")
	`,
		},
		{
			desc:               "5 hosts",
			input:              5,
			expectedHumanLabel: "Influx 2.x max of all CPU metrics, random    5 hosts, random 8h0m0s by 1h",
			expectedHumanDesc:  "Influx 2.x max of all CPU metrics, random    5 hosts, random 8h0m0s by 1h: 1970-01-01T00:37:12Z",
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 1970-01-01T00:37:12Z, stop: 1970-01-01T08:37:12Z)
	|> filter(fn: (r) => r._measurement == "cpu" and (r._field == 'usage_user' or r._field == 'usage_system' or r._field == 'usage_idle' or r._field == 'usage_nice' or r._field == 'usage_iowait' or r._field == 'usage_irq' or r._field == 'usage_softirq' or r._field == 'usage_steal' or r._field == 'usage_guest' or r._field == 'usage_guest_nice') and (r.hostname == 'host_9' or r.hostname == 'host_5' or r.hostname == 'host_1' or r.hostname == 'host_7' or r.hostname == 'host_2'))
	|> aggregateWindow(every: 1h, fn: max)
	|> yield(name: "max")
	`,
		},
	}

	testFunc := func(d *Devops, c testCase) query.Query {
		q := d.GenerateEmptyQuery()
		d.MaxAllCPU(q, c.input, devops.MaxAllDuration)
		return q
	}

	start := time.Unix(0, 0)
	end := start.Add(devops.MaxAllDuration).Add(time.Hour)

	runTestCases(t, testFunc, start, end, cases)
}

func TestLastPointPerHost(t *testing.T) {
	expectedHumanLabel := "Influx 2.x last row per host"
	expectedHumanDesc := "Influx 2.x last row per host: cpu"
	// expectedQuery := `SELECT * from cpu group by "hostname" order by time desc limit 1`
	expectedQuery := `
	from(bucket: "benchmark")
	|> range(start: 0)
	|> filter(fn: (r) => r._measurement == "cpu")
	|> group(columns: ["hostname"])
	|> last()
	`
	v := url.Values{}
	v.Set("q", expectedQuery)
	expectedPath := fmt.Sprintf("/api/v2/query")

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

	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc, expectedPath, expectedQuery)
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
			expectedHumanLabel: "Influx 2.x CPU over threshold, all hosts",
			expectedHumanDesc:  "Influx 2.x CPU over threshold, all hosts: 1970-01-01T00:54:10Z",
			// expectedQuery: "SELECT * from cpu " +
			// 	"where usage_user > 90.0  and " +
			// 	"time >= '1970-01-01T00:54:10Z' and time < '1970-01-01T12:54:10Z'",
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 1970-01-01T00:54:10Z, stop: 1970-01-01T12:54:10Z)
	|> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user" and r._value > 90.0)
	|> yield(name: "high_cpu")
	`,
		},
		{
			desc:               "1 host",
			input:              1,
			expectedHumanLabel: "Influx 2.x CPU over threshold, 1 host(s)",
			expectedHumanDesc:  "Influx 2.x CPU over threshold, 1 host(s): 1970-01-01T00:47:30Z",
			// expectedQuery: "SELECT * from cpu " +
			// 	"where usage_user > 90.0 and (r.hostname == 'host_5') and " +
			// 	"time >= '1970-01-01T00:47:30Z' and time < '1970-01-01T12:47:30Z'",
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 1970-01-01T00:47:30Z, stop: 1970-01-01T12:47:30Z)
	|> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user" and r._value > 90.0 and (r.hostname == 'host_5'))
	|> yield(name: "high_cpu")
	`,
		},
		{
			desc:               "5 hosts",
			input:              5,
			expectedHumanLabel: "Influx 2.x CPU over threshold, 5 host(s)",
			expectedHumanDesc:  "Influx 2.x CPU over threshold, 5 host(s): 1970-01-01T00:17:45Z",
			// expectedQuery: "SELECT * from cpu " +
			// 	"where usage_user > 90.0 and " +
			// 	"(r.hostname == 'host_9' or r.hostname == 'host_5' or r.hostname == 'host_1' or r.hostname == 'host_7' or r.hostname == 'host_2') and " +
			// 	"time >= '1970-01-01T00:17:45Z' and time < '1970-01-01T12:17:45Z'",
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 1970-01-01T00:17:45Z, stop: 1970-01-01T12:17:45Z)
	|> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user" and r._value > 90.0 and (r.hostname == 'host_9' or r.hostname == 'host_5' or r.hostname == 'host_1' or r.hostname == 'host_7' or r.hostname == 'host_2'))
	|> yield(name: "high_cpu")
	`,
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
	// influxql := "SELECT * from cpu where usage_user > 90.0 and time < '2017-01-01'"
	influxql := `
	from(bucket: "benchmark")
	|> range(start: 0, stop: 2017-01-01T00:00:00Z)
	|> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user" and r._value > 90.0)
	|> yield(name: "usage_user")
	`
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

	d.fillInQuery(q, humanLabel, humanDesc, influxql)
	if got := string(q.HumanLabel); got != humanLabel {
		t.Errorf("filled query mislabeled: got %s want %s", got, humanLabel)
	}
	if got := string(q.HumanDescription); got != humanDesc {
		t.Errorf("filled query mis-described: got %s want %s", got, humanDesc)
	}
	if got := string(q.Method); got != "POST" {
		t.Errorf("filled query has wrong method: got %s want POST", got)
	}
	v := url.Values{}
	v.Set("q", influxql)
	if got := string(q.Path); got != "/api/v2/query" {
		t.Errorf("filled query has wrong path: got %s want /api/v2/query", got)
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
				expectedPath := fmt.Sprintf("/api/v2/query")

				verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, expectedPath, c.expectedQuery)
			}

		})
	}
}

func verifyQuery(t *testing.T, q query.Query, humanLabel, humanDesc, path string, raw_query string) {
	influxql, ok := q.(*query.HTTP)

	if !ok {
		t.Fatal("Filled query is not *query.HTTP type")
	}

	if got := string(influxql.HumanLabel); got != humanLabel {
		t.Errorf("incorrect human label:\ngot\n%s\nwant\n%s", got, humanLabel)
	}

	if got := string(influxql.HumanDescription); got != humanDesc {
		t.Errorf("incorrect human description:\ngot\n%s\nwant\n%s", got, humanDesc)
	}

	if got := string(influxql.Method); got != "POST" {
		t.Errorf("incorrect method:\ngot\n%s\nwant POST", got)
	}

	if got := string(influxql.Path); got != path {
		t.Errorf("incorrect path:\ngot\n%s\nwant\n%s", got, path)
	}

	if got := string(influxql.RawQuery); got != raw_query {
		t.Errorf("incorrect raw query:\ngot\n%s\nwant\n%s", got, raw_query)
	}

	if influxql.Body == nil || string(influxql.Body) != raw_query {
		t.Errorf("body not nil, got %+v", influxql.Body)
	}
}
