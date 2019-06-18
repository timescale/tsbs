package siridb

import (
	"math/rand"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/query"
)

func TestDevopsGetHostWhereWithHostnames(t *testing.T) {
	cases := []struct {
		desc      string
		hostnames []string
		want      string
	}{
		{
			desc:      "single host",
			hostnames: []string{"foo1"},
			want:      "(`foo1`)",
		},
		{
			desc:      "multi host (2)",
			hostnames: []string{"foo1", "foo2"},
			want:      "(`foo1`|`foo2`)",
		},
		{
			desc:      "multi host (3)",
			hostnames: []string{"foo1", "foo2", "foo3"},
			want:      "(`foo1`|`foo2`|`foo3`)",
		},
	}

	for _, c := range cases {
		d := NewDevops(time.Now(), time.Now(), 10)

		if got := d.getHostWhereWithHostnames(c.hostnames); got != c.want {
			t.Errorf("%s: incorrect output: got %s want %s", c.desc, got, c.want)
		}
	}
}

func TestDevopsGetMetricWhereWithMetrics(t *testing.T) {
	cases := []struct {
		desc    string
		metrics []string
		want    string
	}{
		{
			desc:    "sigle metric",
			metrics: []string{"foo"},
			want:    "(`foo`)",
		},
		{
			desc:    "Multi metrics",
			metrics: []string{"foo", "bar"},
			want:    "(`foo`|`bar`)",
		},
	}

	for _, c := range cases {
		d := NewDevops(time.Now(), time.Now(), 10)

		if got := d.getMetricWhereString(c.metrics); got != c.want {
			t.Errorf("%s: incorrect output: got %s want %s", c.desc, got, c.want)
		}
	}
}

func TestGroupByTime(t *testing.T) {
	cases := []testCase{
		{
			desc:               "one metric",
			input:              1,
			expectedHumanLabel: "SiriDB 1 cpu metric(s), random    1 hosts, random 1s by 1m",
			expectedHumanDesc: "SiriDB 1 cpu metric(s), random    1 hosts, " +
				"random 1s by 1m: 1970-01-01T01:09:26Z",
			expectedQuery: "select max(1m) " +
				"from (`host_9`) & (`usage_user`) " +
				"between '1970-01-01T01:09:26Z' and '1970-01-01T01:09:27Z' " +
				"merge as 'max (`usage_user`) " +
				"for (`host_9`)' using max(1)",
		},
		{
			desc:               "two metrics",
			input:              2,
			expectedHumanLabel: "SiriDB 2 cpu metric(s), random    1 hosts, random 1s by 1m",
			expectedHumanDesc: "SiriDB 2 cpu metric(s), random    1 hosts, " +
				"random 1s by 1m: 1970-01-01T00:45:11Z",
			expectedQuery: "select max(1m) " +
				"from (`host_5`) & (`usage_user`|`usage_system`) " +
				"between '1970-01-01T00:45:11Z' and '1970-01-01T00:45:12Z'",
		},
	}

	testFunc := func(d *Devops, c testCase) query.Query {
		q := d.GenerateEmptyQuery()
		d.GroupByTime(q, 1, c.input, time.Second)
		return q
	}

	start := time.Unix(0, 0)
	end := start.Add(2 * time.Hour)

	runTestCases(t, testFunc, start, end, cases)
}

func TestGroupByOrderByLimit(t *testing.T) {
	cases := []testCase{
		{
			desc:               "happy path",
			expectedHumanLabel: "SiriDB max cpu over last 5 min-intervals (random end)",
			expectedHumanDesc: "SiriDB max cpu over last 5 min-intervals " +
				"(random end): 1970-01-01T01:16:22Z",
			expectedQuery: "select max(1m) " +
				"from `usage_user` " +
				"between '1970-01-01 01:16:22Z' - 5m and '1970-01-01 01:16:00Z' " +
				"merge as 'max usage user of the last 5 aggregate readings' using max(1)",
		},
	}

	testFunc := func(d *Devops, c testCase) query.Query {
		q := d.GenerateEmptyQuery()
		d.GroupByOrderByLimit(q)
		return q
	}

	start := time.Unix(0, 0)
	end := start.Add(2 * time.Hour)

	runTestCases(t, testFunc, start, end, cases)
}

func TestGroupByTimeAndPrimaryTag(t *testing.T) {
	cases := []testCase{
		{
			desc:    "negative metrics",
			input:   -1,
			fail:    true,
			failMsg: "cannot get 0 metrics",
		},
		{
			desc:    "zero metrics",
			input:   0,
			fail:    true,
			failMsg: "cannot get 0 metrics",
		},
		{
			desc:               "one metric",
			input:              1,
			expectedHumanLabel: "SiriDB mean of 1 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "SiriDB mean of 1 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:47:30Z",
			expectedQuery: "select mean(1h) from (`usage_user`) " +
				"between '1970-01-01T00:47:30Z' and '1970-01-01T12:47:30Z'",
		},
		{
			desc:               "5 metrics",
			input:              5,
			expectedHumanLabel: "SiriDB mean of 5 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "SiriDB mean of 5 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:37:12Z",
			expectedQuery: "select mean(1h) " +
				"from (`usage_user`|`usage_system`|`usage_idle`|`usage_nice`|`usage_iowait`) " +
				"between '1970-01-01T00:37:12Z' and '1970-01-01T12:37:12Z'",
		},
		{
			desc:    "more metrics then it exists",
			input:   99,
			fail:    true,
			failMsg: "too many metrics asked for",
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
			desc:    "negative hosts",
			input:   -1,
			fail:    true,
			failMsg: "number of hosts cannot be < 1; got -1",
		},
		{
			desc:    "zero hosts",
			input:   0,
			fail:    true,
			failMsg: "number of hosts cannot be < 1; got 0",
		},
		{
			desc:               "1 host",
			input:              1,
			expectedHumanLabel: "SiriDB max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h",
			expectedHumanDesc: "SiriDB max of all CPU metrics, random    1 hosts, " +
				"random 8h0m0s by 1h: 1970-01-01T00:47:30Z",
			expectedQuery: "select max(1h) from (`host_5`) & `cpu` " +
				"between '1970-01-01T00:47:30Z' and '1970-01-01T08:47:30Z'",
		},
		{
			desc:               "5 hosts",
			input:              5,
			expectedHumanLabel: "SiriDB max of all CPU metrics, random    5 hosts, random 8h0m0s by 1h",
			expectedHumanDesc: "SiriDB max of all CPU metrics, random    5 hosts, " +
				"random 8h0m0s by 1h: 1970-01-01T00:17:45Z",
			expectedQuery: "select max(1h) " +
				"from (`host_9`|`host_5`|`host_1`|`host_7`|`host_2`) & `cpu` " +
				"between '1970-01-01T00:17:45Z' and '1970-01-01T08:17:45Z'",
		},
		{
			desc:    "more hosts then cardinality (11)",
			input:   11,
			fail:    true,
			failMsg: "number of hosts (11) larger than total hosts. See --scale (10)",
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
	cases := []testCase{
		{
			desc:               "happy path",
			expectedHumanLabel: "SiriDB last row per host",
			expectedHumanDesc:  "SiriDB last row per host",
			expectedQuery:      "select last() from `cpu`",
		},
	}

	testFunc := func(d *Devops, c testCase) query.Query {
		q := d.GenerateEmptyQuery()
		d.LastPointPerHost(q)
		return q
	}

	start := time.Unix(0, 0)
	end := start.Add(2 * time.Hour)

	runTestCases(t, testFunc, start, end, cases)
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
			expectedHumanLabel: "SiriDB CPU over threshold, all hosts",
			expectedHumanDesc:  "SiriDB CPU over threshold, all hosts: 1970-01-01T00:16:22Z",
			expectedQuery: "select filter(> 90) from `usage_user`  " +
				"between '1970-01-01T00:16:22Z' and '1970-01-01T12:16:22Z'",
		},
		{
			desc:               "one host",
			input:              1,
			expectedHumanLabel: "SiriDB CPU over threshold, 1 host(s)",
			expectedHumanDesc:  "SiriDB CPU over threshold, 1 host(s): 1970-01-01T00:47:30Z",
			expectedQuery: "select filter(> 90) from `usage_user` & (`host_9`) " +
				"between '1970-01-01T00:47:30Z' and '1970-01-01T12:47:30Z'",
		},
		{
			desc:               "5 hosts",
			input:              5,
			expectedHumanLabel: "SiriDB CPU over threshold, 5 host(s)",
			expectedHumanDesc:  "SiriDB CPU over threshold, 5 host(s): 1970-01-01T00:08:59Z",
			expectedQuery: "select filter(> 90) " +
				"from `usage_user` & (`host_5`|`host_9`|`host_1`|`host_7`|`host_2`) " +
				"between '1970-01-01T00:08:59Z' and '1970-01-01T12:08:59Z'",
		},
		{
			desc:    "more hosts then cardinality (11)",
			input:   11,
			fail:    true,
			failMsg: "number of hosts (11) larger than total hosts. See --scale (10)",
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
	siriql := "select filter(> 90) from `usage_user` before '2017-01-01'"
	d := NewDevops(time.Now(), time.Now(), 10)
	qi := d.GenerateEmptyQuery()
	q := qi.(*query.SiriDB)
	if len(q.HumanLabel) != 0 {
		t.Errorf("empty query has non-zero length human label")
	}
	if len(q.HumanDescription) != 0 {
		t.Errorf("empty query has non-zero length human desc")
	}
	if len(q.SqlQuery) != 0 {
		t.Errorf("empty query has non-zero length sql")
	}

	d.fillInQuery(q, humanLabel, humanDesc, siriql)
	if got := string(q.HumanLabel); got != humanLabel {
		t.Errorf("filled query mislabeled: got %s want %s", got, humanLabel)
	}
	if got := string(q.HumanDescription); got != humanDesc {
		t.Errorf("filled query mis-described: got %s want %s", got, humanDesc)
	}
	if got := string(q.SqlQuery); got != siriql {
		t.Errorf("Wrong query: got %s want %s", got, siriql)
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
			d := NewDevops(s, e, 10)

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

				verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedQuery)
			}

		})
	}
}

func verifyQuery(t *testing.T, q query.Query, humanLabel, humanDesc, sqlQuery string) {
	siridbq, ok := q.(*query.SiriDB)

	if !ok {
		t.Fatal("Filled query is not *query.SiriDB type")
	}

	if got := string(siridbq.HumanLabel); got != humanLabel {
		t.Errorf("incorrect human label:\ngot\n%s\nwant\n%s", got, humanLabel)
	}

	if got := string(siridbq.HumanDescription); got != humanDesc {
		t.Errorf("incorrect human description:\ngot\n%s\nwant\n%s", got, humanDesc)
	}

	if got := string(siridbq.SqlQuery); got != sqlQuery {
		t.Errorf("incorrect query:\ngot\n%s\nwant\n%s", got, sqlQuery)
	}
}
