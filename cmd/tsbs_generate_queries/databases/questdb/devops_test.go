package questdb

import (
	"math/rand"
	"net/url"
	"regexp"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/pkg/query"
)

func TestDevopsGroupByTime(t *testing.T) {
	expectedHumanLabel := "QuestDB 1 cpu metric(s), random    1 hosts, random 1s by 1m"
	expectedHumanDesc := "QuestDB 1 cpu metric(s), random    1 hosts, random 1s by 1m: 1970-01-01T00:05:58Z"
	expectedQuery := "SELECT timestamp, max(usage_user) AS max_usage_user FROM cpu " +
		"WHERE hostname IN ('host_9') AND timestamp >= '1970-01-01T00:05:58Z' AND timestamp < '1970-01-01T00:05:59Z' SAMPLE BY 1m"

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

	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc, expectedQuery)
}

func TestDevopsGroupByOrderByLimit(t *testing.T) {
	expectedHumanLabel := "QuestDB max cpu over last 5 min-intervals (random end)"
	expectedHumanDesc := "QuestDB max cpu over last 5 min-intervals (random end): 1970-01-01T01:16:22Z"
	expectedQuery := "SELECT timestamp AS minute, max(usage_user) FROM cpu " +
		"WHERE timestamp < '1970-01-01T01:16:22Z' SAMPLE BY 1m LIMIT 5"

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

	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc, expectedQuery)
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
			expectedHumanLabel: "QuestDB mean of 1 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "QuestDB mean of 1 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:16:22Z",
			expectedQuery: "SELECT timestamp, hostname, avg(usage_user) AS avg_usage_user FROM cpu " +
				"WHERE timestamp >= '1970-01-01T00:16:22Z' AND timestamp < '1970-01-01T12:16:22Z' " +
				"SAMPLE BY 1h GROUP BY timestamp, hostname",
		},
		{
			desc:               "5 metrics",
			input:              5,
			expectedHumanLabel: "QuestDB mean of 5 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "QuestDB mean of 5 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:54:10Z",
			expectedQuery: "SELECT timestamp, hostname, avg(usage_user) AS avg_usage_user, avg(usage_system) AS avg_usage_system, avg(usage_idle) AS avg_usage_idle, avg(usage_nice) AS avg_usage_nice, avg(usage_iowait) AS avg_usage_iowait FROM cpu " +
				"WHERE timestamp >= '1970-01-01T00:54:10Z' AND timestamp < '1970-01-01T12:54:10Z' " +
				"SAMPLE BY 1h GROUP BY timestamp, hostname",
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
			expectedHumanLabel: "QuestDB max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h",
			expectedHumanDesc:  "QuestDB max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h: 1970-01-01T00:54:10Z",
			expectedQuery: "SELECT hour(timestamp) AS hour, max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle, max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq, max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest, max(usage_guest_nice) AS max_usage_guest_nice FROM cpu " +
				"WHERE hostname IN ('host_3') AND timestamp >= '1970-01-01T00:54:10Z' AND timestamp < '1970-01-01T08:54:10Z' " +
				"SAMPLE BY 1h",
		},
		{
			desc:               "5 hosts",
			input:              5,
			expectedHumanLabel: "QuestDB max of all CPU metrics, random    5 hosts, random 8h0m0s by 1h",
			expectedHumanDesc:  "QuestDB max of all CPU metrics, random    5 hosts, random 8h0m0s by 1h: 1970-01-01T00:37:12Z",
			expectedQuery: "SELECT hour(timestamp) AS hour, max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle, max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq, max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest, max(usage_guest_nice) AS max_usage_guest_nice FROM cpu " +
				"WHERE hostname IN ('host_9', 'host_5', 'host_1', 'host_7', 'host_2') AND timestamp >= '1970-01-01T00:37:12Z' AND timestamp < '1970-01-01T08:37:12Z' " +
				"SAMPLE BY 1h",
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
	expectedHumanLabel := "QuestDB last row per host"
	expectedHumanDesc := "QuestDB last row per host"
	expectedQuery := `SELECT * FROM cpu latest by hostname`

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

	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc, expectedQuery)
}

func TestHighCPUForHosts(t *testing.T) {
	cases := []testCase{
		{
			desc:    "negative hosts",
			input:   -1,
			fail:    true,
			failMsg: "nHosts cannot be negative",
		},
		{
			desc:               "zero hosts",
			input:              0,
			expectedHumanLabel: "QuestDB CPU over threshold, all hosts",
			expectedHumanDesc:  "QuestDB CPU over threshold, all hosts: 1970-01-01T00:54:10Z",
			expectedQuery: "SELECT * FROM cpu " +
				"WHERE usage_user > 90.0 AND " +
				"timestamp >= '1970-01-01T00:54:10Z' AND timestamp < '1970-01-01T12:54:10Z'",
		},
		{
			desc:               "1 host",
			input:              1,
			expectedHumanLabel: "QuestDB CPU over threshold, 1 host(s)",
			expectedHumanDesc:  "QuestDB CPU over threshold, 1 host(s): 1970-01-01T00:47:30Z",
			expectedQuery: "SELECT * FROM cpu " +
				"WHERE usage_user > 90.0 AND hostname IN ('host_5') AND " +
				"timestamp >= '1970-01-01T00:47:30Z' AND timestamp < '1970-01-01T12:47:30Z'",
		},
		{
			desc:               "5 hosts",
			input:              5,
			expectedHumanLabel: "QuestDB CPU over threshold, 5 host(s)",
			expectedHumanDesc:  "QuestDB CPU over threshold, 5 host(s): 1970-01-01T00:17:45Z",
			expectedQuery: "SELECT * FROM cpu " +
				"WHERE usage_user > 90.0 AND " +
				"hostname IN ('host_9', 'host_5', 'host_1', 'host_7', 'host_2') AND " +
				"timestamp >= '1970-01-01T00:17:45Z' AND timestamp < '1970-01-01T12:17:45Z'",
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
				verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedQuery)
			}

		})
	}
}

func verifyQuery(t *testing.T, q query.Query, humanLabel, humanDesc, expectedSql string) {
	sql, ok := q.(*query.HTTP)

	if !ok {
		t.Fatal("Filled query is not *query.HTTP type")
	}

	if got := string(sql.HumanLabel); got != humanLabel {
		t.Errorf("incorrect human label:\ngot\n%s\nwant\n%s", got, humanLabel)
	}

	if got := string(sql.HumanDescription); got != humanDesc {
		t.Errorf("incorrect human description:\ngot\n%s\nwant\n%s", got, humanDesc)
	}

	if got := string(sql.Method); got != "GET" {
		t.Errorf("incorrect method:\ngot\n%s\nwant GET", got)
	}

	uri := string(sql.Path)
	u, err := url.Parse(uri)
	if err != nil {
		t.Errorf("Failed to decode %s: %s", uri, err)
	}
	actualSql := normaliseField(u.Query()["query"][0])

	if expectedSql != actualSql {
		t.Errorf("expcted %s, actual %s", expectedSql, actualSql)
	}
}

func normaliseField(fieldValue string) string {
	m1 := regexp.MustCompile("^\\s+")
	m2 := regexp.MustCompile("\\s+$")
	m3 := regexp.MustCompile("\\s+")
	fieldValue = m1.ReplaceAllString(fieldValue, "")
	fieldValue = m2.ReplaceAllString(fieldValue, "")
	fieldValue = m3.ReplaceAllString(fieldValue, " ")
	return fieldValue
}
