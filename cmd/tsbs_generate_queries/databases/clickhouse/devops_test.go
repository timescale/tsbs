package clickhouse

import (
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/query"
)

func TestDevopsGetHostWhereWithHostnames(t *testing.T) {
	cases := []struct {
		desc      string
		hostnames []string
		useTags   bool
		want      string
	}{
		{
			desc:      "single host - no tags",
			hostnames: []string{"foo1"},
			useTags:   false,
			want:      "(hostname = 'foo1')",
		},
		{
			desc:      "multi host - no tags",
			hostnames: []string{"foo1", "foo2"},
			useTags:   false,
			want:      "(hostname = 'foo1' OR hostname = 'foo2')",
		},
		{
			desc:      "single host - w/ tags",
			hostnames: []string{"foo1"},
			useTags:   true,
			want:      "tags_id IN (SELECT id FROM tags WHERE hostname IN ('foo1'))",
		},
		{
			desc:      "multi host - w/ tags",
			hostnames: []string{"foo1", "foo2"},
			useTags:   true,
			want:      "tags_id IN (SELECT id FROM tags WHERE hostname IN ('foo1','foo2'))",
		},
	}

	for _, c := range cases {
		d := NewDevops(time.Now(), time.Now(), 10)
		d.UseTags = c.useTags

		if got := d.getHostWhereWithHostnames(c.hostnames); got != c.want {
			t.Errorf("%s: incorrect output: got %s want %s", c.desc, got, c.want)
		}
	}
}

func TestDevopsGetSelectClausesAggMetrics(t *testing.T) {
	cases := []struct {
		desc    string
		agg     string
		metrics []string
		want    string
	}{
		{
			desc:    "single metric - max",
			agg:     "max",
			metrics: []string{"foo"},
			want:    "max(foo) AS max_foo",
		},
		{
			desc:    "multiple metric - max",
			agg:     "max",
			metrics: []string{"foo", "bar"},
			want:    "max(foo) AS max_foo,max(bar) AS max_bar",
		},
		{
			desc:    "multiple metric - avg",
			agg:     "avg",
			metrics: []string{"foo", "bar"},
			want:    "avg(foo) AS avg_foo,avg(bar) AS avg_bar",
		},
	}

	for _, c := range cases {
		d := NewDevops(time.Now(), time.Now(), 10)

		if got := strings.Join(d.getSelectClausesAggMetrics(c.agg, c.metrics), ","); got != c.want {
			t.Errorf("%s: incorrect output: got %s want %s", c.desc, got, c.want)
		}
	}
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
			expectedHumanLabel: "ClickHouse max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h",
			expectedHumanDesc: "ClickHouse max of all CPU metrics, random    1 hosts, " +
				"random 8h0m0s by 1h: 1970-01-01T00:47:30Z",
			expectedQuery: `
        SELECT
            toStartOfHour(created_at) AS hour,
            max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle, max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq, max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest, max(usage_guest_nice) AS max_usage_guest_nice
        FROM cpu
        WHERE (hostname = 'host_5') AND (created_at >= '1970-01-01 00:47:30') AND (created_at < '1970-01-01 08:47:30')
        GROUP BY hour
        ORDER BY hour
        `,
		},
		{
			desc:               "5 hosts",
			input:              5,
			expectedHumanLabel: "ClickHouse max of all CPU metrics, random    5 hosts, random 8h0m0s by 1h",
			expectedHumanDesc:  "ClickHouse max of all CPU metrics, random    5 hosts, random 8h0m0s by 1h: 1970-01-01T00:17:45Z",
			expectedQuery: `
        SELECT
            toStartOfHour(created_at) AS hour,
            max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle, max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq, max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest, max(usage_guest_nice) AS max_usage_guest_nice
        FROM cpu
        WHERE (hostname = 'host_9' OR hostname = 'host_5' OR hostname = 'host_1' OR hostname = 'host_7' OR hostname = 'host_2') AND (created_at >= '1970-01-01 00:17:45') AND (created_at < '1970-01-01 08:17:45')
        GROUP BY hour
        ORDER BY hour
        `,
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
			expectedHumanLabel: "ClickHouse mean of 1 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "ClickHouse mean of 1 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:16:22Z",
			expectedQuery: `
        SELECT
            hour,
            hostname,
            mean_usage_user
        FROM
        (
            SELECT
                toStartOfHour(created_at) AS hour,
                tags_id AS id,
                avg(usage_user) AS mean_usage_user
            FROM cpu
            WHERE (created_at >= '1970-01-01 00:16:22') AND (created_at < '1970-01-01 12:16:22')
            GROUP BY
                hour,
                id
        ) AS cpu_avg
        
        ORDER BY
            hour ASC,
            hostname
        `,
		},
		{
			desc:               "5 metrics",
			input:              5,
			expectedHumanLabel: "ClickHouse mean of 5 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "ClickHouse mean of 5 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:54:10Z",
			expectedQuery: `
        SELECT
            hour,
            hostname,
            mean_usage_user, mean_usage_system, mean_usage_idle, mean_usage_nice, mean_usage_iowait
        FROM
        (
            SELECT
                toStartOfHour(created_at) AS hour,
                tags_id AS id,
                avg(usage_user) AS mean_usage_user, avg(usage_system) AS mean_usage_system, avg(usage_idle) AS mean_usage_idle, avg(usage_nice) AS mean_usage_nice, avg(usage_iowait) AS mean_usage_iowait
            FROM cpu
            WHERE (created_at >= '1970-01-01 00:54:10') AND (created_at < '1970-01-01 12:54:10')
            GROUP BY
                hour,
                id
        ) AS cpu_avg
        
        ORDER BY
            hour ASC,
            hostname
        `,
		},
		{
			desc:               "use tags",
			input:              5,
			devopsUseTags:      true,
			expectedHumanLabel: "ClickHouse mean of 5 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "ClickHouse mean of 5 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:47:30Z",
			expectedQuery: `
        SELECT
            hour,
            hostname,
            mean_usage_user, mean_usage_system, mean_usage_idle, mean_usage_nice, mean_usage_iowait
        FROM
        (
            SELECT
                toStartOfHour(created_at) AS hour,
                tags_id AS id,
                avg(usage_user) AS mean_usage_user, avg(usage_system) AS mean_usage_system, avg(usage_idle) AS mean_usage_idle, avg(usage_nice) AS mean_usage_nice, avg(usage_iowait) AS mean_usage_iowait
            FROM cpu
            WHERE (created_at >= '1970-01-01 00:47:30') AND (created_at < '1970-01-01 12:47:30')
            GROUP BY
                hour,
                id
        ) AS cpu_avg
        ANY INNER JOIN tags USING (id)
        ORDER BY
            hour ASC,
            hostname
        `,
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

func TestGroupByOrderByLimit(t *testing.T) {
	cases := []testCase{
		{
			desc:               "happy path",
			expectedHumanLabel: "ClickHouse max cpu over last 5 min-intervals (random end)",
			expectedHumanDesc:  "ClickHouse max cpu over last 5 min-intervals (random end): 1970-01-01T01:16:22Z",
			expectedQuery: `
        SELECT
            toStartOfMinute(created_at) AS minute,
            max(usage_user)
        FROM cpu
        WHERE created_at < '1970-01-01 01:16:22'
        GROUP BY minute
        ORDER BY minute DESC
        LIMIT 5
        `,
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
			expectedHumanLabel: "ClickHouse CPU over threshold, all hosts",
			expectedHumanDesc:  "ClickHouse CPU over threshold, all hosts: 1970-01-01T00:16:22Z",
			expectedQuery: `
        SELECT *
        FROM cpu
        PREWHERE (usage_user > 90.0) AND (created_at >= '1970-01-01 00:16:22') AND (created_at <  '1970-01-01 12:16:22') 
        `,
		},
		{
			desc:               "one host",
			input:              1,
			expectedHumanLabel: "ClickHouse CPU over threshold, 1 host(s)",
			expectedHumanDesc:  "ClickHouse CPU over threshold, 1 host(s): 1970-01-01T00:47:30Z",
			expectedQuery: `
        SELECT *
        FROM cpu
        PREWHERE (usage_user > 90.0) AND (created_at >= '1970-01-01 00:47:30') AND (created_at <  '1970-01-01 12:47:30') AND ((hostname = 'host_9'))
        `,
		},
		{
			desc:               "5 hosts",
			input:              5,
			expectedHumanLabel: "ClickHouse CPU over threshold, 5 host(s)",
			expectedHumanDesc:  "ClickHouse CPU over threshold, 5 host(s): 1970-01-01T00:08:59Z",
			expectedQuery: `
        SELECT *
        FROM cpu
        PREWHERE (usage_user > 90.0) AND (created_at >= '1970-01-01 00:08:59') AND (created_at <  '1970-01-01 12:08:59') AND ((hostname = 'host_5' OR hostname = 'host_9' OR hostname = 'host_1' OR hostname = 'host_7' OR hostname = 'host_2'))
        `,
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

func TestLastPointPerHost(t *testing.T) {
	cases := []testCase{
		{
			desc:               "happy path",
			expectedHumanLabel: "ClickHouse last row per host",
			expectedHumanDesc:  "ClickHouse last row per host",
			expectedQuery: `
            SELECT DISTINCT(hostname), *
            FROM cpu
            ORDER BY
                hostname ASC,
                created_at DESC
            `,
		},
		{
			desc:               "use tags",
			devopsUseTags:      true,
			expectedHumanLabel: "ClickHouse last row per host",
			expectedHumanDesc:  "ClickHouse last row per host",
			expectedQuery: `
            SELECT *
            FROM
            (
                SELECT *
                FROM cpu
                WHERE (tags_id, created_at) IN
                (
                    SELECT
                        tags_id,
                        max(created_at)
                    FROM cpu
                    GROUP BY tags_id
                )
            ) AS c
            ANY INNER JOIN tags AS t ON c.tags_id = t.id
            ORDER BY
                t.hostname ASC,
                c.time DESC
            `,
		},
	}
	testFunc := func(d *Devops, c testCase) query.Query {
		q := d.GenerateEmptyQuery()
		d.LastPointPerHost(q)
		return q
	}

	start := time.Unix(0, 0)
	end := start.Add(devops.HighCPUDuration).Add(time.Hour)

	runTestCases(t, testFunc, start, end, cases)
}

func TestGroupByTime(t *testing.T) {
	cases := []testCase{
		{
			desc:               "happy path",
			input:              1,
			expectedHumanLabel: "ClickHouse 1 cpu metric(s), random    1 hosts, random 1s by 1m",
			expectedHumanDesc:  "ClickHouse 1 cpu metric(s), random    1 hosts, random 1s by 1m: 1970-01-01T01:09:26Z",
			expectedQuery: `
        SELECT
            toStartOfMinute(created_at) AS minute,
            max(usage_user) AS max_usage_user
        FROM cpu
        WHERE (hostname = 'host_9') AND (created_at >= '1970-01-01 01:09:26') AND (created_at < '1970-01-01 01:09:27')
        GROUP BY minute
        ORDER BY minute ASC
        `,
		},
	}

	testFunc := func(d *Devops, c testCase) query.Query {
		q := d.GenerateEmptyQuery()
		d.GroupByTime(q, c.input, 1, time.Second)
		return q
	}

	start := time.Unix(0, 0)
	end := start.Add(2 * time.Hour)

	runTestCases(t, testFunc, start, end, cases)
}

type testCase struct {
	desc               string
	input              int
	devopsUseTags      bool
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
			d.UseTags = c.devopsUseTags

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
	clickhouseql, ok := q.(*query.ClickHouse)

	if !ok {
		t.Fatal("Filled query is not *query.Clickhouse type")
	}

	if got := string(clickhouseql.HumanLabel); got != humanLabel {
		t.Errorf("incorrect human label:\ngot\n%s\nwant\n%s", got, humanLabel)
	}

	if got := string(clickhouseql.HumanDescription); got != humanDesc {
		t.Errorf("incorrect human description:\ngot\n%s\nwant\n%s", got, humanDesc)
	}

	if got := string(clickhouseql.SqlQuery); got != sqlQuery {
		t.Errorf("incorrect query:\ngot\n%s\nwant\n%s", got, sqlQuery)
	}
}
