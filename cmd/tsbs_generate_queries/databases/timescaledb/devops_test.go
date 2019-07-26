package timescaledb

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/andreyvit/diff"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/query"
)

func TestDevopsGetHostWhereWithHostnames(t *testing.T) {
	cases := []struct {
		desc      string
		hostnames []string
		useJSON   bool
		useTags   bool
		want      string
	}{
		{
			desc:      "single host - no json or tags",
			hostnames: []string{"foo1"},
			useJSON:   false,
			useTags:   false,
			want:      "(hostname = 'foo1')",
		},
		{
			desc:      "multi host - no json or tags",
			hostnames: []string{"foo1", "foo2"},
			useJSON:   false,
			useTags:   false,
			want:      "(hostname = 'foo1' OR hostname = 'foo2')",
		},
		{
			desc:      "single host - w/ json",
			hostnames: []string{"foo1"},
			useJSON:   true,
			useTags:   false,
			want:      "tags_id IN (SELECT id FROM tags WHERE tagset @> '{\"hostname\": \"foo1\"}')",
		},
		{
			desc:      "multi host - w/ json",
			hostnames: []string{"foo1", "foo2"},
			useJSON:   true,
			useTags:   false,
			want:      "tags_id IN (SELECT id FROM tags WHERE tagset @> '{\"hostname\": \"foo1\"}' OR tagset @> '{\"hostname\": \"foo2\"}')",
		},
		{
			desc:      "single host - w/ tags",
			hostnames: []string{"foo1"},
			useJSON:   false,
			useTags:   true,
			want:      "tags_id IN (SELECT id FROM tags WHERE hostname IN ('foo1'))",
		},
		{
			desc:      "multi host - w/ tags",
			hostnames: []string{"foo1", "foo2"},
			useJSON:   false,
			useTags:   true,
			want:      "tags_id IN (SELECT id FROM tags WHERE hostname IN ('foo1','foo2'))",
		},
	}

	for _, c := range cases {
		b := BaseGenerator{
			UseJSON: c.useJSON,
			UseTags: c.useTags,
		}
		dq, err := b.NewDevops(time.Now(), time.Now(), 10)
		if err != nil {
			t.Fatalf("Error while creating devops generator")
		}
		d := dq.(*Devops)

		if got := d.getHostWhereWithHostnames(c.hostnames); got != c.want {
			t.Errorf("%s: incorrect output: got %s want %s", c.desc, got, c.want)
		}
	}
}

func TestDevopsGetHostWhereString(t *testing.T) {
	cases := []struct {
		nHosts int
		want   string
	}{
		{
			nHosts: 1,
			want:   "(hostname = 'host_5')",
		},
		{
			nHosts: 2,
			want:   "(hostname = 'host_5' OR hostname = 'host_9')",
		},
		{
			nHosts: 5,
			want:   "(hostname = 'host_5' OR hostname = 'host_9' OR hostname = 'host_3' OR hostname = 'host_1' OR hostname = 'host_7')",
		},
	}

	for _, c := range cases {
		rand.Seed(123)
		b := BaseGenerator{}
		dq, err := b.NewDevops(time.Now(), time.Now(), 10)
		if err != nil {
			t.Fatalf("Error while creating devops generator")
		}
		d := dq.(*Devops)

		if got := d.getHostWhereString(c.nHosts); got != c.want {
			t.Errorf("incorrect output for %d hosts: got %s want %s", c.nHosts, got, c.want)
		}
	}
}

func TestDevopsGetTimeBucket(t *testing.T) {
	b := BaseGenerator{}
	dq, err := b.NewDevops(time.Now(), time.Now(), 10)
	if err != nil {
		t.Fatalf("Error while creating devops generator")
	}
	d := dq.(*Devops)

	seconds := 60
	want := fmt.Sprintf(nonTimeBucketFmt, seconds, seconds)
	if got := d.getTimeBucket(seconds); got != want {
		t.Errorf("incorrect non time bucket format: got %s want %s", got, want)
	}

	d.UseTimeBucket = true
	want = fmt.Sprintf(timeBucketFmt, seconds)
	if got := d.getTimeBucket(seconds); got != want {
		t.Errorf("incorrect time bucket format: got %s want %s", got, want)
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
			want:    "max(foo) as max_foo",
		},
		{
			desc:    "multiple metric - max",
			agg:     "max",
			metrics: []string{"foo", "bar"},
			want:    "max(foo) as max_foo,max(bar) as max_bar",
		},
		{
			desc:    "multiple metric - avg",
			agg:     "avg",
			metrics: []string{"foo", "bar"},
			want:    "avg(foo) as avg_foo,avg(bar) as avg_bar",
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		dq, err := b.NewDevops(time.Now(), time.Now(), 10)
		if err != nil {
			t.Fatalf("Error while creating devops generator")
		}
		d := dq.(*Devops)

		if got := strings.Join(d.getSelectClausesAggMetrics(c.agg, c.metrics), ","); got != c.want {
			t.Errorf("%s: incorrect output: got %s want %s", c.desc, got, c.want)
		}
	}
}

func TestDevopsGroupByTime(t *testing.T) {
	expectedHumanLabel := "TimescaleDB 1 cpu metric(s), random    1 hosts, random 1s by 1m"
	expectedHumanDesc := "TimescaleDB 1 cpu metric(s), random    1 hosts, random 1s by 1m: 1970-01-01T00:05:58Z"
	expectedHypertable := "cpu"
	expectedSQLQuery := `SELECT time_bucket('60 seconds', time) AS minute,
        max(usage_user) as max_usage_user
        FROM cpu
        WHERE (hostname = 'host_9') AND time >= '1970-01-01 00:05:58.646325 +0000' AND time < '1970-01-01 00:05:59.646325 +0000'
        GROUP BY minute ORDER BY minute ASC`

	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(time.Hour)
	b := BaseGenerator{
		UseTimeBucket: true,
	}
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

	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc, expectedHypertable, expectedSQLQuery)
}

func TestGroupByOrderByLimit(t *testing.T) {
	expectedHumanLabel := "TimescaleDB max cpu over last 5 min-intervals (random end)"
	expectedHumanDesc := "TimescaleDB max cpu over last 5 min-intervals (random end): 1970-01-01T01:16:22Z"
	expectedHypertable := "cpu"
	expectedSQLQuery := `SELECT time_bucket('60 seconds', time) AS minute, max(usage_user)
        FROM cpu
        WHERE time < '1970-01-01 01:16:22.646325 +0000'
        GROUP BY minute
        ORDER BY minute DESC
        LIMIT 5`

	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(2 * time.Hour)
	b := BaseGenerator{
		UseTimeBucket: true,
	}
	dq, err := b.NewDevops(s, e, 10)
	if err != nil {
		t.Fatalf("Error while creating devops generator")
	}
	d := dq.(*Devops)

	q := d.GenerateEmptyQuery()
	d.GroupByOrderByLimit(q)

	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc, expectedHypertable, expectedSQLQuery)
}

func TestGroupByTimeAndPrimaryTag(t *testing.T) {
	cases := []struct {
		desc               string
		useJSON            bool
		useTags            bool
		expectedHumanLabel string
		expectedHumanDesc  string
		expectedHypertable string
		expectedSQLQuery   string
	}{
		{
			desc:               "no JSON or tags",
			expectedHumanLabel: "TimescaleDB mean of 1 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "TimescaleDB mean of 1 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:16:22Z",
			expectedHypertable: "cpu",
			expectedSQLQuery: `
        WITH cpu_avg AS (
          SELECT time_bucket('3600 seconds', time) as hour, tags_id,
          avg(usage_user) as mean_usage_user
          FROM cpu
          WHERE time >= '1970-01-01 00:16:22.646325 +0000' AND time < '1970-01-01 12:16:22.646325 +0000'
          GROUP BY hour, tags_id
        )
        SELECT hour, hostname, mean_usage_user
        FROM cpu_avg
        
        ORDER BY hour, hostname`,
		},
		{
			desc:               "use JSON",
			useJSON:            true,
			expectedHumanLabel: "TimescaleDB mean of 1 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "TimescaleDB mean of 1 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:54:10Z",
			expectedHypertable: "cpu",
			expectedSQLQuery: `
        WITH cpu_avg AS (
          SELECT time_bucket('3600 seconds', time) as hour, tags_id,
          avg(usage_user) as mean_usage_user
          FROM cpu
          WHERE time >= '1970-01-01 00:54:10.138978 +0000' AND time < '1970-01-01 12:54:10.138978 +0000'
          GROUP BY hour, tags_id
        )
        SELECT hour, tags->>'hostname', mean_usage_user
        FROM cpu_avg
        JOIN tags ON cpu_avg.tags_id = tags.id
        ORDER BY hour, tags->>'hostname'`,
		},
		{
			desc:               "use tags",
			useTags:            true,
			expectedHumanLabel: "TimescaleDB mean of 1 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "TimescaleDB mean of 1 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:47:30Z",
			expectedHypertable: "cpu",
			expectedSQLQuery: `
        WITH cpu_avg AS (
          SELECT time_bucket('3600 seconds', time) as hour, tags_id,
          avg(usage_user) as mean_usage_user
          FROM cpu
          WHERE time >= '1970-01-01 00:47:30.894865 +0000' AND time < '1970-01-01 12:47:30.894865 +0000'
          GROUP BY hour, tags_id
        )
        SELECT hour, tags.hostname, mean_usage_user
        FROM cpu_avg
        JOIN tags ON cpu_avg.tags_id = tags.id
        ORDER BY hour, tags.hostname`,
		},
		{
			desc:               "enable JSON and tags but use JSON",
			useJSON:            true,
			useTags:            true,
			expectedHumanLabel: "TimescaleDB mean of 1 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "TimescaleDB mean of 1 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:37:12Z",
			expectedHypertable: "cpu",
			expectedSQLQuery: `
        WITH cpu_avg AS (
          SELECT time_bucket('3600 seconds', time) as hour, tags_id,
          avg(usage_user) as mean_usage_user
          FROM cpu
          WHERE time >= '1970-01-01 00:37:12.342805 +0000' AND time < '1970-01-01 12:37:12.342805 +0000'
          GROUP BY hour, tags_id
        )
        SELECT hour, tags->>'hostname', mean_usage_user
        FROM cpu_avg
        JOIN tags ON cpu_avg.tags_id = tags.id
        ORDER BY hour, tags->>'hostname'`,
		},
	}

	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(devops.DoubleGroupByDuration).Add(time.Hour)

	numMetrics := 1

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			b := BaseGenerator{
				UseJSON:       c.useJSON,
				UseTags:       c.useTags,
				UseTimeBucket: true,
			}
			dq, err := b.NewDevops(s, e, 10)
			if err != nil {
				t.Fatalf("Error while creating devops generator")
			}
			d := dq.(*Devops)

			q := d.GenerateEmptyQuery()
			d.GroupByTimeAndPrimaryTag(q, numMetrics)

			verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedHypertable, c.expectedSQLQuery)
		})
	}
}

func TestMaxAllCPU(t *testing.T) {
	expectedHumanLabel := "TimescaleDB max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h"
	expectedHumanDesc := "TimescaleDB max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h: 1970-01-01T00:16:22Z"
	expectedHypertable := "cpu"
	expectedSQLQuery := `SELECT time_bucket('3600 seconds', time) AS hour,
        max(usage_user) as max_usage_user, max(usage_system) as max_usage_system, max(usage_idle) as max_usage_idle, ` +
		"max(usage_nice) as max_usage_nice, max(usage_iowait) as max_usage_iowait, max(usage_irq) as max_usage_irq, " +
		"max(usage_softirq) as max_usage_softirq, max(usage_steal) as max_usage_steal, max(usage_guest) as max_usage_guest, " +
		`max(usage_guest_nice) as max_usage_guest_nice
        FROM cpu
        WHERE (hostname = 'host_9') AND time >= '1970-01-01 00:16:22.646325 +0000' AND time < '1970-01-01 08:16:22.646325 +0000'
        GROUP BY hour ORDER BY hour`
	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(devops.MaxAllDuration).Add(time.Hour)

	b := BaseGenerator{
		UseTimeBucket: true,
	}
	dq, err := b.NewDevops(s, e, 10)
	if err != nil {
		t.Fatalf("Error while creating devops generator")
	}
	d := dq.(*Devops)

	q := d.GenerateEmptyQuery()
	d.MaxAllCPU(q, 1)
	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc, expectedHypertable, expectedSQLQuery)
}

func TestLastPointPerHost(t *testing.T) {
	cases := []struct {
		desc               string
		useJSON            bool
		useTags            bool
		expectedHumanLabel string
		expectedHumanDesc  string
		expectedHypertable string
		expectedSQLQuery   string
	}{
		{
			desc:               "no JSON or tags",
			expectedHumanLabel: "TimescaleDB last row per host",
			expectedHumanDesc:  "TimescaleDB last row per host",
			expectedHypertable: "cpu",
			expectedSQLQuery:   "SELECT DISTINCT ON (hostname) * FROM cpu ORDER BY hostname, time DESC",
		},
		{
			desc:               "use JSON",
			useJSON:            true,
			expectedHumanLabel: "TimescaleDB last row per host",
			expectedHumanDesc:  "TimescaleDB last row per host",
			expectedHypertable: "cpu",
			expectedSQLQuery: "SELECT DISTINCT ON (t.tagset->>'hostname') * FROM tags t INNER JOIN LATERAL(SELECT * " +
				"FROM cpu c WHERE c.tags_id = t.id ORDER BY time DESC LIMIT 1) AS b ON true ORDER BY t.tagset->>'hostname', b.time DESC",
		},
		{
			desc:               "use tags",
			useTags:            true,
			expectedHumanLabel: "TimescaleDB last row per host",
			expectedHumanDesc:  "TimescaleDB last row per host",
			expectedHypertable: "cpu",
			expectedSQLQuery: "SELECT DISTINCT ON (t.hostname) * FROM tags t INNER JOIN LATERAL(SELECT * FROM cpu c " +
				"WHERE c.tags_id = t.id ORDER BY time DESC LIMIT 1) AS b ON true ORDER BY t.hostname, b.time DESC",
		},
		{
			desc:               "enable JSON and tags but use tags",
			useJSON:            true,
			useTags:            true,
			expectedHumanLabel: "TimescaleDB last row per host",
			expectedHumanDesc:  "TimescaleDB last row per host",
			expectedHypertable: "cpu",
			expectedSQLQuery: "SELECT DISTINCT ON (t.hostname) * FROM tags t INNER JOIN LATERAL(SELECT * FROM cpu c " +
				"WHERE c.tags_id = t.id ORDER BY time DESC LIMIT 1) AS b ON true ORDER BY t.hostname, b.time DESC",
		},
	}

	rand.Seed(123) // Setting seed for testing purposes.

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			b := BaseGenerator{
				UseJSON: c.useJSON,
				UseTags: c.useTags,
			}
			dq, err := b.NewDevops(time.Now(), time.Now(), 10)
			if err != nil {
				t.Fatalf("Error while creating devops generator")
			}
			d := dq.(*Devops)

			q := d.GenerateEmptyQuery()
			d.LastPointPerHost(q)
			verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedHypertable, c.expectedSQLQuery)
		})
	}
}

func TestHighCPUForHosts(t *testing.T) {
	cases := []struct {
		desc               string
		nHosts             int
		expectedHumanLabel string
		expectedHumanDesc  string
		expectedHypertable string
		expectedSQLQuery   string
	}{
		{
			desc:               "zero hosts",
			nHosts:             0,
			expectedHumanLabel: "TimescaleDB CPU over threshold, all hosts",
			expectedHumanDesc:  "TimescaleDB CPU over threshold, all hosts: 1970-01-01T00:16:22Z",
			expectedHypertable: "cpu",
			expectedSQLQuery: "SELECT * FROM cpu WHERE usage_user > 90.0 and time >= '1970-01-01 00:16:22.646325 +0000'" +
				" AND time < '1970-01-01 12:16:22.646325 +0000' ",
		},
		{
			desc:               "one host",
			nHosts:             1,
			expectedHumanLabel: "TimescaleDB CPU over threshold, 1 host(s)",
			expectedHumanDesc:  "TimescaleDB CPU over threshold, 1 host(s): 1970-01-01T00:47:30Z",
			expectedHypertable: "cpu",
			expectedSQLQuery: "SELECT * FROM cpu WHERE usage_user > 90.0 and time >= '1970-01-01 00:47:30.894865 +0000'" +
				" AND time < '1970-01-01 12:47:30.894865 +0000' AND (hostname = 'host_9')",
		},
		{
			desc:               "five hosts",
			nHosts:             5,
			expectedHumanLabel: "TimescaleDB CPU over threshold, 5 host(s)",
			expectedHumanDesc:  "TimescaleDB CPU over threshold, 5 host(s): 1970-01-01T00:08:59Z",
			expectedHypertable: "cpu",
			expectedSQLQuery: "SELECT * FROM cpu WHERE usage_user > 90.0 and time >= '1970-01-01 00:08:59.080812 +0000'" +
				" AND time < '1970-01-01 12:08:59.080812 +0000' AND (hostname = 'host_5' OR hostname = 'host_9' " +
				"OR hostname = 'host_1' OR hostname = 'host_7' OR hostname = 'host_2')",
		},
	}

	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(devops.HighCPUDuration).Add(time.Hour)

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			b := BaseGenerator{}
			dq, err := b.NewDevops(s, e, 10)
			if err != nil {
				t.Fatalf("Error while creating devops generator")
			}
			d := dq.(*Devops)

			q := d.GenerateEmptyQuery()
			d.HighCPUForHosts(q, c.nHosts)

			verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedHypertable, c.expectedSQLQuery)
		})
	}
}

func verifyQuery(t *testing.T, q query.Query, humanLabel, humanDesc, hypertable, sqlQuery string) {
	tsq, ok := q.(*query.TimescaleDB)

	if !ok {
		t.Fatal("Filled query is not *query.TimescaleDB type")
	}

	if got := string(tsq.HumanLabel); got != humanLabel {
		t.Errorf("incorrect human label:\ngot\n%s\nwant\n%s", got, humanLabel)
	}

	if got := string(tsq.HumanDescription); got != humanDesc {
		t.Errorf("incorrect human description:\ngot\n%s\nwant\n%s", got, humanDesc)
	}

	if got := string(tsq.Hypertable); got != hypertable {
		t.Errorf("incorrect hypertable:\ngot\n%s\nwant\n%s", got, hypertable)
	}

	if got := string(tsq.SqlQuery); got != sqlQuery {
		t.Errorf("incorrect SQL query:\ndiff\n%s\ngot\n%s\nwant\n%s", diff.CharacterDiff(got, sqlQuery), got, sqlQuery)
	}
}
