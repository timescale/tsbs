package timestream

import (
	"fmt"
	"github.com/andreyvit/diff"
	"github.com/timescale/tsbs/pkg/query"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
)

// getMeasureNameWhereString
func TestDevopsGetMeasureNameWhereString(t *testing.T) {
	cases := []struct {
		desc      string
		hostnames []string
		want      string
	}{
		{
			desc:      "single host",
			hostnames: []string{"foo1"},
			want:      "(hostname = 'foo1')",
		},
		{
			desc:      "multi host",
			hostnames: []string{"foo1", "foo2"},
			want:      "(hostname = 'foo1' OR hostname = 'foo2')",
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
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

func TestDevopsGetMeasureNameWhere(t *testing.T) {
	cases := []struct {
		desc         string
		measureNames []string
		want         string
	}{
		{
			desc:         "single measure",
			measureNames: []string{"foo1"},
			want:         "(measure_name = 'foo1')",
		},
		{
			desc:         "multi host",
			measureNames: []string{"foo1", "foo2"},
			want:         "(measure_name = 'foo1' OR measure_name = 'foo2')",
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		dq, err := b.NewDevops(time.Now(), time.Now(), 10)
		if err != nil {
			t.Fatalf("Error while creating devops generator")
		}
		d := dq.(*Devops)

		if got := d.getMeasureNameWhereString(c.measureNames); got != c.want {
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
	want := fmt.Sprintf(timeBucketFmt, seconds)
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
			want:    "max(case when measure_name = 'foo' THEN measure_value::double ELSE NULL END) as max_foo",
		},
		{
			desc:    "multiple metric - max",
			agg:     "max",
			metrics: []string{"foo", "bar"},
			want: "max(case when measure_name = 'foo' THEN measure_value::double ELSE NULL END) as max_foo," +
				"max(case when measure_name = 'bar' THEN measure_value::double ELSE NULL END) as max_bar",
		},
		{
			desc:    "multiple metric - avg",
			agg:     "avg",
			metrics: []string{"foo", "bar"},
			want: "avg(case when measure_name = 'foo' THEN measure_value::double ELSE NULL END) as avg_foo," +
				"avg(case when measure_name = 'bar' THEN measure_value::double ELSE NULL END) as avg_bar",
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
	expectedHumanLabel := "Timestream 1 cpu metric(s), random    1 hosts, random 1s by 1m"
	expectedHumanDesc := "Timestream 1 cpu metric(s), random    1 hosts, random 1s by 1m: 1970-01-01T00:05:58Z"
	expectedTable := "cpu"
	expectedSQLQuery := `SELECT bin(time, 60s) AS minute,
        max(case when measure_name = 'usage_user' THEN measure_value::double ELSE NULL END) as max_usage_user
        FROM "db"."cpu"
        WHERE (measure_name = 'usage_user') AND (hostname = 'host_9') AND time >= '1970-01-01 00:05:58.646325 +0000' AND time < '1970-01-01 00:05:59.646325 +0000'
        GROUP BY 1 ORDER BY 1 ASC`

	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(time.Hour)
	b := BaseGenerator{DBName: "db"}
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

	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc, expectedTable, expectedSQLQuery)
}

func TestGroupByOrderByLimit(t *testing.T) {
	expectedHumanLabel := "Timestream max cpu over last 5 min-intervals (random end)"
	expectedHumanDesc := "Timestream max cpu over last 5 min-intervals (random end): 1970-01-01T01:16:22Z"
	expectedTable := "cpu"
	expectedSQLQuery := `SELECT bin(time, 60s) AS minute, max(measure_value::double) as max_usage_user
        FROM "b"."cpu"
        WHERE time < '1970-01-01 01:16:22.646325 +0000' AND measure_name = 'usage_user'
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 5`

	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(2 * time.Hour)
	b := BaseGenerator{
		DBName: "b",
	}
	dq, err := b.NewDevops(s, e, 10)
	if err != nil {
		t.Fatalf("Error while creating devops generator")
	}
	d := dq.(*Devops)

	q := d.GenerateEmptyQuery()
	d.GroupByOrderByLimit(q)

	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc, expectedTable, expectedSQLQuery)
}

func TestGroupByTimeAndPrimaryTag(t *testing.T) {
	cases := []struct {
		desc               string
		expectedHumanLabel string
		expectedHumanDesc  string
		expectedTable      string
		expectedSQLQuery   string
		numMetrics         int
	}{
		{
			desc:               "1 metric",
			expectedHumanLabel: "Timestream mean of 1 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "Timestream mean of 1 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:16:22Z",
			expectedTable:      "cpu",
			expectedSQLQuery: `
        SELECT bin(time, 3600s) as hour, 
			hostname,
			avg (case when measure_name = 'usage_user' THEN measure_value::double ELSE NULL END) as mean_usage_user
		FROM "b"."cpu"
		WHERE time >= '1970-01-01 00:16:22.646325 +0000' AND time < '1970-01-01 12:16:22.646325 +0000'
		GROUP BY 1, 2`,
			numMetrics: 1,
		}, {
			desc:               "3 metric",
			expectedHumanLabel: "Timestream mean of 3 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "Timestream mean of 3 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:54:10Z",
			expectedTable:      "cpu",
			expectedSQLQuery: `
        SELECT bin(time, 3600s) as hour, 
			hostname,
			avg (case when measure_name = 'usage_user' THEN measure_value::double ELSE NULL END) as mean_usage_user,
			avg (case when measure_name = 'usage_system' THEN measure_value::double ELSE NULL END) as mean_usage_system,
			avg (case when measure_name = 'usage_idle' THEN measure_value::double ELSE NULL END) as mean_usage_idle
		FROM "b"."cpu"
		WHERE time >= '1970-01-01 00:54:10.138978 +0000' AND time < '1970-01-01 12:54:10.138978 +0000'
		GROUP BY 1, 2`,
			numMetrics: 3,
		},
	}

	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(devops.DoubleGroupByDuration).Add(time.Hour)

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			b := BaseGenerator{
				DBName: "b",
			}
			dq, err := b.NewDevops(s, e, 10)
			if err != nil {
				t.Fatalf("Error while creating devops generator")
			}
			d := dq.(*Devops)

			q := d.GenerateEmptyQuery()
			d.GroupByTimeAndPrimaryTag(q, c.numMetrics)

			verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedTable, c.expectedSQLQuery)
		})
	}
}

func TestMaxAllCPU(t *testing.T) {
	expectedHumanLabel := "Timestream max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h"
	expectedHumanDesc := "Timestream max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h: 1970-01-01T00:16:22Z"
	expectedTable := "cpu"
	expectedSQLQuery := `SELECT bin(time, 3600s) AS hour,
			max(case when measure_name = 'usage_user' THEN measure_value::double ELSE NULL END) as max_usage_user,
			max(case when measure_name = 'usage_system' THEN measure_value::double ELSE NULL END) as max_usage_system,
			max(case when measure_name = 'usage_idle' THEN measure_value::double ELSE NULL END) as max_usage_idle,
			max(case when measure_name = 'usage_nice' THEN measure_value::double ELSE NULL END) as max_usage_nice,
			max(case when measure_name = 'usage_iowait' THEN measure_value::double ELSE NULL END) as max_usage_iowait,
			max(case when measure_name = 'usage_irq' THEN measure_value::double ELSE NULL END) as max_usage_irq,
			max(case when measure_name = 'usage_softirq' THEN measure_value::double ELSE NULL END) as max_usage_softirq,
			max(case when measure_name = 'usage_steal' THEN measure_value::double ELSE NULL END) as max_usage_steal,
			max(case when measure_name = 'usage_guest' THEN measure_value::double ELSE NULL END) as max_usage_guest,
			max(case when measure_name = 'usage_guest_nice' THEN measure_value::double ELSE NULL END) as max_usage_guest_nice
		FROM "b"."cpu"
		WHERE (hostname = 'host_9') AND time >= '1970-01-01 00:16:22.646325 +0000' AND time < '1970-01-01 08:16:22.646325 +0000'
		GROUP BY 1 ORDER BY 1`
	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(devops.MaxAllDuration).Add(time.Hour)

	b := BaseGenerator{
		DBName: "b",
	}
	dq, err := b.NewDevops(s, e, 10)
	if err != nil {
		t.Fatalf("Error while creating devops generator")
	}
	d := dq.(*Devops)

	q := d.GenerateEmptyQuery()
	d.MaxAllCPU(q, 1)
	verifyQuery(t, q, expectedHumanLabel, expectedHumanDesc, expectedTable, expectedSQLQuery)
}

func TestLastPointPerHost(t *testing.T) {
	cases := []struct {
		desc               string
		expectedHumanLabel string
		expectedHumanDesc  string
		expectedTable      string
		expectedSQLQuery   string
	}{
		{
			desc:               "last recorded value per host",
			expectedHumanLabel: "Timestream last row per host",
			expectedHumanDesc:  "Timestream last row per host",
			expectedTable:      "cpu",
			expectedSQLQuery: `
	WITH latest_recorded_time AS (
		SELECT 
			hostname,
			measure_name,
			max(time) as latest_time
		FROM "b"."cpu"
		GROUP BY 1, 2
	)
	SELECT b.hostname, 
		b.measure_name, 
		b.measure_value::double, 
		b.time
	FROM latest_recorded_time a
	JOIN "b"."cpu" b
	ON a.hostname = b.hostname AND a.latest_time = b.time AND a.measure_name = b.measure_name
	ORDER BY hostname, measure_name`,
		},
	}

	rand.Seed(123) // Setting seed for testing purposes.

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			b := BaseGenerator{
				DBName: "b",
			}
			dq, err := b.NewDevops(time.Now(), time.Now(), 10)
			if err != nil {
				t.Fatalf("Error while creating devops generator")
			}
			d := dq.(*Devops)

			q := d.GenerateEmptyQuery()
			d.LastPointPerHost(q)
			verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedTable, c.expectedSQLQuery)
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
			expectedHumanLabel: "Timestream CPU over threshold, all hosts",
			expectedHumanDesc:  "Timestream CPU over threshold, all hosts: 1970-01-01T00:16:22Z",
			expectedHypertable: "cpu",
			expectedSQLQuery: `
		WITH usage_over_ninety AS (
			SELECT time, 
				hostname
			FROM "b"."cpu"
			WHERE measure_name = 'usage_user' AND measure_value::double > 90
				AND time >= '1970-01-01 00:16:22.646325 +0000' AND time < '1970-01-01 12:16:22.646325 +0000'
				
		)
		SELECT * 
		FROM "b"."cpu" a
		JOIN usage_over_ninety b ON a.hostname = b.hostname AND a.time = b.time`,
		},
		{
			desc:               "one host",
			nHosts:             1,
			expectedHumanLabel: "Timestream CPU over threshold, 1 host(s)",
			expectedHumanDesc:  "Timestream CPU over threshold, 1 host(s): 1970-01-01T00:47:30Z",
			expectedHypertable: "cpu",
			expectedSQLQuery: `
		WITH usage_over_ninety AS (
			SELECT time, 
				hostname
			FROM "b"."cpu"
			WHERE measure_name = 'usage_user' AND measure_value::double > 90
				AND time >= '1970-01-01 00:47:30.894865 +0000' AND time < '1970-01-01 12:47:30.894865 +0000'
				AND (hostname = 'host_9')
		)
		SELECT * 
		FROM "b"."cpu" a
		JOIN usage_over_ninety b ON a.hostname = b.hostname AND a.time = b.time`,
		},
		{
			desc:               "five hosts",
			nHosts:             5,
			expectedHumanLabel: "Timestream CPU over threshold, 5 host(s)",
			expectedHumanDesc:  "Timestream CPU over threshold, 5 host(s): 1970-01-01T00:08:59Z",
			expectedHypertable: "cpu",
			expectedSQLQuery: `
		WITH usage_over_ninety AS (
			SELECT time, 
				hostname
			FROM "b"."cpu"
			WHERE measure_name = 'usage_user' AND measure_value::double > 90
				AND time >= '1970-01-01 00:08:59.080812 +0000' AND time < '1970-01-01 12:08:59.080812 +0000'
				AND (hostname = 'host_5' OR hostname = 'host_9' OR hostname = 'host_1' OR hostname = 'host_7' OR hostname = 'host_2')
		)
		SELECT * 
		FROM "b"."cpu" a
		JOIN usage_over_ninety b ON a.hostname = b.hostname AND a.time = b.time`,
		},
	}

	rand.Seed(123) // Setting seed for testing purposes.
	s := time.Unix(0, 0)
	e := s.Add(devops.HighCPUDuration).Add(time.Hour)

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			b := BaseGenerator{DBName: "b"}
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

func verifyQuery(t *testing.T, q query.Query, humanLabel, humanDesc, table, sqlQuery string) {
	tsq, ok := q.(*query.Timestream)

	if !ok {
		t.Fatal("Filled query is not *query.TimescaleDB type")
	}

	if got := string(tsq.HumanLabel); got != humanLabel {
		t.Errorf("incorrect human label:\ngot\n%s\nwant\n%s", got, humanLabel)
	}

	if got := string(tsq.HumanDescription); got != humanDesc {
		t.Errorf("incorrect human description:\ngot\n%s\nwant\n%s", got, humanDesc)
	}

	if got := string(tsq.Table); got != table {
		t.Errorf("incorrect table:\ngot\n%s\nwant\n%s", got, table)
	}

	if got := string(tsq.SqlQuery); got != sqlQuery {
		t.Errorf("incorrect SQL query:\ndiff\n%s\ngot\n%s\nwant\n%s", diff.CharacterDiff(got, sqlQuery), got, sqlQuery)
	}
}
