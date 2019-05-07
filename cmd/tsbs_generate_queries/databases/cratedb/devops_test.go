package cratedb

import (
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/query"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDevopsGetSelectAggClauses(t *testing.T) {
	d := NewDevops(time.Now(), time.Now(), 10)
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
			desc: "multiple metric - max",
			agg:  "max", metrics: []string{"foo", "bar"},

			want: "max(foo) AS max_foo, max(bar) AS max_bar",
		},
		{
			desc:    "multiple metric - avg",
			agg:     "avg",
			metrics: []string{"foo", "bar"},
			want:    "avg(foo) AS avg_foo, avg(bar) AS avg_bar",
		},
	}

	for _, c := range cases {
		got := strings.Join(d.getSelectAggClauses(c.agg, c.metrics), ", ");
		if got != c.want {
			t.Errorf("%s: incorrect output: got %s want %s", c.desc, got, c.want)
		}
	}
}

func TestDevopsMaxAllCPUQuery(t *testing.T) {
	// return the same set of random hosts deterministic
	rand.Seed(100)

	start := time.Date(2006, 1, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2006, 1, 1, 20, 0, 0, 0, time.UTC)
	d := NewDevops(start, end, 10)

	want := &query.CrateDB{
		Table: []byte("cpu"),
		SqlQuery: []byte(fmt.Sprintf(`
		SELECT
			date_trunc('hour', ts) AS hour,
			%s
		FROM cpu
		WHERE tags['hostname'] IN ('host_8', 'host_0')
		  AND ts >= 1136112913823
		  AND ts < 1136141713823
		GROUP BY hour
		ORDER BY hour`,
			strings.Join(d.getSelectAggClauses(
				"max", devops.GetAllCPUMetrics()), ", ")),
		)}

	got := &query.CrateDB{}
	d.MaxAllCPU(got, 2)

	if !reflect.DeepEqual(want.SqlQuery, got.SqlQuery) {
		t.Errorf("incorrect sql query:\ngot: %s\n want:\n %s",
			got.SqlQuery, want.SqlQuery)
	}
	if !reflect.DeepEqual(want.Table, got.Table) {
		t.Errorf("incorrect table:\ngot: %s\n want:\n %s",
			got.SqlQuery, want.SqlQuery)
	}
}

func TestDevopsGroupByTimeAndPrimaryTagQuery(t *testing.T) {
	// return the same set of random hosts deterministic
	rand.Seed(100)

	start := time.Date(2006, 1, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2006, 1, 10, 20, 0, 0, 0, time.UTC)
	d := NewDevops(start, end, 10)

	want := &query.CrateDB{
		Table: []byte("cpu"),
		SqlQuery: []byte(`
		SELECT
			date_trunc('hour', ts) AS hour,
			mean(usage_user) AS mean_usage_user, mean(usage_system) AS mean_usage_system
		FROM cpu
		WHERE ts >= 1136357713823
		  AND ts < 1136400913823
		GROUP BY hour, tags['hostname']
		ORDER BY hour`),
	}

	got := &query.CrateDB{}
	d.GroupByTimeAndPrimaryTag(got, 2)

	if !reflect.DeepEqual(want.SqlQuery, got.SqlQuery) {
		t.Errorf("incorrect sql query:\ngot: %s\n want:\n %s",
			got.SqlQuery, want.SqlQuery)
	}
	if !reflect.DeepEqual(want.Table, got.Table) {
		t.Errorf("incorrect table:\ngot: %s\n want:\n %s",
			got.SqlQuery, want.SqlQuery)
	}
}

func TestDevopsGroupByOrderByLimitQuery(t *testing.T) {
	start := time.Date(2006, 1, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2006, 1, 10, 20, 0, 0, 0, time.UTC)
	d := NewDevops(start, end, 10)

	want := &query.CrateDB{
		Table: []byte("cpu"),
		SqlQuery: []byte(`
		SELECT
			date_trunc('minute', ts) as minute,
			max(usage_user)
		FROM cpu
		WHERE ts < 1136416682472
		GROUP BY minute
		ORDER BY minute DESC
		LIMIT 5`),
	}

	got := &query.CrateDB{}
	d.GroupByOrderByLimit(got)

	if !reflect.DeepEqual(want.SqlQuery, got.SqlQuery) {
		t.Errorf("incorrect sql query:\ngot: %s\n want:\n %s",
			got.SqlQuery, want.SqlQuery)
	}
	if !reflect.DeepEqual(want.Table, got.Table) {
		t.Errorf("incorrect table:\ngot: %s\n want:\n %s",
			got.SqlQuery, want.SqlQuery)
	}
}

func TestDevopsLastPointPerHostQuery(t *testing.T) {
	start := time.Date(2006, 1, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2006, 1, 10, 20, 0, 0, 0, time.UTC)
	d := NewDevops(start, end, 10)

	want := &query.CrateDB{
		Table: []byte("cpu"),
		SqlQuery: []byte(`
		SELECT *
		FROM
		  (
			SELECT tags['hostname'] AS host, max(ts) AS max_ts
			FROM cpu
			GROUP BY tags['hostname']
		  ) t, cpu c
		WHERE t.max_ts = c.ts
		  AND t.host = c.tags['hostname']`),
	}

	got := &query.CrateDB{}
	d.LastPointPerHost(got)

	if !reflect.DeepEqual(want.SqlQuery, got.SqlQuery) {
		t.Errorf("incorrect sql query:\ngot: %s\n want:\n %s",
			got.SqlQuery, want.SqlQuery)
	}
	if !reflect.DeepEqual(want.Table, got.Table) {
		t.Errorf("incorrect table:\ngot: %s\n want:\n %s",
			got.SqlQuery, want.SqlQuery)
	}
}

func TestDevopsHighCPUForHostsQuery(t *testing.T) {
	// return the same set of random hosts deterministic
	rand.Seed(100)
	start := time.Date(2006, 1, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2006, 1, 10, 20, 0, 0, 0, time.UTC)
	d := NewDevops(start, end, 10)

	want := &query.CrateDB{
		Table: []byte("cpu"),
		SqlQuery: []byte(`
		SELECT *
		FROM cpu
		WHERE usage_user > 90.0
		  AND ts >= 1136357713823
		  AND ts < 1136400913823
		  AND tags['hostname'] IN ('host_8', 'host_0')`),
	}

	got := &query.CrateDB{}
	d.HighCPUForHosts(got, 2)

	if !reflect.DeepEqual(want.SqlQuery, got.SqlQuery) {
		t.Errorf("incorrect sql query:\ngot: %s\n want:\n %s",
			got.SqlQuery, want.SqlQuery)
	}
	if !reflect.DeepEqual(want.Table, got.Table) {
		t.Errorf("incorrect table:\ngot: %s\n want:\n %s",
			got.SqlQuery, want.SqlQuery)
	}
}

func TestDevopsGroupByTimeQuery(t *testing.T) {
	// return the same set of random hosts deterministic
	rand.Seed(101)

	start := time.Date(2006, 1, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2006, 1, 1, 20, 0, 0, 0, time.UTC)
	d := NewDevops(start, end, 10)

	want := &query.CrateDB{
		Table: []byte("cpu"),
		SqlQuery: []byte(`
		SELECT
			date_trunc('minute', ts) as minute,
			max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system
		FROM cpu
		WHERE tags['hostname'] IN ('host_2', 'host_5')
		  AND ts >= 1136115302666
		  AND ts < 1136144102666
		GROUP BY minute
		ORDER BY minute ASC`),
	}

	got := &query.CrateDB{}
	d.GroupByTime(got, 2, 2, devops.MaxAllDuration)

	if !reflect.DeepEqual(want.SqlQuery, got.SqlQuery) {
		t.Errorf("incorrect sql query:\ngot: %s\n want:\n %s",
			got.SqlQuery, want.SqlQuery)
	}
	if !reflect.DeepEqual(want.Table, got.Table) {
		t.Errorf("incorrect table:\ngot: %s\n want:\n %s",
			got.SqlQuery, want.SqlQuery)
	}
}
