package zigtsdb

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/pkg/query"
)

func acquireGenerator(t *testing.T, interval time.Duration, scale int) *Devops {
	t.Helper()
	g := &BaseGenerator{}
	start := time.Unix(0, 0).UTC()
	end := start.Add(interval)
	qg, err := g.NewDevops(start, end, scale)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	return qg.(*Devops)
}

var hostClauseRe = regexp.MustCompile(`\(hostname = 'host_\d+'(?: OR hostname = 'host_\d+')*\)`)

// normaliseHosts replaces the random host clause with <HOSTS:n> where n is
// the number of distinct hosts, so expectations are independent of the PRNG.
func normaliseHosts(t *testing.T, stmt string, scale int) string {
	t.Helper()
	return hostClauseRe.ReplaceAllStringFunc(stmt, func(clause string) string {
		hosts := regexp.MustCompile(`host_(\d+)`).FindAllStringSubmatch(clause, -1)
		seen := map[string]bool{}
		for _, h := range hosts {
			if seen[h[1]] {
				t.Fatalf("duplicate host in clause %s", clause)
			}
			seen[h[1]] = true
			var id int
			if _, err := fmtSscan(h[1], &id); err != nil || id < 0 || id >= scale {
				t.Fatalf("host id %s out of range [0,%d)", h[1], scale)
			}
		}
		return "<HOSTS:" + itoa(len(hosts)) + ">"
	})
}

// statements returns every ZQL statement packed into the query: the first
// one decoded from Path, the rest raw from Body.
func statements(t *testing.T, q *query.HTTP) []string {
	t.Helper()
	path := string(q.Path)
	if !strings.HasPrefix(path, "/query?zql=") {
		t.Fatalf("unexpected path %q", path)
	}
	first, err := url.QueryUnescape(strings.TrimPrefix(path, "/query?zql="))
	if err != nil {
		t.Fatalf("path is not url-encoded: %s", err)
	}
	out := []string{first}
	if len(q.Body) > 0 {
		out = append(out, strings.Split(string(q.Body), "\n")...)
	}
	return out
}

func TestDevopsQueries(t *testing.T) {
	const ts = "time >= 0 AND time < 3600000000000"
	cases := map[string]struct {
		fn        func(g *Devops, q *query.HTTP)
		want      []string
		expToFail bool
	}{
		"GroupByTime_1_1": {
			fn: func(g *Devops, q *query.HTTP) { g.GroupByTime(q, 1, 1, time.Hour) },
			want: []string{
				"SELECT max(value) FROM cpu_usage_user WHERE <HOSTS:1> AND " + ts + " GROUP BY time_bucket(time, 1 m)",
			},
		},
		"GroupByTime_5_1": {
			fn: func(g *Devops, q *query.HTTP) { g.GroupByTime(q, 5, 1, time.Hour) },
			want: []string{
				"SELECT max(value) FROM cpu_usage_user WHERE <HOSTS:5> AND " + ts + " GROUP BY time_bucket(time, 1 m)",
			},
		},
		"GroupByTime_1_2_two_statements": {
			fn: func(g *Devops, q *query.HTTP) { g.GroupByTime(q, 1, 2, time.Hour) },
			want: []string{
				"SELECT max(value) FROM cpu_usage_user WHERE <HOSTS:1> AND " + ts + " GROUP BY time_bucket(time, 1 m)",
				"SELECT max(value) FROM cpu_usage_system WHERE <HOSTS:1> AND " + ts + " GROUP BY time_bucket(time, 1 m)",
			},
		},
		"GroupByTimeAndPrimaryTag_2": {
			fn: func(g *Devops, q *query.HTTP) { g.GroupByTimeAndPrimaryTag(q, 2) },
			want: []string{
				"SELECT avg(value) FROM cpu_usage_user WHERE time >= 0 AND time < 43200000000000 GROUP BY hostname, time_bucket(time, 1 h)",
				"SELECT avg(value) FROM cpu_usage_system WHERE time >= 0 AND time < 43200000000000 GROUP BY hostname, time_bucket(time, 1 h)",
			},
		},
		"MaxAllCPU_1": {
			fn: func(g *Devops, q *query.HTTP) { g.MaxAllCPU(q, 1, devops.MaxAllDuration) },
			want: func() []string {
				var out []string
				for _, m := range devops.GetAllCPUMetrics() {
					out = append(out, "SELECT max(value) FROM cpu_"+m+" WHERE <HOSTS:1> AND time >= 0 AND time < 28800000000000 GROUP BY time_bucket(time, 1 h)")
				}
				return out
			}(),
		},
		"GroupByOrderByLimit": {
			fn: func(g *Devops, q *query.HTTP) { g.GroupByOrderByLimit(q) },
			want: []string{
				"SELECT max(value) FROM cpu_usage_user WHERE time < 3600000000000 GROUP BY time_bucket(time, 1 m) ORDER BY time DESC LIMIT 5",
			},
		},
		"LastPointPerHost": {
			fn: func(g *Devops, q *query.HTTP) { g.LastPointPerHost(q) },
			want: []string{
				"SELECT last(value) FROM cpu_usage_user GROUP BY hostname",
			},
		},
		"HighCPUForHosts_1": {
			fn: func(g *Devops, q *query.HTTP) { g.HighCPUForHosts(q, 1) },
			want: []string{
				"SELECT * FROM cpu_usage_user WHERE value > 90.0 AND <HOSTS:1> AND time >= 0 AND time < 43200000000000 LIMIT 1000000",
			},
		},
		"HighCPUForHosts_all": {
			fn: func(g *Devops, q *query.HTTP) { g.HighCPUForHosts(q, 0) },
			want: []string{
				"SELECT * FROM cpu_usage_user WHERE value > 90.0 AND time >= 0 AND time < 43200000000000 LIMIT 1000000",
			},
		},
		"GroupByTime_negative_metrics": {
			fn:        func(g *Devops, q *query.HTTP) { g.GroupByTime(q, 1, -1, time.Hour) },
			expToFail: true,
		},
		"GroupByTime_negative_hosts": {
			fn:        func(g *Devops, q *query.HTTP) { g.GroupByTime(q, -1, 1, time.Hour) },
			expToFail: true,
		},
	}
	// interval = window + 1ns so MustRandWindow is deterministic (start = 0)
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			window := time.Hour
			switch name {
			case "GroupByTimeAndPrimaryTag_2", "HighCPUForHosts_1", "HighCPUForHosts_all":
				window = 12 * time.Hour
			case "MaxAllCPU_1":
				window = devops.MaxAllDuration
			}
			g := acquireGenerator(t, window+time.Nanosecond, 10)
			q := g.GenerateEmptyQuery().(*query.HTTP)
			if tc.expToFail {
				defer func() {
					if recover() == nil {
						t.Errorf("expected panic")
					}
				}()
				tc.fn(g, q)
				return
			}
			tc.fn(g, q)
			got := statements(t, q)
			for i := range got {
				got[i] = normaliseHosts(t, got[i], 10)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("expected %d statements, got %d: %q", len(tc.want), len(got), got)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("statement %d mismatch\n got: %s\nwant: %s", i, got[i], tc.want[i])
				}
			}
			if string(q.Method) != http.MethodGet {
				t.Fatalf("expected GET, got %s", q.Method)
			}
			if strings.Contains(string(q.Path), "+") {
				t.Fatalf("path must percent-encode spaces, not use '+': %s", q.Path)
			}
			if len(q.HumanLabel) == 0 || len(q.HumanDescription) == 0 {
				t.Fatalf("label/description must be set")
			}
			if normaliseHosts(t, string(q.RawQuery), 10) != strings.Join(got, ";\n") {
				t.Fatalf("RawQuery must carry every statement")
			}
		})
	}
}

func TestStatementByteCapIsEnforced(t *testing.T) {
	g := acquireGenerator(t, time.Hour, 10)
	q := g.GenerateEmptyQuery().(*query.HTTP)
	defer func() {
		if recover() == nil {
			t.Errorf("expected panic for an oversized statement")
		}
	}()
	g.fillInQuery(q, "x", "x", nil, []string{strings.Repeat("a", maxStatementBytes+1)})
}

func TestLabelsUseDatabaseName(t *testing.T) {
	g := acquireGenerator(t, time.Hour, 10)
	q := g.GenerateEmptyQuery().(*query.HTTP)
	g.LastPointPerHost(q)
	if !strings.HasPrefix(string(q.HumanLabel), "ZigTSDB") {
		t.Fatalf("label should be prefixed with ZigTSDB, got %q", q.HumanLabel)
	}
}

func fmtSscan(s string, id *int) (int, error) { return fmt.Sscan(s, id) }
func itoa(i int) string                       { return strconv.Itoa(i) }
