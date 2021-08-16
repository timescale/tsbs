package victoriametrics

import (
	"math/rand"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/pkg/query"
)

func Test_what(t *testing.T) {
	testCases := map[string]struct {
		fn        func(g *Devops, q *query.HTTP)
		expQuery  string
		expStep   string
		expToFail bool
	}{
		"GroupByTime_1_1": {
			fn: func(g *Devops, q *query.HTTP) {
				g.GroupByTime(q, 1, 1, time.Hour)
			},
			expQuery: "max(max_over_time(cpu_usage_user{hostname='host_5'}[1m])) by (__name__)",
			expStep:  "60",
		},
		"GroupByTime_5_1": {
			fn: func(g *Devops, q *query.HTTP) {
				g.GroupByTime(q, 5, 1, time.Hour)
			},
			expQuery: "max(max_over_time(cpu_usage_user{hostname=~'host_5|host_9|host_3|host_1|host_7'}[1m])) by (__name__)",
			expStep:  "60",
		},
		"GroupByTime_5_5": {
			fn: func(g *Devops, q *query.HTTP) {
				g.GroupByTime(q, 5, 5, time.Hour)
			},
			expQuery: "max(max_over_time({__name__=~'cpu_(usage_user|usage_system|usage_idle|usage_nice|usage_iowait)', hostname=~'host_5|host_9|host_3|host_1|host_7'}[1m])) by (__name__)",
			expStep:  "60",
		},
		"GroupByTimeAndPrimaryTag": {
			fn: func(g *Devops, q *query.HTTP) {
				g.GroupByTimeAndPrimaryTag(q, 5)
			},
			expQuery: "avg(avg_over_time({__name__=~'cpu_(usage_user|usage_system|usage_idle|usage_nice|usage_iowait)'}[1h])) by (__name__, hostname)",
			expStep:  "3600",
		},
		"MaxAllCPU": {
			fn: func(g *Devops, q *query.HTTP) {
				g.MaxAllCPU(q, 5, devops.MaxAllDuration)
			},
			expQuery: "max(max_over_time({__name__=~'cpu_(usage_user|usage_system|usage_idle|usage_nice|usage_iowait|usage_irq|usage_softirq|usage_steal|usage_guest|usage_guest_nice)', hostname=~'host_5|host_9|host_3|host_1|host_7'}[1h])) by (__name__)",
			expStep:  "3600",
		},
		"GroupByOrderByLimit": {
			fn: func(g *Devops, q *query.HTTP) {
				g.GroupByOrderByLimit(q)
			},
			expToFail: true,
		},
		"LastPointPerHost": {
			fn: func(g *Devops, q *query.HTTP) {
				g.LastPointPerHost(q)
			},
			expToFail: true,
		},
		"HighCPUForHosts": {
			fn: func(g *Devops, q *query.HTTP) {
				g.HighCPUForHosts(q, 6)
			},
			expToFail: true,
		},
		"GroupByTime_negative_metrics": {
			fn: func(g *Devops, q *query.HTTP) {
				g.GroupByTime(q, 1, -1, time.Hour)
			},
			expToFail: true,
		},
		"GroupByTime_negative_hosts": {
			fn: func(g *Devops, q *query.HTTP) {
				g.GroupByTime(q, -1, 1, time.Hour)
			},
			expToFail: true,
		},
	}
	g := acquireGenerator(t, time.Hour*24, 10)
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			rand.Seed(123) // Setting seed for testing purposes.
			q := g.GenerateEmptyQuery().(*query.HTTP)
			if tc.expToFail {
				func() {
					defer func() {
						if recover() == nil {
							t.Errorf("expected to panice")
						}
					}()
					tc.fn(g, q)
				}()
				return
			}

			tc.fn(g, q)
			vals, err := url.ParseQuery(string(q.Path))
			if err != nil {
				t.Fatalf("unexpected err while parsing query: %s", err)
			}
			checkEqual(t, "query", tc.expQuery, vals.Get("query"))
			checkEqual(t, "step", tc.expStep, vals.Get("step"))
			checkEqual(t, "method", http.MethodGet, string(q.Method))
		})
	}
}

func checkEqual(t *testing.T, name, a, b string) {
	if a != b {
		t.Fatalf("values for %q are not equal \na: %q \nb: %q", name, a, b)
	}
}

func acquireGenerator(t *testing.T, interval time.Duration, scale int) *Devops {
	b := &BaseGenerator{}
	s := time.Unix(0, 0)
	e := s.Add(interval)
	g, err := b.NewDevops(s, e, scale)
	if err != nil {
		t.Fatalf("Error while creating devops generator")
	}
	return g.(*Devops)
}
