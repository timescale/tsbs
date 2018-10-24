package clickhouse

import (
	"strings"
	"testing"
	"time"
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
