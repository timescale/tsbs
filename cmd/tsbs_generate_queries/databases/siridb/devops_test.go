package siridb

import (
	"testing"
	"time"

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
