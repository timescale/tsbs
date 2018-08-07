package influx

import (
	"net/url"
	"strings"
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
			want:      "(hostname = 'foo1')",
		},
		{
			desc:      "multi host (2)",
			hostnames: []string{"foo1", "foo2"},
			want:      "(hostname = 'foo1' or hostname = 'foo2')",
		},
		{
			desc:      "multi host (3)",
			hostnames: []string{"foo1", "foo2", "foo3"},
			want:      "(hostname = 'foo1' or hostname = 'foo2' or hostname = 'foo3')",
		},
	}

	for _, c := range cases {
		d := NewDevops(time.Now(), time.Now(), 10)

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
			want:    "max(foo)",
		},
		{
			desc:    "multiple metric - max",
			agg:     "max",
			metrics: []string{"foo", "bar"},
			want:    "max(foo),max(bar)",
		},
		{
			desc:    "multiple metric - avg",
			agg:     "avg",
			metrics: []string{"foo", "bar"},
			want:    "avg(foo),avg(bar)",
		},
	}

	for _, c := range cases {
		d := NewDevops(time.Now(), time.Now(), 10)

		if got := strings.Join(d.getSelectClausesAggMetrics(c.agg, c.metrics), ","); got != c.want {
			t.Errorf("%s: incorrect output: got %s want %s", c.desc, got, c.want)
		}
	}
}

func TestDevopsFillInQuery(t *testing.T) {
	humanLabel := "this is my label"
	humanDesc := "and now my description"
	influxql := "SELECT * from cpu where usage_user > 90.0 and time < '2017-01-01'"
	d := NewDevops(time.Now(), time.Now(), 10)
	qi := d.GenerateEmptyQuery()
	q := qi.(*query.HTTP)
	if len(q.HumanLabel) != 0 {
		t.Errorf("empty query has non-zero length human label")
	}
	if len(q.HumanDescription) != 0 {
		t.Errorf("empty query has non-zero length human desc")
	}
	if len(q.Method) != 0 {
		t.Errorf("empty query has non-zero length method")
	}
	if len(q.Path) != 0 {
		t.Errorf("empty query has non-zero length path")
	}

	d.fillInQuery(q, humanLabel, humanDesc, influxql)
	if got := string(q.HumanLabel); got != humanLabel {
		t.Errorf("filled query mislabeled: got %s want %s", got, humanLabel)
	}
	if got := string(q.HumanDescription); got != humanDesc {
		t.Errorf("filled query mis-described: got %s want %s", got, humanDesc)
	}
	if got := string(q.Method); got != "GET" {
		t.Errorf("filled query has wrong method: got %s want GET", got)
	}
	v := url.Values{}
	v.Set("q", influxql)
	encoded := v.Encode()
	if got := string(q.Path); got != "/query?"+encoded {
		t.Errorf("filled query has wrong path: got %s want /query?%s", got, encoded)
	}
}
