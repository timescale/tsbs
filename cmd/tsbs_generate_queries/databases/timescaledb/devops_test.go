package timescaledb

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
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
		d := NewDevops(time.Now(), time.Now(), 10)
		d.UseJSON = c.useJSON
		d.UseTags = c.useTags

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
		d := NewDevops(time.Now(), time.Now(), 10)

		if got := d.getHostWhereString(c.nHosts); got != c.want {
			t.Errorf("incorrect output for %d hosts: got %s want %s", c.nHosts, got, c.want)
		}
	}
}

func TestDevopsGetTimeBucket(t *testing.T) {
	d := NewDevops(time.Now(), time.Now(), 10)
	d.UseTimeBucket = false

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
		d := NewDevops(time.Now(), time.Now(), 10)

		if got := strings.Join(d.getSelectClausesAggMetrics(c.agg, c.metrics), ","); got != c.want {
			t.Errorf("%s: incorrect output: got %s want %s", c.desc, got, c.want)
		}
	}
}
