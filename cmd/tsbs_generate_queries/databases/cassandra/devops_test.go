package cassandra

import (
	"strings"
	"testing"
	"time"

	"github.com/timescale/tsbs/query"
)

func TestDevopsGetHostWhereWithHostnames(t *testing.T) {
	cases := []struct {
		desc      string
		hostnames []string
		want      []string
	}{
		{
			desc:      "single host",
			hostnames: []string{"foo1"},
			want:      []string{"hostname=foo1"},
		},
		{
			desc:      "multi host",
			hostnames: []string{"foo1", "foo2"},
			want:      []string{"hostname=foo1", "hostname=foo2"},
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		d, err := b.NewDevops(time.Now(), time.Now(), 10)

		if err != nil {
			t.Fatalf("Error while creating devops generator")
		}

		got := d.(*Devops).getHostWhereWithHostnames(c.hostnames)
		if len(got) != len(c.want) {
			t.Errorf("%s: incorrect output len: got %d want %d", c.desc, len(got), len(c.want))
		}
		for i := range c.want {
			if got[i] != c.want[i] {
				t.Errorf("%s: incorrect output at %d: got %s want %s", c.desc, i, got[i], c.want[i])
			}
		}
	}
}

func TestDevopsFillInQuery(t *testing.T) {
	humanLabel := "this is my label"
	humanDesc := "and now my description"
	aggType := "sum"
	fields := []string{"foo1, foo2"}
	tags := [][]string{{"foo=val", "bar=val2"}}
	now := time.Now()

	b := BaseGenerator{}
	dq, err := b.NewDevops(now, now.Add(time.Nanosecond), 10)
	if err != nil {
		t.Fatalf("Error while creating devops generator")
	}
	d := dq.(*Devops)

	qi := d.GenerateEmptyQuery()
	q := qi.(*query.Cassandra)
	if len(q.HumanLabel) != 0 {
		t.Errorf("empty query has non-zero length human label")
	}
	if len(q.HumanDescription) != 0 {
		t.Errorf("empty query has non-zero length human desc")
	}
	if len(q.AggregationType) != 0 {
		t.Errorf("empty query has non-zero length agg type")
	}
	if len(q.MeasurementName) != 0 {
		t.Errorf("empty query has non-zero length measurement name")
	}
	if len(q.FieldName) != 0 {
		t.Errorf("empty query has non-zero length field name")
	}
	if len(q.TagSets) != 0 {
		t.Errorf("empty query has non-zero length tagset")
	}

	d.fillInQuery(q, humanLabel, humanDesc, aggType, fields, d.Interval, tags)

	if got := string(q.HumanLabel); got != humanLabel {
		t.Errorf("filled query mislabeled: got %s want %s", got, humanLabel)
	}
	if got := string(q.HumanDescription); got != humanDesc {
		t.Errorf("filled query mis-described: got %s want %s", got, humanDesc)
	}
	if got := string(q.AggregationType); got != aggType {
		t.Errorf("filled query has wrong agg type: got %s want %s", got, aggType)
	}
	if got := string(q.MeasurementName); got != "cpu" {
		t.Errorf("filled query has wrong measurement name: got %s want %s", got, "cpu")
	}
	if got := string(q.FieldName); got != strings.Join(fields, ",") {
		t.Errorf("filled query has wrong fields: got %s want %s", got, strings.Join(fields, ","))
	}
	if got := q.TimeStart.UnixNano(); got != now.UnixNano() {
		t.Errorf("filled query start time wrong: got %d want %d", got, now.UnixNano())
	}
	if got := q.TimeEnd.UnixNano(); got != now.UnixNano()+1 {
		t.Errorf("filled query end time wrong: got %d want %d", got, now.UnixNano()+1)
	}
	if got := len(q.TagSets); got != len(tags) {
		t.Errorf("filled query has wrong tagset length: got %d want %d", got, len(tags))
	}
	for i := range tags {
		if got := len(q.TagSets[i]); got != len(tags[i]) {
			t.Errorf("tag set len %d not equal: got %d want %d", i, got, len(tags[i]))
		}
		for j := range tags[i] {
			if got := q.TagSets[i][j]; got != tags[i][j] {
				t.Errorf("tag set at %d,%d incorrect: got %s want %s", i, j, got, tags[i][j])
			}
		}
	}
}
