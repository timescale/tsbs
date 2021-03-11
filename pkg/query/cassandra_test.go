package query

import (
	"testing"
	"time"
)

func TestNewCassandra(t *testing.T) {
	check := func(q *Cassandra) {
		testValidNewQuery(t, q)
		if got := len(q.MeasurementName); got != 0 {
			t.Errorf("new query has non-0 measurement name: got %d", got)
		}
		if got := len(q.FieldName); got != 0 {
			t.Errorf("new query has non-0 field name: got %d", got)
		}
		if got := len(q.AggregationType); got != 0 {
			t.Errorf("new query has non-0 agg type: got %d", got)
		}
		if got := len(q.ForEveryN); got != 0 {
			t.Errorf("new query has non-0 for every N: got %d", got)
		}
		if got := len(q.WhereClause); got != 0 {
			t.Errorf("new query has non-0 where clause: got %d", got)
		}
		if got := len(q.OrderBy); got != 0 {
			t.Errorf("new query has non-0 order by: got %d", got)
		}
		if got := len(q.TagSets); got != 0 {
			t.Errorf("new query has non-0 tag sets len: got %d", got)
		}
		if got := q.Limit; got != 0 {
			t.Errorf("new query has non-0 limit: got %d", got)
		}
		if got := q.GroupByDuration; got != 0 {
			t.Errorf("new query has non-0 group by duration: got %v", q.GroupByDuration)
		}
	}
	q := NewCassandra()
	check(q)
	q.HumanLabel = []byte("foo")
	q.HumanDescription = []byte("bar")
	q.MeasurementName = []byte("baz")
	q.FieldName = []byte("quaz")
	q.AggregationType = []byte("client")
	q.GroupByDuration = time.Second
	q.ForEveryN = []byte("5m")
	q.WhereClause = []byte("TRUE > FALSE")
	q.OrderBy = []byte("quaz ASC")
	q.Limit = 5
	q.TagSets = append(q.TagSets, []string{"foo"})
	q.SetID(1)
	if got := string(q.HumanLabelName()); got != "foo" {
		t.Errorf("incorrect label name: got %s", got)
	}
	if got := string(q.HumanDescriptionName()); got != "bar" {
		t.Errorf("incorrect desc: got %s", got)
	}
	q.Release()

	// Since we use a pool, check that the next one is reset
	q = NewCassandra()
	check(q)
	q.Release()
}

func TestCassandraSetAndGetID(t *testing.T) {
	for i := 0; i < 2; i++ {
		q := NewCassandra()
		testSetAndGetID(t, q)
		q.Release()
	}
}
