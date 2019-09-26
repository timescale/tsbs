package query

import "testing"

func TestNewTimescaleDB(t *testing.T) {
	check := func(tq *TimescaleDB) {
		testValidNewQuery(t, tq)
		if got := len(tq.Hypertable); got != 0 {
			t.Errorf("new query has non-0 hypertable label: got %d", got)
		}
		if got := len(tq.SqlQuery); got != 0 {
			t.Errorf("new query has non-0 sql query: got %d", got)
		}
	}
	tq := NewTimescaleDBQueryFn().(*TimescaleDB)
	check(tq)
	tq.HumanLabel = []byte("foo")
	tq.HumanDescription = []byte("bar")
	tq.Hypertable = []byte("table")
	tq.SqlQuery = []byte("SELECT * FROM *")
	tq.SetID(1)
	if got := string(tq.HumanLabelName()); got != "foo" {
		t.Errorf("incorrect label name: got %s", got)
	}
	if got := string(tq.HumanDescriptionName()); got != "bar" {
		t.Errorf("incorrect desc: got %s", got)
	}
	tq.Release()
}

func TestTimescaleDBSetAndGetID(t *testing.T) {
	for i := 0; i < 2; i++ {
		q := NewTimescaleDBQueryFn().(*TimescaleDB)
		testSetAndGetID(t, q)
		q.Release()
	}
}
