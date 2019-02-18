package query

import "testing"

func TestNewSiriDB(t *testing.T) {
	check := func(sq *SiriDB) {
		testValidNewQuery(t, sq)
		if got := len(sq.SqlQuery); got != 0 {
			t.Errorf("new query has non-0 sql query: got %d", got)
		}
	}
	sq := NewSiriDB()
	check(sq)
	sq.HumanLabel = []byte("foo")
	sq.HumanDescription = []byte("bar")
	sq.SqlQuery = []byte("SELECT * FROM *")
	sq.SetID(1)
	if got := string(sq.HumanLabelName()); got != "foo" {
		t.Errorf("incorrect label name: got %s", got)
	}
	if got := string(sq.HumanDescriptionName()); got != "bar" {
		t.Errorf("incorrect desc: got %s", got)
	}
	sq.Release()

	// Since we use a pool, check that the next one is reset
	sq = NewSiriDB()
	check(sq)
	sq.Release()
}

func TestSiriDBSetAndGetID(t *testing.T) {
	for i := 0; i < 2; i++ {
		q := NewSiriDB()
		testSetAndGetID(t, q)
		q.Release()
	}
}
