package query

import "testing"

func TestNewHTTP(t *testing.T) {
	check := func(q *HTTP) {
		testValidNewQuery(t, q)
		if got := len(q.Method); got != 0 {
			t.Errorf("new query has non-0 method: got %d", got)
		}
		if got := len(q.Path); got != 0 {
			t.Errorf("new query has non-0 path: got %d", got)
		}
		if got := len(q.Body); got != 0 {
			t.Errorf("new query has non-0 body: got %d", got)
		}
		if got := q.StartTimestamp; got != 0 {
			t.Errorf("new query has non-0 start time: got %d", got)
		}
		if got := q.EndTimestamp; got != 0 {
			t.Errorf("new query has non-0 end time: got %d", got)
		}
	}
	q := NewHTTP()
	check(q)
	q.HumanLabel = []byte("foo")
	q.HumanDescription = []byte("bar")
	q.Method = []byte("POST")
	q.Path = []byte("/home")
	q.Body = []byte("bazbazbaz")
	q.StartTimestamp = 1
	q.EndTimestamp = 5
	q.SetID(1)
	if got := string(q.HumanLabelName()); got != "foo" {
		t.Errorf("incorrect label name: got %s", got)
	}
	if got := string(q.HumanDescriptionName()); got != "bar" {
		t.Errorf("incorrect desc: got %s", got)
	}
	q.Release()

	// Since we use a pool, check that the next one is reset
	q = NewHTTP()
	check(q)
	q.Release()
}

func TestHTTPSetAndGetID(t *testing.T) {
	for i := 0; i < 2; i++ {
		q := NewHTTP()
		testSetAndGetID(t, q)
		q.Release()
	}
}
