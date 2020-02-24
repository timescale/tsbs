package query

import "testing"

func testValidNewQuery(t *testing.T, q Query) {
	if got := len(q.HumanLabelName()); got != 0 {
		t.Errorf("new query has non-0 human label: got %d", got)
	}
	if got := len(q.HumanDescriptionName()); got != 0 {
		t.Errorf("new query has non-0 human desc: got %d", got)
	}
	if got := q.GetID(); got != 0 {
		t.Errorf("new query has non-0 id: got %d", got)
	}
}

func testSetAndGetID(t *testing.T, q Query) {
	if got := q.GetID(); got != 0 {
		t.Errorf("new query does not have 0 id: got %d", got)
	}
	q.SetID(100)
	if got := q.GetID(); got != 100 {
		t.Errorf("GetID returned incorrect id: got %d", got)
	}
}
