package query

import (
	"testing"

	"github.com/globalsign/mgo/bson"
)

func TestNewMongo(t *testing.T) {
	check := func(q *Mongo) {
		testValidNewQuery(t, q)
		if got := len(q.CollectionName); got != 0 {
			t.Errorf("new query has non-0 collection name: got %d", got)
		}
		if got := len(q.BsonDoc); got != 0 {
			t.Errorf("new query has non-0 bson doc: got %d", got)
		}
	}
	q := NewMongoQueryFn().(*Mongo)
	check(q)
	q.HumanLabel = []byte("foo")
	q.HumanDescription = []byte("bar")
	q.BsonDoc = append(q.BsonDoc, bson.M{})
	q.CollectionName = []byte("baz")
	q.SetID(1)
	if got := string(q.HumanLabelName()); got != "foo" {
		t.Errorf("incorrect label name: got %s", got)
	}
	if got := string(q.HumanDescriptionName()); got != "bar" {
		t.Errorf("incorrect desc: got %s", got)
	}
	q.Release()
}

func TestMongoSetAndGetID(t *testing.T) {
	for i := 0; i < 2; i++ {
		q := NewMongoQueryFn().(*Mongo)
		testSetAndGetID(t, q)
		q.Release()
	}
}
