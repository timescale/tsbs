package query

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
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
	q := NewMongo()
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

	// Since we use a pool, check that the next one is reset
	q = NewMongo()
	check(q)
	q.Release()
}

func TestMongoSetAndGetID(t *testing.T) {
	for i := 0; i < 2; i++ {
		q := NewMongo()
		testSetAndGetID(t, q)
		q.Release()
	}
}
