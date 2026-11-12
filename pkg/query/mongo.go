package query

import (
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
)

// Mongo encodes a Mongo request. This will be serialized for use
// by the tsbs_run_queries_mongo program.
type Mongo struct {
	HumanLabel       []byte
	HumanDescription []byte
	CollectionName   []byte
	BsonDoc          []bson.M
	id               uint64
}

// MongoPool is a sync.Pool of Mongo Query types
var MongoPool = sync.Pool{
	New: func() interface{} {
		return &Mongo{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},
			CollectionName:   []byte{},
			BsonDoc:          []bson.M{},
		}
	},
}

// NewMongo returns a new Mongo Query instance
func NewMongo() *Mongo {
	return MongoPool.Get().(*Mongo)
}

// GetID returns the ID of this Query
func (q *Mongo) GetID() uint64 {
	return q.id
}

// SetID sets the ID for this Query
func (q *Mongo) SetID(id uint64) {
	q.id = id
}

// String produces a debug-ready description of a Query.
func (q *Mongo) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s", q.HumanLabel, q.HumanDescription)
}

// HumanLabelName returns the human readable name of this Query
func (q *Mongo) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (q *Mongo) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *Mongo) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0
	q.CollectionName = q.CollectionName[:0]
	q.BsonDoc = nil

	MongoPool.Put(q)
}
