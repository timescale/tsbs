package main

import (
	"fmt"
	"sync"
	"time"
)

type Query interface {
	Release()
	HumanLabelName() []byte
	HumanDescriptionName() []byte
	fmt.Stringer
}

var HTTPQueryPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &HTTPQuery{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},
			Method:           []byte{},
			Path:             []byte{},
			Body:             []byte{},
			StartTimestamp:   0,
			EndTimestamp:     0,
		}
	},
}

// HTTPQuery encodes an HTTP request. This will typically by serialized for use
// by the query_benchmarker program.
type HTTPQuery struct {
	HumanLabel       []byte
	HumanDescription []byte
	Method           []byte
	Path             []byte
	Body             []byte
	StartTimestamp   int64
	EndTimestamp     int64
}

func NewHTTPQuery() *HTTPQuery {
	return HTTPQueryPool.Get().(*HTTPQuery)
}

// String produces a debug-ready description of a Query.
func (q *HTTPQuery) String() string {
	return fmt.Sprintf("HumanLabel: \"%s\", HumanDescription: \"%s\", Method: \"%s\", Path: \"%s\", Body: \"%s\"", q.HumanLabel, q.HumanDescription, q.Method, q.Path, q.Body)
}

func (q *HTTPQuery) HumanLabelName() []byte {
	return q.HumanLabel
}
func (q *HTTPQuery) HumanDescriptionName() []byte {
	return q.HumanDescription
}

func (q *HTTPQuery) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.Method = q.Method[:0]
	q.Path = q.Path[:0]
	q.Body = q.Body[:0]
	q.StartTimestamp = 0
	q.EndTimestamp = 0

	HTTPQueryPool.Put(q)
}

// CassandraQuery encodes a Cassandra request. This will be serialized for use
// by the query_benchmarker program.
type CassandraQuery struct {
	HumanLabel       []byte
	HumanDescription []byte

	MeasurementName []byte // e.g. "cpu"
	FieldName       []byte // e.g. "usage_user"
	AggregationType []byte // e.g. "avg" or "sum". used literally in the cassandra query.
	TimeStart       time.Time
	TimeEnd         time.Time
	GroupByDuration time.Duration
	TagSets         [][]string // semantically, each subgroup is OR'ed and they are all AND'ed together
}

var CassandraQueryPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &CassandraQuery{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},
			MeasurementName:  []byte{},
			FieldName:        []byte{},
			AggregationType:  []byte{},
			TagSets:          [][]string{},
		}
	},
}

func NewCassandraQuery() *CassandraQuery {
	return CassandraQueryPool.Get().(*CassandraQuery)
}

// String produces a debug-ready description of a Query.
func (q *CassandraQuery) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, MeasurementName: %s, AggregationType: %s, TimeStart: %s, TimeEnd: %s, GroupByDuration: %s, TagSets: %s", q.HumanLabel, q.HumanDescription, q.MeasurementName, q.AggregationType, q.TimeStart, q.TimeEnd, q.GroupByDuration, q.TagSets)
}

func (q *CassandraQuery) HumanLabelName() []byte {
	return q.HumanLabel
}
func (q *CassandraQuery) HumanDescriptionName() []byte {
	return q.HumanDescription
}

func (q *CassandraQuery) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]

	q.MeasurementName = q.MeasurementName[:0]
	q.FieldName = q.FieldName[:0]
	q.AggregationType = q.AggregationType[:0]
	q.GroupByDuration = 0
	q.TimeStart = time.Time{}
	q.TimeEnd = time.Time{}
	q.TagSets = q.TagSets[:0]

	CassandraQueryPool.Put(q)
}

// MongoQuery encodes a Mongo request. This will be serialized for use
// by the query_benchmarker program.
type MongoQuery struct {
	HumanLabel       []byte
	HumanDescription []byte
	DatabaseName     []byte
	CollectionName   []byte
	BsonDoc          []M

	// these are only for debugging. the data is encoded in BsonDoc.
	MeasurementName []byte // e.g. "cpu"
	FieldName       []byte // e.g. "usage_user"
	AggregationType []byte // e.g. "avg" or "sum"
	TimeStart       time.Time
	TimeEnd         time.Time
	GroupByDuration time.Duration
	TagSets         [][]string // semantically, each subgroup is OR'ed and they are all AND'ed together
}

var MongoQueryPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &MongoQuery{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},
			DatabaseName:     []byte{},
			CollectionName:   []byte{},
			BsonDoc:          []M{},

			MeasurementName: []byte{},
			FieldName:       []byte{},
			AggregationType: []byte{},
			TagSets:         [][]string{},
		}
	},
}

func NewMongoQuery() *MongoQuery {
	return MongoQueryPool.Get().(*MongoQuery)
}

// String produces a debug-ready description of a Query.
func (q *MongoQuery) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, MeasurementName: %s, AggregationType: %s, TimeStart: %s, TimeEnd: %s, GroupByDuration: %s, TagSets: %s", q.HumanLabel, q.HumanDescription, q.MeasurementName, q.AggregationType, q.TimeStart, q.TimeEnd, q.GroupByDuration, q.TagSets)
}

func (q *MongoQuery) HumanLabelName() []byte {
	return q.HumanLabel
}
func (q *MongoQuery) HumanDescriptionName() []byte {
	return q.HumanDescription
}

func (q *MongoQuery) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.DatabaseName = q.DatabaseName[:0]
	q.CollectionName = q.CollectionName[:0]
	q.BsonDoc = nil

	q.MeasurementName = q.MeasurementName[:0]
	q.FieldName = q.FieldName[:0]
	q.AggregationType = q.AggregationType[:0]
	q.GroupByDuration = 0
	q.TimeStart = time.Time{}
	q.TimeEnd = time.Time{}
	q.TagSets = q.TagSets[:0]

	MongoQueryPool.Put(q)
}
