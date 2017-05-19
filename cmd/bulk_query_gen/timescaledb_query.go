package main

import (
	"fmt"
	"sync"
)

// TimescaleDBQuery encodes a TimescaleDB request. This will be serialized for use
// by the query_benchmarker program.
type TimescaleDBQuery struct {
	HumanLabel       []byte
	HumanDescription []byte

	NamespaceName []byte // e.g. "cpu"
	SqlQuery      []byte
}

var TimescaleDBQueryPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &TimescaleDBQuery{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},
			NamespaceName:    []byte{},
			SqlQuery:         []byte{},
		}
	},
}

func NewTimescaleDBQuery() *TimescaleDBQuery {
	return TimescaleDBQueryPool.Get().(*TimescaleDBQuery)
}

// String produces a debug-ready description of a Query.
func (q *TimescaleDBQuery) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, NamespaceName: %s, Query: %s", q.HumanLabel, q.HumanDescription, q.NamespaceName, q.SqlQuery)
}

func (q *TimescaleDBQuery) HumanLabelName() []byte {
	return q.HumanLabel
}
func (q *TimescaleDBQuery) HumanDescriptionName() []byte {
	return q.HumanDescription
}

func (q *TimescaleDBQuery) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]

	q.NamespaceName = q.NamespaceName[:0]
	q.SqlQuery = q.SqlQuery[:0]

	TimescaleDBQueryPool.Put(q)
}
