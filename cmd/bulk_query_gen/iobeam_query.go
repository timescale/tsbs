package main

import (
	"fmt"
	"sync"
)

// IobeamQuery encodes a Iobeam request. This will be serialized for use
// by the query_benchmarker program.
type IobeamQuery struct {
	HumanLabel       []byte
	HumanDescription []byte

	NamespaceName []byte // e.g. "cpu"
	FieldName     []byte // e.g. "usage_user"
	SqlQuery      []byte
}

var IobeamQueryPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &IobeamQuery{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},
			NamespaceName:    []byte{},
			FieldName:        []byte{},
			SqlQuery:         []byte{},
		}
	},
}

func NewIobeamQuery() *IobeamQuery {
	return IobeamQueryPool.Get().(*IobeamQuery)
}

// String produces a debug-ready description of a Query.
func (q *IobeamQuery) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, NamespaceName: %s, FieldName: %s, Query: %s", q.HumanLabel, q.HumanDescription, q.NamespaceName, q.FieldName, q.SqlQuery)
}

func (q *IobeamQuery) HumanLabelName() []byte {
	return q.HumanLabel
}
func (q *IobeamQuery) HumanDescriptionName() []byte {
	return q.HumanDescription
}

func (q *IobeamQuery) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]

	q.NamespaceName = q.NamespaceName[:0]
	q.FieldName = q.FieldName[:0]
	q.SqlQuery = q.SqlQuery[:0]

	IobeamQueryPool.Put(q)
}
