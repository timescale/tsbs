package query

import (
	"fmt"
	"sync"
)

// Timestream encodes a Timestream request. This will be serialized for use
// by the tsbs_run_queries_timestream program.
type Timestream struct {
	HumanLabel       []byte
	HumanDescription []byte

	Table    []byte // e.g. "cpu"
	SqlQuery []byte
	id       uint64
}

// TimestreamPool is a sync.Pool of Timestream Query types
var TimestreamPool = sync.Pool{
	New: func() interface{} {
		return &Timestream{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			Table:            make([]byte, 0, 50),
			SqlQuery:         make([]byte, 0, 1024),
		}
	},
}

// NewTimestream returns a new Timestream Query instance
func NewTimestream() *Timestream {
	return TimestreamPool.Get().(*Timestream)
}

// GetID returns the ID of this Query
func (q *Timestream) GetID() uint64 {
	return q.id
}

// SetID sets the ID for this Query
func (q *Timestream) SetID(n uint64) {
	q.id = n
}

// String produces a debug-ready description of a Query.
func (q *Timestream) String() string {
	return fmt.Sprintf(
		"HumanLabel: %s, HumanDescription: %s, Table: %s, Query: %s",
		q.HumanLabel, q.HumanDescription, q.Table, q.SqlQuery,
	)
}

// HumanLabelName returns the human readable name of this Query
func (q *Timestream) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (q *Timestream) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *Timestream) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0
	q.Table = q.Table[:0]
	q.SqlQuery = q.SqlQuery[:0]

	TimestreamPool.Put(q)
}
