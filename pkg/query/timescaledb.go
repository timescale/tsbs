package query

import (
	"fmt"
	"sync"
)

// TimescaleDB encodes a TimescaleDB request. This will be serialized for use
// by the tsbs_run_queries_timescaledb program.
type TimescaleDB struct {
	HumanLabel       []byte
	HumanDescription []byte

	Hypertable []byte // e.g. "cpu"
	SqlQuery   []byte
	id         uint64
}

// TimescaleDBPool is a sync.Pool of TimescaleDB Query types
var TimescaleDBPool = sync.Pool{
	New: func() interface{} {
		return &TimescaleDB{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			Hypertable:       make([]byte, 0, 1024),
			SqlQuery:         make([]byte, 0, 1024),
		}
	},
}

// NewTimescaleDB returns a new TimescaleDB Query instance
func NewTimescaleDB() *TimescaleDB {
	return TimescaleDBPool.Get().(*TimescaleDB)
}

// GetID returns the ID of this Query
func (q *TimescaleDB) GetID() uint64 {
	return q.id
}

// SetID sets the ID for this Query
func (q *TimescaleDB) SetID(n uint64) {
	q.id = n
}

// String produces a debug-ready description of a Query.
func (q *TimescaleDB) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Hypertable: %s, Query: %s", q.HumanLabel, q.HumanDescription, q.Hypertable, q.SqlQuery)
}

// HumanLabelName returns the human readable name of this Query
func (q *TimescaleDB) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (q *TimescaleDB) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *TimescaleDB) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0

	q.Hypertable = q.Hypertable[:0]
	q.SqlQuery = q.SqlQuery[:0]

	TimescaleDBPool.Put(q)
}
