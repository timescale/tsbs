package query

import (
	"fmt"
	"sync"
)

// ClickHouse encodes a ClickHouse query.
// This will be serialized for use by the tsbs_run_queries_clickhouse program.
type ClickHouse struct {
	HumanLabel       []byte
	HumanDescription []byte

	Table    []byte // e.g. "cpu"
	SqlQuery []byte
	id       uint64
}

// ClickHousePool is a sync.Pool of ClickHouse Query types
var ClickHousePool = sync.Pool{
	New: func() interface{} {
		return &ClickHouse{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			Table:            make([]byte, 0, 1024),
			SqlQuery:         make([]byte, 0, 1024),
		}
	},
}

// NewClickHouse returns a new ClickHouse Query instance
func NewClickHouse() *ClickHouse {
	return ClickHousePool.Get().(*ClickHouse)
}

// GetID returns the ID of this Query
func (ch *ClickHouse) GetID() uint64 {
	return ch.id
}

// SetID sets the ID for this Query
func (ch *ClickHouse) SetID(n uint64) {
	ch.id = n
}

// String produces a debug-ready description of a Query.
func (ch *ClickHouse) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Table: %s, Query: %s", ch.HumanLabel, ch.HumanDescription, ch.Table, ch.SqlQuery)
}

// HumanLabelName returns the human readable name of this Query
func (ch *ClickHouse) HumanLabelName() []byte {
	return ch.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (ch *ClickHouse) HumanDescriptionName() []byte {
	return ch.HumanDescription
}

// Release resets and returns this Query to its pool
func (ch *ClickHouse) Release() {
	ch.HumanLabel = ch.HumanLabel[:0]
	ch.HumanDescription = ch.HumanDescription[:0]

	ch.Table = ch.Table[:0]
	ch.SqlQuery = ch.SqlQuery[:0]

	ClickHousePool.Put(ch)
}
