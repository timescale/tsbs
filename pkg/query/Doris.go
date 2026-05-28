package query

import (
	"fmt"
	"sync"
)

// Doris encodes a Doris query.
// This will be serialized for use by the tsbs_run_queries_Doris program.
type Doris struct {
	HumanLabel       []byte
	HumanDescription []byte

	Table    []byte // e.g. "cpu"
	SqlQuery []byte
	id       uint64
}

// DorisPool is a sync.Pool of Doris Query types
var DorisPool = sync.Pool{
	New: func() interface{} {
		return &Doris{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			Table:            make([]byte, 0, 1024),
			SqlQuery:         make([]byte, 0, 1024),
		}
	},
}

// NewDoris returns a new Doris Query instance
func NewDoris() *Doris {
	return DorisPool.Get().(*Doris)
}

// GetID returns the ID of this Query
func (ch *Doris) GetID() uint64 {
	return ch.id
}

// SetID sets the ID for this Query
func (ch *Doris) SetID(n uint64) {
	ch.id = n
}

// String produces a debug-ready description of a Query.
func (ch *Doris) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Table: %s, Query: %s", ch.HumanLabel, ch.HumanDescription, ch.Table, ch.SqlQuery)
}

// HumanLabelName returns the human-readable name of this Query
func (ch *Doris) HumanLabelName() []byte {
	return ch.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (ch *Doris) HumanDescriptionName() []byte {
	return ch.HumanDescription
}

// Release resets and returns this Query to its pool
func (ch *Doris) Release() {
	ch.HumanLabel = ch.HumanLabel[:0]
	ch.HumanDescription = ch.HumanDescription[:0]

	ch.Table = ch.Table[:0]
	ch.SqlQuery = ch.SqlQuery[:0]

	DorisPool.Put(ch)
}
