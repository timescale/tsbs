package query

import (
	"fmt"
	"sync"
)

type CeresDB struct {
	HumanLabel       []byte
	HumanDescription []byte

	Table    []byte
	SqlQuery []byte
	id       uint64
}

// CeresDBPool is a sync.Pool of CeresDB Query types
var CeresDBPool = sync.Pool{
	New: func() interface{} {
		return &CeresDB{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			Table:            make([]byte, 0, 1024),
			SqlQuery:         make([]byte, 0, 1024),
		}
	},
}

// NewCeresDB returns a new CeresDB Query instance
func NewCeresDB() *CeresDB {
	return CeresDBPool.Get().(*CeresDB)
}

// GetID returns the ID of this Query
func (ch *CeresDB) GetID() uint64 {
	return ch.id
}

// SetID sets the ID for this Query
func (ch *CeresDB) SetID(n uint64) {
	ch.id = n
}

// String produces a debug-ready description of a Query.
func (ch *CeresDB) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Table: %s, Query: %s", ch.HumanLabel, ch.HumanDescription, ch.Table, ch.SqlQuery)
}

// HumanLabelName returns the human readable name of this Query
func (ch *CeresDB) HumanLabelName() []byte {
	return ch.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (ch *CeresDB) HumanDescriptionName() []byte {
	return ch.HumanDescription
}

// Release resets and returns this Query to its pool
func (ch *CeresDB) Release() {
	ch.HumanLabel = ch.HumanLabel[:0]
	ch.HumanDescription = ch.HumanDescription[:0]

	ch.Table = ch.Table[:0]
	ch.SqlQuery = ch.SqlQuery[:0]

	CeresDBPool.Put(ch)
}
