package query

import (
	"fmt"
	"sync"
)

// CrateDB encodes a CrateDB request. This will be serialized for use
// by the tsbs_run_queries_cratedb program.
type CrateDB struct {
	HumanLabel       []byte
	HumanDescription []byte

	Table    []byte // e.g. "cpu"
	SqlQuery []byte
	id       uint64
}

var CrateDBPool = sync.Pool{
	New: func() interface{} {
		return &CrateDB{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			Table:            make([]byte, 0, 1024),
			SqlQuery:         make([]byte, 0, 1024),
		}
	},
}

func NewCrateDB() *CrateDB {
	return CrateDBPool.Get().(*CrateDB)
}

func (q *CrateDB) GetID() uint64 {
	return q.id
}

func (q *CrateDB) SetID(n uint64) {
	q.id = n
}

// String produces a debug-ready description of a Query.
func (q *CrateDB) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Table: %s, Query: %s",
		q.HumanLabel, q.HumanDescription, q.Table, q.SqlQuery)
}

func (q *CrateDB) HumanLabelName() []byte {
	return q.HumanLabel
}

func (q *CrateDB) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *CrateDB) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0

	q.Table = q.Table[:0]
	q.SqlQuery = q.SqlQuery[:0]

	CrateDBPool.Put(q)
}
