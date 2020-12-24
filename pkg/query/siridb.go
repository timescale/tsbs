package query

import (
	"fmt"
	"sync"
)

type SiriDB struct {
	HumanLabel       []byte
	HumanDescription []byte
	SqlQuery         []byte
	id               uint64
}

var SiriDBPool = sync.Pool{
	New: func() interface{} {
		return &SiriDB{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			SqlQuery:         make([]byte, 0, 1024),
		}
	},
}

func NewSiriDB() *SiriDB {
	return SiriDBPool.Get().(*SiriDB)
}

// GetID returns the ID of this Query
func (q *SiriDB) GetID() uint64 {
	return q.id
}

// SetID sets the ID for this Query
func (q *SiriDB) SetID(id uint64) {
	q.id = id
}

// String produces a debug-ready description of a Query.
func (q *SiriDB) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s", q.HumanLabel, q.HumanDescription)
}

// HumanLabelName returns the human readable name of this Query
func (q *SiriDB) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (q *SiriDB) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *SiriDB) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0
	q.SqlQuery = q.SqlQuery[:0]

	SiriDBPool.Put(q)
}
