package query

import (
	"fmt"
	"sync"
)

type TDengine struct {
	id               uint64
	HumanLabel       []byte
	HumanDescription []byte
	Hypertable       []byte
	SqlQuery         []byte
}

var TDenginePool = sync.Pool{
	New: func() interface{} {
		return &TDengine{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			Hypertable:       make([]byte, 0, 1024),
			SqlQuery:         make([]byte, 0, 1024),
		}
	},
}

func NewTDengine() *TDengine {
	return TDenginePool.Get().(*TDengine)
}

func (q *TDengine) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0

	q.Hypertable = q.Hypertable[:0]
	q.SqlQuery = q.SqlQuery[:0]
	TDenginePool.Put(q)
}

func (q *TDengine) HumanLabelName() []byte {
	return q.HumanLabel
}

func (q *TDengine) HumanDescriptionName() []byte {
	return q.HumanDescription
}

func (q *TDengine) GetID() uint64 {
	return q.id
}

func (q *TDengine) SetID(n uint64) {
	q.id = n
}

func (q *TDengine) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Hypertable: %s, Query: %s", q.HumanLabel, q.HumanDescription, q.Hypertable, q.SqlQuery)
}
