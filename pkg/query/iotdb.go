package query

import (
	"fmt"
	"sync"
	// CRTODO: 删掉不需要的内容
	// "github.com/apache/iotdb-client-go/client"
)

// IoTDB encodes a IoTDB request. This will be serialized for use
// by the tsbs_run_queries_iotdb program.
type IoTDB struct {
	HumanLabel       []byte
	HumanDescription []byte

	// CRTODO: 设计好这里的结构，这里可能还需要其他东西
	// ClientSession client.Session
	SqlQuery []byte
	id       uint64
}

// IoTDBPool is a sync.Pool of IoTDB Query types
var IoTDBPool = sync.Pool{
	New: func() interface{} {
		return &IoTDB{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},

			SqlQuery: []byte{},
		}
	},
}

// NewIoTDB returns a new IoTDB Query instance
func NewIoTDB() *IoTDB {
	return IoTDBPool.Get().(*IoTDB)
}

// GetID returns the ID of this Query
func (q *IoTDB) GetID() uint64 {
	return q.id
}

// SetID sets the ID for this Query
func (q *IoTDB) SetID(id uint64) {
	q.id = id
}

// String produces a debug-ready description of a Query.
func (q *IoTDB) String() string {
	return fmt.Sprintf(
		"HumanLabel: %s, HumanDescription: %s, Query: %s",
		q.HumanLabel, q.HumanDescription, q.SqlQuery,
	)
}

// HumanLabelName returns the human readable name of this Query
func (q *IoTDB) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (q *IoTDB) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *IoTDB) Release() {
	// CRTODO: 弄清楚这里是否还需要其他的操作
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0
	q.SqlQuery = q.SqlQuery[:0]

	IoTDBPool.Put(q)
}
