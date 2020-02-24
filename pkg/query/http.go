package query

import (
	"fmt"
	"sync"
)

// HTTP encodes an HTTP request. This will typically by serialized for use
// by the appropriate tsbs_run_queries program.
type HTTP struct {
	HumanLabel       []byte
	HumanDescription []byte
	Method           []byte
	Path             []byte
	Body             []byte
	RawQuery         []byte
	StartTimestamp   int64
	EndTimestamp     int64
	id               uint64
}

// HTTPPool is a sync.Pool of HTTP Query types
var HTTPPool = sync.Pool{
	New: func() interface{} {
		return &HTTP{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},
			Method:           []byte{},
			Path:             []byte{},
			Body:             []byte{},
			RawQuery:         []byte{},
			StartTimestamp:   0,
			EndTimestamp:     0,
		}
	},
}

// NewHTTP returns a new HTTP type Query
func NewHTTP() *HTTP {
	return HTTPPool.Get().(*HTTP)
}

// GetID returns the ID of this Query
func (q *HTTP) GetID() uint64 {
	return q.id
}

// SetID sets the ID for this Query
func (q *HTTP) SetID(n uint64) {
	q.id = n
}

// String produces a debug-ready description of a Query.
func (q *HTTP) String() string {
	return fmt.Sprintf("HumanLabel: \"%s\", HumanDescription: \"%s\", Method: \"%s\", Path: \"%s\", Body: \"%s\"", q.HumanLabel, q.HumanDescription, q.Method, q.Path, q.Body)
}

// HumanLabelName returns the human readable name of this Query
func (q *HTTP) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (q *HTTP) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *HTTP) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0
	q.Method = q.Method[:0]
	q.Path = q.Path[:0]
	q.Body = q.Body[:0]
	q.StartTimestamp = 0
	q.EndTimestamp = 0

	HTTPPool.Put(q)
}
