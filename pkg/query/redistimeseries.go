package query

import (
	"fmt"
	"sync"
)

// RedisTimeSeries encodes a RedisTimeSeries request. This will be serialized for use
// by the tsbs_run_queries_redistimeseries program.
type RedisTimeSeries struct {
	HumanLabel       []byte
	HumanDescription []byte

	RedisQueries [][][]byte
	CommandNames [][]byte
	id           uint64
	ApplyFunctor bool
	Functor      string
}

// RedisTimeSeriesPool is a sync.Pool of RedisTimeSeries Query types
var RedisTimeSeriesPool = sync.Pool{
	New: func() interface{} {
		queries := make([][][]byte, 0, 0)
		commands := make([][]byte, 0, 0)
		return &RedisTimeSeries{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			RedisQueries:     queries,
			CommandNames:     commands,
			ApplyFunctor:     false,
		}
	},
}

// NewRedisTimeSeries returns a new RedisTimeSeries Query instance
func NewRedisTimeSeries() *RedisTimeSeries {
	return RedisTimeSeriesPool.Get().(*RedisTimeSeries)
}

// GetID returns the ID of this Query
func (q *RedisTimeSeries) GetID() uint64 {
	return q.id
}

// SetID sets the ID for this Query
func (q *RedisTimeSeries) SetID(n uint64) {
	q.id = n
}

// SetApplyFunctor sets the flag for group by timestamp on a MultiRange Serie
func (q *RedisTimeSeries) SetApplyFunctor(value bool) {
	q.ApplyFunctor = value
}

func (q *RedisTimeSeries) SetFunctor(f string) {
	q.Functor = f
}

// GetCommandName returns the command used for this Query
func (q *RedisTimeSeries) AddQuery(query [][]byte, commandname []byte) {
	q.RedisQueries = append(q.RedisQueries, query)
	q.CommandNames = append(q.CommandNames, commandname)
}

// GetCommandName returns the command used for this Query
func (q *RedisTimeSeries) GetCommandName(pos int) []byte {
	return q.CommandNames[pos]
}

// String produces a debug-ready description of a Query.
func (q *RedisTimeSeries) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Query: %s", q.HumanLabel, q.HumanDescription, q.RedisQueries)
}

// HumanLabelName returns the human readable name of this Query
func (q *RedisTimeSeries) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (q *RedisTimeSeries) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *RedisTimeSeries) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0

	q.RedisQueries = q.RedisQueries[:0]
	q.CommandNames = q.CommandNames[:0]

	RedisTimeSeriesPool.Put(q)
}
