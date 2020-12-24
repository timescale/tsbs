package timestream

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
	"sync"
)

// NewBatchFactory returns a object pool backed
// batch factory that produces batches that hold
// timestream deserialized points
func NewBatchFactory() *batchFactory {
	// TODO modify targets.BatchFactory to have
	// a Return method so the pool is not passed around
	// different objects
	pool := &sync.Pool{New: func() interface{} {
		return &batch{rows: make(map[string][]deserializedPoint)}
	}}
	return &batchFactory{pool: pool}
}

// batch implements targets.Batch interface
type batch struct {
	// keep the rows per table
	rows map[string][]deserializedPoint
	// total number of points
	cnt uint
}

func (b *batch) Len() uint {
	return b.cnt
}

func (b *batch) Append(item data.LoadedPoint) {
	var point deserializedPoint
	point = *item.Data.(*deserializedPoint)
	table := point.table
	b.rows[table] = append(b.rows[table], point)
	b.cnt++
}

func (b *batch) reset() {
	b.rows = map[string][]deserializedPoint{}
	b.cnt = 0
}

// batchFactory implements the targets.BatchFactory interface
type batchFactory struct {
	pool *sync.Pool
}

func (b *batchFactory) New() targets.Batch {
	return b.pool.Get().(*batch)
}
