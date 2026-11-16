package tdengine

import (
	"bytes"
	"sync"

	"github.com/spaolacci/murmur3"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
)

// indexer is used to consistently send the same hostnames to the same worker
type indexer struct {
	buffer        *bytes.Buffer
	prefix        []byte
	partitions    int
	hashEndGroups []uint32
	tmp           map[string]uint
}

func (i *indexer) GetIndex(item data.LoadedPoint) uint {
	p := item.Data.(*point)
	idx, exist := i.tmp[p.subTable]
	if exist {
		return idx
	}
	i.buffer.Write(i.prefix)
	i.buffer.WriteString(p.subTable)
	hash := murmur3.Sum32WithSeed(i.buffer.Bytes(), 0x12345678)
	i.buffer.Reset()
	for j := 0; j < i.partitions; j++ {
		if hash <= i.hashEndGroups[j] {
			idx = uint(j)
			break
		}
	}
	i.tmp[p.subTable] = idx
	return idx
}

// point is a single row of data keyed by which superTable it belongs
type point struct {
	sqlType    byte
	superTable string
	subTable   string
	fieldCount int
	sql        string
}

var GlobalTable = sync.Map{}

type hypertableArr struct {
	createSql   []*point
	m           map[string][]string
	totalMetric uint64
	cnt         uint
}

func (ha *hypertableArr) Len() uint {
	return ha.cnt
}

func (ha *hypertableArr) Append(item data.LoadedPoint) {
	that := item.Data.(*point)
	if that.sqlType == Insert {
		ha.m[that.subTable] = append(ha.m[that.subTable], that.sql)
		ha.totalMetric += uint64(that.fieldCount)
		ha.cnt++
	} else {
		ha.createSql = append(ha.createSql, that)
	}
}

func (ha *hypertableArr) Reset() {
	ha.m = map[string][]string{}
	ha.cnt = 0
	ha.createSql = ha.createSql[:0]
}

type BatchFactory struct {
	pool *sync.Pool
}

func (b *BatchFactory) New() targets.Batch {
	return b.pool.Get().(*hypertableArr)
}

func NewBatchFactory() *BatchFactory {
	pool := &sync.Pool{New: func() interface{} {
		return &hypertableArr{
			m: map[string][]string{},
		}
	}}
	return &BatchFactory{pool: pool}
}
