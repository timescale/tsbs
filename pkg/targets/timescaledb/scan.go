package timescaledb

import (
	"hash/fnv"
	"strings"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
)

const (
	defaultReadSize = 4 << 20 // 4 MB
)

// hostnameIndexer is used to consistently send the same hostnames to the same worker
type hostnameIndexer struct {
	partitions uint
}

func (i *hostnameIndexer) GetIndex(item data.LoadedPoint) uint {
	p := item.Data.(*point)
	hostname := strings.SplitN(p.row.tags, ",", 2)[0]
	h := fnv.New32a()
	h.Write([]byte(hostname))
	return uint(h.Sum32()) % i.partitions
}

// point is a single row of data keyed by which hypertable it belongs
type point struct {
	hypertable string
	row        *insertData
}

type hypertableArr struct {
	m   map[string][]*insertData
	cnt uint
}

func (ha *hypertableArr) Len() uint {
	return ha.cnt
}

func (ha *hypertableArr) Append(item data.LoadedPoint) {
	that := item.Data.(*point)
	k := that.hypertable
	ha.m[k] = append(ha.m[k], that.row)
	ha.cnt++
}

type factory struct{}

func (f *factory) New() targets.Batch {
	return &hypertableArr{
		m:   map[string][]*insertData{},
		cnt: 0,
	}
}
