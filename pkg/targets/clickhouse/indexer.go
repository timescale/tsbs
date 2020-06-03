package clickhouse

import (
	"hash/fnv"
	"strings"

	"github.com/timescale/tsbs/pkg/data"
)

// hostnameIndexer is used to consistently send the same hostnames to the same queue
type hostnameIndexer struct {
	partitions uint
}

// scan.PointIndexer interface implementation
func (i *hostnameIndexer) GetIndex(item data.LoadedPoint) uint {
	p := item.Data.(*point)
	hostname := strings.SplitN(p.row.tags, ",", 2)[0]
	h := fnv.New32a()
	h.Write([]byte(hostname))
	return uint(h.Sum32()) % i.partitions
}
