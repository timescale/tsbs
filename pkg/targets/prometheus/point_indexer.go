package prometheus

import (
	"math/rand"
	"strings"
	"sync"

	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
	"go.uber.org/atomic"
)

func newSeriesIDPointIndexer(maxIndex uint) targets.PointIndexer {
	return &seriesIDPointIndexer{
		indexCache: &sync.Map{},
		maxIndex:   maxIndex,
	}
}

func newRandomPointIndexer(numPartitions uint) targets.PointIndexer {
	return &randomPointIndexer{int(numPartitions)}
}

var lastSeriesID atomic.Uint64

type seriesIDPointIndexer struct {
	maxIndex   uint
	indexCache *sync.Map
}

type randomPointIndexer struct {
	numPartitions int
}

func (s *seriesIDPointIndexer) GetIndex(item data.LoadedPoint) uint {
	var ts prompb.TimeSeries
	ts = *item.Data.(*prompb.TimeSeries)

	labelString := nilDelimitedLabelsToStr(ts.Labels)

	index, exists := s.indexCache.Load(labelString)
	if exists {
		return index.(uint)
	}

	id := lastSeriesID.Inc()
	newIndex := uint(id) % s.maxIndex
	s.indexCache.Store(labelString, newIndex)
	return newIndex
}

func (s *randomPointIndexer) GetIndex(data.LoadedPoint) uint {
	newIndex := rand.Intn(s.numPartitions)
	return uint(newIndex)
}

// {key1=val1, key2=val2} => (key<nil>val<nil><nil>key<nil>val)?
func nilDelimitedLabelsToStr(labels []prompb.Label) string {
	length := len(labels)
	if length == 0 {
		return ""
	}
	expectedStrLen := (length - 1) * 3 // 2 for the 2 \x00 chars between pairs, and 1 for between the key and value
	for i := 0; i < length; i++ {
		expectedStrLen += len(labels[i].Name) + len(labels[i].Value)
	}

	// the string representation is
	// (key<nil>val<nil><nil>key<nil>val)?
	builder := strings.Builder{}
	builder.Grow(expectedStrLen)

	for i := 0; i < length; i++ {
		builder.WriteString(labels[i].Name)
		builder.WriteString("\x00")
		builder.WriteString(labels[i].Value)
		if i < length-1 {
			builder.WriteString("\x00\x00")
		}
	}

	return builder.String()
}
