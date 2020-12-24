package akumuli

import (
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
	"sync"
)

func NewBenchmark(loadFileName, endpoint string, bufPool *sync.Pool) targets.Benchmark {
	return &benchmark{
		loadFileName: loadFileName,
		endpoint:     endpoint,
		bufPool:      bufPool,
	}
}

type benchmark struct {
	loadFileName string
	endpoint     string
	bufPool      *sync.Pool
}

func (b *benchmark) GetDataSource() targets.DataSource {
	return &fileDataSource{reader: load.GetBufferedReader(b.loadFileName)}
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{bufPool: b.bufPool}
}

func (b *benchmark) GetPointIndexer(n uint) targets.PointIndexer {
	return &pointIndexer{nchan: n}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{endpoint: b.endpoint, bufPool: b.bufPool}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{}
}
