package timescaledb

import (
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
)

func NewBenchmark(opts *LoadingOptions, loader *load.BenchmarkRunner) targets.Benchmark {
	return &benchmark{opts, newFileDataSource(loader.FileName)}
}

type benchmark struct {
	opts *LoadingOptions
	ds   source.DataSource
}

func (b *benchmark) GetDataSource() source.DataSource {
	return b.ds
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	if b.opts.HashWorkers {
		return &hostnameIndexer{partitions: maxPartitions}
	}
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{
		connStr: b.opts.GetConnectString(),
		connDB:  b.opts.ConnDB,
		ds:      b.ds,
	}
}
