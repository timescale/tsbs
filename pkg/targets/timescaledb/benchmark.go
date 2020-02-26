package timescaledb

import (
	"bufio"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
)

func NewBenchmark(opts *LoadingOptions, loader *load.BenchmarkRunner) targets.Benchmark {
	return &benchmark{opts, loader}
}

type benchmark struct {
	opts   *LoadingOptions
	loader *load.BenchmarkRunner
}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) targets.PointDecoder {
	return &decoder{scanner: bufio.NewScanner(br)}
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
		br:      b.loader.GetBufferedReader(),
		connStr: b.opts.GetConnectString(),
		connDB:  b.opts.ConnDB,
	}
}
