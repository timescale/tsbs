package timescaledb

import (
	"bufio"
	"github.com/timescale/tsbs/load"
)

func NewBenchmark(opts *ProgramOptions, loader *load.BenchmarkRunner) load.Benchmark {
	return &benchmark{opts, loader}
}

type benchmark struct {
	opts   *ProgramOptions
	loader *load.BenchmarkRunner
}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{scanner: bufio.NewScanner(br)}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) load.PointIndexer {
	if b.opts.HashWorkers {
		return &hostnameIndexer{partitions: maxPartitions}
	}
	return &load.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return &dbCreator{
		br:      b.loader.GetBufferedReader(),
		connStr: b.opts.GetConnectString(),
		connDB:  b.opts.ConnDB,
	}
}
