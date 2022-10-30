package main

import (
	"bufio"

	"github.com/apache/iotdb-client-go/client"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
)

func newBenchmark(clientConfig client.Config, loaderConfig load.BenchmarkRunnerConfig) (targets.Benchmark, error) {
	return &iotdbBenchmark{
		cilentConfig: clientConfig,
		loaderConfig: loaderConfig,
	}, nil
}

type iotdbBenchmark struct {
	cilentConfig client.Config
	loaderConfig load.BenchmarkRunnerConfig
}

func (b *iotdbBenchmark) GetDataSource() targets.DataSource {
	return &fileDataSource{scanner: bufio.NewScanner(load.GetBufferedReader(b.loaderConfig.FileName))}
}

func (b *iotdbBenchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *iotdbBenchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *iotdbBenchmark) GetProcessor() targets.Processor {
	return &processor{}
}

func (b *iotdbBenchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{}
}
