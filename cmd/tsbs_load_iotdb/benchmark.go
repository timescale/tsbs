package main

import (
	"bufio"

	"github.com/apache/iotdb-client-go/client"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
)

func newBenchmark(clientConfig client.Config, loaderConfig load.BenchmarkRunnerConfig) targets.Benchmark {
	return &iotdbBenchmark{
		cilentConfig:   clientConfig,
		loaderConfig:   loaderConfig,
		recordsMaxRows: recordsMaxRows,
	}
}

type iotdbBenchmark struct {
	cilentConfig   client.Config
	loaderConfig   load.BenchmarkRunnerConfig
	recordsMaxRows int
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
	return &processor{
		recordsMaxRows:       b.recordsMaxRows,
		loadToSCV:            loadToSCV,
		csvFilepathPrefix:    csvFilepathPrefix,
		useAlignedTimeseries: useAlignedTimeseries,
		storeTags:            storeTags,
	}
}

func (b *iotdbBenchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{
		loadToSCV: loadToSCV,
	}
}
