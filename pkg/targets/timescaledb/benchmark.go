package timescaledb

import (
	"github.com/timescale/tsbs/internal/inputs"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
)

func newBenchmark(opts *LoadingOptions, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	if dataSourceConfig.Type == source.FileDataSourceType {
		return &benchmark{
			opts: opts,
			ds:   newFileDataSource(dataSourceConfig.File.Location),
		}, nil
	}

	dataGenerator := &inputs.DataGenerator{}
	simulator, err := dataGenerator.CreateSimulator(dataSourceConfig.Simulator)
	if err != nil {
		return nil, err
	}
	return &benchmark{
		opts: opts,
		ds:   newSimulationDataSource(simulator)}, nil
}

type benchmark struct {
	opts *LoadingOptions
	ds   targets.DataSource
}

func (b *benchmark) GetDataSource() targets.DataSource {
	return b.ds
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	if maxPartitions > 1 {
		return &hostnameIndexer{partitions: maxPartitions}
	}
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{opts: b.opts}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{
		opts:    b.opts,
		connStr: b.opts.GetConnectString(),
		connDB:  b.opts.ConnDB,
		ds:      b.ds,
	}
}
