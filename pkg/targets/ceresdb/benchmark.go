package ceresdb

import (
	"bufio"
	"bytes"
	"errors"
	"sync"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
	"github.com/blagojts/viper"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
)

type SpecificConfig struct {
	CeresdbAddr   string `yaml:"ceresdbAddr" mapstructure:"ceresdbAddr"`
	StorageFormat string `yaml:"storageFormat" mapstructure:"storageFormat"`
	RowGroupSize  int64  `yaml:"rowGroupSize" mapstructure:"rowGroupSize"`
	PrimaryKeys   string `yaml:"primaryKeys" mapstructure:"primaryKeys"`
	PartitionKeys string `yaml:"partitionKeys" mapstructure:"partitionKeys"`
	PartitionNum  uint32 `yaml:"partitionKeys" mapstructure:"partitionNum"`
	AccessMode    string `yaml:"accessMode" mapstructure:"accessMode"`
	UpdateMode    string `yaml:"updateMode" mapstructure:"updateMode"`
}

func parseSpecificConfig(v *viper.Viper) (*SpecificConfig, error) {
	var conf SpecificConfig
	if err := v.Unmarshal(&conf); err != nil {
		return nil, err
	}
	return &conf, nil
}

// loader.Benchmark interface implementation
type benchmark struct {
	config     *SpecificConfig
	dataSource targets.DataSource
	client     ceresdb.Client
}

func NewBenchmark(config *SpecificConfig, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	if dataSourceConfig.Type != source.FileDataSourceType {
		return nil, errors.New("only FILE data source type is supported for CeresDB")
	}

	br := load.GetBufferedReader(dataSourceConfig.File.Location)
	dataSource := &fileDataSource{
		scanner: bufio.NewScanner(br),
	}

	client, err := NewClient(config.CeresdbAddr, config.AccessMode, ceresdb.WithDefaultDatabase("public"))
	if err != nil {
		panic(err)
	}
	return &benchmark{
		config,
		dataSource,
		client,
	}, nil
}

func (b *benchmark) GetDataSource() targets.DataSource {
	return b.dataSource
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	bufPool := sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 16*1024*1024))
		},
	}
	return &factory{bufPool: &bufPool}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{addr: b.config.CeresdbAddr, client: b.client}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{
		config: b.config,
		ds:     b.dataSource,
	}
}

type factory struct {
	bufPool *sync.Pool
}

func (f *factory) New() targets.Batch {
	return &batch{}
}
