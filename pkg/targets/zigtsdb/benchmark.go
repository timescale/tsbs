package zigtsdb

import (
	"bufio"
	"errors"

	"github.com/blagojts/viper"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
)

// SpecificConfig holds the zigtsdb loader settings (flags or YAML).
type SpecificConfig struct {
	ServerURLs   []string `yaml:"urls" mapstructure:"urls"`
	IngestPath   string   `yaml:"ingest-path" mapstructure:"ingest-path"`
	MaxBodyBytes int      `yaml:"max-body-bytes" mapstructure:"max-body-bytes"`
	MaxRetries   int      `yaml:"max-retries" mapstructure:"max-retries"`
}

func parseSpecificConfig(v *viper.Viper) (*SpecificConfig, error) {
	var conf SpecificConfig
	if err := v.Unmarshal(&conf); err != nil {
		return nil, err
	}
	return &conf, nil
}

func (c *SpecificConfig) applyDefaults() error {
	if len(c.ServerURLs) == 0 {
		return errors.New("zigtsdb: missing `urls`")
	}
	if c.IngestPath == "" {
		c.IngestPath = defaultIngestPath
	}
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = defaultMaxBodyBytes
	}
	if c.MaxRetries < 0 {
		c.MaxRetries = defaultMaxRetries
	}
	return nil
}

type benchmark struct {
	conf       *SpecificConfig
	dataSource targets.DataSource
}

// NewBenchmark builds the loader benchmark; only FILE data sources are supported.
func NewBenchmark(conf *SpecificConfig, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	if dataSourceConfig.Type != source.FileDataSourceType {
		return nil, errors.New("only FILE data source type is supported for zigtsdb")
	}
	if err := conf.applyDefaults(); err != nil {
		return nil, err
	}
	br := load.GetBufferedReader(dataSourceConfig.File.Location)
	return &benchmark{
		conf:       conf,
		dataSource: &fileDataSource{scanner: bufio.NewScanner(br)},
	}, nil
}

func (b *benchmark) GetDataSource() targets.DataSource {
	return b.dataSource
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	// A 10k-row cpu-only batch expands to ~23 MB of wire lines.
	return newFactory(32 * 1024 * 1024)
}

func (b *benchmark) GetPointIndexer(_ uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{
		urls:       b.conf.ServerURLs,
		ingestPath: b.conf.IngestPath,
		maxBody:    b.conf.MaxBodyBytes,
		maxRetries: b.conf.MaxRetries,
	}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{url: b.conf.ServerURLs[0]}
}
