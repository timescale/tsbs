package config

import (
	"fmt"

	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

const ErrEmptyQueryType = "query type cannot be empty"

// QueryGeneratorConfig is the GeneratorConfig that should be used with a
// QueryGenerator. It includes all the fields from a BaseConfig, as well as
// options that are specific to generating the queries to test against a
// database, such as the query type and individual database options.
type QueryGeneratorConfig struct {
	common.BaseConfig
	Limit                uint64 `mapstructure:"queries"`
	QueryType            string `mapstructure:"query-type"`
	InterleavedGroupID   uint   `mapstructure:"interleaved-generation-group-id"`
	InterleavedNumGroups uint   `mapstructure:"interleaved-generation-groups"`

	// TODO - I think this needs some rethinking, but a simple, elegant solution escapes me right now
	TimescaleUseJSON       bool `mapstructure:"timescale-use-json"`
	TimescaleUseTags       bool `mapstructure:"timescale-use-tags"`
	TimescaleUseTimeBucket bool `mapstructure:"timescale-use-time-bucket"`

	ClickhouseUseTags bool `mapstructure:"clickhouse-use-tags"`

	MongoUseNaive bool   `mapstructure:"mongo-use-naive"`
	DbName        string `mapstructure:"db-name"`
}

// Validate checks that the values of the QueryGeneratorConfig are reasonable.
func (c *QueryGeneratorConfig) Validate() error {
	err := c.BaseConfig.Validate()
	if err != nil {
		return err
	}

	if c.QueryType == "" {
		return fmt.Errorf(ErrEmptyQueryType)
	}

	err = utils.ValidateGroups(c.InterleavedGroupID, c.InterleavedNumGroups)
	return err
}

func (c *QueryGeneratorConfig) AddToFlagSet(fs *pflag.FlagSet) {
	c.BaseConfig.AddToFlagSet(fs)
	fs.Uint64("queries", 1000, "Number of queries to generate.")
	fs.String("query-type", "", "Query type. (Choices are in the use case matrix.)")

	fs.Uint("interleaved-generation-group-id", 0,
		"Group (0-indexed) to perform round-robin serialization within. Use this to scale up data generation to multiple processes.")
	fs.Uint("interleaved-generation-groups", 1,
		"The number of round-robin serialization groups. Use this to scale up data generation to multiple processes.")

	fs.Bool("clickhouse-use-tags", true, "ClickHouse only: Use separate tags table when querying")
	fs.Bool("mongo-use-naive", true, "MongoDB only: Generate queries for the 'naive' data storage format for Mongo")
	fs.Bool("timescale-use-json", false, "TimescaleDB only: Use separate JSON tags table when querying")
	fs.Bool("timescale-use-tags", true, "TimescaleDB only: Use separate tags table when querying")
	fs.Bool("timescale-use-time-bucket", true, "TimescaleDB only: Use time bucket. Set to false to test on native PostgreSQL")

	fs.String("db-name", "benchmark", "Specify database name. Timestream requires it in order to generate the queries")
}
