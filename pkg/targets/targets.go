package targets

import (
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/source"
)

// Formats supported for generation
const (
	FormatCassandra   = "cassandra"
	FormatClickhouse  = "clickhouse"
	FormatInflux      = "influx"
	FormatMongo       = "mongo"
	FormatSiriDB      = "siridb"
	FormatTimescaleDB = "timescaledb"
	FormatAkumuli     = "akumuli"
	FormatCrateDB     = "cratedb"
	FormatPrometheus  = "prometheus"
	FormatVictoriaMetrics = "victoriametrics"
)

func SupportedFormats() []string {
	return []string{
		FormatCassandra,
		FormatClickhouse,
		FormatInflux,
		FormatMongo,
		FormatSiriDB,
		FormatTimescaleDB,
		FormatAkumuli,
		FormatCrateDB,
		FormatPrometheus,
		FormatVictoriaMetrics,
	}
}

type ImplementedTarget interface {
	Benchmark() Benchmark
	ParseLoaderConfig(v *viper.Viper) (interface{}, error)
}

// Batch is an aggregate of points for a particular data system.
// It needs to have a way to measure it's size to make sure
// it does not get too large and it needs a way to append a point
type Batch interface {
	Len() int
	Append(*data.LoadedPoint)
}

// PointIndexer determines the index of the Batch (and subsequently the channel)
// that a particular point belongs to
type PointIndexer interface {
	// GetIndex returns a partition for the given Point
	GetIndex(*data.LoadedPoint) int
}

// ConstantIndexer always puts the item on a single channel. This is the typical
// use case where all the workers share the same channel
type ConstantIndexer struct{}

// GetIndex returns a constant index (0) regardless of Point
func (i *ConstantIndexer) GetIndex(_ *data.LoadedPoint) int {
	return 0
}

// BatchFactory returns a new empty batch for storing points.
type BatchFactory interface {
	// New returns a new Batch to add Points to
	New() Batch
}

// Benchmark is an interface that represents the skeleton of a program
// needed to run an insert or load benchmark.
type Benchmark interface {
	// GetDataSource returns the DataSource to use for this Benchmark
	GetDataSource() source.DataSource

	// GetBatchFactory returns the BatchFactory to use for this Benchmark
	GetBatchFactory() BatchFactory

	// GetPointIndexer returns the PointIndexer to use for this Benchmark
	GetPointIndexer(maxPartitions uint) PointIndexer

	// GetProcessor returns the Processor to use for this Benchmark
	GetProcessor() Processor

	// GetDBCreator returns the DBCreator to use for this Benchmark
	GetDBCreator() DBCreator
}
