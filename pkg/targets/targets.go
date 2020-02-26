package targets

import (
	"bufio"
	"github.com/spf13/viper"
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
	Append(*Point)
}

// Point acts as a 'holder' for the internal representation of a point in a given load client.
// Instead of using interface{} as a return type, we get compile safety by using Point
type Point struct {
	Data interface{}
}

// NewPoint creates a Point with the provided data as the internal representation
func NewPoint(data interface{}) *Point {
	return &Point{Data: data}
}

// PointIndexer determines the index of the Batch (and subsequently the channel)
// that a particular point belongs to
type PointIndexer interface {
	// GetIndex returns a partition for the given Point
	GetIndex(*Point) int
}

// ConstantIndexer always puts the item on a single channel. This is the typical
// use case where all the workers share the same channel
type ConstantIndexer struct{}

// GetIndex returns a constant index (0) regardless of Point
func (i *ConstantIndexer) GetIndex(_ *Point) int {
	return 0
}

// BatchFactory returns a new empty batch for storing points.
type BatchFactory interface {
	// New returns a new Batch to add Points to
	New() Batch
}

// PointDecoder decodes the next data point in the process of scanning.
type PointDecoder interface {
	//Decode creates a Point from a data stream
	Decode(*bufio.Reader) *Point
}

// Benchmark is an interface that represents the skeleton of a program
// needed to run an insert or load benchmark.
type Benchmark interface {
	// GetPointDecoder returns the PointDecoder to use for this Benchmark
	GetPointDecoder(br *bufio.Reader) PointDecoder

	// GetBatchFactory returns the BatchFactory to use for this Benchmark
	GetBatchFactory() BatchFactory

	// GetPointIndexer returns the PointIndexer to use for this Benchmark
	GetPointIndexer(maxPartitions uint) PointIndexer

	// GetProcessor returns the Processor to use for this Benchmark
	GetProcessor() Processor

	// GetDBCreator returns the DBCreator to use for this Benchmark
	GetDBCreator() DBCreator
}

