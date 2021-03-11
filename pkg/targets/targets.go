package targets

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

type ImplementedTarget interface {
	Benchmark(targetDB string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper) (Benchmark, error)
	Serializer() serialize.PointSerializer
	// TargetSpecificFlags adds to the supplied flagSet a number of target-specific
	// flags that will be enabled only when executing a command for this specific
	// target database.
	// flagPrefix is a string that should be concatenated with the names of all flags defined here
	// it is needed to prevent namespace collisions and the ability to override properties
	// defined in the yaml config
	TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet)
	TargetName() string
}

// Batch is an aggregate of points for a particular data system.
// It needs to have a way to measure it's size to make sure
// it does not get too large and it needs a way to append a point
type Batch interface {
	Len() uint
	Append(data.LoadedPoint)
}

// PointIndexer determines the index of the Batch (and subsequently the channel)
// that a particular point belongs to
type PointIndexer interface {
	// GetIndex returns a partition for the given Point
	GetIndex(data.LoadedPoint) uint
}

// ConstantIndexer always puts the item on a single channel. This is the typical
// use case where all the workers share the same channel
type ConstantIndexer struct{}

// GetIndex returns a constant index (0) regardless of Point
func (i *ConstantIndexer) GetIndex(_ data.LoadedPoint) uint {
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
	GetDataSource() DataSource

	// GetBatchFactory returns the BatchFactory to use for this Benchmark
	GetBatchFactory() BatchFactory

	// GetPointIndexer returns the PointIndexer to use for this Benchmark
	GetPointIndexer(maxPartitions uint) PointIndexer

	// GetProcessor returns the Processor to use for this Benchmark
	GetProcessor() Processor

	// GetDBCreator returns the DBCreator to use for this Benchmark
	GetDBCreator() DBCreator
}

type DataSource interface {
	NextItem() data.LoadedPoint
	Headers() *common.GeneratedDataHeaders
}
