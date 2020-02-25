package inputs

import (
	"bufio"
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	serialize2 "github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/data/usecases/devops"
	"github.com/timescale/tsbs/pkg/data/usecases/iot"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/akumuli"
	"github.com/timescale/tsbs/pkg/targets/cassandra"
	"github.com/timescale/tsbs/pkg/targets/crate"
	"github.com/timescale/tsbs/pkg/targets/influx"
	"github.com/timescale/tsbs/pkg/targets/mongo"
	"github.com/timescale/tsbs/pkg/targets/prometheus"
	"github.com/timescale/tsbs/pkg/targets/siridb"
	"github.com/timescale/tsbs/pkg/targets/timescaledb"
	"io"
	"math"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/spf13/pflag"
)

// Error messages when using a DataGenerator
const (
	ErrNoConfig          = "no GeneratorConfig provided"
	ErrInvalidDataConfig = "invalid config: DataGenerator needs a DataGeneratorConfig"

	errLogIntervalZero     = "cannot have log interval of 0"
	errTotalGroupsZero     = "incorrect interleaved groups configuration: total groups = 0"
	errInvalidGroupsFmt    = "incorrect interleaved groups configuration: id %d >= total groups %d"
	errCannotParseTimeFmt  = "cannot parse time from string '%s': %v"
	errMaxMetricCountValue = "max metric count per host has to be greater than 0"
)

const defaultLogInterval = 10 * time.Second

// DataGeneratorConfig is the GeneratorConfig that should be used with a
// DataGenerator. It includes all the fields from a BaseConfig, as well as some
// options that are specific to generating the data for database write operations,
// such as the initial scale and how spaced apart data points should be in time.
type DataGeneratorConfig struct {
	BaseConfig
	Limit                 uint64        `mapstructure:"max-data-points"`
	InitialScale          uint64        `mapstructure:"initial-scale"`
	LogInterval           time.Duration `mapstructure:"log-interval"`
	InterleavedGroupID    uint          `mapstructure:"interleaved-generation-group-id"`
	InterleavedNumGroups  uint          `mapstructure:"interleaved-generation-groups"`
	MaxMetricCountPerHost uint64        `mapstructure:"max-metric-count"`
}

// Validate checks that the values of the DataGeneratorConfig are reasonable.
func (c *DataGeneratorConfig) Validate() error {
	err := c.BaseConfig.Validate()
	if err != nil {
		return err
	}

	if c.InitialScale == 0 {
		c.InitialScale = c.BaseConfig.Scale
	}

	if c.LogInterval == 0 {
		return fmt.Errorf(errLogIntervalZero)
	}

	err = validateGroups(c.InterleavedGroupID, c.InterleavedNumGroups)

	if c.Use == useCaseDevopsGeneric && c.MaxMetricCountPerHost < 1 {
		return fmt.Errorf(errMaxMetricCountValue)
	}

	return err
}

func (c *DataGeneratorConfig) AddToFlagSet(fs *pflag.FlagSet) {
	c.BaseConfig.AddToFlagSet(fs)
	fs.Uint64("max-data-points", 0, "Limit the number of data points to generate, 0 = no limit")
	fs.Uint64("initial-scale", 0, "Initial scaling variable specific to the use case (e.g., devices in 'devops'). 0 means to use -scale value")
	fs.Duration("log-interval", defaultLogInterval, "Duration between data points")

	fs.Uint("interleaved-generation-group-id", 0,
		"Group (0-indexed) to perform round-robin serialization within. Use this to scale up data generation to multiple processes.")
	fs.Uint("interleaved-generation-groups", 1,
		"The number of round-robin serialization groups. Use this to scale up data generation to multiple processes.")
	fs.Uint64("max-metric-count", 0, "Max number of metric fields to generate per host. Used only in devops-generic use-case")
}

// DataGenerator is a type of Generator for creating data that will be consumed
// by a database's write/insert operations. The output is specific to the type
// of database, but is consumed by TSBS loaders like tsbs_load_timescaledb.
type DataGenerator struct {
	// Out is the writer where data should be written. If nil, it will be
	// os.Stdout unless File is specified in the GeneratorConfig passed to
	// Generate.
	Out io.Writer

	config  *DataGeneratorConfig
	tsStart time.Time
	tsEnd   time.Time

	// bufOut represents the buffered writer that should actually be passed to
	// any operations that write out data.
	bufOut *bufio.Writer
}

func (g *DataGenerator) init(config GeneratorConfig) error {
	if config == nil {
		return fmt.Errorf(ErrNoConfig)
	}
	switch config.(type) {
	case *DataGeneratorConfig:
	default:
		return fmt.Errorf(ErrInvalidDataConfig)
	}
	g.config = config.(*DataGeneratorConfig)

	err := g.config.Validate()
	if err != nil {
		return err
	}

	g.tsStart, err = ParseUTCTime(g.config.TimeStart)
	if err != nil {
		return fmt.Errorf(errCannotParseTimeFmt, g.config.TimeStart, err)
	}
	g.tsEnd, err = ParseUTCTime(g.config.TimeEnd)
	if err != nil {
		return fmt.Errorf(errCannotParseTimeFmt, g.config.TimeEnd, err)
	}

	if g.Out == nil {
		g.Out = os.Stdout
	}
	g.bufOut, err = getBufferedWriter(g.config.File, g.Out)
	if err != nil {
		return err
	}

	return nil
}

func (g *DataGenerator) Generate(config GeneratorConfig) error {
	err := g.init(config)
	if err != nil {
		return err
	}

	rand.Seed(g.config.Seed)

	scfg, err := g.getSimulatorConfig(g.config)
	if err != nil {
		return err
	}

	sim := scfg.NewSimulator(g.config.LogInterval, g.config.Limit)
	serializer, err := g.getSerializer(sim, g.config.Format)
	if err != nil {
		return err
	}

	return g.runSimulator(sim, serializer, g.config)
}

func (g *DataGenerator) runSimulator(sim common.Simulator, serializer serialize2.PointSerializer, dgc *DataGeneratorConfig) error {
	defer g.bufOut.Flush()

	currGroupID := uint(0)
	point := data.NewPoint()
	for !sim.Finished() {
		write := sim.Next(point)
		if !write {
			point.Reset()
			continue
		}

		// in the default case this is always true
		if currGroupID == dgc.InterleavedGroupID {
			err := serializer.Serialize(point, g.bufOut)
			if err != nil {
				return fmt.Errorf("can not serialize point: %s", err)
			}
		}
		point.Reset()

		currGroupID = (currGroupID + 1) % dgc.InterleavedNumGroups
	}
	return nil
}

func (g *DataGenerator) getSimulatorConfig(dgc *DataGeneratorConfig) (common.SimulatorConfig, error) {
	var ret common.SimulatorConfig
	var err error
	switch dgc.Use {
	case useCaseDevops:
		ret = &devops.DevopsSimulatorConfig{
			Start: g.tsStart,
			End:   g.tsEnd,

			InitHostCount:   dgc.InitialScale,
			HostCount:       dgc.Scale,
			HostConstructor: devops.NewHost,
		}
	case useCaseIoT:
		ret = &iot.SimulatorConfig{
			Start: g.tsStart,
			End:   g.tsEnd,

			InitGeneratorScale:   dgc.InitialScale,
			GeneratorScale:       dgc.Scale,
			GeneratorConstructor: iot.NewTruck,
		}
	case useCaseCPUOnly:
		ret = &devops.CPUOnlySimulatorConfig{
			Start: g.tsStart,
			End:   g.tsEnd,

			InitHostCount:   dgc.InitialScale,
			HostCount:       dgc.Scale,
			HostConstructor: devops.NewHostCPUOnly,
		}
	case useCaseCPUSingle:
		ret = &devops.CPUOnlySimulatorConfig{
			Start: g.tsStart,
			End:   g.tsEnd,

			InitHostCount:   dgc.InitialScale,
			HostCount:       dgc.Scale,
			HostConstructor: devops.NewHostCPUSingle,
		}
	case useCaseDevopsGeneric:
		if dgc.InitialScale == dgc.Scale {
			// if no initial scale argument given we will start with 50%. The ower bound is 1
			dgc.InitialScale = uint64(math.Max(float64(1), float64(dgc.Scale/2)))
		}
		ret = &devops.GenericMetricsSimulatorConfig{
			DevopsSimulatorConfig: &devops.DevopsSimulatorConfig{
				Start: g.tsStart,
				End:   g.tsEnd,

				InitHostCount:   dgc.InitialScale,
				HostCount:       dgc.Scale,
				HostConstructor: devops.NewHostGenericMetrics,
				MaxMetricCount:  dgc.MaxMetricCountPerHost,
			},
		}
	default:
		err = fmt.Errorf("unknown use case: '%s'", dgc.Use)
	}
	return ret, err
}

func (g *DataGenerator) getSerializer(sim common.Simulator, format string) (serialize2.PointSerializer, error) {
	var ret serialize2.PointSerializer
	var err error

	switch format {
	case targets.FormatCassandra:
		ret = &cassandra.Serializer{}
	case targets.FormatInflux:
		fallthrough
	case FormatVictoriaMetrics:
		ret = &influx.Serializer{}
	case targets.FormatMongo:
		ret = &mongo.Serializer{}
	case targets.FormatSiriDB:
		ret = &siridb.Serializer{}
	case targets.FormatAkumuli:
		ret = akumuli.NewAkumuliSerializer()
	case targets.FormatCrateDB:
		g.writeHeader(sim)
		ret = &crate.Serializer{}
	case targets.FormatClickhouse:
		fallthrough
	case targets.FormatTimescaleDB:
		g.writeHeader(sim)
		ret = &timescaledb.Serializer{}
	case targets.FormatPrometheus:
		ret, err = prometheus.NewPrometheusSerializer(g.bufOut)
	default:
		err = fmt.Errorf(errUnknownFormatFmt, format)
	}

	return ret, err
}

func (g *DataGenerator) writeHeader(sim common.Simulator) {
	g.bufOut.WriteString("tags")
	types := sim.TagTypes()
	for i, key := range sim.TagKeys() {
		g.bufOut.WriteString(",")
		g.bufOut.Write(key)
		g.bufOut.WriteString(" ")
		g.bufOut.WriteString(types[i].String())
	}
	g.bufOut.WriteString("\n")
	// sort the keys so the header is deterministic
	keys := make([]string, 0)
	fields := sim.Fields()
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, measurementName := range keys {
		g.bufOut.WriteString(measurementName)
		for _, field := range fields[measurementName] {
			g.bufOut.WriteString(",")
			g.bufOut.Write(field)
		}
		g.bufOut.WriteString("\n")
	}
	g.bufOut.WriteString("\n")
}
