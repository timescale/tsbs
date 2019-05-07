package inputs

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

// Error messages when using a DataGenerator
const (
	ErrNoConfig          = "no GeneratorConfig provided"
	ErrInvalidDataConfig = "invalid config: DataGenerator needs a DataGeneratorConfig"

	errLogIntervalZero    = "cannot have log interval of 0"
	errTotalGroupsZero    = "incorrect interleaved groups configuration: total groups = 0"
	errInvalidGroupsFmt   = "incorrect interleaved groups configuration: id %d >= total groups %d"
	errCannotParseTimeFmt = "cannot parse time from string '%s': %v"
)

const defaultLogInterval = 10 * time.Second

// DataGeneratorConfig is the GeneratorConfig that should be used with a
// DataGenerator. It includes all the fields from a BaseConfig, as well as some
// options that are specific to generating the data for database write operations,
// such as the initial scale and how spaced apart data points should be in time.
type DataGeneratorConfig struct {
	BaseConfig
	InitialScale         uint64
	LogInterval          time.Duration
	InterleavedGroupID   uint
	InterleavedNumGroups uint
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
	return err
}

func (c *DataGeneratorConfig) AddToFlagSet(fs *flag.FlagSet) {
	c.BaseConfig.AddToFlagSet(fs)
	flag.Uint64Var(&c.InitialScale, "initial-scale", 0, "Initial scaling variable specific to the use case (e.g., devices in 'devops'). 0 means to use -scale value")
	flag.DurationVar(&c.LogInterval, "log-interval", defaultLogInterval, "Duration between host data points")

	flag.UintVar(&c.InterleavedGroupID, "interleaved-generation-group-id", 0,
		"Group (0-indexed) to perform round-robin serialization within. Use this to scale up data generation to multiple processes.")
	flag.UintVar(&c.InterleavedNumGroups, "interleaved-generation-groups", 1,
		"The number of round-robin serialization groups. Use this to scale up data generation to multiple processes.")

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

func (g *DataGenerator) runSimulator(sim common.Simulator, serializer serialize.PointSerializer, dgc *DataGeneratorConfig) error {
	defer g.bufOut.Flush()

	currGroupID := uint(0)
	point := serialize.NewPoint()
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
	default:
		err = fmt.Errorf("unknown use case: '%s'", dgc.Use)
	}
	return ret, err
}

func (g *DataGenerator) getSerializer(sim common.Simulator, format string) (serialize.PointSerializer, error) {
	var ret serialize.PointSerializer
	var err error

	switch format {
	case FormatCassandra:
		ret = &serialize.CassandraSerializer{}
	case FormatInflux:
		ret = &serialize.InfluxSerializer{}
	case FormatMongo:
		ret = &serialize.MongoSerializer{}
	case FormatSiriDB:
		ret = &serialize.SiriDBSerializer{}
	case FormatCrateDB:
		g.writeHeader(sim)
		ret = &serialize.CrateDBSerializer{}
	case FormatClickhouse:
		fallthrough
	case FormatTimescaleDB:
		g.writeHeader(sim)
		ret = &serialize.TimescaleDBSerializer{}
	default:
		err = fmt.Errorf(errUnknownFormatFmt, format)
	}

	return ret, err
}

func (g *DataGenerator) writeHeader(sim common.Simulator) {
	g.bufOut.WriteString("tags")
	for _, key := range sim.TagKeys() {
		g.bufOut.WriteString(",")
		g.bufOut.Write(key)
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
