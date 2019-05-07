package inputs

import (
	"bufio"
	"encoding/gob"
	"flag"
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases/cratedb"
	"io"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases/cassandra"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases/clickhouse"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases/influx"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases/mongo"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases/siridb"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases/timescaledb"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
)

// Error messages when using a QueryGenerator
const (
	ErrInvalidQueryConfig = "invalid config: QueryGenerator needs a QueryGeneratorConfig"
	ErrEmptyQueryType     = "query type cannot be empty"

	errBadQueryTypeFmt        = "invalid query type for use case '%s': '%s'"
	errCouldNotDebugFmt       = "could not write debug output: %v"
	errCouldNotEncodeQueryFmt = "could not encode query: %v"
	errCouldNotQueryStatsFmt  = "could not output query stats: %v"
)

// QueryGeneratorConfig is the GeneratorConfig that should be used with a
// QueryGenerator. It includes all the fields from a BaseConfig, as well as
// options that are specific to generating the queries to test against a
// database, such as the query type and individual database options.
type QueryGeneratorConfig struct {
	BaseConfig
	QueryType            string
	InterleavedGroupID   uint
	InterleavedNumGroups uint

	// TODO - I think this needs some rethinking, but a simple, elegant solution escapes me right now
	TimescaleUseJSON       bool
	TimescaleUseTags       bool
	TimescaleUseTimeBucket bool

	ClickhouseUseTags bool

	MongoUseNaive bool
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

	err = validateGroups(c.InterleavedGroupID, c.InterleavedNumGroups)
	return err
}

func (c *QueryGeneratorConfig) AddToFlagSet(fs *flag.FlagSet) {
	c.BaseConfig.AddToFlagSet(fs)
	flag.StringVar(&c.QueryType, "query-type", "", "Query type. (Choices are in the use case matrix.)")

	flag.UintVar(&c.InterleavedGroupID, "interleaved-generation-group-id", 0,
		"Group (0-indexed) to perform round-robin serialization within. Use this to scale up data generation to multiple processes.")
	flag.UintVar(&c.InterleavedNumGroups, "interleaved-generation-groups", 1,
		"The number of round-robin serialization groups. Use this to scale up data generation to multiple processes.")

}

// QueryGenerator is a type of Generator for creating queries to test against a
// database. The output is specific to the type of database (due to each using
// different querying techniques, e.g. SQL or REST), but is consumed by TSBS
// query runners like tsbs_run_queries_timescaledb.
type QueryGenerator struct {
	// Out is the writer where data should be written. If nil, it will be
	// os.Stdout unless File is specified in the GeneratorConfig passed to
	// Generate.
	Out io.Writer
	// DebugOut is where non-generated messages should be written. If nil, it
	// will be os.Stderr.
	DebugOut io.Writer

	config        *QueryGeneratorConfig
	useCaseMatrix map[string]map[string]utils.QueryFillerMaker
	tsStart       time.Time
	tsEnd         time.Time

	// bufOut represents the buffered writer that should actually be passed to
	// any operations that write out data.
	bufOut *bufio.Writer
}

// NewQueryGenerator returns a QueryGenerator that is set up to work with a given
// useCaseMatrix, which tells it how to generate the given query type for a use
// case.
func NewQueryGenerator(useCaseMatrix map[string]map[string]utils.QueryFillerMaker) *QueryGenerator {
	return &QueryGenerator{
		useCaseMatrix: useCaseMatrix,
	}
}

func (g *QueryGenerator) Generate(config GeneratorConfig) error {
	err := g.init(config)
	if err != nil {
		return err
	}

	useGen, err := g.getUseCaseGenerator(g.config)
	if err != nil {
		return err
	}

	filler := g.useCaseMatrix[g.config.Use][g.config.QueryType](useGen)

	return g.runQueryGeneration(useGen, filler, g.config)
}

func (g *QueryGenerator) init(config GeneratorConfig) error {
	if config == nil {
		return fmt.Errorf(ErrNoConfig)
	}
	switch config.(type) {
	case *QueryGeneratorConfig:
	default:
		return fmt.Errorf(ErrInvalidDataConfig)
	}
	g.config = config.(*QueryGeneratorConfig)

	err := g.config.Validate()
	if err != nil {
		return err
	}

	if _, ok := g.useCaseMatrix[g.config.Use]; !ok {
		return fmt.Errorf(errBadUseFmt, g.config.Use)
	}

	if _, ok := g.useCaseMatrix[g.config.Use][g.config.QueryType]; !ok {
		return fmt.Errorf(errBadQueryTypeFmt, g.config.Use, g.config.QueryType)
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

	if g.DebugOut == nil {
		g.DebugOut = os.Stderr
	}

	return nil
}

func (g *QueryGenerator) getUseCaseGenerator(c *QueryGeneratorConfig) (utils.QueryGenerator, error) {
	var ret utils.QueryGenerator
	scale := int(c.Scale) // TODO: make all the Devops constructors use a uint64

	switch c.Format {
	case FormatCassandra:
		ret = cassandra.NewDevops(g.tsStart, g.tsEnd, scale)
	case FormatClickhouse:
		temp := clickhouse.NewDevops(g.tsStart, g.tsEnd, scale)
		temp.UseTags = c.ClickhouseUseTags
		ret = temp
	case FormatInflux:
		ret = influx.NewDevops(g.tsStart, g.tsEnd, scale)
	case FormatMongo:
		if c.MongoUseNaive {
			ret = mongo.NewNaiveDevops(g.tsStart, g.tsEnd, scale)
		} else {
			ret = mongo.NewDevops(g.tsStart, g.tsEnd, scale)
		}
	case FormatSiriDB:
		ret = siridb.NewDevops(g.tsStart, g.tsEnd, scale)
	case FormatCrateDB:
		ret = cratedb.NewDevops(g.tsStart, g.tsEnd, scale)
	case FormatTimescaleDB:
		temp := timescaledb.NewDevops(g.tsStart, g.tsEnd, scale)
		temp.UseJSON = c.TimescaleUseJSON
		temp.UseTags = c.TimescaleUseTags
		temp.UseTimeBucket = c.TimescaleUseTimeBucket
		ret = temp
	default:
		return nil, fmt.Errorf(errUnknownFormatFmt, c.Format)
	}
	return ret, nil
}

func (g *QueryGenerator) runQueryGeneration(useGen utils.QueryGenerator, filler utils.QueryFiller, c *QueryGeneratorConfig) error {
	stats := make(map[string]int64)
	currentGroup := uint(0)
	enc := gob.NewEncoder(g.bufOut)
	defer g.bufOut.Flush()

	rand.Seed(g.config.Seed)
	//fmt.Println(g.config.Seed)
	if g.config.Debug > 0 {
		_, err := fmt.Fprintf(g.DebugOut, "using random seed %d\n", g.config.Seed)
		if err != nil {
			return fmt.Errorf(errCouldNotDebugFmt, err)
		}
	}

	for i := 0; i < int(c.Limit); i++ {
		q := useGen.GenerateEmptyQuery()
		q = filler.Fill(q)

		if currentGroup == c.InterleavedGroupID {
			err := enc.Encode(q)
			if err != nil {
				return fmt.Errorf(errCouldNotEncodeQueryFmt, err)
			}
			stats[string(q.HumanLabelName())]++

			if c.Debug > 0 {
				var debugMsg string
				if c.Debug == 1 {
					debugMsg = string(q.HumanLabelName())
				} else if c.Debug == 2 {
					debugMsg = string(q.HumanDescriptionName())
				} else if c.Debug >= 3 {
					debugMsg = q.String()
				}

				_, err = fmt.Fprintf(g.DebugOut, debugMsg+"\n")
				if err != nil {
					return fmt.Errorf(errCouldNotDebugFmt, err)
				}
			}
		}
		q.Release()

		currentGroup++
		if currentGroup == c.InterleavedNumGroups {
			currentGroup = 0
		}
	}

	// Print stats:
	keys := []string{}
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, err := fmt.Fprintf(g.DebugOut, "%s: %d points\n", k, stats[k])
		if err != nil {
			return fmt.Errorf(errCouldNotQueryStatsFmt, err)
		}
	}
	return nil
}
