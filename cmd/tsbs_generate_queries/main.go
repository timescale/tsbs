// tsbs_generate_queries generates queries for various use cases. Its output will
// be consumed by the corresponding tsbs_run_queries_ program.
package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
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
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
)

var useCaseMatrix = map[string]map[string]utils.QueryFillerMaker{
	"devops": {
		devops.LabelSingleGroupby + "-1-1-1":  devops.NewSingleGroupby(1, 1, 1),
		devops.LabelSingleGroupby + "-1-1-12": devops.NewSingleGroupby(1, 1, 12),
		devops.LabelSingleGroupby + "-1-8-1":  devops.NewSingleGroupby(1, 8, 1),
		devops.LabelSingleGroupby + "-5-1-1":  devops.NewSingleGroupby(5, 1, 1),
		devops.LabelSingleGroupby + "-5-1-12": devops.NewSingleGroupby(5, 1, 12),
		devops.LabelSingleGroupby + "-5-8-1":  devops.NewSingleGroupby(5, 8, 1),
		devops.LabelMaxAll + "-1":             devops.NewMaxAllCPU(1),
		devops.LabelMaxAll + "-8":             devops.NewMaxAllCPU(8),
		devops.LabelDoubleGroupby + "-1":      devops.NewGroupBy(1),
		devops.LabelDoubleGroupby + "-5":      devops.NewGroupBy(5),
		devops.LabelDoubleGroupby + "-all":    devops.NewGroupBy(devops.GetCPUMetricsLen()),
		devops.LabelGroupbyOrderbyLimit:       devops.NewGroupByOrderByLimit,
		devops.LabelHighCPU + "-all":          devops.NewHighCPU(0),
		devops.LabelHighCPU + "-1":            devops.NewHighCPU(1),
		devops.LabelLastpoint:                 devops.NewLastPointPerHost,
	},
}

const defaultWriteSize = 4 << 20 // 4 MB

// Program option vars:
var (
	fatal = log.Fatalf

	generator utils.DevopsGenerator
	filler    utils.QueryFiller

	queryCount int
	fileName   string

	seed  int64
	debug int

	timescaleUseJSON       bool
	timescaleUseTags       bool
	timescaleUseTimeBucket bool

	clickhouseUseTags bool

	interleavedGenerationGroupID uint
	interleavedGenerationGroups  uint
)

func getGenerator(format string, start, end time.Time, scale int) utils.DevopsGenerator {
	if format == "cassandra" {
		return cassandra.NewDevops(start, end, scale)
	} else if format == "clickhouse" {
		tgen := clickhouse.NewDevops(start, end, scale)
		tgen.UseTags = clickhouseUseTags
		return tgen
	} else if format == "influx" {
		return influx.NewDevops(start, end, scale)
	} else if format == "mongo" {
		return mongo.NewDevops(start, end, scale)
	} else if format == "mongo-naive" {
		return mongo.NewNaiveDevops(start, end, scale)
	} else if format == "siridb" {
		return siridb.NewDevops(start, end, scale)
	} else if format == "timescaledb" {
		tgen := timescaledb.NewDevops(start, end, scale)
		tgen.UseJSON = timescaleUseJSON
		tgen.UseTags = timescaleUseTags
		tgen.UseTimeBucket = timescaleUseTimeBucket
		return tgen
	}

	panic(fmt.Sprintf("no devops generator specified for format '%s'", format))
}

// GetBufferedWriter returns the buffered Writer that should be used for generated output
func GetBufferedWriter(fileName string) *bufio.Writer {
	// Prepare output file/STDOUT
	if len(fileName) > 0 {
		// Write output to file
		file, err := os.Create(fileName)
		if err != nil {
			fatal("cannot open file for write %s: %v", fileName, err)
		}
		return bufio.NewWriterSize(file, defaultWriteSize)
	}

	// Write output to STDOUT
	return bufio.NewWriterSize(os.Stdout, defaultWriteSize)
}

// Parse args:
func init() {
	useCaseMatrix["cpu-only"] = useCaseMatrix["devops"]
	// Change the Usage function to print the use case matrix of choices:
	oldUsage := flag.Usage
	flag.Usage = func() {
		oldUsage()

		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "The use case matrix of choices is:\n")
		for uc, queryTypes := range useCaseMatrix {
			for qt := range queryTypes {
				fmt.Fprintf(os.Stderr, "  use case: %s, query type: %s\n", uc, qt)
			}
		}
	}

	var format string
	var useCase string
	var queryType string
	var scale int
	var timestampStartStr string
	var timestampEndStr string

	flag.StringVar(&format, "format", "", "Format to emit. (Choices are in the use case matrix.)")
	flag.StringVar(&useCase, "use-case", "", "Use case to model. (Choices are in the use case matrix.)")
	flag.StringVar(&queryType, "query-type", "", "Query type. (Choices are in the use case matrix.)")

	flag.IntVar(&scale, "scale", 1, "Scaling variable (must be the equal to the scalevar used for data generation).")
	flag.IntVar(&queryCount, "queries", 1000, "Number of queries to generate.")

	flag.BoolVar(&timescaleUseJSON, "timescale-use-json", false, "TimescaleDB only: Use separate JSON tags table when querying")
	flag.BoolVar(&timescaleUseTags, "timescale-use-tags", true, "TimescaleDB only: Use separate tags table when querying")
	flag.BoolVar(&timescaleUseTimeBucket, "timescale-use-time-bucket", true, "TimescaleDB only: Use time bucket. Set to false to test on native PostgreSQL")

	flag.BoolVar(&clickhouseUseTags, "clickhouse-use-tags", true, "ClickHouse only: Use separate tags table when querying")

	flag.StringVar(&timestampStartStr, "timestamp-start", "2016-01-01T00:00:00Z", "Beginning timestamp (RFC3339).")
	flag.StringVar(&timestampEndStr, "timestamp-end", "2016-01-02T06:00:00Z", "Ending timestamp (RFC3339).")

	flag.Int64Var(&seed, "seed", 0, "PRNG seed (default, or 0, uses the current timestamp).")
	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1) (default 0).")

	flag.UintVar(&interleavedGenerationGroupID, "interleaved-generation-group-id", 0, "Group (0-indexed) to perform round-robin serialization within. Use this to scale up data generation to multiple processes.")
	flag.UintVar(&interleavedGenerationGroups, "interleaved-generation-groups", 1, "The number of round-robin serialization groups. Use this to scale up data generation to multiple processes.")

	flag.StringVar(&fileName, "file", "", "File name to write generated queries to")

	flag.Parse()

	if !(interleavedGenerationGroupID < interleavedGenerationGroups) {
		fatal("incorrect interleaved groups configuration")
	}

	if _, ok := useCaseMatrix[useCase]; !ok {
		fatal("invalid use case specifier: '%s'", useCase)
	}

	if _, ok := useCaseMatrix[useCase][queryType]; !ok {
		fatal("invalid query type specifier: '%s'", queryType)
	}

	// the default seed is the current timestamp:
	if seed == 0 {
		seed = int64(time.Now().Nanosecond())
	}
	fmt.Fprintf(os.Stderr, "using random seed %d\n", seed)

	// Parse timestamps:
	var err error
	timestampStart, err := time.Parse(time.RFC3339, timestampStartStr)
	if err != nil {
		fatal(err.Error())
	}
	timestampStart = timestampStart.UTC()
	timestampEnd, err := time.Parse(time.RFC3339, timestampEndStr)
	if err != nil {
		fatal(err.Error())
	}
	timestampEnd = timestampEnd.UTC()

	// Make the query generator:
	generator = getGenerator(format, timestampStart, timestampEnd, scale)
	filler = useCaseMatrix[useCase][queryType](generator)
}

func main() {
	rand.Seed(seed)
	// Set up bookkeeping:
	stats := make(map[string]int64)

	// Get output writer
	out := GetBufferedWriter(fileName)
	defer func() {
		err := out.Flush()
		if err != nil {
			fatal(err.Error())
		}
	}()

	// Create request instances, serializing them to stdout and collecting
	// counts for each kind. If applicable, only prints queries that
	// belong to this interleaved group id:
	currentInterleavedGroup := uint(0)

	enc := gob.NewEncoder(out)
	for i := 0; i < queryCount; i++ {
		q := generator.GenerateEmptyQuery()
		q = filler.Fill(q)

		if currentInterleavedGroup == interleavedGenerationGroupID {
			err := enc.Encode(q)
			if err != nil {
				fatal("encoder %v", err)
			}
			stats[string(q.HumanLabelName())]++

			if debug == 1 {
				_, err := fmt.Fprintf(os.Stderr, "%s\n", q.HumanLabelName())
				if err != nil {
					fatal(err.Error())
				}
			} else if debug == 2 {
				_, err := fmt.Fprintf(os.Stderr, "%s\n", q.HumanDescriptionName())
				if err != nil {
					fatal(err.Error())
				}
			} else if debug >= 3 {
				_, err := fmt.Fprintf(os.Stderr, "%s\n", q.String())
				if err != nil {
					fatal(err.Error())
				}
			}
		}
		q.Release()

		currentInterleavedGroup++
		if currentInterleavedGroup == interleavedGenerationGroups {
			currentInterleavedGroup = 0
		}
	}

	// Print stats:
	keys := []string{}
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, err := fmt.Fprintf(os.Stderr, "%s: %d points\n", k, stats[k])
		if err != nil {
			fatal(err.Error())
		}
	}
}
