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
)

var useCaseMatrix = map[string]map[string]QueryFillerMaker{
	"devops": {
		"1-host-1-hr":             NewDevopsSingleGroupby(1, 1, 1),
		"1-host-12-hr":            NewDevopsSingleGroupby(1, 1, 12),
		"8-host-1-hr":             NewDevopsSingleGroupby(1, 8, 1),
		"5-metrics-1-host-1-hr":   NewDevopsSingleGroupby(5, 1, 1),
		"5-metrics-1-host-12-hr":  NewDevopsSingleGroupby(5, 1, 12),
		"5-metrics-8-host-1-hr":   NewDevopsSingleGroupby(5, 8, 1),
		"cpu-max-all-single-host": NewDevopsMaxAllCPU(1),
		"cpu-max-all-eight-hosts": NewDevopsMaxAllCPU(1),
		"groupby":                 NewDevopsGroupBy(1),
		"groupby-5":               NewDevopsGroupBy(5),
		"groupby-all":             NewDevopsGroupBy(len(cpuMetrics)),
		"groupby-orderby-limit":   NewDevopsGroupByOrderByLimit,
		"high-cpu-all-hosts":      NewDevopsHighCPU(0),
		"high-cpu-1-host":         NewDevopsHighCPU(1),
		"lastpoint":               NewDevopsLastPointPerHost,
	},
}

// Program option vars:
var (
	generator DevopsGenerator
	filler    QueryFiller

	queryCount int

	seed  int64
	debug int

	timescaleUseJSON bool
	timescaleUseTags bool

	interleavedGenerationGroupID uint
	interleavedGenerationGroups  uint
)

func getGenerator(format string, start, end time.Time, scale int) DevopsGenerator {
	if format == "cassandra" {
		return newCassandraDevopsCommon(start, end, scale)
	} else if format == "influx" {
		return newInfluxDevopsCommon(start, end, scale)
	} else if format == "mongo" {
		return newMongoDevopsCommon(start, end, scale)
	} else if format == "mongo-naive" {
		return newMongoNaiveDevopsCommon(start, end, scale)
	} else if format == "timescaledb" {
		tgen := newTimescaleDBDevopsCommon(start, end, scale)
		tgen.useJSON = timescaleUseJSON
		tgen.useTags = timescaleUseTags
		return tgen
	}

	panic(fmt.Sprintf("no devops generator specified for format '%s'", format))
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

	var useCase, queryType, format, timestampStartStr, timestampEndStr string
	var scaleVar int

	flag.StringVar(&format, "format", "", "Format to emit. (Choices are in the use case matrix.)")
	flag.StringVar(&useCase, "use-case", "", "Use case to model. (Choices are in the use case matrix.)")
	flag.StringVar(&queryType, "query-type", "", "Query type. (Choices are in the use case matrix.)")

	flag.IntVar(&scaleVar, "scale-var", 1, "Scaling variable (must be the equal to the scalevar used for data generation).")
	flag.IntVar(&queryCount, "queries", 1000, "Number of queries to generate.")

	flag.BoolVar(&timescaleUseJSON, "timescale-use-json", false, "Use separate JSON tags table when querying")
	flag.BoolVar(&timescaleUseTags, "timescale-use-tags", true, "Use separate tags table when querying")

	flag.StringVar(&timestampStartStr, "timestamp-start", "2016-01-01T00:00:00Z", "Beginning timestamp (RFC3339).")
	flag.StringVar(&timestampEndStr, "timestamp-end", "2016-01-02T06:00:00Z", "Ending timestamp (RFC3339).")

	flag.Int64Var(&seed, "seed", 0, "PRNG seed (default, or 0, uses the current timestamp).")
	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1) (default 0).")

	flag.UintVar(&interleavedGenerationGroupID, "interleaved-generation-group-id", 0, "Group (0-indexed) to perform round-robin serialization within. Use this to scale up data generation to multiple processes.")
	flag.UintVar(&interleavedGenerationGroups, "interleaved-generation-groups", 1, "The number of round-robin serialization groups. Use this to scale up data generation to multiple processes.")

	flag.Parse()

	if !(interleavedGenerationGroupID < interleavedGenerationGroups) {
		log.Fatal("incorrect interleaved groups configuration")
	}

	if _, ok := useCaseMatrix[useCase]; !ok {
		log.Fatalf("invalid use case specifier: '%s'", useCase)
	}

	if _, ok := useCaseMatrix[useCase][queryType]; !ok {
		log.Fatalf("invalid query type specifier: '%s'", queryType)
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
		log.Fatal(err)
	}
	timestampStart = timestampStart.UTC()
	timestampEnd, err := time.Parse(time.RFC3339, timestampEndStr)
	if err != nil {
		log.Fatal(err)
	}
	timestampEnd = timestampEnd.UTC()

	// Make the query generator:
	generator = getGenerator(format, timestampStart, timestampEnd, scaleVar)
	filler = useCaseMatrix[useCase][queryType](generator)
}

func main() {
	rand.Seed(seed)
	// Set up bookkeeping:
	stats := make(map[string]int64)

	// Set up output buffering:
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()

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
				log.Fatal("encoder ", err)
			}
			stats[string(q.HumanLabelName())]++

			if debug == 1 {
				_, err := fmt.Fprintf(os.Stderr, "%s\n", q.HumanLabelName())
				if err != nil {
					log.Fatal(err)
				}
			} else if debug == 2 {
				_, err := fmt.Fprintf(os.Stderr, "%s\n", q.HumanDescriptionName())
				if err != nil {
					log.Fatal(err)
				}
			} else if debug >= 3 {
				_, err := fmt.Fprintf(os.Stderr, "%s\n", q.String())
				if err != nil {
					log.Fatal(err)
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
			log.Fatal(err)
		}
	}
}
