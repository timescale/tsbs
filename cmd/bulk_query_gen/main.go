// bulk_query_gen generates queries for various use cases. Its output will
// be consumed by query_benchmarker.
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

var measurements = []string{
	"redis",
	"net",
	"nginx",
	"postgresl",
	"kernel",
	"mem",
	"cpu",
	"diskio",
	"disk",
}

// query generator choices {use-case, query-type, format}
// (This object is shown to the user when flag.Usage is called.)
var useCaseMatrix = map[string]map[string]map[string]QueryGeneratorMaker{
	"devops": {
		"1-host-1-hr": {
			"cassandra":   NewCassandraDevopsSingleHost,
			"es-http":     NewElasticSearchDevopsSingleHost,
			"influx-http": NewInfluxDevopsSingleHost,
			"mongo":       NewMongoDevopsSingleHost,
			"opentsdb":    NewOpenTSDBDevopsSingleHost,
			"timescaledb": NewTimescaleDBDevopsSingleHost,
		},
		"1-host-12-hr": {
			"cassandra":   NewCassandraDevopsSingleHost12hr,
			"es-http":     NewElasticSearchDevopsSingleHost12hr,
			"influx-http": NewInfluxDevopsSingleHost12hr,
			"mongo":       NewMongoDevopsSingleHost12hr,
			"opentsdb":    NewOpenTSDBDevopsSingleHost12hr,
			"timescaledb": NewTimescaleDBDevopsSingleHost12hr,
		},
		"8-host-1-hr": {
			"cassandra":   NewCassandraDevops8Hosts,
			"es-http":     NewElasticSearchDevops8Hosts,
			"influx-http": NewInfluxDevops8Hosts,
			"mongo":       NewMongoDevops8Hosts1Hr,
			"opentsdb":    NewOpenTSDBDevops8Hosts,
			"timescaledb": NewTimescaleDBDevops8Hosts,
		},
		"groupby": {
			"cassandra":   NewCassandraDevopsGroupBy,
			"es-http":     NewElasticSearchDevopsGroupBy,
			"influx-http": NewInfluxDevopsGroupBy,
			"timescaledb": NewTimescaleDBDevopsGroupBy,
		},
		"5-metrics-1-host-1-hr": {
			"cassandra":   NewCassandraDevops5Metrics(1, 1),
			"influx-http": NewInfluxDevops5Metrics1Host1Hr,
			"timescaledb": NewTimescaleDBDevops5Metrics(1, 1),
		},
		"5-metrics-1-host-12-hr": {
			"cassandra":   NewCassandraDevops5Metrics(1, 12),
			"influx-http": NewInfluxDevops5Metrics1Host12Hrs,
			"timescaledb": NewTimescaleDBDevops5Metrics(1, 12),
		},
		"5-metrics-8-host-1-hr": {
			"cassandra":   NewCassandraDevops5Metrics(8, 1),
			"influx-http": NewInfluxDevops5Metrics8Hosts1Hr,
			"timescaledb": NewTimescaleDBDevops5Metrics(8, 1),
		},
		"lastpoint": {
			"cassandra":   NewCassandraDevopsLastPointPerHost,
			"timescaledb": NewTimescaleDBDevopsLastPointPerHost,
			"influx-http": NewInfluxDevopsLastPointPerHost,
		},
		"high-cpu": {
			"cassandra":   NewCassandraDevopsHighCPU,
			"influx-http": NewInfluxDevopsHighCPU,
			"timescaledb": NewTimescaleDBDevopsHighCPU,
		},
		"high-cpu-and-field": {
			"cassandra":   NewCassandraDevopsHighCPUAndField,
			"influx-http": NewInfluxDevopsHighCPUAndField,
			"timescaledb": NewTimescaleDBDevopsHighCPUAndField,
		},
		"multiple-ors": {
			"influx-http": NewInfluxDevopsMultipleOrs,
			"timescaledb": NewTimescaleDBDevopsMultipleOrs,
		},
		"multiple-ors-by-host": {
			"influx-http": NewInfluxDevopsMultipleOrsByHost,
			"timescaledb": NewTimescaleDBDevopsMultipleOrsByHost,
		},
		"cpu-max-all-single-host": {
			"cassandra":   NewCassandraDevopsAllMaxCPU(1),
			"influx-http": NewInfluxDevopsAllMaxCPUOneHost,
			"timescaledb": NewTimescaleDBDevopsAllMaxCPU(1),
		},
		"cpu-max-all-eight-hosts": {
			"cassandra":   NewCassandraDevopsAllMaxCPU(8),
			"influx-http": NewInfluxDevopsAllMaxCPUEightHosts,
			"timescaledb": NewTimescaleDBDevopsAllMaxCPU(8),
		},
		"groupby-orderby-limit": {
			"cassandra":   NewCassandraDevopsGroupByOrderByLimit,
			"influx-http": NewInfluxDevopsGroupByOrderByLimit,
			"timescaledb": NewTimescaleDBDevopsGroupByOrderByLimit,
		},
	},
}

// Program option vars:
var (
	useCase   string
	queryType string
	format    string

	scaleVar   int
	queryCount int

	dbName string // TODO(rw): make this a map[string]string -> DatabaseConfig

	timestampStartStr string
	timestampEndStr   string

	timestampStart time.Time
	timestampEnd   time.Time

	seed  int64
	debug int

	interleavedGenerationGroupID uint
	interleavedGenerationGroups  uint
)

// Parse args:
func init() {
	// Change the Usage function to print the use case matrix of choices:
	oldUsage := flag.Usage
	flag.Usage = func() {
		oldUsage()

		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "The use case matrix of choices is:\n")
		for uc, queryTypes := range useCaseMatrix {
			for qt, formats := range queryTypes {
				for f := range formats {
					fmt.Fprintf(os.Stderr, "  use case: %s, query type: %s, format: %s\n", uc, qt, f)
				}
			}
		}
	}

	flag.StringVar(&format, "format", "influx-http", "Format to emit. (Choices are in the use case matrix.)")
	flag.StringVar(&useCase, "use-case", "devops", "Use case to model. (Choices are in the use case matrix.)")
	flag.StringVar(&queryType, "query-type", "", "Query type. (Choices are in the use case matrix.)")

	flag.IntVar(&scaleVar, "scale-var", 1, "Scaling variable (must be the equal to the scalevar used for data generation).")
	flag.IntVar(&queryCount, "queries", 1000, "Number of queries to generate.")

	flag.StringVar(&dbName, "db", "benchmark_db", "Database for influx to use (ignored for ElasticSearch).")

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
		log.Fatal("invalid use case specifier")
	}

	if _, ok := useCaseMatrix[useCase][queryType]; !ok {
		log.Fatal("invalid query type specifier")
	}

	if _, ok := useCaseMatrix[useCase][queryType][format]; !ok {
		log.Fatal("invalid format specifier")
	}

	// the default seed is the current timestamp:
	if seed == 0 {
		seed = int64(time.Now().Nanosecond())
	}
	fmt.Fprintf(os.Stderr, "using random seed %d\n", seed)

	// Parse timestamps:
	var err error
	timestampStart, err = time.Parse(time.RFC3339, timestampStartStr)
	if err != nil {
		log.Fatal(err)
	}
	timestampStart = timestampStart.UTC()
	timestampEnd, err = time.Parse(time.RFC3339, timestampEndStr)
	if err != nil {
		log.Fatal(err)
	}
	timestampEnd = timestampEnd.UTC()
}

func main() {
	rand.Seed(seed)

	// TODO(rw): Parse this from the CLI (maybe).
	dbConfig := DatabaseConfig{
		"database-name": dbName,
	}

	// Make the query generator:
	maker := useCaseMatrix[useCase][queryType][format]
	generator := maker(dbConfig, timestampStart, timestampEnd)

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
		q := generator.Dispatch(i, scaleVar)

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
