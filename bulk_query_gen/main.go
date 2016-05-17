// bulk_query_gen generates queries for various use cases. Its output will
// typically be consume by query_benchmarker.
//
// Output formats: InfluxDB, ElasticSearch.
//
// Query style use cases: Devops.
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

// query generator choices {use-case, query-type, format}
var useCaseMatrix = map[string]map[string]map[string]QueryGeneratorMaker {
	"devops": {
		"single-host": {
			"influx-http": NewInfluxDevopsSingleHost,
			"es-http": NewElasticSearchDevopsSingleHost,
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
)

// Parse args:
func init() {
	oldUsage := flag.Usage
	flag.Usage = func() {
		oldUsage()
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "The use case matrix of choices is:\n")
		for uc, queryTypes := range useCaseMatrix {
			for qt, formats := range queryTypes {
				for ff := range formats {
					fmt.Fprintf(os.Stderr, "  use case: %s, query type: %s, format: %s\n", uc, qt, ff)
				}
			}
		}

	}
	flag.StringVar(&format, "format", "influx-http", "Format to emit. (Choices are in the use case matrix.)")
	flag.StringVar(&useCase, "use-case", "devops", "Use case to model. (Choices are in the use case matrix.)")
	flag.StringVar(&queryType, "query-type", "", "Query type. (Choices are in the use case matrix.)")
	flag.IntVar(&scaleVar, "scale-var", 1, "Scaling variable (must be the equal to the scalevar used for data generation).")
	flag.IntVar(&queryCount, "queries", 1000, "Number of queries to generate.")

	flag.StringVar(&dbName, "db", "benchmark_db", "Database for influx to use (ignored for elastic)")

	flag.StringVar(&timestampStartStr, "timestamp-start", "2016-01-01T00:00:00Z", "Beginning timestamp (RFC3339).")
	flag.StringVar(&timestampEndStr, "timestamp-end", "2016-01-01T06:00:00Z", "Ending timestamp (RFC3339).")

	flag.Int64Var(&seed, "seed", 0, "PRNG seed (default, or 0, uses the current timestamp).")
	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1) (default 0).")

	flag.Parse()

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

	dbConfig := DatabaseConfig{
		"database-name": dbName,
	}

	qgMaker := useCaseMatrix[useCase][queryType][format]
	var generator QueryGenerator = qgMaker(dbConfig, timestampStart, timestampEnd)

	stats := make(map[string]int64)

	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()

	// create request instances, serializing them to stdout and collecting
	// counts for each kind:
	enc := gob.NewEncoder(out)
	q := &Query{}
	for i := 0; i < queryCount; i++ {
		generator.Dispatch(i, q, scaleVar)
		err := enc.Encode(q)
		if err != nil {
			log.Fatal(err)
		}
		stats[string(q.HumanLabel)]++

		if debug == 1 {
			_, err := fmt.Fprintf(os.Stderr, "%s\n", q.HumanLabel)
			if err != nil {
				log.Fatal(err)
			}
		} else if debug == 2 {
			_, err := fmt.Fprintf(os.Stderr, "%s\n", q.HumanDescription)
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

	// print stats:
	keys := []string{}
	for k, _ := range stats {
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
