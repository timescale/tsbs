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

// Output data format choices:
var formatChoices = []string{"influx-http", "es-http"}

// Use case choices:
var useCaseChoices = []string{"devops"}

// Program option vars:
var (
	format     string
	useCase    string
	scaleVar   int
	queryCount int

	dbName string

	timestampStartStr string
	timestampEndStr   string

	timestampStart time.Time
	timestampEnd   time.Time

	seed  int64
	debug int
)

// Parse args:
func init() {
	flag.StringVar(&format, "format", formatChoices[0], "Format to emit. (choices: influx-http, es-http)")
	flag.StringVar(&useCase, "use-case", useCaseChoices[0], "Use case to model. (choices: devops, iot)")
	flag.IntVar(&scaleVar, "scale-var", 1, "Scaling variable (must be the equal to the scalevar used for data generation).")
	flag.IntVar(&queryCount, "queries", 1000, "Number of queries to generate.")

	flag.StringVar(&dbName, "db", "benchmark_db", "Database for influx to use (ignored for elastic)")

	flag.StringVar(&timestampStartStr, "timestamp-start", "2016-01-01T00:00:00-00:00", "Beginning timestamp (RFC3339).")
	flag.StringVar(&timestampEndStr, "timestamp-end", "2016-02-01T00:00:00-00:00", "Ending timestamp (RFC3339).")

	flag.Int64Var(&seed, "seed", 0, "PRNG seed (default, or 0, uses the current timestamp).")
	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1) (default 0).")

	flag.Parse()

	validFormat := false
	for _, s := range formatChoices {
		if s == format {
			validFormat = true
			break
		}
	}
	if !validFormat {
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
	timestampEnd, err = time.Parse(time.RFC3339, timestampEndStr)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	rand.Seed(seed)

	var generator QueryGenerator
	switch useCase {
	case "devops":
		switch format {
		case "influx-http":
			generator = NewInfluxDevops(dbName, timestampStart, timestampEnd)
		case "es-http":
			generator = NewElasticSearchDevops(timestampStart, timestampEnd)
		default:
			panic("invalid format")
		}
	default:
		panic("invalid use case")
	}

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
