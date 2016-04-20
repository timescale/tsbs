// bulk_data_gen generates queries from pre-specified use cases.
//
// Supported formats:
// InfluxDB
// ElasticSearch
//
// Supported use cases:
// Devops: scale_var is the number of hosts to simulate, with log messages
//         every 10 seconds.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

// Output data format choices:
var formatChoices = []string{"influx-http", "es-http"}

// Use case choices:
var useCaseChoices = []string{"devops"}

// Program option vars:
var (
	daemonUrl string
	dbName    string

	format  string
	useCase string

	scaleVar int64

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
	flag.StringVar(&dbName, "db", "benchmark_db", "Database for influx to use")

	flag.StringVar(&useCase, "use-case", useCaseChoices[0], "Use case to model. (choices: devops, iot)")
	flag.Int64Var(&scaleVar, "scale-var", 1000, "Scaling variable specific to the use case.")

	flag.StringVar(&timestampStartStr, "timestamp-start", "2016-01-01T00:00:00-00:00", "Beginning timestamp (RFC3339).")
	flag.StringVar(&timestampEndStr, "timestamp-end", "2016-02-01T00:00:00-00:00", "Ending timestamp (RFC3339).")

	flag.Int64Var(&seed, "seed", 0, "PRNG seed (default, or 0, uses the current timestamp).")
	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1, 2) (default 0).")

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

	out := bufio.NewWriterSize(os.Stdout, 4<<20)
	defer out.Flush()

	var devops Devops
	switch useCase {
	case "devops":
		devops = NewInfluxDevops(dbName, timestampStart, timestampEnd)
	default:
		panic("unreachable")
	}

	req := &Request{}
	for i := 0; i < 100; i++ {
		devops.AvgCPUUsageDayByHour(req)
		fmt.Printf("%#v\n", req)
	}
}
