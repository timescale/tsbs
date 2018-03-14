// tsbs_generate_data generates time series data from pre-specified use cases.
//
// Supported formats:
// TimescaleDB psuedo-CSV format
// InfluxDB bulk load format
// Cassandra query format

// Supported use cases:
// devops: scale-var is the number of hosts to simulate, with log messages
//         every log-interval seconds.
// cpu-only: same as `devops` but only generate metrics for CPU
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"
)

// Output data format choices:
var formatChoices = []string{"cassandra", "influx", "timescaledb"}

// Use case choices:
var useCaseChoices = []string{"devops", "cpu-only"}

// Program option vars:
var (
	format  string
	useCase string

	initScaleVar uint64
	scaleVar     uint64
	seed         int64
	debug        int

	timestampStart time.Time
	timestampEnd   time.Time

	interleavedGenerationGroupID uint
	interleavedGenerationGroups  uint

	logInterval time.Duration
)

// Parse args:
func init() {
	var timestampStartStr string
	var timestampEndStr string
	flag.StringVar(&format, "format", "", fmt.Sprintf("Format to emit. (choices: %s)", strings.Join(formatChoices, ", ")))

	flag.StringVar(&useCase, "use-case", "", "Use case to model. (choices: devops, cpu-only)")

	flag.Uint64Var(&initScaleVar, "initial-scale-var", 0, "Initial scaling variable specific to the use case (e.g., devices in 'devops'). 0 means to use -scale-var value")
	flag.Uint64Var(&scaleVar, "scale-var", 1, "Scaling variable specific to the use case (e.g., devices in 'devops').")

	flag.StringVar(&timestampStartStr, "timestamp-start", "2016-01-01T00:00:00Z", "Beginning timestamp (RFC3339).")
	flag.StringVar(&timestampEndStr, "timestamp-end", "2016-01-02T06:00:00Z", "Ending timestamp (RFC3339).")

	flag.Int64Var(&seed, "seed", 0, "PRNG seed (0 uses the current timestamp). (default 0)")
	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1, 2). (default 0)")

	flag.UintVar(&interleavedGenerationGroupID, "interleaved-generation-group-id", 0, "Group (0-indexed) to perform round-robin serialization within. Use this to scale up data generation to multiple processes.")
	flag.UintVar(&interleavedGenerationGroups, "interleaved-generation-groups", 1, "The number of round-robin serialization groups. Use this to scale up data generation to multiple processes.")

	flag.DurationVar(&logInterval, "log-interval", 10*time.Second, "Duration between host data points")
	flag.Parse()

	if !(interleavedGenerationGroupID < interleavedGenerationGroups) {
		log.Fatal("incorrect interleaved groups configuration")
	}

	if initScaleVar == 0 {
		initScaleVar = scaleVar
	}

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
	timestampStart = timestampStart.UTC()
	timestampEnd, err = time.Parse(time.RFC3339, timestampEndStr)
	if err != nil {
		log.Fatal(err)
	}
	timestampEnd = timestampEnd.UTC()
}

func main() {
	rand.Seed(seed)

	out := bufio.NewWriterSize(os.Stdout, 4<<20)
	defer out.Flush()

	var cfg simulatorConfig
	switch useCase {
	case "devops":
		cfg = &DevopsSimulatorConfig{
			Start: timestampStart,
			End:   timestampEnd,

			InitHostCount:   initScaleVar,
			HostCount:       scaleVar,
			HostConstructor: NewHost,
		}
	case "cpu-only":
		cfg = &CPUOnlySimulatorConfig{
			Start: timestampStart,
			End:   timestampEnd,

			InitHostCount:   initScaleVar,
			HostCount:       scaleVar,
			HostConstructor: NewHost,
		}
	default:
		log.Fatalf("unknown use case: '%s'", useCase)
	}
	sim := cfg.ToSimulator(logInterval)

	var serializer func(*Point, io.Writer) error
	switch format {
	case "cassandra":
		serializer = (*Point).SerializeCassandra
	case "influx":
		serializer = (*Point).SerializeInfluxBulk
	case "timescaledb":
		out.WriteString("tags")
		for _, key := range MachineTagKeys {
			out.WriteString(",")
			out.Write(key)
		}
		out.WriteString("\n")
		for measurementName, fields := range sim.Fields() {
			out.WriteString(measurementName)
			for _, field := range fields {
				out.WriteString(",")
				out.Write(field)

			}
			out.WriteString("\n")
		}
		out.WriteString("\n")

		serializer = (*Point).SerializeTimescaleDB
	default:
		log.Fatalf("unknown format: '%s'", format)
	}

	currentInterleavedGroup := uint(0)
	point := MakeUsablePoint()

	for !sim.Finished() {
		write := sim.Next(point)
		if !write {
			point.Reset()
			continue
		}

		// in the default case this is always true
		if currentInterleavedGroup == interleavedGenerationGroupID {
			err := serializer(point, out)
			if err != nil {
				log.Fatal(err)
			}

		}
		point.Reset()

		currentInterleavedGroup++
		if currentInterleavedGroup == interleavedGenerationGroups {
			currentInterleavedGroup = 0
		}
	}

	err := out.Flush()
	if err != nil {
		log.Fatal(err.Error())
	}
}
