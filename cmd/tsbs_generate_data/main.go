// tsbs_generate_data generates time series data from pre-specified use cases.
//
// Supported formats:
// Cassandra CSV format
// InfluxDB bulk load format
// MongoDB BSON format
// TimescaleDB pseudo-CSV format

// Supported use cases:
// devops: scale-var is the number of hosts to simulate, with log messages
//         every log-interval seconds.
// cpu-only: same as `devops` but only generate metrics for CPU
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/common"
	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/devops"
	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/serialize"
)

const (
	// Output data format choices (alphabetical order)
	formatCassandra   = "cassandra"
	formatInflux      = "influx"
	formatMongo       = "mongo"
	formatTimescaleDB = "timescaledb"

	// Use case choices (make sure to update TestGetConfig if adding a new one)
	useCaseCPUOnly   = "cpu-only"
	useCaseCPUSingle = "cpu-single"
	useCaseDevops    = "devops"
)

// semi-constants
var (
	formatChoices = []string{formatCassandra, formatInflux, formatMongo, formatTimescaleDB}
	// allows for testing
	fatal = log.Fatalf
)

// Program option vars:
var (
	format      string
	useCase     string
	profileFile string

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
	flag.StringVar(&profileFile, "profile-file", "", "File to which to write go profiling data")

	flag.DurationVar(&logInterval, "log-interval", 10*time.Second, "Duration between host data points")
	flag.Parse()

	if !(interleavedGenerationGroupID < interleavedGenerationGroups) {
		log.Fatal("incorrect interleaved groups configuration")
	}

	if initScaleVar == 0 {
		initScaleVar = scaleVar
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
	if !validateFormat(format) {
		log.Fatal("invalid format specifier")
	}

	if len(profileFile) > 0 {
		defer startMemoryProfile(profileFile)()
	}

	rand.Seed(seed)
	out := bufio.NewWriterSize(os.Stdout, 4<<20)
	defer out.Flush()

	cfg := getConfig(useCase)
	sim := cfg.ToSimulator(logInterval)
	serializer := getSerializer(sim, format, out)

	currentInterleavedGroup := uint(0)
	point := serialize.NewPoint()
	for !sim.Finished() {
		write := sim.Next(point)
		if !write {
			point.Reset()
			continue
		}

		// in the default case this is always true
		if currentInterleavedGroup == interleavedGenerationGroupID {
			err := serializer.Serialize(point, out)
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

func validateFormat(format string) bool {
	for _, s := range formatChoices {
		if s == format {
			return true
		}
	}
	return false
}

func getConfig(useCase string) common.SimulatorConfig {
	switch useCase {
	case useCaseDevops:
		return &devops.DevopsSimulatorConfig{
			Start: timestampStart,
			End:   timestampEnd,

			InitHostCount:   initScaleVar,
			HostCount:       scaleVar,
			HostConstructor: devops.NewHost,
		}
	case useCaseCPUOnly:
		return &devops.CPUOnlySimulatorConfig{
			Start: timestampStart,
			End:   timestampEnd,

			InitHostCount:   initScaleVar,
			HostCount:       scaleVar,
			HostConstructor: devops.NewHostCPUOnly,
		}
	case useCaseCPUSingle:
		return &devops.CPUOnlySimulatorConfig{
			Start: timestampStart,
			End:   timestampEnd,

			InitHostCount:   initScaleVar,
			HostCount:       scaleVar,
			HostConstructor: devops.NewHostCPUSingle,
		}
	default:
		fatal("unknown use case: '%s'", useCase)
		return nil
	}
}

func getSerializer(sim common.Simulator, format string, out *bufio.Writer) serialize.PointSerializer {
	switch format {
	case formatCassandra:
		return &serialize.CassandraSerializer{}
	case formatInflux:
		return &serialize.InfluxSerializer{}
	case formatMongo:
		return &serialize.MongoSerializer{}
	case formatTimescaleDB:
		out.WriteString("tags")
		for _, key := range devops.MachineTagKeys {
			out.WriteString(",")
			out.Write(key)
		}
		out.WriteString("\n")
		// sort the keys so the header is deterministic
		keys := make([]string, 0)
		fields := sim.Fields()
		for k := range fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, measurementName := range keys {
			out.WriteString(measurementName)
			for _, field := range fields[measurementName] {
				out.WriteString(",")
				out.Write(field)

			}
			out.WriteString("\n")
		}
		out.WriteString("\n")

		return &serialize.TimescaleDBSerializer{}
	default:
		fatal("unknown format: '%s'", format)
		return nil
	}
}

// startMemoryProfile sets up memory profiling to be written to profileFile. It
// returns a function to cleanup/write that should be deferred by the caller
func startMemoryProfile(profileFile string) func() {
	f, err := os.Create(profileFile)
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}

	stop := func() {
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}

	// Catches ctrl+c signals
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c

		fmt.Fprintln(os.Stderr, "\ncaught interrupt, stopping profile")
		stop()

		os.Exit(0)
	}()

	return stop
}
