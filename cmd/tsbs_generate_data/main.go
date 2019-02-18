// tsbs_generate_data generates time series data from pre-specified use cases.
//
// Supported formats:
// Cassandra CSV format
// ClickHouse pseudo-CSV format (the same as for TimescaleDB)
// InfluxDB bulk load format
// MongoDB BSON format
// TimescaleDB pseudo-CSV format (the same as for ClickHouse)

// Supported use cases:
// devops: scale is the number of hosts to simulate, with log messages
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
	"os/signal"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

const (
	// Output data format choices (alphabetical order)
	formatCassandra   = "cassandra"
	formatClickhouse  = "clickhouse"
	formatInflux      = "influx"
	formatMongo       = "mongo"
	formatSiriDB      = "siridb"
	formatTimescaleDB = "timescaledb"

	// Use case choices (make sure to update TestGetConfig if adding a new one)
	useCaseCPUOnly   = "cpu-only"
	useCaseCPUSingle = "cpu-single"
	useCaseDevops    = "devops"

	errTotalGroupsZero  = "incorrect interleaved groups configuration: total groups = 0"
	errInvalidGroupsFmt = "incorrect interleaved groups configuration: id %d >= total groups %d"
	errInvalidFormatFmt = "invalid format specifier: %v (valid choices: %v)"

	defaultWriteSize = 4 << 20 // 4 MB
)

// semi-constants
var (
	formatChoices = []string{
		formatCassandra,
		formatClickhouse,
		formatInflux,
		formatMongo,
		formatSiriDB,
		formatTimescaleDB,
	}
	useCaseChoices = []string{
		useCaseCPUOnly,
		useCaseCPUSingle,
		useCaseDevops,
	}
	// allows for testing
	fatal = log.Fatalf
)

// parseableFlagVars are flag values that need sanitization or re-parsing after
// being set, e.g., to convert from string to time.Time or re-setting the value
// based on a special '0' value
type parseableFlagVars struct {
	timestampStartStr string
	timestampEndStr   string
	seed              int64
	initialScale      uint64
}

// Program option vars:
var (
	format      string
	useCase     string
	profileFile string

	initialScale uint64
	scale        uint64
	seed         int64
	debug        int

	timestampStart time.Time
	timestampEnd   time.Time

	interleavedGenerationGroupID   uint
	interleavedGenerationGroupsNum uint

	logInterval   time.Duration
	maxDataPoints uint64
	fileName      string
)

// parseTimeFromString parses string-represented time of the format 2006-01-02T15:04:05Z07:00
func parseTimeFromString(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		fatal("can not parse time from string '%s': %v", s, err)
		return time.Time{}
	}
	return t.UTC()
}

// validateGroups checks validity of combination groupID and totalGroups
func validateGroups(groupID, totalGroupsNum uint) (bool, error) {
	if totalGroupsNum == 0 {
		// Need at least one group
		return false, fmt.Errorf(errTotalGroupsZero)
	}
	if groupID >= totalGroupsNum {
		// Need reasonable groupID
		return false, fmt.Errorf(errInvalidGroupsFmt, groupID, totalGroupsNum)
	}
	return true, nil
}

// validateFormat checks whether format is valid (i.e., one of formatChoices)
func validateFormat(format string) bool {
	for _, s := range formatChoices {
		if s == format {
			return true
		}
	}
	return false
}

// validateUseCase checks whether use-case is valid (i.e., one of useCaseChoices)
func validateUseCase(useCase string) bool {
	for _, s := range useCaseChoices {
		if s == useCase {
			return true
		}
	}
	return false
}

// postFlagParse assigns parseable flags
func postFlagParse(flags parseableFlagVars) {
	if flags.initialScale == 0 {
		initialScale = scale
	} else {
		initialScale = flags.initialScale
	}

	// the default seed is the current timestamp:
	if flags.seed == 0 {
		seed = int64(time.Now().Nanosecond())
	} else {
		seed = flags.seed
	}
	fmt.Fprintf(os.Stderr, "using random seed %d\n", seed)

	// Parse timestamps
	timestampStart = parseTimeFromString(flags.timestampStartStr)
	timestampEnd = parseTimeFromString(flags.timestampEndStr)
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
	pfv := parseableFlagVars{}

	flag.StringVar(&format, "format", "", fmt.Sprintf("Format to emit. (choices: %s)", strings.Join(formatChoices, ", ")))

	flag.StringVar(&useCase, "use-case", "", fmt.Sprintf("Use case to model. (choices: %s)", strings.Join(useCaseChoices, ", ")))

	flag.Uint64Var(&pfv.initialScale, "initial-scale", 0, "Initial scaling variable specific to the use case (e.g., devices in 'devops'). 0 means to use -scale value")
	flag.Uint64Var(&scale, "scale", 1, "Scaling value specific to the use case (e.g., devices in 'devops').")

	flag.StringVar(&pfv.timestampStartStr, "timestamp-start", "2016-01-01T00:00:00Z", "Beginning timestamp (RFC3339).")
	flag.StringVar(&pfv.timestampEndStr, "timestamp-end", "2016-01-02T06:00:00Z", "Ending timestamp (RFC3339).")

	flag.Int64Var(&pfv.seed, "seed", 0, "PRNG seed (0 uses the current timestamp). (default 0)")

	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1, 2). (default 0)")

	flag.UintVar(&interleavedGenerationGroupID, "interleaved-generation-group-id", 0,
		"Group (0-indexed) to perform round-robin serialization within. Use this to scale up data generation to multiple processes.")
	flag.UintVar(&interleavedGenerationGroupsNum, "interleaved-generation-groups", 1,
		"The number of round-robin serialization groups. Use this to scale up data generation to multiple processes.")

	flag.StringVar(&profileFile, "profile-file", "", "File to which to write go profiling data")

	flag.DurationVar(&logInterval, "log-interval", 10*time.Second, "Duration between host data points")
	flag.Uint64Var(&maxDataPoints, "max-data-points", 0, "Limit the number of data points to generate, 0 = no limit")
	flag.StringVar(&fileName, "file", "", "File name to write generated data to")

	flag.Parse()

	postFlagParse(pfv)
}

func main() {
	if ok, err := validateGroups(interleavedGenerationGroupID, interleavedGenerationGroupsNum); !ok {
		fatal("incorrect interleaved groups specification: %v", err)
	}
	if ok := validateFormat(format); !ok {
		fatal("invalid format specified: %v (valid choices: %v)", format, formatChoices)
	}
	if ok := validateUseCase(useCase); !ok {
		fatal("invalid use-case specified: %v (valid choices: %v)", useCase, useCaseChoices)
	}

	if len(profileFile) > 0 {
		defer startMemoryProfile(profileFile)()
	}

	rand.Seed(seed)

	// Get output writer
	out := GetBufferedWriter(fileName)
	defer func() {
		err := out.Flush()
		if err != nil {
			fatal(err.Error())
		}
	}()

	cfg := getConfig(useCase)
	sim := cfg.NewSimulator(logInterval, maxDataPoints)
	serializer := getSerializer(sim, format, out)

	runSimulator(sim, serializer, out, interleavedGenerationGroupID, interleavedGenerationGroupsNum)
}

func runSimulator(sim common.Simulator, serializer serialize.PointSerializer, out io.Writer, groupID, totalGroups uint) {
	currGroupID := uint(0)
	point := serialize.NewPoint()
	for !sim.Finished() {
		write := sim.Next(point)
		if !write {
			point.Reset()
			continue
		}

		// in the default case this is always true
		if currGroupID == groupID {
			err := serializer.Serialize(point, out)
			if err != nil {
				fatal("can not serialize point: %s", err)
				return
			}
		}
		point.Reset()

		currGroupID = (currGroupID + 1) % totalGroups
	}
}

func getConfig(useCase string) common.SimulatorConfig {
	switch useCase {
	case useCaseDevops:
		return &devops.DevopsSimulatorConfig{
			Start: timestampStart,
			End:   timestampEnd,

			InitHostCount:   initialScale,
			HostCount:       scale,
			HostConstructor: devops.NewHost,
		}
	case useCaseCPUOnly:
		return &devops.CPUOnlySimulatorConfig{
			Start: timestampStart,
			End:   timestampEnd,

			InitHostCount:   initialScale,
			HostCount:       scale,
			HostConstructor: devops.NewHostCPUOnly,
		}
	case useCaseCPUSingle:
		return &devops.CPUOnlySimulatorConfig{
			Start: timestampStart,
			End:   timestampEnd,

			InitHostCount:   initialScale,
			HostCount:       scale,
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
	case formatSiriDB:
		return &serialize.SiriDBSerializer{}
	case formatClickhouse:
		fallthrough
	case formatTimescaleDB:
		out.WriteString("tags")
		for _, key := range sim.TagKeys() {
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
