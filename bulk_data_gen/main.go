// bulk_data_gen generates time series data.
//
// Supported formats are:
// InfluxDB bulk load format
// ElasticSearch bulk load format
//
// The options provide ways to change the following parameters:
//   # total points to write
//   # measurements to spread points across
//   # tag key/value pairs to spread points across (per measurement)
//   standard deviations and means of the field values
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"
)

// Output data format choices:
var formatChoices = []string{"influx-bulk", "es-bulk"}

// Program option vars:
var (
	daemonUrl string
	dbName    string

	measurements uint
	fields       uint
	// TODO(rw): valueStrategy: generate from other distributions?
	batchSize uint
	tagKeys   uint
	tagValues uint
	points    uint
	// TODO(rw): precision?

	measurementNameLen uint
	fieldNameLen       uint
	tagKeyLen          uint
	tagValueLen        uint

	stdDevsStr string
	meansStr   string

	stdDevs []float64
	means   []float64

	timestampStartStr string
	timestampEndStr   string

	timestampStart time.Time
	timestampEnd   time.Time

	seed  int64
	debug int

	format string
)

// Parse args:
func init() {
	flag.StringVar(&format, "format", formatChoices[0], "Format to emit. (choices: influx-bulk, es-bulk)")

	flag.UintVar(&measurements, "measurements", 1, "Number of measurements to create.")
	flag.UintVar(&fields, "fields", 1, "Number of fields to populate per point.")
	flag.UintVar(&batchSize, "batch-size", 1000, "Number of points to write per request.")
	flag.UintVar(&tagKeys, "tag-keys", 1, "Number of tag keys to generate per point. These are generated per measurement.")
	flag.UintVar(&tagValues, "tag-values", 1, "Number of tag values to generate per tag key. These are generated per measurement.")
	flag.UintVar(&points, "points", 10000, "Number of points to generate. Points are split evenly across all measurements.")

	flag.UintVar(&measurementNameLen, "measurement-name-len", 5, "Length of generated measurement names.")
	flag.UintVar(&fieldNameLen, "field-name-len", 5, "Length of generated field names.")
	flag.UintVar(&tagKeyLen, "tag-key-len", 5, "Length of generated tag keys.")
	flag.UintVar(&tagValueLen, "tag-value-len", 5, "Length of generated tag values.")

	flag.StringVar(&stdDevsStr, "std-devs", "1.0", "Comma-separated std deviations for generating field values. Will be repeated to satisfy all generators. Example: 1.0,2.0,3.0")
	flag.StringVar(&meansStr, "means", "0.0", "Comma-separated means for generating field values. Will be repeated to satisfy all generators. Example: 0.0,1.0,2.0")

	flag.StringVar(&timestampStartStr, "timestamp-start", "2016-01-01T00:00:00-07:00", "Beginning timestamp (RFC3339).")
	flag.StringVar(&timestampEndStr, "timestamp-end", "2016-02-01T00:00:00-07:00", "Ending timestamp (RFC3339).")

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

	// Parse std dev numbers, cycling the slice to fill up `fields` items:
	stdDevNums := make([]float64, 0)
	for _, s := range strings.Split(stdDevsStr, ",") {
		var n float64
		if _, err := fmt.Sscanf(s, "%f", &n); err != nil {
			log.Fatalf("std-devs parsing failure: %s", err)
		}
		stdDevNums = append(stdDevNums, n)
	}

	stdDevs = make([]float64, int(fields))
	for i := 0; i < int(fields); i++ {
		stdDevs[i] = stdDevNums[i%len(stdDevNums)]
	}

	// Parse mean numbers, cycling the slice to fill up `fields` items:
	meanNums := make([]float64, 0)
	for _, s := range strings.Split(meansStr, ",") {
		var n float64
		if _, err := fmt.Sscanf(s, "%f", &n); err != nil {
			log.Fatalf("means parsing failure: %s", err)
		}
		meanNums = append(meanNums, n)
	}
	means = make([]float64, int(fields))
	for i := 0; i < int(fields); i++ {
		means[i] = meanNums[i%len(meanNums)]
	}

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

	if debug >= 1 {
		fmt.Printf("stddevs: %v\n", stdDevs)
		fmt.Printf("means: %v\n", means)
	}
}

func main() {
	rand.Seed(seed)

	out := bufio.NewWriterSize(os.Stdout, 1<<20)
	defer out.Flush()

	perGeneratorCount := points / measurements
	if perGeneratorCount * measurements < points {
		perGeneratorCount++
	}

	// Initialize generators, one for each measurement:
	generators := make([]*MeasurementGenerator, measurements)
	for i := 0; i < len(generators); i++ {
		config := MeasurementGeneratorConfig{
			Count: int64(perGeneratorCount),

			NameLen: int(measurementNameLen),

			TagKeyCount:   int(tagKeys),
			TagKeyLen:     int(tagKeyLen),
			TagValueCount: int(tagValues),
			TagValueLen:   int(tagValueLen),

			FieldCount:   int(fields),
			FieldNameLen: int(fieldNameLen),
			FieldStdDevs: stdDevs,
			FieldMeans:   means,

			TimestampStart: timestampStart,
			TimestampEnd:   timestampEnd,
		}
		if err := config.Validate(); err != nil {
			log.Fatal(err)
		}

		g := NewMeasurementGenerator(&config)
		generators[i] = &g
		if debug >= 1 {

			fmt.Fprintf(os.Stderr, "generator %d:\n", i)
			fmt.Fprintf(os.Stderr, "  timestamp start: %s\n", g.TimestampStart)
			fmt.Fprintf(os.Stderr, "  timestamp end: %s\n", g.TimestampEnd)
			fmt.Fprintf(os.Stderr, "  timestamp increment: %s\n", g.TimestampIncrement)
		}
	}

	generatorIdx := 0
	bytesWritten := int64(0)
	for i := int64(0); i < int64(points); i++ {
		// Construct the next point and write it to the buffer:
		g := generators[generatorIdx]
		g.Next()

		switch format {
		case "influx-bulk":
			err := g.P.SerializeInfluxBulk(out)
			if err != nil {
				log.Fatal(err)
			}
		case "es-bulk":
			err := g.P.SerializeESBulk(out)
			if err != nil {
				log.Fatal(err)
			}
		default: panic("unreachable")
		}

		// increment and wrap around the generator index:
		generatorIdx++
		generatorIdx %= len(generators)
	}
	fmt.Fprintf(os.Stderr, "created %d points across %d measurements (%.2fMB)\n", points,
		len(generators), float64(bytesWritten)/(1<<20))
	for _, g := range generators {
		fmt.Fprintf(os.Stderr, "  %s: %d points. tag pairs: %d, fields: %d, stddevs: %v, means: %v\n",
			g.Name, g.Seen,
			g.Config.TagKeyCount*g.Config.TagValueCount,
			g.Config.FieldCount, g.FieldStdDevs, g.FieldMeans)
	}
}
