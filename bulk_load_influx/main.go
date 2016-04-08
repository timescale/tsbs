// bulk_load_influx loads an InfluxDB daemon with generated data.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
//
// The options provide ways to change the following parameters:
//   # total points to write
//   # measurements to spread points across
//   # tag key/value pairs to spread points across (per measurement)
//   standard deviations and means of the field values
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Used for generating random field names:
const letters = "abcdefghijklmnopqrstuvwxyz"

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

	seed  int64
	debug int
)

// Parse args:
func init() {
	flag.StringVar(&daemonUrl, "url", "http://localhost:8086", "Influxd URL.")
	flag.StringVar(&dbName, "db", "benchmark_db", "Database name to use.")

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

	flag.Int64Var(&seed, "seed", 0, "PRNG seed (default, or 0, uses the current timestamp).")
	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1, 2) (default 0).")

	flag.Parse()

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

	if debug >= 1 {
		fmt.Printf("stddevs: %v\n", stdDevs)
		fmt.Printf("means: %v\n", means)
	}
}

func main() {
	err := createDb(daemonUrl, dbName)
	if err != nil {
		log.Fatal(err)
	}

	rand.Seed(seed)

	// client and buf are reused between batches:
	client := &http.Client{}
	buf := bytes.NewBuffer(make([]byte, 0, 16<<20))

	// Initialize generators, one for each measurement:
	generators := make([]*MeasurementGenerator, measurements)
	for i := 0; i < len(generators); i++ {
		config := MeasurementGeneratorConfig{
			NameLen: int(measurementNameLen),

			TagKeyCount:   int(tagKeys),
			TagKeyLen:     int(tagKeyLen),
			TagValueCount: int(tagValues),
			TagValueLen:   int(tagValueLen),

			FieldCount:   int(fields),
			FieldNameLen: int(fieldNameLen),
			FieldStdDevs: stdDevs,
			FieldMeans:   means,
		}
		if err = config.Validate(); err != nil {
			log.Fatal(err)
		}

		g := NewMeasurementGenerator(&config)
		generators[i] = &g
	}

	generatorIdx := 0
	bytesWritten := int64(0)
	thisBatch := uint(0)
	batchStart := time.Now()
	for i := int64(0); i < int64(points); i++ {
		// Construct the next point and write it to the buffer:
		g := generators[generatorIdx]
		g.Next()

		err = g.P.Serialize(buf)
		if err != nil {
			log.Fatal(err)
		}
		thisBatch++

		// flush the buffer if the batch size is met, or if this is
		// the last point:
		if thisBatch == batchSize || i+1 == int64(points) {
			if debug >= 2 {
				// print the buffer to transmit to stderr:
				_, err := os.Stderr.Write(buf.Bytes())
				if err != nil {
					log.Fatal(err)
				}
			}

			// flush to DB:
			bytesWritten += int64(len(buf.Bytes()))
			err = flushToDatabase(client, daemonUrl, dbName, buf)
			if err != nil {
				log.Fatal(err)
			}
			batchEnd := time.Now()

			if debug >= 1 {
				fmt.Printf("wrote %d points in %7.2fms\n",
					thisBatch,
					batchEnd.Sub(batchStart).Seconds()*1e3)
			}

			batchStart = batchEnd
			buf.Reset()
			thisBatch = 0
		}

		// increment and wrap around the generator index:
		generatorIdx++
		generatorIdx %= len(generators)

	}
	fmt.Printf("wrote %d points across %d measurements (%.2fMB)\n", points,
		len(generators), float64(bytesWritten)/(1<<20))
	for _, g := range generators {
		fmt.Printf("  %s: %d points. tag pairs: %d, fields: %d, stddevs: %v, means: %v\n",
			g.Name, g.Count,
			g.Config.TagKeyCount*g.Config.TagValueCount,
			g.Config.FieldCount, g.FieldStdDevs, g.FieldMeans)
	}
}

// flushToDatabase writes the payload from the given source to the standard
// InfluxDB bulk write endpoint. Note that the data in the source reader must
// conform to the InfluxDB line protocol.
func flushToDatabase(client *http.Client, daemonUrl, dbName string, r io.Reader) error {
	u, err := url.Parse(daemonUrl)
	if err != nil {
		return err
	}

	// Construct the URL, which looks like:
	// http://localhost:8086/write?db=benchmark_db
	u.Path = "write"
	v := u.Query()
	v.Set("db", dbName)
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("POST", u.String(), r)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		return fmt.Errorf("bad batch write")
	}

	return nil
}

func createDb(daemon_url, dbname string) error {
	u, err := url.Parse(daemon_url)
	if err != nil {
		return err
	}

	// serialize params the right way:
	u.Path = "query"
	v := u.Query()
	v.Set("q", fmt.Sprintf("CREATE DATABASE %s", dbname))
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// does the body need to be read into the void?

	if resp.StatusCode != 200 {
		return fmt.Errorf("bad db create")
	}
	return nil
}
