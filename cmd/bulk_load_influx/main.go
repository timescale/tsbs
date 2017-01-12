// bulk_load_influx loads an InfluxDB daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb-comparisons/util/telemetry"
	"github.com/pkg/profile"
	"github.com/valyala/fasthttp"
)

// Program option vars:
var (
	csvDaemonUrls           string
	daemonUrls              []string
	dbName                  string
	replicationFactor       int
	workers                 int
	itemLimit               int64
	batchSize               int
	backoff                 time.Duration
	timeLimit               time.Duration
	progressInterval        time.Duration
	doLoad                  bool
	doDBCreate              bool
	useGzip                 bool
	doAbortOnExist          bool
	memprofile              bool
	telemetryHost           string
	telemetryStderr         bool
	telemetryBatchSize      uint
	telemetryExperimentName string
)

// Global vars
var (
	bufPool             sync.Pool
	batchChan           chan *bytes.Buffer
	inputDone           chan struct{}
	workersGroup        sync.WaitGroup
	backingOffChans     []chan bool
	backingOffDones     []chan struct{}
	telemetryChanPoints chan *telemetry.Point
	telemetryChanDone   chan struct{}
	telemetryHostname   string

	progressIntervalItems uint64
)

// Parse args:
func init() {
	flag.StringVar(&csvDaemonUrls, "urls", "http://localhost:8086", "InfluxDB URLs, comma-separated. Will be used in a round-robin fashion.")
	flag.StringVar(&dbName, "db", "benchmark_db", "Database name.")
	flag.IntVar(&replicationFactor, "replication-factor", 2, "Cluster replication factor (only applies to clustered databases).")
	flag.IntVar(&batchSize, "batch-size", 5000, "Batch size (1 line of input = 1 item).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.Int64Var(&itemLimit, "item-limit", -1, "Number of items to read from stdin before quitting. (1 item per 1 line of input.)")
	flag.DurationVar(&backoff, "backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	flag.DurationVar(&timeLimit, "time-limit", -1, "Maximum duration to run (-1 is the default: no limit).")
	flag.DurationVar(&progressInterval, "progress-interval", -1, "Duration between printing progress messages.")
	flag.BoolVar(&useGzip, "gzip", true, "Whether to gzip encode requests (default true).")
	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")
	flag.BoolVar(&doDBCreate, "do-db-create", true, "Whether to create the database.")
	flag.BoolVar(&doAbortOnExist, "do-abort-on-exist", true, "Whether to abort if the destination database already exists.")
	flag.BoolVar(&memprofile, "memprofile", false, "Whether to write a memprofile (file automatically determined).")
	flag.StringVar(&telemetryHost, "telemetry-host", "", "InfluxDB host to write telegraf telemetry to (optional).")
	flag.StringVar(&telemetryExperimentName, "telemetry-experiment-name", "unnamed_experiment", "Experiment name for telemetry.")
	flag.BoolVar(&telemetryStderr, "telemetry-stderr", false, "Whether to write telemetry also to stderr.")
	flag.UintVar(&telemetryBatchSize, "telemetry-batch-size", 10, "Telemetry batch size (lines).")

	flag.Parse()

	daemonUrls = strings.Split(csvDaemonUrls, ",")
	if len(daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	fmt.Printf("daemon URLs: %v\n", daemonUrls)

	if telemetryHost != "" {
		fmt.Printf("telemetry destination: %v\n", telemetryHost)
		if telemetryBatchSize == 0 {
			panic("invalid telemetryBatchSize")
		}

		var err error
		telemetryHostname, err = os.Hostname()
		if err != nil {
			log.Fatalf("os.Hostname() error: %s", err.Error())
		}
	}
}

func main() {
	if memprofile {
		p := profile.Start(profile.MemProfile)
		defer p.Stop()
	}
	if doLoad && doDBCreate {
		// check that there are no pre-existing databases:
		existingDatabases, err := listDatabases(daemonUrls[0])
		if err != nil {
			log.Fatal(err)
		}

		if len(existingDatabases) > 0 {
			if doAbortOnExist {
				log.Fatalf("There are databases already in the data store. If you know what you are doing, run the command:\ncurl 'http://localhost:8086/query?q=drop%%20database%%20%s'\n", existingDatabases[0])
			} else {
				log.Printf("Info: there are databases already in the data store.")
			}
		}

		if len(existingDatabases) == 0 {
			err = createDb(daemonUrls[0], dbName, replicationFactor)
			if err != nil {
				log.Fatal(err)
			}
			time.Sleep(1000 * time.Millisecond)
		}
	}

	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	batchChan = make(chan *bytes.Buffer, workers)
	inputDone = make(chan struct{})

	backingOffChans = make([]chan bool, workers)
	backingOffDones = make([]chan struct{}, workers)

	if telemetryHost != "" {
		telemetryCollector := telemetry.NewCollector(telemetryHost, "telegraf")
		telemetryChanPoints, telemetryChanDone = telemetry.EZRunAsync(telemetryCollector, telemetryBatchSize, telemetryExperimentName, telemetryStderr)
	}

	for i := 0; i < workers; i++ {
		daemonUrl := daemonUrls[i%len(daemonUrls)]
		backingOffChans[i] = make(chan bool, 100)
		backingOffDones[i] = make(chan struct{})
		workersGroup.Add(1)
		cfg := HTTPWriterConfig{
			DebugInfo:      fmt.Sprintf("worker #%d, dest url: %s", i, daemonUrl),
			Host:           daemonUrl,
			Database:       dbName,
			BackingOffChan: backingOffChans[i],
			BackingOffDone: backingOffDones[i],
		}
		go processBatches(NewHTTPWriter(cfg), backingOffChans[i], backingOffDones[i], telemetryChanPoints, fmt.Sprintf("%d", i))
		go processBackoffMessages(i, backingOffChans[i], backingOffDones[i])
	}

	if progressInterval >= 0 {
		go func() {
			start := time.Now()
			for end := range time.NewTicker(progressInterval).C {
				n := atomic.SwapUint64(&progressIntervalItems, 0)

				//absoluteMillis := end.Add(-progressInterval).UnixNano() / 1e6
				absoluteMillis := start.UTC().UnixNano() / 1e6
				fmt.Printf("[interval_progress_items] %dms, %d\n", absoluteMillis, n)
				start = end
			}
		}()
	}

	start := time.Now()
	itemsRead, bytesRead := scan(batchSize)

	<-inputDone
	close(batchChan)

	workersGroup.Wait()

	for i := range backingOffChans {
		close(backingOffChans[i])
		<-backingOffDones[i]
	}

	end := time.Now()
	took := end.Sub(start)
	itemsRate := float64(itemsRead) / float64(took.Seconds())
	bytesRate := float64(bytesRead) / float64(took.Seconds())

	if telemetryHost != "" {
		close(telemetryChanPoints)
		<-telemetryChanDone
	}

	fmt.Printf("loaded %d items in %fsec with %d workers (mean rate %f/sec, %.2fMB/sec from stdin)\n", itemsRead, took.Seconds(), workers, itemsRate, bytesRate/(1<<20))
}

// scan reads one item at a time from stdin. 1 item = 1 line.
// When the requested number of items per batch is met, send a batch over batchChan for the workers to write.
func scan(itemsPerBatch int) (int64, int64) {
	buf := bufPool.Get().(*bytes.Buffer)

	var n int
	var itemsRead, bytesRead int64
	newline := []byte("\n")
	var deadline time.Time
	if timeLimit >= 0 {
		deadline = time.Now().Add(timeLimit)
	}

	var batchItemCount uint64

	scanner := bufio.NewScanner(bufio.NewReaderSize(os.Stdin, 4*1024*1024))
outer:
	for scanner.Scan() {
		if itemsRead == itemLimit {
			break
		}

		itemsRead++
		batchItemCount++

		buf.Write(scanner.Bytes())
		buf.Write(newline)

		n++
		if n >= itemsPerBatch {
			atomic.AddUint64(&progressIntervalItems, batchItemCount)
			batchItemCount = 0

			bytesRead += int64(buf.Len())
			batchChan <- buf
			buf = bufPool.Get().(*bytes.Buffer)
			n = 0

			if timeLimit >= 0 && time.Now().After(deadline) {
				break outer
			}

		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		batchChan <- buf
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)

	return itemsRead, bytesRead
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(w *HTTPWriter, backoffSrc chan bool, backoffDst chan struct{}, telemetrySink chan *telemetry.Point, telemetryWorkerLabel string) {
	var batchesSeen int64
	for batch := range batchChan {
		batchesSeen++

		var bodySize int

		// Write the batch: try until backoff is not needed.
		if doLoad {
			var err error
			for {
				if useGzip {
					compressedBatch := bufPool.Get().(*bytes.Buffer)
					fasthttp.WriteGzip(compressedBatch, batch.Bytes())
					bodySize = len(compressedBatch.Bytes())
					_, err = w.WriteLineProtocol(compressedBatch.Bytes(), true)
					// Return the compressed batch buffer to the pool.
					compressedBatch.Reset()
					bufPool.Put(compressedBatch)
				} else {
					bodySize = len(batch.Bytes())
					_, err = w.WriteLineProtocol(batch.Bytes(), false)
				}

				if err == BackoffError {
					backoffSrc <- true
					time.Sleep(backoff)
				} else {
					backoffSrc <- false
					break
				}
			}
			if err != nil {
				log.Fatalf("Error writing: %s\n", err.Error())
			}
		}

		// Return the batch buffer to the pool.
		batch.Reset()
		bufPool.Put(batch)

		// Report telemetry, if applicable:
		if telemetrySink != nil {
			p := telemetry.GetPointFromGlobalPool()
			p.Init("benchmark_write", time.Now().UnixNano())
			p.AddTag("src_hostname", telemetryHostname)
			p.AddTag("dst_hostname", w.Host)
			p.AddTag("worker_id", telemetryWorkerLabel)
			p.AddInt64Field("worker_req_num", batchesSeen)
			p.AddBoolField("gzip", useGzip)
			p.AddInt64Field("body_bytes", int64(bodySize))
			telemetrySink <- p
		}
	}
	workersGroup.Done()
}

func processBackoffMessages(workerId int, src chan bool, dst chan struct{}) {
	var totalBackoffSecs float64
	var start time.Time
	last := false
	for this := range src {
		if this && !last {
			start = time.Now()
			last = true
		} else if !this && last {
			took := time.Now().Sub(start)
			fmt.Printf("[worker %d] backoff took %.02fsec\n", workerId, took.Seconds())
			totalBackoffSecs += took.Seconds()
			last = false
			start = time.Now()
		}
	}
	fmt.Printf("[worker %d] backoffs took a total of %fsec of runtime\n", workerId, totalBackoffSecs)
	dst <- struct{}{}
}

func createDb(daemon_url, dbname string, replicationFactor int) error {
	u, err := url.Parse(daemon_url)
	if err != nil {
		return err
	}

	// serialize params the right way:
	u.Path = "query"
	v := u.Query()
	v.Set("consistency", "all")
	v.Set("q", fmt.Sprintf("CREATE DATABASE %s WITH REPLICATION %d", dbname, replicationFactor))
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

// listDatabases lists the existing databases in InfluxDB.
func listDatabases(daemonUrl string) ([]string, error) {
	u := fmt.Sprintf("%s/query?q=show%%20databases", daemonUrl)
	resp, err := http.Get(u)
	if err != nil {
		return nil, fmt.Errorf("listDatabases error: %s", err.Error())
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Do ad-hoc parsing to find existing database names:
	// {"results":[{"series":[{"name":"databases","columns":["name"],"values":[["_internal"],["benchmark_db"]]}]}]}%
	type listingType struct {
		Results []struct {
			Series []struct {
				Values [][]string
			}
		}
	}
	var listing listingType
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return nil, err
	}

	ret := []string{}
	for _, nestedName := range listing.Results[0].Series[0].Values {
		name := nestedName[0]
		// the _internal database is skipped:
		if name == "_internal" {
			continue
		}
		ret = append(ret, name)
	}
	return ret, nil
}
