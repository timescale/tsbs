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

	"bitbucket.org/440-labs/influxdb-comparisons/load"

	"github.com/pkg/profile"
	"github.com/valyala/fasthttp"
)

// Program option vars:
var (
	csvDaemonUrls     string
	daemonUrls        []string
	replicationFactor int
	backoff           time.Duration
	useGzip           bool
	doAbortOnExist    bool
	memprofile        bool
	consistency       string
)

// Global vars
var (
	loader          *load.BenchmarkRunner
	bufPool         sync.Pool
	backingOffChans []chan bool
	backingOffDones []chan struct{}

	rowCount    uint64
	metricCount uint64
)

var consistencyChoices = map[string]struct{}{
	"any":    struct{}{},
	"one":    struct{}{},
	"quorum": struct{}{},
	"all":    struct{}{},
}

// Parse args:
func init() {
	loader = load.GetBenchmarkRunner()

	flag.StringVar(&csvDaemonUrls, "urls", "http://localhost:8086", "InfluxDB URLs, comma-separated. Will be used in a round-robin fashion.")
	flag.IntVar(&replicationFactor, "replication-factor", 1, "Cluster replication factor (only applies to clustered databases).")
	flag.StringVar(&consistency, "consistency", "all", "Write consistency. Must be one of: any, one, quorum, all.")
	flag.DurationVar(&backoff, "backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	flag.BoolVar(&useGzip, "gzip", true, "Whether to gzip encode requests (default true).")
	flag.BoolVar(&doAbortOnExist, "do-abort-on-exist", true, "Whether to abort if the destination database already exists.")
	flag.BoolVar(&memprofile, "memprofile", false, "Whether to write a memprofile (file automatically determined).")

	flag.Parse()

	if _, ok := consistencyChoices[consistency]; !ok {
		log.Fatalf("invalid consistency settings")
	}

	daemonUrls = strings.Split(csvDaemonUrls, ",")
	if len(daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	fmt.Printf("daemon URLs: %v\n", daemonUrls)
}

type benchmark struct {
	l        *load.BenchmarkRunner
	channels []*load.DuplexChannel
}

func (b *benchmark) Work(wg *sync.WaitGroup, i int) {
	daemonURL := daemonUrls[i%len(daemonUrls)]
	backingOffChans[i] = make(chan bool, 100)
	backingOffDones[i] = make(chan struct{})
	cfg := HTTPWriterConfig{
		DebugInfo:      fmt.Sprintf("worker #%d, dest url: %s", i, daemonURL),
		Host:           daemonURL,
		Database:       loader.DatabaseName(),
		BackingOffChan: backingOffChans[i],
		BackingOffDone: backingOffDones[i],
	}
	go processBatches(wg, NewHTTPWriter(cfg, consistency), b.channels[0], backingOffChans[i], backingOffDones[i])
	go processBackoffMessages(i, backingOffChans[i], backingOffDones[i])
}

func (b *benchmark) Scan(batchSize int, limit int64, br *bufio.Reader) int64 {
	decoder := &decoder{scanner: bufio.NewScanner(br)}
	return load.Scan(b.channels, batchSize, limit, br, decoder, &factory{})
}

func (b *benchmark) Close() {
	for _, c := range b.channels {
		c.Close()
	}
}

func (b *benchmark) Cleanup() {
	for i := range backingOffChans {
		close(backingOffChans[i])
		<-backingOffDones[i]
	}
}

func main() {
	if memprofile {
		p := profile.Start(profile.MemProfile)
		defer p.Stop()
	}
	if loader.DoLoad() && loader.DoInit() {
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
			err = createDb(daemonUrls[0], loader.DatabaseName(), replicationFactor)
			if err != nil {
				log.Fatal(err)
			}
			time.Sleep(time.Second)
		}
	}

	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	backingOffChans = make([]chan bool, loader.NumWorkers())
	backingOffDones = make([]chan struct{}, loader.NumWorkers())
	channels := []*load.DuplexChannel{load.NewDuplexChannel(loader.NumWorkers())}

	br := bufio.NewReaderSize(os.Stdin, 4*1024*1024)
	b := &benchmark{l: loader, channels: channels}
	loader.RunBenchmark(b, br, &metricCount, &rowCount)
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(wg *sync.WaitGroup, w *HTTPWriter, c *load.DuplexChannel, backoffSrc chan bool, backoffDst chan struct{}) {
	for item := range c.GetWorkerChannel() {
		batch := item.(*batch)

		// Write the batch: try until backoff is not needed.
		if loader.DoLoad() {
			var err error
			for {
				if useGzip {
					compressedBatch := bufPool.Get().(*bytes.Buffer)
					fasthttp.WriteGzip(compressedBatch, batch.buf.Bytes())
					_, err = w.WriteLineProtocol(compressedBatch.Bytes(), true)
					// Return the compressed batch buffer to the pool.
					compressedBatch.Reset()
					bufPool.Put(compressedBatch)
				} else {
					_, err = w.WriteLineProtocol(batch.buf.Bytes(), false)
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
		atomic.AddUint64(&metricCount, batch.metrics)
		atomic.AddUint64(&rowCount, batch.rows)

		// Return the batch buffer to the pool.
		batch.buf.Reset()
		bufPool.Put(batch.buf)
		c.SendToScanner()
	}
	wg.Done()
}

func processBackoffMessages(workerID int, src chan bool, dst chan struct{}) {
	var totalBackoffSecs float64
	var start time.Time
	last := false
	for this := range src {
		if this && !last {
			start = time.Now()
			last = true
		} else if !this && last {
			took := time.Now().Sub(start)
			fmt.Printf("[worker %d] backoff took %.02fsec\n", workerID, took.Seconds())
			totalBackoffSecs += took.Seconds()
			last = false
			start = time.Now()
		}
	}
	fmt.Printf("[worker %d] backoffs took a total of %fsec of runtime\n", workerID, totalBackoffSecs)
	dst <- struct{}{}
}

func createDb(daemonURL, dbName string, replicationFactor int) error {
	u, err := url.Parse(daemonURL)
	if err != nil {
		return err
	}

	// serialize params the right way:
	u.Path = "query"
	v := u.Query()
	v.Set("consistency", "all")
	v.Set("q", fmt.Sprintf("CREATE DATABASE %s WITH REPLICATION %d", dbName, replicationFactor))
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
func listDatabases(daemonURL string) ([]string, error) {
	u := fmt.Sprintf("%s/query?q=show%%20databases", daemonURL)
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
