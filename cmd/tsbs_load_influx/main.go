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
	"strings"
	"sync"
	"time"

	"bitbucket.org/440-labs/tsbs/load"
	"github.com/pkg/profile"
	"github.com/valyala/fasthttp"
)

// Program option vars:
var (
	daemonURLs        []string
	replicationFactor int
	backoff           time.Duration
	useGzip           bool
	doAbortOnExist    bool
	memprofile        bool
	consistency       string
)

// Global vars
var (
	loader  *load.BenchmarkRunner
	bufPool sync.Pool
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
	var csvDaemonURLs string

	flag.StringVar(&csvDaemonURLs, "urls", "http://localhost:8086", "InfluxDB URLs, comma-separated. Will be used in a round-robin fashion.")
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

	daemonURLs = strings.Split(csvDaemonURLs, ",")
	if len(daemonURLs) == 0 {
		log.Fatal("missing 'urls' flag")
	}
}

type benchmark struct{}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{scanner: bufio.NewScanner(br)}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(_ uint) load.PointIndexer {
	return &load.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{}
}

func main() {
	if memprofile {
		p := profile.Start(profile.MemProfile)
		defer p.Stop()
	}
	if loader.DoLoad() && loader.DoInit() {
		daemonURL := daemonURLs[0] // pick first one since it always exists

		// check that there are no pre-existing databases:
		existingDatabases, err := listDatabases(daemonURL)
		if err != nil {
			log.Fatal(err)
		}

		for _, db := range existingDatabases {
			if db == loader.DatabaseName() && doAbortOnExist {
				log.Fatalf("Database already exists. If you know what you're doing, run:\ncurl 'http://localhost:8086/query?q=drop%%20database%%20%s'\n", db)
			}
		}

		err = removeDatabase(daemonURL, loader.DatabaseName())
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second)

		err = createDb(daemonURL, loader.DatabaseName(), replicationFactor)
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second)
	}

	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	loader.RunBenchmark(&benchmark{}, load.SingleQueue)
}

type processor struct {
	backingOffChan chan bool
	backingOffDone chan struct{}
	httpWriter     *HTTPWriter
}

func (p *processor) Init(numWorker int, _ bool) {
	daemonURL := daemonURLs[numWorker%len(daemonURLs)]
	p.backingOffChan = make(chan bool, 100)
	p.backingOffDone = make(chan struct{})
	cfg := HTTPWriterConfig{
		DebugInfo:      fmt.Sprintf("worker #%d, dest url: %s", numWorker, daemonURL),
		Host:           daemonURL,
		Database:       loader.DatabaseName(),
		BackingOffChan: p.backingOffChan,
		BackingOffDone: p.backingOffDone,
	}
	p.httpWriter = NewHTTPWriter(cfg, consistency)
	go processBackoffMessages(numWorker, p.backingOffChan, p.backingOffDone)
}

func (p *processor) Close(_ bool) {
	close(p.backingOffChan)
	<-p.backingOffDone
}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)

	// Write the batch: try until backoff is not needed.
	if doLoad {
		var err error
		for {
			if useGzip {
				compressedBatch := bufPool.Get().(*bytes.Buffer)
				fasthttp.WriteGzip(compressedBatch, batch.buf.Bytes())
				_, err = p.httpWriter.WriteLineProtocol(compressedBatch.Bytes(), true)
				// Return the compressed batch buffer to the pool.
				compressedBatch.Reset()
				bufPool.Put(compressedBatch)
			} else {
				_, err = p.httpWriter.WriteLineProtocol(batch.buf.Bytes(), false)
			}

			if err == BackoffError {
				p.backingOffChan <- true
				time.Sleep(backoff)
			} else {
				p.backingOffChan <- false
				break
			}
		}
		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}
	}
	metricCnt := batch.metrics
	rowCnt := batch.rows

	// Return the batch buffer to the pool.
	batch.buf.Reset()
	bufPool.Put(batch.buf)
	return metricCnt, rowCnt
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

func removeDatabase(daemonURL, dbName string) error {
	u := fmt.Sprintf("%s/query?q=drop+database+%s", daemonURL, dbName)
	resp, err := http.Post(u, "text/plain", nil)
	if err != nil {
		return fmt.Errorf("drop db error: %s", err.Error())
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("drop db returned non-200 code: %d", resp.StatusCode)
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
