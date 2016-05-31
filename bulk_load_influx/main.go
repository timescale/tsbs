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
	"sync"
	"time"
)

// Program option vars:
var (
	daemonUrl string
	dbName    string
	workers   int
	batchSize int
	backoff   time.Duration
	doLoad    bool
)

// Global vars
var (
	bufPool      sync.Pool
	batchChan    chan *bytes.Buffer
	inputDone    chan struct{}
	workersGroup sync.WaitGroup
)

// Parse args:
func init() {
	flag.StringVar(&daemonUrl, "url", "http://localhost:8086", "Influxd URL.")
	flag.StringVar(&dbName, "db", "benchmark_db", "Database name.")
	flag.IntVar(&batchSize, "batch-size", 5000, "Batch size (input lines).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.DurationVar(&backoff, "backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")

	flag.Parse()
}

func main() {
	if doLoad {
		// check that there are no pre-existing databases:
		existingDatabases, err := listDatabases(daemonUrl)
		if err != nil {
			log.Fatal(err)
		}

		if len(existingDatabases) > 0 {
			log.Fatalf("There are databases already in the data store. If you know what you are doing, run the command:\ncurl 'http://localhost:8086/query?q=drop%%20database%%20%s'\n", existingDatabases[0])
		}

		err = createDb(daemonUrl, dbName)
		if err != nil {
			log.Fatal(err)
		}
	}

	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	batchChan = make(chan *bytes.Buffer, workers)
	inputDone = make(chan struct{})

	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		cfg := HTTPWriterConfig{
			Host:     daemonUrl,
			Database: dbName,
		}
		go processBatches(NewHTTPWriter(cfg))
	}

	start := time.Now()
	itemsRead := scan(batchSize)

	<-inputDone
	close(batchChan)
	workersGroup.Wait()

	end := time.Now()
	took := end.Sub(start)
	rate := float64(itemsRead) / float64(took.Seconds())

	fmt.Printf("loaded %d items in %fsec with %d workers (mean rate %f/sec)\n", itemsRead, took.Seconds(), workers, rate)
}

// scan reads one line at a time from stdin.
// When the requested number of lines per batch is met, send a batch over batchChan for the workers to write.
func scan(linesPerBatch int) int64 {
	buf := bufPool.Get().(*bytes.Buffer)

	var n int
	var itemsRead int64
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		itemsRead++

		buf.Write(scanner.Bytes())
		buf.Write([]byte("\n"))

		n++
		if n >= linesPerBatch {
			batchChan <- buf
			buf = bufPool.Get().(*bytes.Buffer)
			n = 0
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

	return itemsRead
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(w LineProtocolWriter) {
	for batch := range batchChan {
		if !doLoad {
			continue
		}
		// Write the batch: try until backoff is not needed.
		var err error
		for {
			_, err = w.WriteLineProtocol(batch.Bytes())
			if err == BackoffError {
				time.Sleep(backoff)
			} else {
				break
			}
		}
		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}

		// Return the batch buffer to the pool.
		batch.Reset()
		bufPool.Put(batch)
	}
	workersGroup.Done()
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
