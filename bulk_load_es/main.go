// bulk_load_es loads an ElasticSearch daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
)

// Program option vars:
var (
	daemonUrl string
	refreshEachBatch bool
	//dbName    string
	workers   int
	batchSize int
)

// Global vars
var (
	bufPool      sync.Pool
	batchChan    chan *bytes.Buffer
	inputDone    chan struct{}
	workersGroup sync.WaitGroup
)

var defaultTemplate = []byte(`
{
  "template": "*",
  "settings": {
    "index": {
      "refresh_interval": "5s"
    }
  },
  "mappings": {
    "point": {
      "_all":            { "enabled": false },
      "_source":         { "enabled": true },
      "properties": {
        "timestamp":    { "type": "date", "doc_values": true }
      }
    }
  }
}
`)

// Parse args:
func init() {
	flag.StringVar(&daemonUrl, "url", "http://localhost:9200", "ElasticSearch URL.")
	flag.BoolVar(&refreshEachBatch, "refresh", true, "Whether each batch is immediately indexed.")

	flag.IntVar(&batchSize, "batch-size", 5000, "Batch size (input items).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")

	flag.Parse()
}

func main() {
	err := createESTemplate(daemonUrl, "measurements_template", defaultTemplate)
	if err != nil {
		log.Fatal(err)
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
			//Database: dbName,
		}
		go processBatches(NewHTTPWriter(cfg, refreshEachBatch))
	}

	scan(batchSize)

	<-inputDone
	close(batchChan)
	workersGroup.Wait()
}

// scan reads lines from stdin. It expects input in the ElasticSearch bulk
// format: two line pairs, the first line being an 'action' and the second line
// being the payload.
func scan(itemsPerBatch int) {
	buf := bufPool.Get().(*bytes.Buffer)

	var n int
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		buf.Write(scanner.Bytes())
		buf.Write([]byte("\n"))

		n++
		if n % 2 == 0 && (n / 2) >= itemsPerBatch {
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
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(w LineProtocolWriter) {
	for batch := range batchChan {
		// Write the batch.
		_, err := w.WriteLineProtocol(batch.Bytes())
		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}

		// Return the batch buffer to the pool.
		batch.Reset()
		bufPool.Put(batch)
	}
	workersGroup.Done()
}

func createESTemplate(daemonUrl, templateName string, templateBody []byte) error {
	u, err := url.Parse(daemonUrl)
	if err != nil {
		return err
	}

	u.Path = fmt.Sprintf("_template/%s", templateName)

	req, err := http.NewRequest("PUT", u.String(), bytes.NewReader(templateBody))
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
		return fmt.Errorf("bad mapping create")
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
