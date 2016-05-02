// bulk_load_es loads an ElasticSearch daemon with data from stdin.
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
	daemonUrl         string
	refreshEachBatch  bool
	workers           int
	batchSize         int
	indexTemplateName string
	doLoad            bool
)

// Global vars
var (
	bufPool      sync.Pool
	batchChan    chan *bytes.Buffer
	inputDone    chan struct{}
	workersGroup sync.WaitGroup
)

// Args parsing vars
var (
	indexTemplateChoices = map[string][]byte{
		"default":     defaultTemplate,
		"aggregation": aggregationTemplate,
	}
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

var aggregationTemplate = []byte(`
{
  "template": "*",
  "settings": {
    "index": {
      "refresh_interval": "5s"
    }
  },
  "mappings": {
    "_default_": {
      "dynamic_templates": [
        {
          "strings_are_indexed_exactly_for_filtering": {
            "match": "*",
            "match_mapping_type": "string",
            "mapping": { "type": "string",  "doc_values": true, "index": "not_analyzed" }
          }
        },
	{
	  "all_other_types_are_in_column_index_but_not_searchable": {
            "match": "*",
            "mapping": { "doc_values": true, "index": "no" }
          }
        }
      ],
      "_all": { "enabled": false },
      "_source": { "enabled": false },
      "properties": {
        "timestamp": { "type": "date", "doc_values": true }
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

	flag.StringVar(&indexTemplateName, "index-template", "default", "ElasticSearch index template to use (choices: default, aggregation).")

	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")

	flag.Parse()

	if _, ok := indexTemplateChoices[indexTemplateName]; !ok {
		log.Fatalf("invalid index template type")
	}
}

func main() {
	if doLoad {
		// check that there are no pre-existing index templates:
		existingIndexTemplates, err := listIndexTemplates(daemonUrl)
		if err != nil {
			log.Fatal(err)
		}

		if len(existingIndexTemplates) > 0 {
			log.Fatal("There are index templates already in the data store. If you know what you are doing, clear them first with a command like:\ncurl -XDELETE 'http://localhost:9200/_template/*'")
		}

		// check that there are no pre-existing indices:
		existingIndices, err := listIndices(daemonUrl)
		if err != nil {
			log.Fatal(err)
		}

		if len(existingIndices) > 0 {
			log.Fatal("There are indices already in the data store. If you know what you are doing, clear them first with a command like:\ncurl -XDELETE 'http://localhost:9200/_all'")
		}

		// create the index template:
		indexTemplate := indexTemplateChoices[indexTemplateName]
		err = createESTemplate(daemonUrl, "measurements_template", indexTemplate)
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
			Host: daemonUrl,
		}
		go processBatches(NewHTTPWriter(cfg, refreshEachBatch))
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

// scan reads lines from stdin. It expects input in the ElasticSearch bulk
// format: two line pairs, the first line being an 'action' and the second line
// being the payload.
func scan(itemsPerBatch int) int64 {
	buf := bufPool.Get().(*bytes.Buffer)

	var n int
	var linesRead int64
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		linesRead++

		buf.Write(scanner.Bytes())
		buf.Write([]byte("\n"))

		n++
		if n%2 == 0 && (n/2) >= itemsPerBatch {
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

	// The ES bulk format uses 2 lines per item:
	if linesRead%2 != 0 {
		log.Fatalf("the number of lines read was not a multiple of 2, which indicates a bad bulk format for Elastic")
	}
	itemsRead := linesRead / 2

	return itemsRead
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(w LineProtocolWriter) {
	for batch := range batchChan {
		if !doLoad {
			continue
		}

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

// listIndexTemplates lists the existing index templates in ElasticSearch.
func listIndexTemplates(daemonUrl string) (map[string]interface{}, error) {
	u := fmt.Sprintf("%s/_template", daemonUrl)
	resp, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var listing map[string]interface{}
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return nil, err
	}

	return listing, nil
}

// listIndices lists the existing indices in ElasticSearch.
func listIndices(daemonUrl string) (map[string]interface{}, error) {
	u := fmt.Sprintf("%s/*", daemonUrl)
	resp, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var listing map[string]interface{}
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return nil, err
	}

	return listing, nil
}
