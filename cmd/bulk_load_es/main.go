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
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/influxdata/influxdb-comparisons/util/telemetry"
	"github.com/valyala/fasthttp"
)

// Program option vars:
var (
	csvDaemonUrls           string
	daemonUrls              []string
	refreshEachBatch        bool
	workers                 int
	batchSize               int
	itemLimit               int64
	indexTemplateName       string
	useGzip                 bool
	doLoad                  bool
	doDBCreate              bool
	numberOfReplicas        uint
	numberOfShards          uint
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
	telemetryChanPoints chan *telemetry.Point
	telemetryChanDone   chan struct{}
	telemetryHostname   string
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
      "refresh_interval": "5s",
      "number_of_replicas": {{.NumberOfReplicas}},
      "number_of_shards": {{.NumberOfShards}}
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
      "refresh_interval": "5s",
      "number_of_replicas": {{.NumberOfReplicas}},
      "number_of_shards": {{.NumberOfShards}}
    }
  },
  "mappings": {
    "_default_": {
      "dynamic_templates": [
        {
          "all_string_fields_can_be_used_for_filtering": {
            "match": "*",
            "match_mapping_type": "string",
            "mapping": {
              "type": "string",
              "doc_values": true,
              "index": "not_analyzed"
            }
          }
        },
        {
          "all_nonstring_fields_are_just_stored_in_column_index": {
            "match": "*",
            "match_mapping_type": "*",
            "mapping": {
              "doc_values": true,
              "index": "no"
            }
          }
        }
      ],
      "_all": { "enabled": false },
      "_source": { "enabled": false },
      "properties": {
        "timestamp": {
          "type": "date",
          "doc_values": true,
          "index": "not_analyzed"
        }
      }
    }
  }
}

`)

// Parse args:
func init() {
	flag.StringVar(&csvDaemonUrls, "urls", "http://localhost:9200", "ElasticSearch URLs, comma-separated. Will be used in a round-robin fashion.")
	flag.BoolVar(&refreshEachBatch, "refresh", true, "Whether each batch is immediately indexed.")
	flag.Int64Var(&itemLimit, "item-limit", -1, "Number of items to read from stdin before quitting. (2 lines of input = 1 item)")

	flag.IntVar(&batchSize, "batch-size", 5000, "Batch size (input items).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")

	flag.StringVar(&indexTemplateName, "index-template", "default", "ElasticSearch index template to use (choices: default, aggregation).")

	flag.BoolVar(&useGzip, "gzip", true, "Whether to gzip encode requests (default true).")

	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")
	flag.BoolVar(&doDBCreate, "do-db-create", true, "Whether to create the database.")

	flag.UintVar(&numberOfReplicas, "number-of-replicas", 0, "Number of ES replicas (note: replicas == replication_factor - 1). Zero replicas means RF of 1.")
	flag.UintVar(&numberOfShards, "number-of-shards", 5, "Number of ES shards. Typically you will set this to the number of nodes in the cluster.")

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

	if _, ok := indexTemplateChoices[indexTemplateName]; !ok {
		log.Fatalf("invalid index template type")
	}
}

func main() {
	if doLoad && doDBCreate {
		// check that there are no pre-existing index templates:
		existingIndexTemplates, err := listIndexTemplates(daemonUrls[0])
		if err != nil {
			log.Fatal(err)
		}

		if len(existingIndexTemplates) > 0 {
			log.Fatal("There are index templates already in the data store. If you know what you are doing, clear them first with a command like:\ncurl -XDELETE 'http://localhost:9200/_template/*'")
		}

		// check that there are no pre-existing indices:
		existingIndices, err := listIndices(daemonUrls[0])
		if err != nil {
			log.Fatal(err)
		}

		if len(existingIndices) > 0 {
			log.Fatal("There are indices already in the data store. If you know what you are doing, clear them first with a command like:\ncurl -XDELETE 'http://localhost:9200/_all'")
		}

		// create the index template:
		indexTemplate := indexTemplateChoices[indexTemplateName]
		err = createESTemplate(daemonUrls[0], "measurements_template", indexTemplate, numberOfReplicas, numberOfShards)
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

	if telemetryHost != "" {
		telemetryCollector := telemetry.NewCollector(telemetryHost, "telegraf")
		telemetryChanPoints, telemetryChanDone = telemetry.EZRunAsync(telemetryCollector, telemetryBatchSize, telemetryExperimentName, telemetryStderr)
	}

	for i := 0; i < workers; i++ {
		daemonUrl := daemonUrls[i%len(daemonUrls)]
		workersGroup.Add(1)
		cfg := HTTPWriterConfig{
			Host: daemonUrl,
		}
		go processBatches(NewHTTPWriter(cfg, refreshEachBatch), telemetryChanPoints, fmt.Sprintf("%d", i))
	}

	start := time.Now()
	itemsRead := scan(batchSize)

	<-inputDone
	close(batchChan)
	workersGroup.Wait()
	end := time.Now()
	took := end.Sub(start)
	rate := float64(itemsRead) / float64(took.Seconds())

	if telemetryHost != "" {
		close(telemetryChanPoints)
		<-telemetryChanDone
	}

	fmt.Printf("loaded %d items in %fsec with %d workers (mean rate %f items/sec)\n", itemsRead, took.Seconds(), workers, rate)
}

// scan reads items from stdin. It expects input in the ElasticSearch bulk
// format: two line pairs, the first line being an 'action' and the second line
// being the payload. (2 lines = 1 item)
func scan(itemsPerBatch int) int64 {
	buf := bufPool.Get().(*bytes.Buffer)

	var n int
	var linesRead int64
	var itemsRead int64
	var itemsThisBatch int
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		linesRead++

		buf.Write(scanner.Bytes())
		buf.Write([]byte("\n"))

		//n++
		if linesRead%2 == 0 {
			itemsRead++
			itemsThisBatch++
		}

		hitLimit := itemLimit >= 0 && itemsRead >= itemLimit

		if itemsThisBatch == itemsPerBatch || hitLimit {
			batchChan <- buf
			buf = bufPool.Get().(*bytes.Buffer)
			itemsThisBatch = 0
		}

		if hitLimit {
			break
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

	return itemsRead
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(w *HTTPWriter, telemetrySink chan *telemetry.Point, telemetryWorkerLabel string) {
	var batchesSeen int64
	for batch := range batchChan {
		batchesSeen++
		if !doLoad {
			continue
		}

		var err error
		var bodySize int

		// Write the batch.
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

		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}

		// Return the batch buffer to the pool.
		batch.Reset()
		bufPool.Put(batch)

		// Report telemetry, if applicable:
		if telemetrySink != nil {
			p := telemetry.GetPointFromGlobalPool()
			p.Init("benchmark_write", time.Now().UnixNano())
			p.AddTag("src_addr", telemetryHostname)
			p.AddTag("dst_addr", w.c.Host)
			p.AddTag("worker_id", telemetryWorkerLabel)
			p.AddInt64Field("worker_req_num", batchesSeen)
			p.AddBoolField("gzip", useGzip)
			p.AddInt64Field("body_bytes", int64(bodySize))
			telemetrySink <- p
		}
	}
	workersGroup.Done()
}

// createESTemplate uses a Go text/template to create an ElasticSearch index
// template. (This terminological conflict is mostly unavoidable).
func createESTemplate(daemonUrl, indexTemplateName string, indexTemplateBodyTemplate []byte, numberOfReplicas, numberOfShards uint) error {
	// set up URL:
	u, err := url.Parse(daemonUrl)
	if err != nil {
		return err
	}
	u.Path = fmt.Sprintf("_template/%s", indexTemplateName)

	// parse and execute the text/template:
	t := template.Must(template.New("index_template").Parse(string(indexTemplateBodyTemplate)))
	var body bytes.Buffer
	params := struct {
		NumberOfReplicas uint
		NumberOfShards   uint
	}{
		NumberOfReplicas: numberOfReplicas,
		NumberOfShards:   numberOfShards,
	}
	err = t.Execute(&body, params)
	if err != nil {
		return err
	}

	// do the HTTP PUT request with the body data:
	req, err := http.NewRequest("PUT", u.String(), bytes.NewReader(body.Bytes()))
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

//func createDb(daemon_url, dbname string) error {
//	u, err := url.Parse(daemon_url)
//	if err != nil {
//		return err
//	}
//
//	// serialize params the right way:
//	u.Path = "query"
//	v := u.Query()
//	v.Set("q", fmt.Sprintf("CREATE DATABASE %s", dbname))
//	u.RawQuery = v.Encode()
//
//	req, err := http.NewRequest("GET", u.String(), nil)
//	if err != nil {
//		return err
//	}
//
//	client := &http.Client{}
//	resp, err := client.Do(req)
//	if err != nil {
//		return err
//	}
//	defer resp.Body.Close()
//	// does the body need to be read into the void?
//
//	if resp.StatusCode != 200 {
//		return fmt.Errorf("bad db create")
//	}
//	return nil
//}

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
