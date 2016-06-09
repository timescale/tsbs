// bulk_load_cassandra loads a Cassandra daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

// Program option vars:
var (
	daemonUrl    string
	workers      int
	batchSize    int
	doLoad       bool
	writeTimeout time.Duration
)

// Global vars
var (
	batchChan    chan *gocql.Batch
	inputDone    chan struct{}
	workersGroup sync.WaitGroup
)

// Parse args:
func init() {
	flag.StringVar(&daemonUrl, "url", "localhost:9042", "Cassandra URL.")

	flag.IntVar(&batchSize, "batch-size", 100, "Batch size (input items).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.DurationVar(&writeTimeout, "write-timeout", 10*time.Second, "Write timeout.")

	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")

	flag.Parse()
}

func main() {
	if doLoad {
		createKeyspace(daemonUrl)
	}

	var session *gocql.Session

	if doLoad {
		cluster := gocql.NewCluster(daemonUrl)
		cluster.Keyspace = "measurements"
		cluster.Timeout = writeTimeout
		cluster.Consistency = gocql.One
		cluster.ProtoVersion = 4
		var err error
		session, err = cluster.CreateSession()
		if err != nil {
			log.Fatal(err)
		}
		defer session.Close()
	}

	batchChan = make(chan *gocql.Batch, workers)
	inputDone = make(chan struct{})

	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		go processBatches(session)
	}

	start := time.Now()
	itemsRead := scan(session, batchSize)

	<-inputDone
	close(batchChan)
	workersGroup.Wait()
	end := time.Now()
	took := end.Sub(start)
	rate := float64(itemsRead) / float64(took.Seconds())

	fmt.Printf("loaded %d items in %fsec with %d workers (mean rate %f/sec)\n", itemsRead, took.Seconds(), workers, rate)
}

// scan reads lines from stdin. It expects input in the Cassandra CQL format.
func scan(session *gocql.Session, itemsPerBatch int) int64 {
	var batch *gocql.Batch
	if doLoad {
		batch = session.NewBatch(gocql.LoggedBatch)
	}

	var n int
	var linesRead int64
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		linesRead++

		if !doLoad {
			continue
		}

		batch.Query(string(scanner.Bytes()))

		n++
		if n >= itemsPerBatch {
			batchChan <- batch
			batch = session.NewBatch(gocql.LoggedBatch)
			n = 0
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		batchChan <- batch
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)

	// The Cassandra query format uses 1 line per item:
	itemsRead := linesRead

	return itemsRead
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(session *gocql.Session) {
	for batch := range batchChan {
		if !doLoad {
			continue
		}

		// Write the batch.
		err := session.ExecuteBatch(batch)
		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}
	}
	workersGroup.Done()
}

func createKeyspace(daemon_url string) {
	cluster := gocql.NewCluster(daemonUrl)
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.Timeout = 10 * time.Second
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	if err := session.Query(`create keyspace measurements with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`).Exec(); err != nil {
		log.Print("if you know what you are doing, drop the keyspace with a command line:")
		log.Print("echo 'drop keyspace measurements;' | cqlsh <host>")
		log.Fatal(err)
	}
	for _, cassandraTypename := range []string{"bigint", "float", "double", "boolean", "blob"} {
		q := fmt.Sprintf(`CREATE TABLE measurements.series_%s (
					series_id text,
					timestamp_ns bigint,
					value %s,
					PRIMARY KEY (series_id, timestamp_ns)
				 )
				 WITH COMPACT STORAGE;`,
				 cassandraTypename, cassandraTypename)
		if err := session.Query(q).Exec(); err != nil {
			log.Fatal(err)
		}
	}
}
