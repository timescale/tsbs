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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/load"

	"github.com/gocql/gocql"
)

// Program option vars:
var (
	hosts             string
	replicationFactor int
	consistencyLevel  string
	writeTimeout      time.Duration
)

// Global vars
var (
	metricCount uint64
	loader      *load.BenchmarkRunner
)

// Map of user specified strings to gocql consistency settings
var consistencyMapping = map[string]gocql.Consistency{
	"ALL":    gocql.All,
	"ANY":    gocql.Any,
	"QUORUM": gocql.Quorum,
	"ONE":    gocql.One,
	"TWO":    gocql.Two,
	"THREE":  gocql.Three,
}

// Parse args:
func init() {
	loader = load.GetBenchmarkRunnerWithBatchSize(100)

	flag.StringVar(&hosts, "hosts", "localhost:9042", "Comma separated list of Cassandra hosts in a cluster.")

	flag.IntVar(&replicationFactor, "replication-factor", 1, "Number of nodes that must have a copy of each key.")
	flag.StringVar(&consistencyLevel, "consistency-level", "ALL", "Desired write consistency level. See Cassandra consistency documentation. Default: ALL")
	flag.DurationVar(&writeTimeout, "write-timeout", 10*time.Second, "Write timeout.")

	flag.Parse()

	if _, ok := consistencyMapping[consistencyLevel]; !ok {
		fmt.Println("Invalid consistency level.")
		os.Exit(1)
	}

}

type benchmark struct {
	l        *load.BenchmarkRunner
	channels []*load.DuplexChannel
	session  *gocql.Session
}

func (b *benchmark) Work(wg *sync.WaitGroup, _ int) {
	go processBatches(wg, b.session, b.channels[0])
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

func main() {
	var session *gocql.Session
	if loader.DoLoad() {
		if loader.DoInit() {
			createKeyspace(hosts)
		}

		cluster := gocql.NewCluster(strings.Split(hosts, ",")...)
		cluster.Keyspace = loader.DatabaseName()
		cluster.Timeout = writeTimeout
		cluster.Consistency = consistencyMapping[consistencyLevel]
		cluster.ProtoVersion = 4
		var err error
		session, err = cluster.CreateSession()
		if err != nil {
			log.Fatal(err)
		}
		defer session.Close()
	}

	channels := []*load.DuplexChannel{load.NewDuplexChannel(loader.NumWorkers())}
	b := &benchmark{l: loader, channels: channels, session: session}

	loader.RunBenchmark(b, &metricCount, nil)
}

// processBatches reads eventsBatches (contains rows of CQL strings) from a
// channel and creates a gocql.LoggedBatch to insert
func processBatches(wg *sync.WaitGroup, session *gocql.Session, ch *load.DuplexChannel) {
	for item := range ch.GetWorkerChannel() {
		events := item.(*eventsBatch)
		if loader.DoLoad() {
			batch := session.NewBatch(gocql.LoggedBatch)
			for _, event := range events.rows {
				batch.Query(event)
			}

			err := session.ExecuteBatch(batch)
			if err != nil {
				log.Fatalf("Error writing: %s\n", err.Error())
			}
		}
		atomic.AddUint64(&metricCount, uint64(len(events.rows)))
		events.rows = events.rows[:0]
		ePool.Put(events)
		ch.SendToScanner()
	}
	wg.Done()
}

func createKeyspace(hosts string) {
	cluster := gocql.NewCluster(strings.Split(hosts, ",")...)
	cluster.Consistency = consistencyMapping[consistencyLevel]
	cluster.ProtoVersion = 4
	cluster.Timeout = 10 * time.Second
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	// Drop the measurements keyspace to avoid errors about existing keyspaces
	if err := session.Query(fmt.Sprintf("drop keyspace if exists %s;", loader.DatabaseName())).Exec(); err != nil {
		log.Fatal(err)
	}

	replicationConfiguration := fmt.Sprintf("{ 'class': 'SimpleStrategy', 'replication_factor': %d }", replicationFactor)
	if err := session.Query(fmt.Sprintf("create keyspace %s with replication = %s;", loader.DatabaseName(), replicationConfiguration)).Exec(); err != nil {
		log.Print("if you know what you are doing, drop the keyspace with a command line:")
		log.Print(fmt.Sprintf("echo 'drop keyspace %s;' | cqlsh <host>", loader.DatabaseName()))
		log.Fatal(err)
	}
	for _, cassandraTypename := range []string{"bigint", "float", "double", "boolean", "blob"} {
		q := fmt.Sprintf(`CREATE TABLE %s.series_%s (
					series_id text,
					timestamp_ns bigint,
					value %s,
					PRIMARY KEY (series_id, timestamp_ns)
				 )
				 WITH COMPACT STORAGE;`,
			loader.DatabaseName(), cassandraTypename, cassandraTypename)
		if err := session.Query(q).Exec(); err != nil {
			log.Fatal(err)
		}
	}
}
