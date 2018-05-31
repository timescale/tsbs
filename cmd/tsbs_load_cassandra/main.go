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
	"time"

	"bitbucket.org/440-labs/tsbs/load"
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
	loader *load.BenchmarkRunner
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
	session *gocql.Session
}

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
	return &processor{b.session}
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

	loader.RunBenchmark(&benchmark{session: session}, load.SingleQueue)
}

type processor struct {
	session *gocql.Session
}

func (p *processor) Init(_ int, _ bool) {}

// ProcessBatch reads eventsBatches which contain rows of CQL strings and
// creates a gocql.LoggedBatch to insert
func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	events := b.(*eventsBatch)

	if doLoad {
		batch := p.session.NewBatch(gocql.LoggedBatch)
		for _, event := range events.rows {
			batch.Query(singleMetricToInsertStatement(event))
		}

		err := p.session.ExecuteBatch(batch)
		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}
	}
	metricCnt := uint64(len(events.rows))
	events.rows = events.rows[:0]
	ePool.Put(events)
	return metricCnt, 0
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

	// Drop the keyspace to avoid errors about existing keyspaces
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
