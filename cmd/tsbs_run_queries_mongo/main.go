// tsbs_run_queries_mongo speed tests Mongo using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided Mongo endpoint using mgo.
//
// TODO(rw): On my machine, this only decodes 700k/sec messages from stdin.
package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// Program option vars:
var (
	daemonURL            string
	databaseName         string
	debug                int
	prettyPrintResponses bool
	doQueries            bool
	timeout              time.Duration
)

// Global vars:
var (
	queryPool           = &query.MongoPool
	queryChan           chan query.Query
	benchmarkComponents *query.BenchmarkComponents
	session             *mgo.Session
)

// Parse args:
func init() {
	// needed for deserializing the mongo query from gob
	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
	gob.Register([]map[string]interface{}{})
	gob.Register(bson.M{})
	gob.Register([]bson.M{})
	benchmarkComponents = query.NewBenchmarkComponents()

	flag.StringVar(&daemonURL, "url", "mongodb://localhost:27017", "Daemon URL.")
	flag.StringVar(&databaseName, "db-name", "benchmark", "Name of database to use for queries")
	flag.IntVar(&debug, "debug", 0, "Whether to print debug messages.")
	flag.BoolVar(&prettyPrintResponses, "print-responses", false, "Pretty print JSON response bodies (for correctness checking) (default false).")
	flag.BoolVar(&doQueries, "do-queries", true, "Whether to perform queries (useful for benchmarking the query executor.)")
	flag.DurationVar(&timeout, "read-timeout", 30*time.Second, "Timeout value for individual queries")

	flag.Parse()
}

func main() {
	var err error
	session, err = mgo.DialWithTimeout(daemonURL, timeout)
	if err != nil {
		log.Fatal(err)
	}
	queryChan = make(chan query.Query, benchmarkComponents.Workers)
	benchmarkComponents.Run(queryPool, queryChan, processQueries)
}

// processQueries reads byte buffers from queryChan and writes them to the
// target server, while tracking latency.
func processQueries(wg *sync.WaitGroup, _ int) {
	sp := benchmarkComponents.StatProcessor
	sess := session.Copy()
	db := sess.DB(databaseName)
	collection := db.C("point_data")
	for q := range queryChan {
		lag, err := oneQuery(collection, q.(*query.Mongo))
		if err != nil {
			panic(err)
		}

		sp.SendStat(q.HumanLabelName(), lag, !sp.PrewarmQueries)

		// If PrewarmQueries is set, we run the query as 'cold' first (see above),
		// then we immediately run it a second time and report that as the 'warm'
		// stat. This guarantees that the warm stat will reflect optimal cache performance.
		if sp.PrewarmQueries {
			lag, err = oneQuery(collection, q.(*query.Mongo))
			if err != nil {
				panic(err)
			}
			sp.SendStat(q.HumanLabelName(), lag, true)
		}
		queryPool.Put(q)
	}
	wg.Done()
}

// oneQuery executes on Query
func oneQuery(collection *mgo.Collection, q *query.Mongo) (float64, error) {
	start := time.Now().UnixNano()
	var err error
	if doQueries {
		pipe := collection.Pipe(q.BsonDoc).AllowDiskUse()
		iter := pipe.Iter()
		if debug > 0 {
			fmt.Println(q.BsonDoc)
		}
		var result map[string]interface{}
		cnt := 0
		for iter.Next(&result) {
			if prettyPrintResponses {
				fmt.Printf("ID %d: %v\n", q.GetID(), result)
			}
			cnt++
		}
		if debug > 0 {
			fmt.Println(cnt)
		}
		err = iter.Close()
	}

	took := time.Now().UnixNano() - start
	lag := float64(took) / 1e6 // milliseconds
	return lag, err
}
