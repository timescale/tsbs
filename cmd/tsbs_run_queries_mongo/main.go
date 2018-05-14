// tsbs_run_queries_mongo speed tests Mongo using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided Mongo endpoint using mgo.
package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// Program option vars:
var (
	daemonURL    string
	databaseName string
	doQueries    bool
	timeout      time.Duration
)

// Global vars:
var (
	benchmarkRunner *query.BenchmarkRunner
	session         *mgo.Session
)

// Parse args:
func init() {
	// needed for deserializing the mongo query from gob
	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
	gob.Register([]map[string]interface{}{})
	gob.Register(bson.M{})
	gob.Register([]bson.M{})
	benchmarkRunner = query.NewBenchmarkRunner()

	flag.StringVar(&daemonURL, "url", "mongodb://localhost:27017", "Daemon URL.")
	flag.StringVar(&databaseName, "db-name", "benchmark", "Name of database to use for queries")
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
	benchmarkRunner.Run(&query.MongoPool, newProcessor)
}

type processor struct {
	qe   *queryExecutor
	sess *mgo.Session
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	p.sess = session.Copy()
	db := p.sess.DB(databaseName)
	p.qe = &queryExecutor{collection: db.C("point_data")}
}

func (p *processor) ProcessQuery(sp *query.StatProcessor, q query.Query) {
	lag, err := p.qe.Do(q)
	if err != nil {
		panic(err)
	}
	sp.SendStat(q.HumanLabelName(), lag, !sp.PrewarmQueries)

	// If PrewarmQueries is set, we run the query as 'cold' first (see above),
	// then we immediately run it a second time and report that as the 'warm'
	// stat. This guarantees that the warm stat will reflect optimal cache performance.
	if sp.PrewarmQueries {
		lag, err = p.qe.Do(q)
		if err != nil {
			panic(err)
		}
		sp.SendStat(q.HumanLabelName(), lag, true)
	}
}

type queryExecutor struct {
	collection *mgo.Collection
}

// Do executes a Query and reports its time to completion and any error
func (qe *queryExecutor) Do(q query.Query) (float64, error) {
	mq := q.(*query.Mongo)
	start := time.Now().UnixNano()
	var err error
	if doQueries {
		pipe := qe.collection.Pipe(mq.BsonDoc).AllowDiskUse()
		iter := pipe.Iter()
		if benchmarkRunner.DebugLevel() > 0 {
			fmt.Println(mq.BsonDoc)
		}
		var result map[string]interface{}
		cnt := 0
		for iter.Next(&result) {
			if benchmarkRunner.DoPrintResponses() {
				fmt.Printf("ID %d: %v\n", q.GetID(), result)
			}
			cnt++
		}
		if benchmarkRunner.DebugLevel() > 0 {
			fmt.Println(cnt)
		}
		err = iter.Close()
	}

	took := time.Now().UnixNano() - start
	lag := float64(took) / 1e6 // milliseconds
	return lag, err
}
