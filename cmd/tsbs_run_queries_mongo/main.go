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

	"bitbucket.org/440-labs/tsbs/query"
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
	collection *mgo.Collection
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	sess := session.Copy()
	db := sess.DB(databaseName)
	p.collection = db.C("point_data")
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	mq := q.(*query.Mongo)
	start := time.Now().UnixNano()
	var err error
	if doQueries {
		pipe := p.collection.Pipe(mq.BsonDoc).AllowDiskUse()
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
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, err
}
