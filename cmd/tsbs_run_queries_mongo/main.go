// tsbs_run_queries_mongo speed tests Mongo using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided Mongo endpoint using mgo.
package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/query"
)

// Program option vars:
var (
	daemonURL string
	timeout   time.Duration
)

// Global vars:
var (
	runner  *query.BenchmarkRunner
	session *mgo.Session
)

// Parse args:
func init() {
	// needed for deserializing the mongo query from gob
	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
	gob.Register([]map[string]interface{}{})
	gob.Register(bson.M{})
	gob.Register([]bson.M{})

	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("url", "mongodb://localhost:27017", "Daemon URL.")
	pflag.Duration("read-timeout", 30*time.Second, "Timeout value for individual queries")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	daemonURL = viper.GetString("url")
	timeout = viper.GetDuration("read-timeout")

	runner = query.NewBenchmarkRunner(config)
}

func main() {
	var err error
	session, err = mgo.DialWithTimeout(daemonURL, timeout)
	if err != nil {
		log.Fatal(err)
	}
	runner.Run(&query.MongoPool, newProcessor)
}

type processor struct {
	collection *mgo.Collection
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	sess := session.Copy()
	db := sess.DB(runner.DatabaseName())
	p.collection = db.C("point_data")
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	mq := q.(*query.Mongo)
	start := time.Now().UnixNano()
	pipe := p.collection.Pipe(mq.BsonDoc).AllowDiskUse()
	iter := pipe.Iter()
	if runner.DebugLevel() > 0 {
		fmt.Println(mq.BsonDoc)
	}
	var result map[string]interface{}
	cnt := 0
	for iter.Next(&result) {
		if runner.DoPrintResponses() {
			fmt.Printf("ID %d: %v\n", q.GetID(), result)
		}
		cnt++
	}
	if runner.DebugLevel() > 0 {
		fmt.Println(cnt)
	}
	err := iter.Close()

	took := time.Now().UnixNano() - start
	lag := float64(took) / 1e6 // milliseconds
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, err
}
