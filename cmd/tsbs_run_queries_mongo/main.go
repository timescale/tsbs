// tsbs_run_queries_mongo speed tests Mongo using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided Mongo endpoint.
package main

import (
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// Program option vars:
var (
	daemonURL string
	timeout   time.Duration
)

// Global vars:
var (
	runner *query.BenchmarkRunner
	client *mongo.Client
)

// Parse args:
func init() {
	// needed for deserializing the mongo query from gob
	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
	gob.Register([]map[string]interface{}{})
	gob.Register(bson.M{})
	gob.Register(bson.D{})
	gob.Register([]bson.M{})
	gob.Register(time.Time{})

	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("url", "mongodb://localhost:27017", "Daemon URL.")
	pflag.Duration("read-timeout", 300*time.Second, "Timeout value for individual queries")

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
	opts := options.Client().ApplyURI(daemonURL).SetSocketTimeout(timeout)
	client, err = mongo.Connect(context.Background(), opts)
	if err != nil {
		log.Fatal(err)
	}
	runner.Run(&query.MongoPool, newProcessor)
}

type processor struct {
	collection *mongo.Collection
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	p.collection = client.Database(runner.DatabaseName()).Collection("point_data")
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	mq := q.(*query.Mongo)
	start := time.Now().UnixNano()

	cursor, err := p.collection.Aggregate(context.Background(), mq.BsonDoc)
	if err != nil {
		log.Fatal(err)
	}

	if runner.DebugLevel() > 0 {
		fmt.Println(mq.BsonDoc)
	}
	cnt := 0
	for cursor.Next(context.Background()) {
		if runner.DoPrintResponses() {
			fmt.Printf("ID %d: %v\n", q.GetID(), cursor.Current)
		}
		cnt++
	}
	if runner.DebugLevel() > 0 {
		fmt.Println(cnt)
	}
	err = cursor.Close(context.Background())

	took := time.Now().UnixNano() - start
	lag := float64(took) / 1e6 // milliseconds
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, err
}
