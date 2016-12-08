// query_benchmarker_mongo speed tests Mongo using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided Mongo endpoint using mgo.
//
// TODO(rw): On my machine, this only decodes 700k/sec messages from stdin.
package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"sort"
	"sync"
	"time"

	"gopkg.in/mgo.v2"
)

// Program option vars:
var (
	daemonUrl            string
	workers              int
	debug                int
	prettyPrintResponses bool
	limit                int64
	burnIn               uint64
	printInterval        uint64
	memProfile           string
	doQueries            bool
)

// Global vars:
var (
	queryPool    sync.Pool
	queryChan    chan *Query
	statPool     sync.Pool
	statChan     chan *Stat
	workersGroup sync.WaitGroup
	statGroup    sync.WaitGroup
)

type S []interface{}
type M map[string]interface{}

// Parse args:
func init() {
	// needed for deserializing the mongo query from gob
	gob.Register(S{})
	gob.Register(M{})
	gob.Register([]M{})

	flag.StringVar(&daemonUrl, "url", "mongodb://localhost:27017", "Daemon URL.")
	flag.IntVar(&workers, "workers", 1, "Number of concurrent requests to make.")
	flag.IntVar(&debug, "debug", 0, "Whether to print debug messages.")
	flag.Int64Var(&limit, "limit", -1, "Limit the number of queries to send.")
	flag.Uint64Var(&burnIn, "burn-in", 0, "Number of queries to ignore before collecting statistics.")
	flag.Uint64Var(&printInterval, "print-interval", 100, "Print timing stats to stderr after this many queries (0 to disable)")
	flag.BoolVar(&prettyPrintResponses, "print-responses", false, "Pretty print JSON response bodies (for correctness checking) (default false).")
	flag.StringVar(&memProfile, "memprofile", "", "Write a memory profile to this file.")
	flag.BoolVar(&doQueries, "do-queries", true, "Whether to perform queries (useful for benchmarking the query executor.)")

	flag.Parse()
}

func main() {
	// Make pools to minimize heap usage:
	queryPool = sync.Pool{
		New: func() interface{} {
			return &Query{
				HumanLabel:       make([]byte, 0, 1024),
				HumanDescription: make([]byte, 0, 1024),
				BsonDoc:          nil,
			}
		},
	}

	statPool = sync.Pool{
		New: func() interface{} {
			return &Stat{
				Label: make([]byte, 0, 1024),
				Value: 0.0,
			}
		},
	}

	// Make data and control channels:
	queryChan = make(chan *Query, workers)
	statChan = make(chan *Stat, workers)

	// Launch the stats processor:
	statGroup.Add(1)
	go processStats()

	// Establish the connection pool:
	session, err := mgo.Dial(daemonUrl)
	if err != nil {
		log.Fatal(err)
	}

	// Launch the query processors:
	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		go processQueries(session)
	}

	// Read in jobs, closing the job channel when done:
	input := bufio.NewReaderSize(os.Stdin, 1<<20)
	wallStart := time.Now()
	scan(input)
	close(queryChan)

	// Block for workers to finish sending requests, closing the stats
	// channel when done:
	workersGroup.Wait()
	close(statChan)

	// Wait on the stat collector to finish (and print its results):
	statGroup.Wait()

	wallEnd := time.Now()
	wallTook := wallEnd.Sub(wallStart)
	_, err = fmt.Printf("wall clock time: %fsec\n", float64(wallTook.Nanoseconds())/1e9)
	if err != nil {
		log.Fatal(err)
	}

	// (Optional) create a memory profile:
	if memProfile != "" {
		f, err := os.Create(memProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}

// scan reads encoded Queries and places them onto the workqueue.
func scan(r io.Reader) {
	dec := gob.NewDecoder(r)

	n := int64(0)
	for {
		if limit >= 0 && n >= limit {
			break
		}

		q := queryPool.Get().(*Query)
		err := dec.Decode(q)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal("decoder", err)
		}

		q.ID = n

		queryChan <- q

		n++

	}
}

// processQueries reads byte buffers from queryChan and writes them to the
// target server, while tracking latency.
func processQueries(session *mgo.Session) {
	for q := range queryChan {
		lag, err := oneQuery(session, q)

		stat := statPool.Get().(*Stat)
		stat.Init(q.HumanLabel, lag)
		statChan <- stat

		queryPool.Put(q)
		if err != nil {
			log.Fatalf("Error during request: %s\n", err.Error())
		}
	}
	workersGroup.Done()
}

// oneQuery executes on Query
func oneQuery(session *mgo.Session, q *Query) (float64, error) {
	start := time.Now().UnixNano()
	var err error
	if doQueries {
		db := session.DB(unsafeBytesToString(q.DatabaseName))
		//fmt.Printf("db: %#v\n", db)
		collection := db.C(unsafeBytesToString(q.CollectionName))
		//fmt.Printf("collection: %#v\n", collection)
		pipe := collection.Pipe(q.BsonDoc)
		iter := pipe.Iter()
		type Result struct {
			Id struct {
				TimeBucket int64 `bson:"time_bucket"`
			} `bson:"_id"`
			Value float64 `bson:"max_value"`
		}

		result := Result{}
		for iter.Next(&result) {
			if prettyPrintResponses {
				t := time.Unix(0, result.Id.TimeBucket).UTC()
				fmt.Printf("ID %d: %s, %f\n", q.ID, t, result.Value)
			}
		}

		err = iter.Close()
	}

	took := time.Now().UnixNano() - start
	lag := float64(took) / 1e6 // milliseconds
	return lag, err
}

// processStats collects latency results, aggregating them into summary
// statistics. Optionally, they are printed to stderr at regular intervals.
func processStats() {
	const allQueriesLabel = "all queries"
	statMapping := map[string]*StatGroup{
		allQueriesLabel: &StatGroup{},
	}

	i := uint64(0)
	for stat := range statChan {
		if i < burnIn {
			i++
			statPool.Put(stat)
			continue
		} else if i == burnIn && burnIn > 0 {
			_, err := fmt.Fprintf(os.Stderr, "burn-in complete after %d queries with %d workers\n", burnIn, workers)
			if err != nil {
				log.Fatal(err)
			}
		}

		if _, ok := statMapping[string(stat.Label)]; !ok {
			statMapping[string(stat.Label)] = &StatGroup{}
		}

		statMapping[allQueriesLabel].Push(stat.Value)
		statMapping[string(stat.Label)].Push(stat.Value)

		statPool.Put(stat)

		i++

		// print stats to stderr (if printInterval is greater than zero):
		if printInterval > 0 && i > 0 && i%printInterval == 0 && (int64(i) < limit || limit < 0) {
			_, err := fmt.Fprintf(os.Stderr, "after %d queries with %d workers:\n", i - burnIn, workers)
			if err != nil {
				log.Fatal(err)
			}
			fprintStats(os.Stderr, statMapping)
			_, err = fmt.Fprintf(os.Stderr, "\n")
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	// the final stats output goes to stdout:
	_, err := fmt.Printf("run complete after %d queries with %d workers:\n", i - burnIn, workers)
	if err != nil {
		log.Fatal(err)
	}
	fprintStats(os.Stdout, statMapping)
	statGroup.Done()
}

// fprintStats pretty-prints stats to the given writer.
func fprintStats(w io.Writer, statGroups map[string]*StatGroup) {
	maxKeyLength := 0
	keys := make([]string, 0, len(statGroups))
	for k := range statGroups {
		if len(k) > maxKeyLength {
			maxKeyLength = len(k)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := statGroups[k]
		minRate := 1e3 / v.Min
		meanRate := 1e3 / v.Mean
		maxRate := 1e3 / v.Max
		paddedKey := fmt.Sprintf("%s", k)
		for len(paddedKey) < maxKeyLength {
			paddedKey += " "
		}
		_, err := fmt.Fprintf(w, "%s : min: %8.2fms (%7.2f/sec), mean: %8.2fms (%7.2f/sec), max: %7.2fms (%6.2f/sec), count: %8d, sum: %5.1fsec \n", paddedKey, v.Min, minRate, v.Mean, meanRate, v.Max, maxRate, v.Count, v.Sum/1e3)
		if err != nil {
			log.Fatal(err)
		}
	}

}
