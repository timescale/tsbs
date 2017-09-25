package benchmarker

import (
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

const (
	LabelAllQueries  = "all queries"
	LabelColdQueries = "cold queries"
	LabelWarmQueries = "warm queries"
)

// QueryScanner is used to read in Queries from a Reader where they are
// Go-encoded and then distribute them to workers
type QueryScanner struct {
	r     io.Reader
	limit uint64
}

// NewQueryScanner returns a new QueryScanner for a given Reader and its limit
func NewQueryScanner(r io.Reader, limit uint64) *QueryScanner {
	return &QueryScanner{
		r:     r,
		limit: limit,
	}
}

// Scan reads encoded Queries and places them into a channel
func (qs *QueryScanner) Scan(pool *sync.Pool, c chan query.Query) {
	dec := gob.NewDecoder(qs.r)

	n := uint64(0)
	for {
		if qs.limit >= 0 && n >= qs.limit {
			break
		}

		q := pool.Get().(query.Query)
		err := dec.Decode(q)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		q.SetID(n)
		c <- q
		n++
	}
}

// StatProcessor is used to collect, analyze, and print query execution statistics.
type StatProcessor struct {
	// C is the channel for Stats to be sent for processing
	C chan *Stat
	// Limit is the number of statistics to analyze before stopping
	Limit uint64
	// BurnIn is the number of statistics to ignore before analyzing
	BurnIn uint64

	printInterval uint64
	statPool      sync.Pool
	wg            sync.WaitGroup
}

// NewStatProcessor returns a StatProcessor which is used to collect, analyze, and
// output statistics for query benchmarker
func NewStatProcessor() *StatProcessor {
	ret := &StatProcessor{
		statPool: GetStatPool(),
	}
	flag.Uint64Var(&ret.BurnIn, "burn-in", 0, "Number of queries to ignore before collecting statistics.")
	flag.Uint64Var(&ret.Limit, "limit", 0, "Limit the number of queries to send, 0 = no limit")
	flag.Uint64Var(&ret.printInterval, "print-interval", 100, "Print timing stats to stderr after this many queries (0 to disable)")

	return ret
}

func (sp *StatProcessor) getStat(partial bool) *Stat {
	ret := sp.statPool.Get().(*Stat)
	ret.IsPartial = partial
	return ret
}

// SendStat sends a new Stat from the pool (to conserve memory) to be processed.
// This Stat is usually the total time taken to execute the query; if you want
// to measure part of the execution, use SendPartialStat.
func (sp *StatProcessor) SendStat(label []byte, value float64, warm bool) {
	stat := sp.getStat(false)
	if warm {
		stat.InitWarm(label, value)
	} else {
		stat.Init(label, value)
	}
	sp.C <- stat
}

// SendPartialStat sends a new Stat from the pool (to conserve memory) to be processed.
// This Stat measures part of the process of a query (e.g. planning, gathering, etc.)
func (sp *StatProcessor) SendPartialStat(label []byte, value float64, warm bool) {
	stat := sp.getStat(true)
	if warm {
		stat.InitWarm(label, value)
	} else {
		stat.Init(label, value)
	}
	sp.C <- stat
}

// Process collects latency results, aggregating them into summary
// statistics. Optionally, they are printed to stderr at regular intervals.
func (sp *StatProcessor) Process(workers int) {
	sp.C = make(chan *Stat, workers)
	sp.wg.Add(1)
	const allQueriesLabel = LabelAllQueries
	statMapping := map[string]*StatGroup{
		allQueriesLabel:  NewStatGroup(sp.Limit),
		LabelColdQueries: NewStatGroup(sp.Limit),
		LabelWarmQueries: NewStatGroup(sp.Limit),
	}

	i := uint64(0)
	for stat := range sp.C {
		if i < sp.BurnIn {
			i++
			sp.statPool.Put(stat)
			continue
		} else if i == sp.BurnIn && sp.BurnIn > 0 {
			_, err := fmt.Fprintf(os.Stderr, "burn-in complete after %d queries with %d workers\n", sp.BurnIn, workers)
			if err != nil {
				log.Fatal(err)
			}
		}
		if _, ok := statMapping[string(stat.Label)]; !ok {
			statMapping[string(stat.Label)] = NewStatGroup(sp.Limit)
		}

		statMapping[string(stat.Label)].Push(stat.Value)

		if !stat.IsPartial {
			statMapping[allQueriesLabel].Push(stat.Value)

			if stat.IsWarm {
				statMapping[LabelWarmQueries].Push(stat.Value)
			} else {
				i++
				statMapping[LabelColdQueries].Push(stat.Value)
			}
		}

		sp.statPool.Put(stat)

		// print stats to stderr (if printInterval is greater than zero):
		if sp.printInterval > 0 && i > 0 && i%sp.printInterval == 0 && (i < sp.Limit || sp.Limit == 0) {
			_, err := fmt.Fprintf(os.Stderr, "after %d queries with %d workers:\n", i-sp.BurnIn, workers)
			if err != nil {
				log.Fatal(err)
			}
			WriteStatGroupMap(os.Stderr, statMapping)
			_, err = fmt.Fprintf(os.Stderr, "\n")
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	// the final stats output goes to stdout:
	_, err := fmt.Printf("run complete after %d queries with %d workers:\n", i-sp.BurnIn, workers)
	if err != nil {
		log.Fatal(err)
	}
	WriteStatGroupMap(os.Stdout, statMapping)
	sp.wg.Done()
}

// Wait blocks until the StatProcessor has finished all the stats on its channel
// and the channel has been closed.
func (sp *StatProcessor) Wait() {
	sp.wg.Wait()
}
