package query

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// statProcessor is used to collect, analyze, and print query execution statistics.
type statProcessor interface {
	getArgs() *statProcessorArgs
	send(stats []*Stat)
	sendWarm(stats []*Stat)
	process(workers uint)
	CloseAndWait()
}

type statProcessorArgs struct {
	prewarmQueries   bool    // PrewarmQueries tells the StatProcessor whether we're running each query twice to prewarm the cache
	limit            *uint64 // limit is the number of statistics to analyze before stopping
	burnIn           uint64  // burnIn is the number of statistics to ignore before analyzing
	printInterval    uint64  // printInterval is how often print intermediate stats (number of queries)
	hdrLatenciesFile string  // hdrLatenciesFile is the filename to Write the High Dynamic Range (HDR) Histogram of Response Latencies to

}

// statProcessor is used to collect, analyze, and print query execution statistics.
type defaultStatProcessor struct {
	args     *statProcessorArgs
	wg       sync.WaitGroup
	c        chan *Stat // c is the channel for Stats to be sent for processing
	opsCount uint64
}

func newStatProcessor(args *statProcessorArgs) statProcessor {
	if args == nil {
		panic("Stat Processor needs args")
	}
	return &defaultStatProcessor{args: args}
}

func (sp *defaultStatProcessor) getArgs() *statProcessorArgs {
	return sp.args
}

func (sp *defaultStatProcessor) send(stats []*Stat) {
	if stats == nil {
		return
	}

	for _, s := range stats {
		sp.c <- s
	}
}

func (sp *defaultStatProcessor) sendWarm(stats []*Stat) {
	if stats == nil {
		return
	}

	for _, s := range stats {
		s.isWarm = true
	}
	sp.send(stats)
}

// process collects latency results, aggregating them into summary
// statistics. Optionally, they are printed to stderr at regular intervals.
func (sp *defaultStatProcessor) process(workers uint) {
	sp.c = make(chan *Stat, workers)
	sp.wg.Add(1)
	const allQueriesLabel = labelAllQueries
	statMapping := map[string]*statGroup{
		allQueriesLabel: newStatGroup(*sp.args.limit),
	}
	// Only needed when differentiating between cold & warm
	if sp.args.prewarmQueries {
		statMapping[labelColdQueries] = newStatGroup(*sp.args.limit)
		statMapping[labelWarmQueries] = newStatGroup(*sp.args.limit)
	}

	i := uint64(0)
	start := time.Now()
	prevTime := start
	prevRequestCount := uint64(0)

	for stat := range sp.c {
		atomic.AddUint64(&sp.opsCount, 1)
		if i < sp.args.burnIn {
			i++
			statPool.Put(stat)
			continue
		} else if i == sp.args.burnIn && sp.args.burnIn > 0 {
			_, err := fmt.Fprintf(os.Stderr, "burn-in complete after %d queries with %d workers\n", sp.args.burnIn, workers)
			if err != nil {
				log.Fatal(err)
			}
		}
		if _, ok := statMapping[string(stat.label)]; !ok {
			statMapping[string(stat.label)] = newStatGroup(*sp.args.limit)
		}

		statMapping[string(stat.label)].push(stat.value)

		if !stat.isPartial {
			statMapping[allQueriesLabel].push(stat.value)

			// Only needed when differentiating between cold & warm
			if sp.args.prewarmQueries {
				if stat.isWarm {
					statMapping[labelWarmQueries].push(stat.value)
				} else {
					statMapping[labelColdQueries].push(stat.value)
				}
			}

			// If we're prewarming queries (i.e., running them twice in a row),
			// only increment the counter for the first (cold) query. Otherwise,
			// increment for every query.
			if !sp.args.prewarmQueries || !stat.isWarm {
				i++
			}
		}

		statPool.Put(stat)

		// print stats to stderr (if printInterval is greater than zero):
		if sp.args.printInterval > 0 && i > 0 && i%sp.args.printInterval == 0 && (i < *sp.args.limit || *sp.args.limit == 0) {
			now := time.Now()
			sinceStart := now.Sub(start)
			took := now.Sub(prevTime)
			intervalQueryRate := float64(sp.opsCount-prevRequestCount) / float64(took.Seconds())
			overallQueryRate := float64(sp.opsCount) / float64(sinceStart.Seconds())
			_, err := fmt.Fprintf(os.Stderr, "After %d queries with %d workers:\nInterval query rate: %0.2f queries/sec\tOverall query rate: %0.2f queries/sec\n",
				i-sp.args.burnIn,
				workers,
				intervalQueryRate,
				overallQueryRate,
			)
			if err != nil {
				log.Fatal(err)
			}
			err = writeStatGroupMap(os.Stderr, statMapping)
			if err != nil {
				log.Fatal(err)
			}
			_, err = fmt.Fprintf(os.Stderr, "\n")
			if err != nil {
				log.Fatal(err)
			}
			prevRequestCount = sp.opsCount
			prevTime = now
		}
	}
	sinceStart := time.Now().Sub(start)
	overallQueryRate := float64(sp.opsCount) / float64(sinceStart.Seconds())
	// the final stats output goes to stdout:
	_, err := fmt.Printf("Run complete after %d queries with %d workers (Overall query rate %0.2f queries/sec):\n", i-sp.args.burnIn, workers, overallQueryRate)
	if err != nil {
		log.Fatal(err)
	}
	err = writeStatGroupMap(os.Stdout, statMapping)
	if err != nil {
		log.Fatal(err)
	}

	if len(sp.args.hdrLatenciesFile) > 0 {
		_, _ = fmt.Printf("Saving High Dynamic Range (HDR) Histogram of Response Latencies to %s\n", sp.args.hdrLatenciesFile)
		var b bytes.Buffer
		bw := bufio.NewWriter(&b)
		_, err = statMapping[allQueriesLabel].latencyHDRHistogram.PercentilesPrint(bw, 10, 1000.0)
		if err != nil {
			log.Fatal(err)
		}
		err = ioutil.WriteFile(sp.args.hdrLatenciesFile, b.Bytes(), 0644)
		if err != nil {
			log.Fatal(err)
		}

	}

	sp.wg.Done()
}

// CloseAndWait closes the stats channel and blocks until the StatProcessor has finished all the stats on its channel.
func (sp *defaultStatProcessor) CloseAndWait() {
	close(sp.c)
	sp.wg.Wait()
}
