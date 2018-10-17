package query

import (
	"fmt"
	"log"
	"os"
	"sync"
)

// statProcessor is used to collect, analyze, and print query execution statistics.
type statProcessor struct {
	prewarmQueries bool       // PrewarmQueries tells the StatProcessor whether we're running each query twice to prewarm the cache
	c              chan *Stat // c is the channel for Stats to be sent for processing
	limit          *uint64    // limit is the number of statistics to analyze before stopping
	burnIn         uint64     // burnIn is the number of statistics to ignore before analyzing
	printInterval  uint64     // printInterval is how often print intermediate stats (number of queries)
	wg             sync.WaitGroup
}

func (sp *statProcessor) sendStats(stats []*Stat) {
	if stats == nil {
		return
	}

	for _, s := range stats {
		sp.c <- s
	}
}

func (sp *statProcessor) sendStatsWarm(stats []*Stat) {
	if stats == nil {
		return
	}

	for _, s := range stats {
		s.isWarm = true
	}
	sp.sendStats(stats)
}

// process collects latency results, aggregating them into summary
// statistics. Optionally, they are printed to stderr at regular intervals.
func (sp *statProcessor) process(workers uint) {
	sp.c = make(chan *Stat, workers)
	sp.wg.Add(1)
	const allQueriesLabel = labelAllQueries
	statMapping := map[string]*statGroup{
		allQueriesLabel: newStatGroup(*sp.limit),
	}
	// Only needed when differentiating between cold & warm
	if sp.prewarmQueries {
		statMapping[labelColdQueries] = newStatGroup(*sp.limit)
		statMapping[labelWarmQueries] = newStatGroup(*sp.limit)
	}

	i := uint64(0)
	for stat := range sp.c {
		if i < sp.burnIn {
			i++
			statPool.Put(stat)
			continue
		} else if i == sp.burnIn && sp.burnIn > 0 {
			_, err := fmt.Fprintf(os.Stderr, "burn-in complete after %d queries with %d workers\n", sp.burnIn, workers)
			if err != nil {
				log.Fatal(err)
			}
		}
		if _, ok := statMapping[string(stat.label)]; !ok {
			statMapping[string(stat.label)] = newStatGroup(*sp.limit)
		}

		statMapping[string(stat.label)].push(stat.value)

		if !stat.isPartial {
			statMapping[allQueriesLabel].push(stat.value)

			// Only needed when differentiating between cold & warm
			if sp.prewarmQueries {
				if stat.isWarm {
					statMapping[labelWarmQueries].push(stat.value)
				} else {
					statMapping[labelColdQueries].push(stat.value)
				}
			}

			// If we're prewarming queries (i.e., running them twice in a row),
			// only increment the counter for the first (cold) query. Otherwise,
			// increment for every query.
			if !sp.prewarmQueries || !stat.isWarm {
				i++
			}
		}

		statPool.Put(stat)

		// print stats to stderr (if printInterval is greater than zero):
		if sp.printInterval > 0 && i > 0 && i%sp.printInterval == 0 && (i < *sp.limit || *sp.limit == 0) {
			_, err := fmt.Fprintf(os.Stderr, "after %d queries with %d workers:\n", i-sp.burnIn, workers)
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
		}
	}

	// the final stats output goes to stdout:
	_, err := fmt.Printf("run complete after %d queries with %d workers:\n", i-sp.burnIn, workers)
	if err != nil {
		log.Fatal(err)
	}
	err = writeStatGroupMap(os.Stdout, statMapping)
	if err != nil {
		log.Fatal(err)
	}
	sp.wg.Done()
}

// CloseAndWait closes the stats channel and blocks until the StatProcessor has finished all the stats on its channel.
func (sp *statProcessor) CloseAndWait() {
	close(sp.c)
	sp.wg.Wait()
}
