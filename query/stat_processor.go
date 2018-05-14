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

func (sp *statProcessor) getStat(partial bool) *Stat {
	if partial {
		return GetPartialStat()
	}
	return GetStat()
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
		s.IsWarm = true
	}
	sp.sendStats(stats)
}

// Process collects latency results, aggregating them into summary
// statistics. Optionally, they are printed to stderr at regular intervals.
func (sp *statProcessor) process(workers int) {
	sp.c = make(chan *Stat, workers)
	sp.wg.Add(1)
	const allQueriesLabel = labelAllQueries
	statMapping := map[string]*StatGroup{
		allQueriesLabel: NewStatGroup(*sp.limit),
	}
	// Only needed when differentiating between cold & warm
	if sp.prewarmQueries {
		statMapping[labelColdQueries] = NewStatGroup(*sp.limit)
		statMapping[labelWarmQueries] = NewStatGroup(*sp.limit)
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
		if _, ok := statMapping[string(stat.Label)]; !ok {
			statMapping[string(stat.Label)] = NewStatGroup(*sp.limit)
		}

		statMapping[string(stat.Label)].Push(stat.Value)

		if !stat.IsPartial {
			statMapping[allQueriesLabel].Push(stat.Value)

			// Only needed when differentiating between cold & warm
			if sp.prewarmQueries {
				if stat.IsWarm {
					statMapping[labelWarmQueries].Push(stat.Value)
				} else {
					statMapping[labelColdQueries].Push(stat.Value)
				}
			}

			// If we're prewarming queries (i.e., running them twice in a row),
			// only increment the counter for the first (cold) query. Otherwise,
			// increment for every query.
			if !sp.prewarmQueries || !stat.IsWarm {
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
			WriteStatGroupMap(os.Stderr, statMapping)
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
	WriteStatGroupMap(os.Stdout, statMapping)
	sp.wg.Done()
}

// CloseAndWait closes the stats channel and blocks until the StatProcessor has finished all the stats on its channel.
func (sp *statProcessor) CloseAndWait() {
	close(sp.c)
	sp.wg.Wait()
}
