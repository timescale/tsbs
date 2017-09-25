package benchmarker

import (
	"flag"
)

const (
	LabelAllQueries  = "all queries"
	LabelColdQueries = "cold queries"
	LabelWarmQueries = "warm queries"
)

type BenchmarkComponents struct {
	Scanner       *QueryScanner
	StatProcessor *StatProcessor
	limit         uint64
}

func NewBenchmarkComponents() *BenchmarkComponents {
	ret := &BenchmarkComponents{}
	sp := &StatProcessor{
		statPool: GetStatPool(),
		Limit:    &ret.limit,
	}
	ret.Scanner = newQueryScanner(&ret.limit)
	ret.StatProcessor = sp
	flag.Uint64Var(&sp.BurnIn, "burn-in", 0, "Number of queries to ignore before collecting statistics.")
	flag.Uint64Var(&ret.limit, "limit", 0, "Limit the number of queries to send, 0 = no limit")
	flag.Uint64Var(&sp.printInterval, "print-interval", 100, "Print timing stats to stderr after this many queries (0 to disable)")

	return ret
}

func (bc *BenchmarkComponents) ResetLimit(limit uint64) {
	bc.limit = limit
}
