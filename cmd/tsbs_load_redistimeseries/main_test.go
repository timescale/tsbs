package main

import (
	"testing"

	"github.com/timescale/tsbs/load"
)

func TestRedisTimeSeriesLoader(t *testing.T) {
	loader.RunBenchmark(&benchmark{dbc: &dbCreator{}}, load.WorkerPerQueue)
}
