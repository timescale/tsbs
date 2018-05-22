package main

import (
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

const (
	allHosts              = "all hosts"
	doubleGroupByDuration = 24 * time.Hour
	highCPUDuration       = 24 * time.Hour
	maxAllDuration        = 8 * time.Hour

	labelSingleGroupby       = "single-groupby"
	labelDoubleGroupby       = "double-groupby"
	labelLastpoint           = "lastpoint"
	labelMaxAll              = "cpu-max-all"
	labelGroupbyOrderbyLimit = "groupby-orderby-limit"
	labelHighCPU             = "high-cpu"
)

type devopsCore struct {
	interval TimeInterval
	scale    int
}

// DevopsGenerator is query generator for a database type that handles the Devops use case
type DevopsGenerator interface {
	GenerateEmptyQuery() query.Query
}

func newDevopsCore(start, end time.Time, scale int) *devopsCore {
	if !start.Before(end) {
		panic("bad time order")
	}

	return &devopsCore{NewTimeInterval(start, end), scale}
}

func (d *devopsCore) getRandomHosts(nHosts int) []string {
	return getRandomHosts(d.scale, nHosts)
}

var cpuMetrics = []string{
	"usage_user",
	"usage_system",
	"usage_idle",
	"usage_nice",
	"usage_iowait",
	"usage_irq",
	"usage_softirq",
	"usage_steal",
	"usage_guest",
	"usage_guest_nice",
}

func getCPUMetricsSlice(numMetrics int) []string {
	if numMetrics <= 0 {
		panic("no metrics given")
	}
	if numMetrics > len(cpuMetrics) {
		panic("too many metrics asked for")
	}
	return cpuMetrics[:numMetrics]
}

// SingleGroupbyFiller is a type that can fill in a single groupby query
type SingleGroupbyFiller interface {
	GroupByTime(query.Query, int, int, time.Duration)
}

// DoubleGroupbyFiller is a type that can fill in a double groupby query
type DoubleGroupbyFiller interface {
	GroupByTimeAndPrimaryTag(query.Query, int)
}

// LastPointFiller is a type that can fill in a last point query
type LastPointFiller interface {
	LastPointPerHost(query.Query)
}

// MaxAllFiller is a type that can fill in a max all CPU metrics query
type MaxAllFiller interface {
	MaxAllCPU(query.Query, int)
}

// GroupbyOrderbyLimitFiller is a type that can fill in a groupby-orderby-limit query
type GroupbyOrderbyLimitFiller interface {
	GroupByOrderByLimit(query.Query)
}

// HighCPUFiller is a type that can fill in a high-cpu query
type HighCPUFiller interface {
	HighCPUForHosts(query.Query, int)
}

func getDoubleGroupByLabel(dbName string, numMetrics int) string {
	return fmt.Sprintf("%s mean of %d metrics, all hosts, random %s by 1hr", dbName, numMetrics, doubleGroupByDuration)
}

func getHighCPULabel(dbName string, nHosts int) string {
	label := dbName + " CPU over threshold, "
	if nHosts > 0 {
		label += fmt.Sprintf("%d host(s)", nHosts)
	} else {
		label += allHosts
	}
	return label
}

func getMaxAllLabel(dbName string, nHosts int) string {
	return fmt.Sprintf("%s max of all CPU fields, random %4d hosts, random %s by 1h", dbName, nHosts, maxAllDuration)
}

func getRandomHosts(scale, nHosts int) []string {
	if nHosts > scale {
		log.Fatalf("number of hosts (%d) larger than --scale-var (%d)", nHosts, scale)
	}

	nn := rand.Perm(scale)[:nHosts]

	hostnames := []string{}
	for _, n := range nn {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}

	return hostnames
}

func panicUnimplementedQuery(dg DevopsGenerator) {
	panic(fmt.Sprintf("database (%v) does not implement query", reflect.TypeOf(dg)))
}
