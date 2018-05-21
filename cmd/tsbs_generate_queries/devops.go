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

// Devops describes a devops query generator.
type Devops interface {
	CPU5Metrics(query.Query, int, int, time.Duration)
	GroupByOrderByLimit(query.Query)
	HighCPUForHosts(query.Query, int, int)
	LastPointPerHost(query.Query)
	MaxAllCPU(query.Query, int, int)
	MaxCPUUsageHourByMinute(query.Query, int, int, time.Duration)
	MeanCPUMetricsDayByHourAllHostsGroupbyHost(query.Query, int)
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

func getHighCPULabel(dbName string, nHosts int) string {
	label := dbName + " CPU over threshold, "
	if nHosts > 0 {
		label += fmt.Sprintf("%d host(s)", nHosts)
	} else {
		label += allHosts
	}
	return label
}

func getRandomHosts(scale, nhosts int) []string {
	if nhosts > scale {
		log.Fatal("nhosts > scaleVar")
	}

	nn := rand.Perm(scale)[:nhosts]

	hostnames := []string{}
	for _, n := range nn {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}

	return hostnames
}

func panicUnimplementedQuery(dg DevopsGenerator) {
	panic(fmt.Sprintf("database (%v) does not implement query", reflect.TypeOf(dg)))
}
