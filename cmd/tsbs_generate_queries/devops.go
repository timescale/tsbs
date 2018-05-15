package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

type devopsCore struct {
	interval TimeInterval
	scale    int
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
