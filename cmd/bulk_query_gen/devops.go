package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

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
