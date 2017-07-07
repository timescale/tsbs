package main

import "bitbucket.org/440-labs/influxdb-comparisons/query"

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
	MaxCPUUsageHourByMinuteOneHost(query.Query, int)
	MaxCPUUsageHourByMinuteTwoHosts(query.Query, int)
	MaxCPUUsageHourByMinuteFourHosts(query.Query, int)
	MaxCPUUsageHourByMinuteEightHosts(query.Query, int)
	MaxCPUUsageHourByMinuteSixteenHosts(query.Query, int)
	MaxCPUUsageHourByMinuteThirtyTwoHosts(query.Query, int)

	MaxCPUUsage12HoursByMinuteOneHost(query.Query, int)

	MeanCPUUsageDayByHourAllHostsGroupbyHost(query.Query, int)

	//CountCPUUsageDayByHourAllHostsGroupbyHost(query.Query, int)

	Dispatch(int, int) query.Query
}

// devopsDispatchAll round-robins through the different devops queries.
func devopsDispatchAll(d Devops, iteration int, q query.Query, scaleVar int) {
	if scaleVar <= 0 {
		panic("logic error: bad scalevar")
	}
	mod := 1
	if scaleVar >= 2 {
		mod++
	}
	if scaleVar >= 4 {
		mod++
	}
	if scaleVar >= 8 {
		mod++
	}
	if scaleVar >= 16 {
		mod++
	}
	if scaleVar >= 32 {
		mod++
	}

	switch iteration % mod {
	case 0:
		d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
	case 1:
		d.MaxCPUUsageHourByMinuteTwoHosts(q, scaleVar)
	case 2:
		d.MaxCPUUsageHourByMinuteFourHosts(q, scaleVar)
	case 3:
		d.MaxCPUUsageHourByMinuteEightHosts(q, scaleVar)
	case 4:
		d.MaxCPUUsageHourByMinuteSixteenHosts(q, scaleVar)
	case 5:
		d.MaxCPUUsageHourByMinuteThirtyTwoHosts(q, scaleVar)
	default:
		panic("logic error in switch statement")
	}
}
