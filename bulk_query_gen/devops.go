package main

// Devops describes a devops query generator.
type Devops interface {
	AvgCPUUsageDayByHour(*Query)
	AvgCPUUsageWeekByHour(*Query)
	AvgCPUUsageMonthByDay(*Query)

	AvgMemAvailableDayByHour(*Query)
	AvgMemAvailableWeekByHour(*Query)
	AvgMemAvailableMonthByDay(*Query)

	MaxCPUUsageHourByMinuteOneHost(*Query, int)
	MaxCPUUsageHourByMinuteTwoHosts(*Query, int)
	MaxCPUUsageHourByMinuteFourHosts(*Query, int)

	Dispatch(int, *Query, int)
}

// DevopsDispatch round-robins through the different devops queries.
func DevopsDispatch(d Devops, iteration int, q *Query, scaleVar int) {
	if scaleVar <= 0 {
		panic("logic error: bad scalevar")
	}
	mod := 7
	if scaleVar >= 2 {
		mod++
	}
	if scaleVar >= 4 {
		mod++
	}
	switch iteration % mod {
	case 0:
		d.AvgCPUUsageDayByHour(q)
	case 1:
		d.AvgCPUUsageWeekByHour(q)
	case 2:
		d.AvgCPUUsageMonthByDay(q)
	case 3:
		d.AvgMemAvailableDayByHour(q)
	case 4:
		d.AvgMemAvailableWeekByHour(q)
	case 5:
		d.AvgMemAvailableMonthByDay(q)
	case 6:
		d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
	case 7:
		if scaleVar < 2 {
			panic("logic error: not enough hosts to make query")
		}
		d.MaxCPUUsageHourByMinuteTwoHosts(q, scaleVar)
	case 8:
		if scaleVar < 4 {
			panic("logic error: not enough hosts to make query")
		}
		d.MaxCPUUsageHourByMinuteFourHosts(q, scaleVar)
	}
}
