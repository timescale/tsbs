package main

// Devops describes a devops query generator.
type Devops interface {
	AvgCPUUsageDayByHour(*Query)
	AvgCPUUsageWeekByHour(*Query)
	AvgCPUUsageMonthByDay(*Query)

	AvgMemAvailableDayByHour(*Query)
	AvgMemAvailableWeekByHour(*Query)
	AvgMemAvailableMonthByDay(*Query)
}

// DevopsDispatch round-robins through the different devops queries.
func DevopsDispatch(d Devops, iteration int, q *Query) {
	switch iteration %6 {
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
	}
}
