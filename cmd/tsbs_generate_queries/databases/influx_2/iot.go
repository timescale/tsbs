package influx_2

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/pkg/query"
)

// IoT produces Influx-specific queries for all the iot query types.
type IoT struct {
	*iot.Core
	*BaseGenerator
}

// NewIoT makes an IoT object ready to generate Queries.
func NewIoT(start, end time.Time, scale int, g *BaseGenerator) *IoT {
	c, err := iot.NewCore(start, end, scale)
	databases.PanicIfErr(err)
	return &IoT{
		Core:          c,
		BaseGenerator: g,
	}
}

func (i *IoT) getTruckFilterClauseWithNames(names []string) string {
	nameClauses := []string{}
	for _, s := range names {
		nameClauses = append(nameClauses, fmt.Sprintf("r.name == '%s'", s))
	}

	combinedHostnameClause := strings.Join(nameClauses, " or ")
	return "(" + combinedHostnameClause + ")"
}

func (i *IoT) getTrucksFilterClause(nTrucks int) string {
	names, err := i.GetRandomTrucks(nTrucks)
	if err != nil {
		panic(err.Error())
	}
	return i.getTruckFilterClauseWithNames(names)
}

// LastLocByTruck finds the truck location for nTrucks.
func (i *IoT) LastLocByTruck(qi query.Query, nTrucks int) {
	fluxString := fmt.Sprintf(`
	from(bucket: "benchmark")
	|> range(start: 0)
	|> filter(fn: (r) => r._measurement == "readings" and %s)
	|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	|> group(columns: ["name", "driver"])
	|> sort(columns: ["_time"], desc: true)
	|> first(column: "latitude")
	|> keep(columns: ["name", "driver", "latitude", "longitude", "_time"])`,
		i.getTrucksFilterClause(nTrucks))

	humanLabel := "Influx 2.x last location by specific truck"
	humanDesc := fmt.Sprintf("%s: random %4d trucks", humanLabel, nTrucks)

	i.fillInQuery(qi, humanLabel, humanDesc, fluxString)
}

// LastLocPerTruck finds all the truck locations along with truck and driver names.
func (i *IoT) LastLocPerTruck(qi query.Query) {
	fluxString := fmt.Sprintf(`
	from(bucket: "benchmark")
	|> range(start: 0)
	|> filter(fn: (r) => r._measurement == "readings" and r.fleet == "%s")
	|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	|> group(columns: ["name", "driver"])
	|> sort(columns: ["_time"], desc: true)
	|> first(column: "latitude")
	|> keep(columns: ["name", "driver", "latitude", "longitude"])`,
		i.GetRandomFleet())

	humanLabel := "Influx 2.x last location per truck"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, fluxString)
}

// TrucksWithLowFuel finds all trucks with low fuel (less than 10%).
func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	fluxString := fmt.Sprintf(`
	from(bucket: "benchmark")
	|> range(start: 0)
	|> filter(fn: (r) => r._measurement == "diagnostics" and r.fleet == "%s" and r._field == "fuel_state" and r._value <= 0.1)
	|> group(columns: ["name", "driver"])
	|> last()
	|> keep(columns: ["name", "driver", "_field", "_value"])`,
		i.GetRandomFleet())

	humanLabel := "Influx 2.x trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, fluxString)
}

// TrucksWithHighLoad finds all trucks that have load over 90%.
func (i *IoT) TrucksWithHighLoad(qi query.Query) {
	fluxString := fmt.Sprintf(`
	from(bucket: "benchmark")
	|> range(start: 0)
	|> filter(fn: (r) => r._measurement == "diagnostics" and r.fleet == "%s")
	|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	|> group(columns: ["name", "driver"])
	|> sort(columns: ["_time"], desc: true)
	|> first(column: "current_load")
	|> filter(fn: (r) => r.current_load >= 0.9 * r.load_capacity)
	|> keep(columns: ["name", "driver", "current_load", "load_capacity"])`,
		i.GetRandomFleet())

	humanLabel := "Influx 2.x trucks with high load"
	humanDesc := fmt.Sprintf("%s: over 90 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, fluxString)
}

// StationaryTrucks finds all trucks that have low average velocity in a time window.
func (i *IoT) StationaryTrucks(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.StationaryDuration)

	fluxString := fmt.Sprintf(`
	from(bucket: "benchmark")
	|> range(start: %s, stop: %s)
	|> filter(fn: (r) => r._measurement == "readings" and r._field == "velocity" and r.fleet == "%s")
	|> group(columns: ["name", "driver", "fleet"])
	|> aggregateWindow(every: 10m, fn: mean, createEmpty: false)
	|> sort(columns: ["_time"], desc: false)
	|> filter(fn: (r) => r._value < 1.0)`,
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
		i.GetRandomFleet())

	humanLabel := "Influx 2.x stationary trucks"
	humanDesc := fmt.Sprintf("%s: with low avg velocity in last 10 minutes", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, fluxString)
}

// TrucksWithLongDrivingSessions finds all trucks that have not stopped at least 20 mins in the last 4 hours.
func (i *IoT) TrucksWithLongDrivingSessions(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.LongDrivingSessionDuration)
	offset := i.Interval.Start().Hour()*3600 + i.Interval.Start().Minute()*60 + i.Interval.Start().Second() // offset for aggregateWindow function

	fluxString := fmt.Sprintf(`
	from(bucket: "benchmark")
	|> range(start: %s, stop: %s)
	|> filter(fn: (r) => r._measurement == "readings" and r._field == "velocity" and r.fleet == "%s")
	|> aggregateWindow(every: 10m, fn: mean, createEmpty: false)
	|> group(columns: ["name", "driver"])
	|> filter(fn: (r) => r._value > 1.0)
	|> aggregateWindow(every: %ds, fn:count, offset: %ds, createEmpty: false)
	|> filter(fn: (r) => r._value > %d)
	`,
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
		i.GetRandomFleet(),
		iot.LongDrivingSessionDuration,
		offset,
		// Calculate number of 10 min intervals that is the max driving duration for the session if we rest 5 mins per hour.
		tenMinutePeriods(5, iot.LongDrivingSessionDuration))

	humanLabel := "Influx 2.x trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: stopped less than 20 mins in 4 hour period", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, fluxString)
}

// TrucksWithLongDailySessions finds all trucks that have driven more than 10 hours in the last 24 hours.
func (i *IoT) TrucksWithLongDailySessions(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.DailyDrivingDuration)
	offset := i.Interval.Start().Hour()*3600 + i.Interval.Start().Minute()*60 + i.Interval.Start().Second() // offset for aggregateWindow function

	fluxString := fmt.Sprintf(`
	from(bucket: "benchmark")
	|> range(start: %s, stop: %s)
	|> filter(fn: (r) => r._measurement == "readings" and r._field == "velocity" and r.fleet == "%s")
	|> aggregateWindow(every: 10m, fn: mean, createEmpty: false)
	|> group(columns: ["name", "driver"])
	|> filter(fn: (r) => r._value > 1.0)
	|> aggregateWindow(every: %ds, fn:count, offset: %ds, createEmpty: false)
	|> filter(fn: (r) => r._value > %d)
	`,
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
		i.GetRandomFleet(),
		iot.LongDrivingSessionDuration,
		offset,
		// Calculate number of 10 min intervals that is the max driving duration for the session if we rest 5 mins per hour.
		tenMinutePeriods(35, iot.DailyDrivingDuration))

	humanLabel := "Influx 2.x trucks with longer daily sessions"
	humanDesc := fmt.Sprintf("%s: drove more than 10 hours in the last 24 hours", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, fluxString)
}

// AvgVsProjectedFuelConsumption calculates average and projected fuel consumption per fleet.
func (i *IoT) AvgVsProjectedFuelConsumption(qi query.Query) {
	fluxString := `
	data = from(bucket: "benchmark")
	|> range(start: 0)
	|> filter(fn: (r) => r._measurement == "readings" and exists r.fleet)
	|> filter(fn: (r) => r._field == "velocity" or r._field == "fuel_consumption" or r._field == "nominal_fuel_consumption")
	|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	|> filter(fn: (r) => r.velocity > 1.0)
	|> group(columns: ["fleet"])

	data
	|> mean(column: "nominal_fuel_consumption")
	|> yield(name: "mean_nominal_fuel_consumption")

	data
	|> mean(column: "fuel_consumption")
	|> yield(name: "mean_fuel_consumption")`

	humanLabel := "Influx 2.x average vs projected fuel consumption per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, fluxString)
}

// AvgDailyDrivingDuration finds the average driving duration per driver.
func (i *IoT) AvgDailyDrivingDuration(qi query.Query) {
	start := i.Interval.Start().Format(time.RFC3339)
	end := i.Interval.End().Format(time.RFC3339)
	fluxString := fmt.Sprintf(`
	from(bucket: "benchmark")
	|> range(start: %s, stop: %s)
	|> filter(fn: (r) => r._measurement == "readings" and r._field == "velocity")
	|> group(columns: ["fleet", "name", "driver"])
	|> aggregateWindow(every: 10m, fn: mean, createEmpty: false)
	|> filter(fn: (r) => r._value > 1.0)
	|> sort(columns: ["_time"], desc: false)
	|> aggregateWindow(every: 1d, fn: count, createEmpty: false)
	|> map(fn: (r) => ({r with _value: r._value / 6.0}))
	|> group(columns: ["fleet", "name", "driver"])
	|> yield(name: "hours_driven")
	`,
		start,
		end,
	)

	humanLabel := "Influx 2.x average driver driving duration per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, fluxString)
}

// AvgDailyDrivingSession finds the average driving session without stopping per driver per day.
func (i *IoT) AvgDailyDrivingSession(qi query.Query) {
	start := i.Interval.Start().Format(time.RFC3339)
	end := i.Interval.End().Format(time.RFC3339)
	fluxString := fmt.Sprintf(`
	import "date"
	import "math"

	from(bucket: "benchmark")
	|> range(start: %s, stop: %s)
	|> filter(fn: (r) => r._measurement == "readings")
	|> filter(fn: (r) => r._field == "velocity")
	|> group(columns: ["fleet", "name", "driver"]) 
	|> aggregateWindow(every: 10m, fn: mean, createEmpty: false)
	|> map(fn: (r) => ({r with driving: if r._value > 5.0 then 1 else 0}))
	|> map(fn: (r) => ({r with day: date.yearDay(t: r._time)}))
	|> map(fn: (r) => ({r with year: date.year(t: r._time)}))
	|> map(fn: (r) => ({r with month: date.month(t: r._time)}))
	|> group(columns: ["fleet", "name", "driver", "year", "day"]) 
	|> reduce(
		identity: {
			currSession: 0,
			sessionSum: 0,
			sessionCount: 0,
			lastState: -1,
			fstDate: 0
		},
		fn: (r, accumulator) => ({
			lastState: r.driving,
			currSession: if (r.driving == 1 and r.driving == accumulator.lastState) then accumulator.currSession + 1 else 0,
			sessionSum: if (r.driving == 1 and r.driving == accumulator.lastState) then accumulator.sessionSum else accumulator.sessionSum + accumulator.currSession,
			sessionCount: if (r.driving == 1 and r.driving == accumulator.lastState) then accumulator.sessionCount else accumulator.sessionCount + 1,
			fstDate: if accumulator.fstDate == 0 then int(v: r._time) else accumulator.fstDate 
		})
	)
	|> map(fn: (r) => ({
		name: r.name,
		driver: r.driver,
		fleet: r.fleet,
		avg_session: (float(v:(r.sessionSum + r.currSession)) * 10.0 / float(v: r.sessionCount)) / 60.0,
		_time: time(v: r.fstDate)
	}))
	|> yield()`,
		start,
		end,
	)

	humanLabel := "Influx 2.x average driver driving session without stopping per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, fluxString)
}

// AvgLoad finds the average load per truck model per fleet.
func (i *IoT) AvgLoad(qi query.Query) {
	fluxString := `
	from(bucket: "benchmark")
	|> range(start: 0)
	|> filter(fn: (r) => r._measurement == "diagnostics")
	|> filter(fn: (r) => r._field == "current_load" or r._field == "load_capacity")
	|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	|> filter(fn: (r) => r.load_capacity > 0.0)
	|> map(fn: (r) => ({r with load_percentage: r.current_load / r.load_capacity}))
	|> group(columns: ["name", "fleet", "model"])
	|> mean(column: "load_percentage")`

	humanLabel := "Influx 2.x average load per truck model per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, fluxString)
}

// DailyTruckActivity returns the number of hours trucks has been active (not out-of-commission) per day per fleet per model.
func (i *IoT) DailyTruckActivity(qi query.Query) {
	start := i.Interval.Start().Format(time.RFC3339)
	end := i.Interval.End().Format(time.RFC3339)
	fluxString := fmt.Sprintf(`
	from(bucket: "benchmark")
	|> range(start: %s, stop: %s)
	|> filter(fn: (r) => r._measurement == "diagnostics" and r._field == "status")
	|> filter(fn: (r) => exists r.model and exists r.fleet)
	|> aggregateWindow(every: 10m, fn: mean, createEmpty: false)
	|> group(columns: ["model", "fleet"])
	|> filter(fn: (r) => r._value < 1)
	|> aggregateWindow(every: 1d, fn: count, createEmpty: false)
	|> map(fn: (r) => ({r with _value: float(v:r._value) / 144.0}))`,
		start,
		end,
	)

	humanLabel := "Influx 2.x daily truck activity per fleet per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, fluxString)
}

// TruckBreakdownFrequency calculates the amount of times a truck model broke down in the last period.
func (i *IoT) TruckBreakdownFrequency(qi query.Query) {
	start := i.Interval.Start().Format(time.RFC3339)
	end := i.Interval.End().Format(time.RFC3339)
	fluxString := fmt.Sprintf(`
	from(bucket: "benchmark")
	|> range(start: %s, stop: %s)
	|> filter(fn: (r) => r._measurement == "diagnostics" and r._field == "status")
	|> filter(fn: (r) => exists r.model and exists r.fleet)
	// First mark each reading as broken (1) or not (0)
	|> map(fn: (r) => ({r with is_broken: if r._value == 0 then 1 else 0})) 
	|> group(columns: ["model"])
	|> aggregateWindow(
		every: 10m,
		fn: (column, tables=<-) => tables
		|> mean(column: "is_broken")
		|> map(fn: (r) => ({r with broken_down: if r.is_broken >= 0.5 then true else false})),
	)
	|> sort(columns: ["_time"])
	|> reduce(
		identity: {
			lastState: -1,
			cnt: 0
		}, 
		fn: (r, accumulator) => ({
			lastState: if r.broken_down == true then 1 else 0,
			cnt: if accumulator.lastState == 0 and r.broken_down == true then accumulator.cnt + 1 else accumulator.cnt
		}) 
	)`,
		start,
		end,
	)

	humanLabel := "Influx 2.x truck breakdown frequency per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, fluxString)
}

// tenMinutePeriods calculates the number of 10 minute periods that can fit in
// the time duration if we subtract the minutes specified by minutesPerHour value.
// E.g.: 4 hours - 5 minutes per hour = 3 hours and 40 minutes = 22 ten minute periods
func tenMinutePeriods(minutesPerHour float64, duration time.Duration) int {
	durationMinutes := duration.Minutes()
	leftover := minutesPerHour * duration.Hours()
	return int((durationMinutes - leftover) / 10)
}
