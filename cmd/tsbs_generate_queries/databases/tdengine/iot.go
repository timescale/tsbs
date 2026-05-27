package tdengine

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/pkg/query"
)

// IoT produces TDengine-specific queries for all the iot query types.
type IoT struct {
	*iot.Core
	*BaseGenerator
}

//last-loc
//single-last-loc
//low-fuel
//avg-vs-projected-fuel-consumption
//avg-daily-driving-duration
//daily-activity

func (i *IoT) getTrucksWhereWithNames(names []string) string {
	var nameClauses []string

	for _, s := range names {
		nameClauses = append(nameClauses, fmt.Sprintf("'%s'", s))
	}
	return fmt.Sprintf("name IN (%s)", strings.Join(nameClauses, ","))
}

// getHostWhereString gets multiple random hostnames and creates a WHERE SQL statement for these hostnames.
func (i *IoT) getTruckWhereString(nTrucks int) string {
	names, err := i.GetRandomTrucks(nTrucks)
	panicIfErr(err)
	return i.getTrucksWhereWithNames(names)
}

// LastLocByTruck finds the truck location for nTrucks.
func (i *IoT) LastLocByTruck(qi query.Query, nTrucks int) {
	sql := fmt.Sprintf(`SELECT last_row(ts),last_row(latitude),last_row(longitude) FROM readings WHERE %s GROUP BY name`,
		i.getTruckWhereString(nTrucks))

	humanLabel := "TDengine last location by specific truck"
	humanDesc := fmt.Sprintf("%s: random %4d trucks", humanLabel, nTrucks)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// LastLocPerTruck finds all the truck locations along with truck and driver names.
func (i *IoT) LastLocPerTruck(qi query.Query) {
	//SELECT last_row(ts),name,driver,latitude,longitude FROM readings WHERE fleet='South' and name IS NOT NULL partition BY name,driver;
	sql := fmt.Sprintf(`SELECT last_row(ts),name,driver,latitude,longitude FROM readings WHERE fleet='%s' and name IS NOT NULL partition BY name,driver`,
		i.GetRandomFleet())

	humanLabel := "TDengine last location per truck"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// TrucksWithLowFuel finds all trucks with low fuel (less than 10%).
func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	//SELECT last_row(ts),name,driver,fuel_state FROM diagnostics WHERE fuel_state <= 0.1 AND fleet = 'South' and name IS NOT NULL GROUP BY name,driver ;
	sql := fmt.Sprintf(`SELECT last_row(ts),name,driver,fuel_state FROM diagnostics WHERE fuel_state <= 0.1 AND fleet = '%s' and name IS NOT NULL GROUP BY name,driver`,
		i.GetRandomFleet())

	humanLabel := "TDengine trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.DiagnosticsTableName, sql)
}

// TrucksWithHighLoad finds all trucks that have load over 90%.
func (i *IoT) TrucksWithHighLoad(qi query.Query) {
	//SELECT ts,name,driver,current_load,load_capacity FROM (SELECT last_row(ts) as ts,name,driver, current_load,load_capacity FROM diagnostics WHERE fleet = 'South' partition by name,driver) WHERE current_load>= (0.9 * load_capacity);
	//pre sql := fmt.Sprintf("SELECT ts,name,driver,current_load,load_capacity FROM (SELECT last_row(ts) as ts,name,driver, current_load,load_capacity FROM diagnostics WHERE fleet = '%s' partition by name,driver) WHERE current_load>= (0.9 * load_capacity)", i.GetRandomFleet())
	sql := fmt.Sprintf("SELECT ts,name,driver,current_load,load_capacity FROM (SELECT last_row(ts) as ts,name,driver, current_load,load_capacity FROM diagnostics WHERE fleet = '%s' partition by name,driver) WHERE current_load>= (0.9 * load_capacity)", i.GetRandomFleet())

	humanLabel := "TDengine trucks with high load"
	humanDesc := fmt.Sprintf("%s: over 90 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.DiagnosticsTableName, sql)
}

// StationaryTrucks finds all trucks that have low average velocity in a time window.
func (i *IoT) StationaryTrucks(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.StationaryDuration)
	//select name,driver from (SELECT name,driver,fleet ,avg(velocity) as mean_velocity FROM readings WHERE ts > '2016-01-01T15:07:21Z' AND ts <= '2016-01-01T16:17:21Z' partition BY name,driver,fleet interval(10m) LIMIT 1) WHERE fleet = 'West' AND mean_velocity < 1 ;
	sql := fmt.Sprintf("select name,driver from (SELECT name,driver,avg(velocity) as mean_velocity FROM readings WHERE ts > %d AND ts <= %d and fleet = '%s' partition BY name,driver interval(10m) LIMIT 1) WHERE mean_velocity < 1;", interval.StartUnixMillis(), interval.EndUnixMillis(), i.GetRandomFleet())
	//sql := fmt.Sprintf("SELECT name,driver FROM readings WHERE ts > %d AND ts <= %d and fleet = '%s' partition BY name,driver,fleet interval(10m) having (avg(velocity) < 1) LIMIT 1;", interval.StartUnixMillis(), interval.EndUnixMillis(), i.GetRandomFleet())
	humanLabel := "TDengine stationary trucks"
	humanDesc := fmt.Sprintf("%s: with low avg velocity in last 10 minutes", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// TrucksWithLongDrivingSessions finds all trucks that have not stopped at least 20 mins in the last 4 hours.
func (i *IoT) TrucksWithLongDrivingSessions(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.LongDrivingSessionDuration)
	//SELECT name,driver FROM(SELECT _wstart as ts,name,driver,avg(velocity) as mean_velocity FROM readings WHERE fleet ="West" AND ts > '2016-01-03T13:46:34Z' AND ts <= '2016-01-03T17:46:34Z' partition BY name,driver interval(10m)) WHERE mean_velocity > 1 GROUP BY name,driver having count(*) > 22
	//pre sql := fmt.Sprintf("SELECT name,driver FROM(SELECT _wstart as ts,name,driver,avg(velocity) as mean_velocity FROM readings WHERE fleet =\"%s\" AND ts > %d AND ts <= %d partition BY name,driver interval(10m)) WHERE mean_velocity > 1 GROUP BY name,driver having count(*) > %d", i.GetRandomFleet(), interval.StartUnixMillis(), interval.EndUnixMillis(), tenMinutePeriods(5, iot.LongDrivingSessionDuration))
	sql := fmt.Sprintf("SELECT name,driver FROM(SELECT name,driver,avg(velocity) as mean_velocity FROM readings WHERE fleet =\"%s\" AND ts > %d AND ts <= %d partition BY name,driver interval(10m)) WHERE mean_velocity > 1 GROUP BY name,driver having count(*) > %d", i.GetRandomFleet(), interval.StartUnixMillis(), interval.EndUnixMillis(), tenMinutePeriods(5, iot.LongDrivingSessionDuration))
	humanLabel := "TDengine trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: stopped less than 20 mins in 4 hour period", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// TrucksWithLongDailySessions finds all trucks that have driven more than 10 hours in the last 24 hours.
func (i *IoT) TrucksWithLongDailySessions(qi query.Query) {
	//SELECT name,driver FROM(SELECT name,driver,avg(velocity) as mean_velocity FROM readings WHERE fleet ='West' AND ts > '2016-01-01T12:31:37Z' AND ts <= '2016-01-05T12:31:37Z' partition BY name,driver interval(10m) ) WHERE mean_velocity > 1 GROUP BY name,driver having count(*) > 60

	interval := i.Interval.MustRandWindow(iot.DailyDrivingDuration)
	sql := fmt.Sprintf("SELECT name,driver FROM(SELECT name,driver,avg(velocity) as mean_velocity FROM readings WHERE fleet ='%s' AND ts > %d AND ts <= %d partition BY name,driver interval(10m) ) WHERE mean_velocity > 1 GROUP BY name,driver having count(*) > %d", i.GetRandomFleet(), interval.StartUnixMillis(), interval.EndUnixMillis(), tenMinutePeriods(35, iot.DailyDrivingDuration))

	humanLabel := "TDengine trucks with longer daily sessions"
	humanDesc := fmt.Sprintf("%s: drove more than 10 hours in the last 24 hours", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// AvgVsProjectedFuelConsumption calculates average and projected fuel consumption per fleet.
func (i *IoT) AvgVsProjectedFuelConsumption(qi query.Query) {
	//select avg(fuel_consumption) as avg_fuel_consumption,avg(nominal_fuel_consumption) as nominal_fuel_consumption from readings where velocity > 1 group by fleet
	sql := fmt.Sprintf("select avg(fuel_consumption) as avg_fuel_consumption,avg(nominal_fuel_consumption) as nominal_fuel_consumption from readings where velocity > 1 group by fleet")
	humanLabel := "TDengine average vs projected fuel consumption per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// AvgDailyDrivingDuration finds the average driving duration per driver.
func (i *IoT) AvgDailyDrivingDuration(qi query.Query) {
	//select fleet,name,driver,avg(hours_driven) as avg_daily_hours  from( select _wstart as ts,fleet,name,driver,count(mv)/6 as hours_driven from ( select _wstart as ts,fleet,tbname,name,driver,avg(velocity) as mv from readings where ts > '2016-01-01T00:00:00Z' and ts < '2016-01-05T00:00:01Z'  partition by fleet,tbname,name,driver interval(10m)  ) where  mv >1  partition by fleet,name,driver interval(1d) )partition by fleet,name,driver ;
	sql := fmt.Sprintf("select fleet,name,driver,avg(hours_driven) as avg_daily_hours  from( select _wstart as ts,fleet,name,driver,count(mv)/6 as hours_driven from ( select _wstart as ts,fleet,tbname,name,driver,avg(velocity) as mv from readings where ts > %d and ts < %d  partition by fleet,tbname,name,driver interval(10m)  ) where  mv >1  partition by fleet,name,driver interval(1d) )partition by fleet,name,driver ;", i.Interval.StartUnixMillis(), i.Interval.EndUnixMillis())

	humanLabel := "TDengine average driver driving duration per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// AvgDailyDrivingSession finds the average driving session without stopping per driver per day.
func (i *IoT) AvgDailyDrivingSession(qi query.Query) {
	//    select _wstart as ts,name,avg(ela) from (select ts,name,ela from (SELECT ts,name, diff(difka) as dif, diff(cast(ts as bigint)) as ela FROM (SELECT ts,name,difka FROM (SELECT ts,name,diff(mv) AS difka FROM (SELECT _wstart as ts,name,cast(cast(floor(avg(velocity)/5) as bool) as int) AS mv FROM readings WHERE name is not null AND ts > 1451637149138 AND ts < 1451637749138 partition by name interval(10m))partition BY name ) WHERE difka!=0 order by ts) partition BY name order by ts ) WHERE dif = -2   partition BY name order by ts )  partition BY name  interval(1d);
	interval := i.Interval
	sql := fmt.Sprintf(" select _wstart as ts,name,avg(ela) from (select ts,name,ela from (SELECT ts,name, diff(difka) as dif, diff(cast(ts as bigint)) as ela FROM (SELECT ts,name,difka FROM (SELECT ts,name,diff(mv) AS difka FROM (SELECT _wstart as ts,name,cast(cast(floor(avg(velocity)/5) as bool) as int) AS mv FROM readings   WHERE name is not null  AND ts > %d AND ts < %d   partition by name interval(10m))partition BY name ) WHERE difka!=0   order by ts) partition BY name order by ts ) WHERE dif = -2   partition BY name order by ts )  partition BY name  interval(1d);", interval.StartUnixMillis(), interval.EndUnixMillis())
	humanLabel := "TDengine average driver driving session without stopping per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// AvgLoad finds the average load per truck model per fleet.
func (i *IoT) AvgLoad(qi query.Query) {
	//select fleet,model,load_capacity,avg(ml/load_capacity)  from(SELECT fleet, model,tbname,load_capacity ,avg(current_load) AS ml FROM diagnostics where name is not null   partition BY  fleet, model,tbname,load_capacity) partition BY fleet, model,load_capacity;
	sql := fmt.Sprintf("select fleet,model,load_capacity,avg(ml/load_capacity)  from(SELECT fleet, model,tbname,load_capacity ,avg(current_load) AS ml FROM diagnostics where name is not null   partition BY  fleet, model,tbname,load_capacity) partition BY fleet, model,load_capacity")

	humanLabel := "TDengine average load per truck model per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// DailyTruckActivity returns the number of hours trucks has been active (not out-of-commission) per day per fleet per model.
func (i *IoT) DailyTruckActivity(qi query.Query) {
	//SELECT _wstart as ts,model,fleet,count(ms1)/144 FROM (SELECT _wstart as ts1,model, fleet,avg(status) AS ms1 FROM diagnostics WHERE ts >= 1451606400000 AND ts < 1451952001000 partition by model, fleet, tbname interval(10m)) WHERE ms1<1 partition by model, fleet interval(1d)
	sql := fmt.Sprintf("SELECT _wstart as ts,model,fleet,count(ms1)/144 FROM (SELECT _wstart as ts1,model, fleet,avg(status) AS ms1 FROM diagnostics WHERE ts >= %d AND ts < %d partition by model, fleet, tbname interval(10m)) WHERE ms1<1 partition by model, fleet interval(1d)", i.Interval.StartUnixMillis(), i.Interval.EndUnixMillis())
	humanLabel := "TDengine daily truck activity per fleet per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// TruckBreakdownFrequency calculates the amount of times a truck model broke down in the last period.
func (i *IoT) TruckBreakdownFrequency(qi query.Query) {
	// SELECT model,count(state_changed) FROM (SELECT _rowts,model,diff(broken_down) AS state_changed FROM (SELECT model,tb,cast(cast(floor(2*(nzs)) as bool) as int) AS broken_down FROM (SELECT _wstart as ts,model,tbname as tb, sum(cast(cast(status as bool) as int))/count(cast(cast(status as bool) as int)) AS nzs FROM diagnostics WHERE ts >= '2016-01-01T00:00:00Z' AND ts < '2023-01-05T00:00:01Z'  partition BY tbname,model interval(10m)) order by ts )  partition BY tb,model )  WHERE state_changed = 1  partition BY model ;
	sql := fmt.Sprintf("SELECT model,count(state_changed) FROM (SELECT _rowts,model,diff(broken_down) AS state_changed FROM (SELECT ts,model,tb,cast(cast(floor(2*(nzs)) as bool) as int) AS broken_down FROM (SELECT _wstart as ts,model,tbname as tb, sum(cast(cast(status as bool) as int))/count(cast(cast(status as bool) as int)) AS nzs FROM diagnostics WHERE ts >= %d AND ts < %d partition BY tbname,model interval(10m)) order by ts) partition BY tb,model) WHERE state_changed = 1 partition BY model", i.Interval.StartUnixMillis(), i.Interval.EndUnixMillis())

	humanLabel := "TDengine truck breakdown frequency per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

func tenMinutePeriods(minutesPerHour float64, duration time.Duration) int {
	durationMinutes := duration.Minutes()
	leftover := minutesPerHour * duration.Hours()
	return int((durationMinutes - leftover) / 10)
}
