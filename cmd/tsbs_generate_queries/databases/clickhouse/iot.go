package clickhouse

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/pkg/query"
)

const (
	iotReadingsTable = "readings"
)

// IoT produces ClickHouse-specific queries for all the iot query types.
type IoT struct {
	*iot.Core
	*BaseGenerator
}

// NewIoT makes an IoT object ready to generate Queries.
func NewIoT(start, end time.Time, scale int, g *BaseGenerator) *IoT {
	c, err := iot.NewCore(start, end, scale)
	panicIfErr(err)
	return &IoT{
		Core:          c,
		BaseGenerator: g,
	}
}

func (i *IoT) columnSelect(column string) string {
	return column
}

func (i *IoT) withAlias(column string) string {
	return fmt.Sprintf("%s AS %s", i.columnSelect(column), column)
}

func (i *IoT) getTrucksWhereWithNames(names []string) string {
	nameClauses := []string{}

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
	name, driver, longitude, latitude := "name", "driver", "longitude", "latitude"

	sql := fmt.Sprintf(`SELECT 
    t.%s, 
    t.%s, 
    r.%s, 
    r.%s
FROM 
(
    SELECT *
    FROM readings
    WHERE (tags_id, time) IN 
    (
        SELECT 
            tags_id, 
            max(time)
        FROM readings
        GROUP BY tags_id
    )
) AS r
INNER JOIN tags AS t ON r.tags_id = t.id
WHERE t.%s`,
		i.withAlias(name),
		i.withAlias(driver),
		i.withAlias(longitude),
		i.withAlias(latitude),
		i.getTruckWhereString(nTrucks))

	humanLabel := "ClickHouse last location by specific truck"
	humanDesc := fmt.Sprintf("%s: random %4d trucks", humanLabel, nTrucks)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// LastLocPerTruck finds all the truck locations along with truck and driver names.
func (i *IoT) LastLocPerTruck(qi query.Query) {
	name, driver, fleet := "name", "driver", "fleet"

	sql := fmt.Sprintf(`SELECT t.%s, t.%s, r.*
FROM 
(
    SELECT *
    FROM readings
    WHERE (tags_id, time) IN 
    (
        SELECT 
            tags_id, 
            max(time)
        FROM readings
        GROUP BY tags_id
    )
) AS r
INNER JOIN tags AS t ON r.tags_id = t.id
WHERE (%s = '%s') AND isNotNull(%s)`,
		i.withAlias(name),
		i.withAlias(driver),
		i.columnSelect(fleet),
		i.GetRandomFleet(),
		name)

	humanLabel := "ClickHouse last location per truck"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// TrucksWithLowFuel finds all trucks with low fuel (less than 10%).
func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	name, driver, fleet := "name", "driver", "fleet"

	sql := fmt.Sprintf(`SELECT t.%s, t.%s, d.*
FROM 
(
    SELECT *
    FROM diagnostics
    WHERE (tags_id, time) IN 
    (
        SELECT 
            tags_id, 
            max(time)
        FROM diagnostics
        GROUP BY tags_id
    )
) AS d
INNER JOIN tags AS t ON d.tags_id = t.id
WHERE isNotNull(%s) and d.fuel_state < 0.1 and t.%s = '%s'`,
		i.withAlias(name),
		i.withAlias(driver),
		i.columnSelect(name),
		i.columnSelect(fleet),
		i.GetRandomFleet())

	humanLabel := "ClickHouse trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.DiagnosticsTableName, sql)
}

// TrucksWithHighLoad finds all trucks that have load over 90%.
func (i *IoT) TrucksWithHighLoad(qi query.Query) {
	name, driver, fleet, load := "name", "driver", "fleet", "load_capacity"

	sql := fmt.Sprintf(`SELECT t.%s, t.%s, d.*
FROM 
(
    SELECT *
    FROM diagnostics
    WHERE (tags_id, time) IN 
    (
        SELECT 
            tags_id, 
            max(time)
        FROM diagnostics
        GROUP BY tags_id
    )
) AS d
INNER JOIN tags AS t ON d.tags_id = t.id
WHERE isNotNull(%s) AND ((d.current_load / t.%s) > 0.9) AND (t.%s = '%s')`,
		i.withAlias(name),
		i.withAlias(driver),
		name,
		load,
		fleet,
		i.GetRandomFleet())

	humanLabel := "ClickHouse trucks with high load"
	humanDesc := fmt.Sprintf("%s: over 90 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.DiagnosticsTableName, sql)
}

// StationaryTrucks finds all trucks that have low average velocity in a time window.
func (i *IoT) StationaryTrucks(qi query.Query) {
	name, driver, fleet := "name", "driver", "fleet"

	interval := i.Interval.MustRandWindow(iot.StationaryDuration)
	sql := fmt.Sprintf(`SELECT t.%s, t.%s
FROM tags AS t
INNER JOIN readings AS r ON r.tags_id = t.id
WHERE (time >= '%s') AND (time < '%s') 
AND isNotNull(t.%s) 
AND (t.%s = '%s')
GROUP BY %s, %s
HAVING avg(r.velocity) > 1`,
		i.withAlias(name),
		i.withAlias(driver),
		interval.Start().Format(clickhouseTimeStringFormat),
		interval.End().Format(clickhouseTimeStringFormat),
		name,
		fleet,
		i.GetRandomFleet(),
		name,
		driver)

	humanLabel := "ClickHouse stationary trucks"
	humanDesc := fmt.Sprintf("%s: with low avg velocity in last 10 minutes", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// TrucksWithLongDrivingSessions finds all trucks that have not stopped at least 20 mins in the last 4 hours.
func (i *IoT) TrucksWithLongDrivingSessions(qi query.Query) {
	name, driver, fleet := "name", "driver", "fleet"

	interval := i.Interval.MustRandWindow(iot.LongDrivingSessionDuration)
	sql := fmt.Sprintf(`SELECT t.%s, t.%s
FROM tags AS t INNER JOIN 
(
    SELECT 
        toStartOfInterval(created_at, toIntervalMinute(10)) AS ten_minutes, 
        tags_id
    FROM readings
    WHERE (time >= '%s') AND (time < '%s')
    GROUP BY 
        ten_minutes,
        tags_id
    HAVING avg(velocity) > 1
    ORDER BY 
        ten_minutes ASC, 
        tags_id ASC
) AS r ON t.id = r.tags_id
WHERE isNotNull(t.%s) AND (t.%s = '%s')
GROUP BY 
    %s, 
    %s
HAVING count(r.ten_minutes) > %d`,
		i.withAlias(name),
		i.withAlias(driver),
		interval.Start().Format(clickhouseTimeStringFormat),
		interval.End().Format(clickhouseTimeStringFormat),
		name,
		fleet,
		i.GetRandomFleet(),
		name,
		driver,
		// Calculate number of 10 min intervals that is the max driving duration for the session if we rest 5 mins per hour.
		tenMinutePeriods(5, iot.LongDrivingSessionDuration))

	humanLabel := "ClickHouse trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: stopped less than 20 mins in 4 hour period", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// TrucksWithLongDailySessions finds all trucks that have driven more than 10 hours in the last 24 hours.
func (i *IoT) TrucksWithLongDailySessions(qi query.Query) {
	name, driver, fleet := "name", "driver", "fleet"

	interval := i.Interval.MustRandWindow(iot.DailyDrivingDuration)
	sql := fmt.Sprintf(`SELECT t.%s, t.%s
FROM tags AS t
INNER JOIN 
(
    SELECT 
        toStartOfInterval(created_at, toIntervalMinute(10)) AS ten_minutes, 
        tags_id
    FROM readings
    WHERE (time >= '%s') AND (time < '%s')
    GROUP BY 
        ten_minutes, 
        tags_id
    HAVING avg(velocity) > 1
    ORDER BY 
        ten_minutes ASC, 
        tags_id ASC
) AS r ON t.id = r.tags_id
WHERE isNotNull(t.%s) AND (t.%s = '%s')
GROUP BY 
    %s, 
    %s
HAVING count(r.ten_minutes) > %d`,
		i.withAlias(name),
		i.withAlias(driver),
		interval.Start().Format(clickhouseTimeStringFormat),
		interval.End().Format(clickhouseTimeStringFormat),
		name,
		fleet,
		i.GetRandomFleet(),
		name,
		driver,
		// Calculate number of 10 min intervals that is the max driving duration for the session if we rest 35 mins per hour.
		tenMinutePeriods(35, iot.DailyDrivingDuration))

	humanLabel := "ClickHouse trucks with longer daily sessions"
	humanDesc := fmt.Sprintf("%s: drove more than 10 hours in the last 24 hours", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// AvgVsProjectedFuelConsumption calculates average and projected fuel consumption per fleet.
func (i *IoT) AvgVsProjectedFuelConsumption(qi query.Query) {
	fleet, consumption, name := "fleet", "nominal_fuel_consumption", "name"

	sql := fmt.Sprintf(`SELECT 
    t.%s, 
    avg(r.fuel_consumption) AS avg_fuel_consumption, 
    avg(t.%s) AS projected_fuel_consumption
FROM tags AS t
INNER JOIN 
(
    SELECT 
        tags_id, 
        fuel_consumption
    FROM readings AS r
    WHERE velocity > 1
) AS r ON r.tags_id = t.id
WHERE isNotNull(t.%s) AND isNotNull(t.%s) AND isNotNull(t.%s)
GROUP BY fleet`,
		i.withAlias(fleet),
		consumption,
		fleet,
		consumption,
		name)

	humanLabel := "ClickHouse average vs projected fuel consumption per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// AvgDailyDrivingDuration finds the average driving duration per driver.
func (i *IoT) AvgDailyDrivingDuration(qi query.Query) {
	name, driver, fleet := "name", "driver", "fleet"

	sql := fmt.Sprintf(`SELECT t.%s, t.%s, t.%s, avg(d.hours) AS avg_daily_hours
FROM
(
    SELECT 
        toStartOfInterval(ten_minutes, toIntervalHour(24)) AS day, 
        tags_id, 
        count(*) / 6 AS hours
    FROM 
    (
        SELECT 
            toStartOfInterval(created_at, toIntervalMinute(10)) AS ten_minutes, 
            tags_id
        FROM readings AS r
        GROUP BY 
            tags_id, 
            ten_minutes
        HAVING avg(velocity) > 1
    ) AS ten_minute_driving_sessions
    GROUP BY 
        day, 
        tags_id
) AS d
INNER JOIN tags AS t ON t.id = d.tags_id
GROUP BY %s, %s, %s`,
		i.withAlias(fleet),
		i.withAlias(name),
		i.withAlias(driver),
		fleet,
		name,
		driver)

	humanLabel := "ClickHouse average driver driving duration per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// AvgDailyDrivingSession finds the average driving session without stopping per driver per day.
func (i *IoT) AvgDailyDrivingSession(qi query.Query) {
	name := "name"

	sql := fmt.Sprintf(`SELECT t.%s, r.day FROM tags AS t
INNER JOIN 
(
    SELECT 
        tags_id, 
        count(date) AS day
    FROM 
    (
        SELECT 
            tags_id, 
            toStartOfInterval(created_at, toIntervalHour(24)) AS date
        FROM readings
        WHERE tags_id NOT IN 
        (
            SELECT DISTINCT tags_id AS tags_id
            FROM 
            (
                SELECT 
                    tags_id, 
                    toStartOfInterval(created_at, toIntervalMinute(10)) AS ten_minute, 
                    avg(velocity) > 5 AS driving
                FROM readings
                GROUP BY 
                    tags_id, 
                    ten_minute
                HAVING driving = 0
                ORDER BY 
                    tags_id ASC, 
                    ten_minute ASC
            )
        )
        GROUP BY 
            tags_id, 
            date
    )
    GROUP BY tags_id
) AS r ON r.tags_id = t.id
WHERE isNotNull(t.name)
GROUP BY 
    %s, 
    day
ORDER BY 
    name ASC, 
    day ASC`,
		i.withAlias(name),
		name)

	humanLabel := "ClickHouse average driver driving session without stopping per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// AvgLoad finds the average load per truck model per fleet.
func (i *IoT) AvgLoad(qi query.Query) {
	fleet, model, load, name := "fleet", "model", "load_capacity", "name"

	sql := fmt.Sprintf(`SELECT t.%s, t.%s, t.%s, avg(d.avg_load / t.%s) AS avg_load_percentage
		FROM tags t
		INNER JOIN (
			SELECT tags_id, avg(current_load) AS avg_load
			FROM diagnostics d
			GROUP BY tags_id
			) d ON t.id = d.tags_id
		WHERE t.%s IS NOT NULL
		GROUP BY fleet, model, load_capacity`,
		i.withAlias(fleet),
		i.withAlias(model),
		i.withAlias(load),
		i.columnSelect(load),
		i.columnSelect(name))

	humanLabel := "ClickHouse average load per truck model per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// DailyTruckActivity returns the number of hours trucks has been active (not out-of-commission) per day per fleet per model.
func (i *IoT) DailyTruckActivity(qi query.Query) {
	fleet, model, name := "fleet", "model", "name"

	sql := fmt.Sprintf(`SELECT t.%s, t.%s, y.day, sum(y.ten_mins_per_day) / 144 AS daily_activity
FROM tags AS t
INNER JOIN 
(
    SELECT 
        toStartOfInterval(created_at, toIntervalHour(24)) AS day, 
        toStartOfInterval(created_at, toIntervalMinute(10)) AS ten_minutes, 
        tags_id, 
        count(*) AS ten_mins_per_day
    FROM diagnostics
    GROUP BY day, ten_minutes, tags_id
    HAVING avg(status) < 1
) AS y ON y.tags_id = t.id
WHERE isNotNull(t.%s)
GROUP BY fleet, model, y.day
ORDER BY y.day ASC`,
		i.withAlias(fleet),
		i.withAlias(model),
		name)

	humanLabel := "ClickHouse daily truck activity per fleet per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// TruckBreakdownFrequency calculates the amount of times a truck model broke down in the last period.
func (i *IoT) TruckBreakdownFrequency(qi query.Query) {
	model, name := "model", "name"

	sql := fmt.Sprintf(`SELECT t.%s, count(*)
FROM tags AS t
INNER JOIN 
(
    SELECT 
        tags_id, 
        toStartOfInterval(created_at, toIntervalMinute(10)) AS ten_minutes, 
        (count(status = 0) / count(*)) >= 0.5 AS broken_down, 
        neighbor(broken_down, 1) AS next_broken_down
    FROM diagnostics
    GROUP BY 
        tags_id, 
        ten_minutes
    ORDER BY 
        tags_id ASC, 
        ten_minutes ASC
) AS b ON t.id = b.tags_id
WHERE isNotNull(t.%s) AND (broken_down = 1) AND (next_broken_down = 1)
GROUP BY model`,
		i.withAlias(model),
		name)

	humanLabel := "ClickHouse truck breakdown frequency per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.DiagnosticsTableName, sql)
}

// tenMinutePeriods calculates the number of 10 minute periods that can fit in
// the time duration if we subtract the minutes specified by minutesPerHour value.
// E.g.: 4 hours - 5 minutes per hour = 3 hours and 40 minutes = 22 ten minute periods
func tenMinutePeriods(minutesPerHour float64, duration time.Duration) int {
	durationMinutes := duration.Minutes()
	leftover := minutesPerHour * duration.Hours()
	return int((durationMinutes - leftover) / 10)
}
