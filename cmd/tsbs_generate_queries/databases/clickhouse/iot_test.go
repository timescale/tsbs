package clickhouse

import (
	"math/rand"
	"testing"
	"time"

	"github.com/timescale/tsbs/pkg/query"
)

const (
	testScale = 10
)

func TestLastLocByTruck(t *testing.T) {
	cases := []testCase{
		{
			desc:    "zero trucks",
			input:   0,
			fail:    true,
			failMsg: "number of trucks cannot be < 1; got 0",
		},
		{
			desc:    "more trucks than scale",
			input:   2 * testScale,
			fail:    true,
			failMsg: "number of trucks (20) larger than total trucks. See --scale (10)",
		},
		{
			desc:  "one truck",
			input: 1,

			expectedHumanLabel: "ClickHouse last location by specific truck",
			expectedHumanDesc:  "ClickHouse last location by specific truck: random    1 trucks",

			expectedQuery: `SELECT 
    t.name AS name, 
    t.driver AS driver, 
    r.longitude AS longitude, 
    r.latitude AS latitude
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
WHERE t.name IN ('truck_5')`,
		},
		{
			desc:  "three truck",
			input: 3,

			expectedHumanLabel: "ClickHouse last location by specific truck",
			expectedHumanDesc:  "ClickHouse last location by specific truck: random    3 trucks",

			expectedQuery: `SELECT 
    t.name AS name, 
    t.driver AS driver, 
    r.longitude AS longitude, 
    r.latitude AS latitude
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
WHERE t.name IN ('truck_9','truck_3','truck_5')`,
		},
	}

	testFunc := func(i *IoT, c testCase) query.Query {
		q := i.GenerateEmptyQuery()
		i.LastLocByTruck(q, c.input)
		return q
	}

	runTestCase(t, testFunc, time.Now(), time.Now(), cases)
}

func TestLastLocPerTruck(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "ClickHouse last location per truck",
			expectedHumanDesc:  "ClickHouse last location per truck",

			expectedQuery: `SELECT t.name AS name, t.driver AS driver, r.*
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
WHERE (fleet = 'South') AND isNotNull(name)`,
		},
	}

	for _, c := range cases {
		rand.Seed(123)
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Now(), time.Now(), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		g.LastLocPerTruck(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedQuery)
	}
}

func TestTrucksWithLowFuel(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "ClickHouse trucks with low fuel",
			expectedHumanDesc:  "ClickHouse trucks with low fuel: under 10 percent",

			expectedQuery: `SELECT t.name AS name, t.driver AS driver, d.*
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
WHERE isNotNull(name) and d.fuel_state < 0.1 and t.fleet = 'South'`,
		},
	}

	for _, c := range cases {
		rand.Seed(123)
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Now(), time.Now(), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		g.TrucksWithLowFuel(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedQuery)
	}
}

func TestTrucksWithHighLoad(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "ClickHouse trucks with high load",
			expectedHumanDesc:  "ClickHouse trucks with high load: over 90 percent",
			expectedQuery: `SELECT t.name AS name, t.driver AS driver, d.*
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
WHERE isNotNull(name) AND ((d.current_load / t.load_capacity) > 0.9) AND (t.fleet = 'South')`,
		},
	}

	for _, c := range cases {
		rand.Seed(123)
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Now(), time.Now(), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		g.TrucksWithHighLoad(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedQuery)
	}
}

func TestStationaryTrucks(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "ClickHouse stationary trucks",
			expectedHumanDesc:  "ClickHouse stationary trucks: with low avg velocity in last 10 minutes",
			expectedQuery: `SELECT t.name AS name, t.driver AS driver
FROM tags AS t
INNER JOIN readings AS r ON r.tags_id = t.id
WHERE (time >= '1970-01-01 00:36:22') AND (time < '1970-01-01 00:46:22') 
AND isNotNull(t.name) 
AND (t.fleet = 'West')
GROUP BY name, driver
HAVING avg(r.velocity) > 1`,
		},
	}

	for _, c := range cases {
		b := &BaseGenerator{}
		g := NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(time.Hour), 10, b)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.StationaryTrucks(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedQuery)
	}
}

func TestTrucksWithLongDrivingSessions(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "ClickHouse trucks with longer driving sessions",
			expectedHumanDesc:  "ClickHouse trucks with longer driving sessions: stopped less than 20 mins in 4 hour period",
			expectedQuery: `SELECT t.name AS name, t.driver AS driver
FROM tags AS t INNER JOIN 
(
    SELECT 
        toStartOfInterval(created_at, toIntervalMinute(10)) AS ten_minutes, 
        tags_id
    FROM readings
    WHERE (time >= '1970-01-01 00:16:22') AND (time < '1970-01-01 04:16:22')
    GROUP BY 
        ten_minutes,
        tags_id
    HAVING avg(velocity) > 1
    ORDER BY 
        ten_minutes ASC, 
        tags_id ASC
) AS r ON t.id = r.tags_id
WHERE isNotNull(t.name) AND (t.fleet = 'West')
GROUP BY 
    name, 
    driver
HAVING count(r.ten_minutes) > 22`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(6*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.TrucksWithLongDrivingSessions(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedQuery)
	}
}

func TestTrucksWithLongDailySessions(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "ClickHouse trucks with longer daily sessions",
			expectedHumanDesc:  "ClickHouse trucks with longer daily sessions: drove more than 10 hours in the last 24 hours",
			expectedQuery: `SELECT t.name AS name, t.driver AS driver
FROM tags AS t
INNER JOIN 
(
    SELECT 
        toStartOfInterval(created_at, toIntervalMinute(10)) AS ten_minutes, 
        tags_id
    FROM readings
    WHERE (time >= '1970-01-01 00:16:22') AND (time < '1970-01-02 00:16:22')
    GROUP BY 
        ten_minutes, 
        tags_id
    HAVING avg(velocity) > 1
    ORDER BY 
        ten_minutes ASC, 
        tags_id ASC
) AS r ON t.id = r.tags_id
WHERE isNotNull(t.name) AND (t.fleet = 'West')
GROUP BY 
    name, 
    driver
HAVING count(r.ten_minutes) > 60`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.TrucksWithLongDailySessions(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedQuery)
	}
}

func TestAvgVsProjectedFuelConsumption(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "ClickHouse average vs projected fuel consumption per fleet",
			expectedHumanDesc:  "ClickHouse average vs projected fuel consumption per fleet",
			expectedQuery: `SELECT 
    t.fleet AS fleet, 
    avg(r.fuel_consumption) AS avg_fuel_consumption, 
    avg(t.nominal_fuel_consumption) AS projected_fuel_consumption
FROM tags AS t
INNER JOIN 
(
    SELECT 
        tags_id, 
        fuel_consumption
    FROM readings AS r
    WHERE velocity > 1
) AS r ON r.tags_id = t.id
WHERE isNotNull(t.fleet) AND isNotNull(t.nominal_fuel_consumption) AND isNotNull(t.name)
GROUP BY fleet`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.AvgVsProjectedFuelConsumption(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedQuery)
	}
}

func TestAvgDailyDrivingDuration(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "ClickHouse average driver driving duration per day",
			expectedHumanDesc:  "ClickHouse average driver driving duration per day",
			expectedQuery: `SELECT t.fleet AS fleet, t.name AS name, t.driver AS driver, avg(d.hours) AS avg_daily_hours
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
GROUP BY fleet, name, driver`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.AvgDailyDrivingDuration(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedQuery)
	}
}

func TestAvgDailyDrivingSession(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "ClickHouse average driver driving session without stopping per day",
			expectedHumanDesc:  "ClickHouse average driver driving session without stopping per day",
			expectedQuery: `SELECT t.name AS name, r.day FROM tags AS t
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
    name, 
    day
ORDER BY 
    name ASC, 
    day ASC`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.AvgDailyDrivingSession(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedQuery)
	}
}

func TestAvgLoad(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "ClickHouse average load per truck model per fleet",
			expectedHumanDesc:  "ClickHouse average load per truck model per fleet",
			expectedQuery: `SELECT t.fleet AS fleet, t.model AS model, t.load_capacity AS load_capacity, avg(d.avg_load / t.load_capacity) AS avg_load_percentage
		FROM tags t
		INNER JOIN (
			SELECT tags_id, avg(current_load) AS avg_load
			FROM diagnostics d
			GROUP BY tags_id
			) d ON t.id = d.tags_id
		WHERE t.name IS NOT NULL
		GROUP BY fleet, model, load_capacity`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.AvgLoad(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedQuery)
	}
}

func TestDailyTruckActivity(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "ClickHouse daily truck activity per fleet per model",
			expectedHumanDesc:  "ClickHouse daily truck activity per fleet per model",

			expectedQuery: `SELECT t.fleet AS fleet, t.model AS model, y.day, sum(y.ten_mins_per_day) / 144 AS daily_activity
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
WHERE isNotNull(t.name)
GROUP BY fleet, model, y.day
ORDER BY y.day ASC`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.DailyTruckActivity(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedQuery)
	}
}

func TestTruckBreakdownFrequency(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "ClickHouse truck breakdown frequency per model",
			expectedHumanDesc:  "ClickHouse truck breakdown frequency per model",

			expectedQuery: `SELECT t.model AS model, count(*)
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
WHERE isNotNull(t.name) AND (broken_down = 1) AND (next_broken_down = 1)
GROUP BY model`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.TruckBreakdownFrequency(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedQuery)
	}
}

func TestTenMinutePeriods(t *testing.T) {
	cases := []struct {
		minutesPerHour float64
		duration       time.Duration
		result         int
	}{
		{
			minutesPerHour: 5.0,
			duration:       4 * time.Hour,
			result:         22,
		},
		{
			minutesPerHour: 10.0,
			duration:       24 * time.Hour,
			result:         120,
		},
		{
			minutesPerHour: 0.0,
			duration:       24 * time.Hour,
			result:         144,
		},
		{
			minutesPerHour: 1.0,
			duration:       0 * time.Minute,
			result:         0,
		},
		{
			minutesPerHour: 0.0,
			duration:       0 * time.Minute,
			result:         0,
		},
		{
			minutesPerHour: 1.0,
			duration:       30 * time.Minute,
			result:         2,
		},
	}

	for _, c := range cases {
		if got := tenMinutePeriods(c.minutesPerHour, c.duration); got != c.result {
			t.Errorf("incorrect result for %.2f minutes per hour, duration %s: got %d want %d", c.minutesPerHour, c.duration.String(), got, c.result)
		}
	}

}

func runTestCase(t *testing.T, testFunc func(*IoT, testCase) query.Query, s time.Time, e time.Time, cases []testCase) {
	rand.Seed(123) // Setting seed for testing purposes.

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			b := BaseGenerator{}
			dq, err := b.NewIoT(s, e, testScale)
			if err != nil {
				t.Fatalf("Error while creating devops generator")
			}
			i := dq.(*IoT)

			if c.fail {
				func() {
					defer func() {
						r := recover()
						if r == nil {
							t.Fatalf("did not panic when should")
						}

						if r != c.failMsg {
							t.Fatalf("incorrect fail message: got %s, want %s", r, c.failMsg)
						}
					}()

					testFunc(i, c)
				}()
			} else {
				q := testFunc(i, c)

				verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedQuery)
			}
		})
	}
}
