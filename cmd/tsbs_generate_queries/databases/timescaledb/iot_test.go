package timescaledb

import (
	"math/rand"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
)

type testCase struct {
	desc               string
	useJSON            bool
	expectedHumanLabel string
	expectedHumanDesc  string
	expectedHypertable string
	expectedSQLQuery   string
}

func TestLastLocPerTruck(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "TimescaleDB last location per truck",
			expectedHumanDesc:  "TimescaleDB last location per truck",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `SELECT t.name AS name, t.driver AS driver, r.*
		FROM tags t INNER JOIN LATERAL
			(SELECT longitude, latitude
			FROM readings r
			WHERE r.tags_id=t.id
			ORDER BY time DESC LIMIT 1)  r ON true
		WHERE t.name IS NOT NULL
		AND t.fleet = 'South'`,
		},

		{
			desc: "use JSON",

			useJSON:            true,
			expectedHumanLabel: "TimescaleDB last location per truck",
			expectedHumanDesc:  "TimescaleDB last location per truck",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `SELECT t.tagset->>'name' AS name, t.tagset->>'driver' AS driver, r.*
		FROM tags t INNER JOIN LATERAL
			(SELECT longitude, latitude
			FROM readings r
			WHERE r.tags_id=t.id
			ORDER BY time DESC LIMIT 1)  r ON true
		WHERE t.tagset->>'name' IS NOT NULL
		AND t.tagset->>'fleet' = 'South'`,
		},
	}

	for _, c := range cases {
		rand.Seed(123)
		b := BaseGenerator{
			UseJSON: c.useJSON,
		}
		ig, err := b.NewIoT(time.Now(), time.Now(), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		g.LastLocPerTruck(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedHypertable, c.expectedSQLQuery)
	}
}

func TestTrucksWithLowFuel(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "TimescaleDB trucks with low fuel",
			expectedHumanDesc:  "TimescaleDB trucks with low fuel: under 10 percent",
			expectedHypertable: iot.DiagnosticsTableName,
			expectedSQLQuery: `SELECT t.name AS name, t.driver AS driver, d.* 
		FROM tags t INNER JOIN LATERAL 
			(SELECT fuel_state 
			FROM diagnostics d 
			WHERE d.tags_id=t.id 
			ORDER BY time DESC LIMIT 1) d ON true 
		WHERE t.name IS NOT NULL
		AND d.fuel_state < 0.1 
		AND t.fleet = 'South'`,
		},

		{
			desc: "use JSON",

			useJSON:            true,
			expectedHumanLabel: "TimescaleDB trucks with low fuel",
			expectedHumanDesc:  "TimescaleDB trucks with low fuel: under 10 percent",
			expectedHypertable: iot.DiagnosticsTableName,
			expectedSQLQuery: `SELECT t.tagset->>'name' AS name, t.tagset->>'driver' AS driver, d.* 
		FROM tags t INNER JOIN LATERAL 
			(SELECT fuel_state 
			FROM diagnostics d 
			WHERE d.tags_id=t.id 
			ORDER BY time DESC LIMIT 1) d ON true 
		WHERE t.tagset->>'name' IS NOT NULL
		AND d.fuel_state < 0.1 
		AND t.tagset->>'fleet' = 'South'`,
		},
	}

	for _, c := range cases {
		rand.Seed(123)
		b := BaseGenerator{
			UseJSON: c.useJSON,
		}
		ig, err := b.NewIoT(time.Now(), time.Now(), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		g.TrucksWithLowFuel(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedHypertable, c.expectedSQLQuery)
	}
}

func TestTrucksWithHighLoad(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "TimescaleDB trucks with high load",
			expectedHumanDesc:  "TimescaleDB trucks with high load: over 90 percent",
			expectedHypertable: iot.DiagnosticsTableName,
			expectedSQLQuery: `SELECT t.name AS name, t.driver AS driver, d.* 
		FROM tags t INNER JOIN LATERAL 
			(SELECT current_load 
			FROM diagnostics d 
			WHERE d.tags_id=t.id 
			ORDER BY time DESC LIMIT 1) d ON true 
		WHERE t.name IS NOT NULL
		AND d.current_load/t.load_capacity > 0.9 
		AND t.fleet = 'South'`,
		},

		{
			desc: "use JSON",

			useJSON:            true,
			expectedHumanLabel: "TimescaleDB trucks with high load",
			expectedHumanDesc:  "TimescaleDB trucks with high load: over 90 percent",
			expectedHypertable: iot.DiagnosticsTableName,
			expectedSQLQuery: `SELECT t.tagset->>'name' AS name, t.tagset->>'driver' AS driver, d.* 
		FROM tags t INNER JOIN LATERAL 
			(SELECT current_load 
			FROM diagnostics d 
			WHERE d.tags_id=t.id 
			ORDER BY time DESC LIMIT 1) d ON true 
		WHERE t.tagset->>'name' IS NOT NULL
		AND d.current_load/t.tagset->>'load_capacity' > 0.9 
		AND t.tagset->>'fleet' = 'South'`,
		},
	}

	for _, c := range cases {
		rand.Seed(123)
		b := BaseGenerator{
			UseJSON: c.useJSON,
		}
		ig, err := b.NewIoT(time.Now(), time.Now(), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		g.TrucksWithHighLoad(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedHypertable, c.expectedSQLQuery)
	}
}

func TestStationaryTrucks(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "TimescaleDB stationary trucks",
			expectedHumanDesc:  "TimescaleDB stationary trucks: with low avg velocity in last 10 minutes",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `SELECT t.name AS name, t.driver AS driver
		FROM tags t 
		INNER JOIN readings r ON r.tags_id = t.id 
		WHERE time >= '1970-01-01 00:36:22.646325 +0000' AND time < '1970-01-01 00:46:22.646325 +0000'
		AND t.name IS NOT NULL
		AND t.fleet = 'West' 
		GROUP BY 1, 2 
		HAVING avg(r.velocity) < 1`,
		},

		{
			desc: "use JSON",

			useJSON:            true,
			expectedHumanLabel: "TimescaleDB stationary trucks",
			expectedHumanDesc:  "TimescaleDB stationary trucks: with low avg velocity in last 10 minutes",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `SELECT t.tagset->>'name' AS name, t.tagset->>'driver' AS driver
		FROM tags t 
		INNER JOIN readings r ON r.tags_id = t.id 
		WHERE time >= '1970-01-01 00:36:22.646325 +0000' AND time < '1970-01-01 00:46:22.646325 +0000'
		AND t.tagset->>'name' IS NOT NULL
		AND t.tagset->>'fleet' = 'West' 
		GROUP BY 1, 2 
		HAVING avg(r.velocity) < 1`,
		},
	}

	for _, c := range cases {
		b := &BaseGenerator{
			UseJSON: c.useJSON,
		}
		g := NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(time.Hour), 10, b)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.StationaryTrucks(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedHypertable, c.expectedSQLQuery)
	}
}

func TestTrucksWithLongDrivingSessions(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "TimescaleDB trucks with longer driving sessions",
			expectedHumanDesc:  "TimescaleDB trucks with longer driving sessions: stopped less than 20 mins in 4 hour period",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `SELECT t.name AS name, t.driver AS driver
		FROM tags t 
		INNER JOIN LATERAL 
			(SELECT  time_bucket('10 minutes', time) AS ten_minutes, tags_id  
			FROM readings 
			WHERE time >= '1970-01-01 00:16:22.646325 +0000' AND time < '1970-01-01 04:16:22.646325 +0000'
			GROUP BY ten_minutes, tags_id  
			HAVING avg(velocity) > 1 
			ORDER BY ten_minutes, tags_id) AS r ON t.id = r.tags_id 
		WHERE t.name IS NOT NULL
		AND t.fleet = 'West'
		GROUP BY name, driver 
		HAVING count(r.ten_minutes) > 22`,
		},

		{
			desc: "use JSON",

			useJSON:            true,
			expectedHumanLabel: "TimescaleDB trucks with longer driving sessions",
			expectedHumanDesc:  "TimescaleDB trucks with longer driving sessions: stopped less than 20 mins in 4 hour period",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `SELECT t.tagset->>'name' AS name, t.tagset->>'driver' AS driver
		FROM tags t 
		INNER JOIN LATERAL 
			(SELECT  time_bucket('10 minutes', time) AS ten_minutes, tags_id  
			FROM readings 
			WHERE time >= '1970-01-01 00:16:22.646325 +0000' AND time < '1970-01-01 04:16:22.646325 +0000'
			GROUP BY ten_minutes, tags_id  
			HAVING avg(velocity) > 1 
			ORDER BY ten_minutes, tags_id) AS r ON t.id = r.tags_id 
		WHERE t.tagset->>'name' IS NOT NULL
		AND t.tagset->>'fleet' = 'West'
		GROUP BY name, driver 
		HAVING count(r.ten_minutes) > 22`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{
			UseJSON: c.useJSON,
		}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(6*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.TrucksWithLongDrivingSessions(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedHypertable, c.expectedSQLQuery)
	}
}

func TestTrucksWithLongDailySessions(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "TimescaleDB trucks with longer daily sessions",
			expectedHumanDesc:  "TimescaleDB trucks with longer daily sessions: drove more than 10 hours in the last 24 hours",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `SELECT t.name AS name, t.driver AS driver
		FROM tags t 
		INNER JOIN LATERAL 
			(SELECT  time_bucket('10 minutes', time) AS ten_minutes, tags_id  
			FROM readings 
			WHERE time >= '1970-01-01 00:16:22.646325 +0000' AND time < '1970-01-02 00:16:22.646325 +0000'
			GROUP BY ten_minutes, tags_id  
			HAVING avg(velocity) > 1 
			ORDER BY ten_minutes, tags_id) AS r ON t.id = r.tags_id 
		WHERE t.name IS NOT NULL
		AND t.fleet = 'West'
		GROUP BY name, driver 
		HAVING count(r.ten_minutes) > 60`,
		},

		{
			desc: "use JSON",

			useJSON:            true,
			expectedHumanLabel: "TimescaleDB trucks with longer daily sessions",
			expectedHumanDesc:  "TimescaleDB trucks with longer daily sessions: drove more than 10 hours in the last 24 hours",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `SELECT t.tagset->>'name' AS name, t.tagset->>'driver' AS driver
		FROM tags t 
		INNER JOIN LATERAL 
			(SELECT  time_bucket('10 minutes', time) AS ten_minutes, tags_id  
			FROM readings 
			WHERE time >= '1970-01-01 00:16:22.646325 +0000' AND time < '1970-01-02 00:16:22.646325 +0000'
			GROUP BY ten_minutes, tags_id  
			HAVING avg(velocity) > 1 
			ORDER BY ten_minutes, tags_id) AS r ON t.id = r.tags_id 
		WHERE t.tagset->>'name' IS NOT NULL
		AND t.tagset->>'fleet' = 'West'
		GROUP BY name, driver 
		HAVING count(r.ten_minutes) > 60`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{
			UseJSON: c.useJSON,
		}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.TrucksWithLongDailySessions(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedHypertable, c.expectedSQLQuery)
	}
}

func TestAvgVsProjectedFuelConsumption(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "TimescaleDB average vs projected fuel consumption per fleet",
			expectedHumanDesc:  "TimescaleDB average vs projected fuel consumption per fleet",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `SELECT t.fleet AS fleet, avg(r.fuel_consumption) AS avg_fuel_consumption, 
		avg(t.nominal_fuel_consumption) AS projected_fuel_consumption
		FROM tags t
		INNER JOIN LATERAL(SELECT tags_id, fuel_consumption FROM readings r WHERE r.tags_id = t.id AND velocity > 1) r ON true
		WHERE t.fleet IS NOT NULL
		AND t.nominal_fuel_consumption IS NOT NULL 
		AND t.name IS NOT NULL
		GROUP BY fleet`,
		},

		{
			desc: "use JSON",

			useJSON:            true,
			expectedHumanLabel: "TimescaleDB average vs projected fuel consumption per fleet",
			expectedHumanDesc:  "TimescaleDB average vs projected fuel consumption per fleet",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `SELECT t.tagset->>'fleet' AS fleet, avg(r.fuel_consumption) AS avg_fuel_consumption, 
		avg(t.tagset->>'nominal_fuel_consumption') AS projected_fuel_consumption
		FROM tags t
		INNER JOIN LATERAL(SELECT tags_id, fuel_consumption FROM readings r WHERE r.tags_id = t.id AND velocity > 1) r ON true
		WHERE t.tagset->>'fleet' IS NOT NULL
		AND t.tagset->>'nominal_fuel_consumption' IS NOT NULL 
		AND t.tagset->>'name' IS NOT NULL
		GROUP BY fleet`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{
			UseJSON: c.useJSON,
		}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.AvgVsProjectedFuelConsumption(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedHypertable, c.expectedSQLQuery)
	}
}

func TestAvgDailyDrivingDuration(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "TimescaleDB average driver driving duration per day",
			expectedHumanDesc:  "TimescaleDB average driver driving duration per day",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `WITH ten_minute_driving_sessions
		AS (
			SELECT time_bucket('10 minutes', TIME) AS ten_minutes, tags_id
			FROM readings r
			GROUP BY tags_id, ten_minutes
			HAVING avg(velocity) > 1
			), daily_total_session
		AS (
			SELECT time_bucket('24 hours', ten_minutes) AS day, tags_id, count(*) / 6 AS hours
			FROM ten_minute_driving_sessions
			GROUP BY day, tags_id
			)
		SELECT t.fleet AS fleet, t.name AS name, t.driver AS driver, avg(d.hours) AS avg_daily_hours
		FROM daily_total_session d
		INNER JOIN tags t ON t.id = d.tags_id
		GROUP BY fleet, name, driver`,
		},

		{
			desc: "use JSON",

			useJSON:            true,
			expectedHumanLabel: "TimescaleDB average driver driving duration per day",
			expectedHumanDesc:  "TimescaleDB average driver driving duration per day",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `WITH ten_minute_driving_sessions
		AS (
			SELECT time_bucket('10 minutes', TIME) AS ten_minutes, tags_id
			FROM readings r
			GROUP BY tags_id, ten_minutes
			HAVING avg(velocity) > 1
			), daily_total_session
		AS (
			SELECT time_bucket('24 hours', ten_minutes) AS day, tags_id, count(*) / 6 AS hours
			FROM ten_minute_driving_sessions
			GROUP BY day, tags_id
			)
		SELECT t.tagset->>'fleet' AS fleet, t.tagset->>'name' AS name, t.tagset->>'driver' AS driver, avg(d.hours) AS avg_daily_hours
		FROM daily_total_session d
		INNER JOIN tags t ON t.id = d.tags_id
		GROUP BY fleet, name, driver`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{
			UseJSON: c.useJSON,
		}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.AvgDailyDrivingDuration(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedHypertable, c.expectedSQLQuery)
	}
}

func TestAvgDailyDrivingSession(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "TimescaleDB average driver driving session without stopping per day",
			expectedHumanDesc:  "TimescaleDB average driver driving session without stopping per day",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `WITH driver_status
		AS (
			SELECT tags_id, time_bucket('10 mins', TIME) AS ten_minutes, avg(velocity) > 5 AS driving
			FROM readings
			GROUP BY tags_id, ten_minutes
			ORDER BY tags_id, ten_minutes
			), driver_status_change
		AS (
			SELECT tags_id, ten_minutes AS start, lead(ten_minutes) OVER (PARTITION BY tags_id ORDER BY ten_minutes) AS stop, driving
			FROM (
				SELECT tags_id, ten_minutes, driving, lag(driving) OVER (PARTITION BY tags_id ORDER BY ten_minutes) AS prev_driving
				FROM driver_status
				) x
			WHERE x.driving <> x.prev_driving
			)
		SELECT t.name AS name, time_bucket('24 hours', start) AS day, avg(age(stop, start)) AS duration
		FROM tags t
		INNER JOIN driver_status_change d ON t.id = d.tags_id
		WHERE t.name IS NOT NULL
		AND d.driving = true
		GROUP BY name, day
		ORDER BY name, day`,
		},

		{
			desc: "use JSON",

			useJSON:            true,
			expectedHumanLabel: "TimescaleDB average driver driving session without stopping per day",
			expectedHumanDesc:  "TimescaleDB average driver driving session without stopping per day",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `WITH driver_status
		AS (
			SELECT tags_id, time_bucket('10 mins', TIME) AS ten_minutes, avg(velocity) > 5 AS driving
			FROM readings
			GROUP BY tags_id, ten_minutes
			ORDER BY tags_id, ten_minutes
			), driver_status_change
		AS (
			SELECT tags_id, ten_minutes AS start, lead(ten_minutes) OVER (PARTITION BY tags_id ORDER BY ten_minutes) AS stop, driving
			FROM (
				SELECT tags_id, ten_minutes, driving, lag(driving) OVER (PARTITION BY tags_id ORDER BY ten_minutes) AS prev_driving
				FROM driver_status
				) x
			WHERE x.driving <> x.prev_driving
			)
		SELECT t.tagset->>'name' AS name, time_bucket('24 hours', start) AS day, avg(age(stop, start)) AS duration
		FROM tags t
		INNER JOIN driver_status_change d ON t.id = d.tags_id
		WHERE t.tagset->>'name' IS NOT NULL
		AND d.driving = true
		GROUP BY name, day
		ORDER BY name, day`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{
			UseJSON: c.useJSON,
		}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.AvgDailyDrivingSession(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedHypertable, c.expectedSQLQuery)
	}
}

func TestAvgLoad(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "TimescaleDB average load per truck model per fleet",
			expectedHumanDesc:  "TimescaleDB average load per truck model per fleet",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `SELECT t.fleet AS fleet, t.model AS model, t.load_capacity AS load_capacity, avg(d.avg_load / t.load_capacity) AS avg_load_percentage
		FROM tags t
		INNER JOIN (
			SELECT tags_id, avg(current_load) AS avg_load
			FROM diagnostics d
			GROUP BY tags_id
			) d ON t.id = d.tags_id
		WHERE t.name IS NOT NULL
		GROUP BY fleet, model, load_capacity`,
		},

		{
			desc: "use JSON",

			useJSON:            true,
			expectedHumanLabel: "TimescaleDB average load per truck model per fleet",
			expectedHumanDesc:  "TimescaleDB average load per truck model per fleet",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `SELECT t.tagset->>'fleet' AS fleet, t.tagset->>'model' AS model, t.tagset->>'load_capacity' AS load_capacity, avg(d.avg_load / t.tagset->>'load_capacity') AS avg_load_percentage
		FROM tags t
		INNER JOIN (
			SELECT tags_id, avg(current_load) AS avg_load
			FROM diagnostics d
			GROUP BY tags_id
			) d ON t.id = d.tags_id
		WHERE t.tagset->>'name' IS NOT NULL
		GROUP BY fleet, model, load_capacity`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{
			UseJSON: c.useJSON,
		}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.AvgLoad(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedHypertable, c.expectedSQLQuery)
	}
}

func TestDailyTruckActivity(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "TimescaleDB daily truck activity per fleet per model",
			expectedHumanDesc:  "TimescaleDB daily truck activity per fleet per model",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `SELECT t.fleet AS fleet, t.model AS model, y.day, sum(y.ten_mins_per_day) / 144 AS daily_activity
		FROM tags t
		INNER JOIN (
			SELECT time_bucket('24 hours', TIME) AS day, time_bucket('10 minutes', TIME) AS ten_minutes, tags_id, count(*) AS ten_mins_per_day
			FROM diagnostics
			GROUP BY day, ten_minutes, tags_id
			HAVING avg(STATUS) < 1
			) y ON y.tags_id = t.id
		WHERE t.name IS NOT NULL
		GROUP BY fleet, model, y.day
		ORDER BY y.day`,
		},

		{
			desc: "use JSON",

			useJSON:            true,
			expectedHumanLabel: "TimescaleDB daily truck activity per fleet per model",
			expectedHumanDesc:  "TimescaleDB daily truck activity per fleet per model",
			expectedHypertable: iot.ReadingsTableName,
			expectedSQLQuery: `SELECT t.tagset->>'fleet' AS fleet, t.tagset->>'model' AS model, y.day, sum(y.ten_mins_per_day) / 144 AS daily_activity
		FROM tags t
		INNER JOIN (
			SELECT time_bucket('24 hours', TIME) AS day, time_bucket('10 minutes', TIME) AS ten_minutes, tags_id, count(*) AS ten_mins_per_day
			FROM diagnostics
			GROUP BY day, ten_minutes, tags_id
			HAVING avg(STATUS) < 1
			) y ON y.tags_id = t.id
		WHERE t.tagset->>'name' IS NOT NULL
		GROUP BY fleet, model, y.day
		ORDER BY y.day`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{
			UseJSON: c.useJSON,
		}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.DailyTruckActivity(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedHypertable, c.expectedSQLQuery)
	}
}

func TestTruckBreakdownFrequency(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "TimescaleDB truck breakdown frequency per model",
			expectedHumanDesc:  "TimescaleDB truck breakdown frequency per model",
			expectedHypertable: iot.DiagnosticsTableName,
			expectedSQLQuery: `WITH breakdown_per_truck_per_ten_minutes
		AS (
			SELECT time_bucket('10 minutes', TIME) AS ten_minutes, tags_id, count(STATUS = 0) / count(*) >= 0.5 AS broken_down
			FROM diagnostics
			GROUP BY ten_minutes, tags_id
			), breakdowns_per_truck
		AS (
			SELECT ten_minutes, tags_id, broken_down, lead(broken_down) OVER (
					PARTITION BY tags_id ORDER BY ten_minutes
					) AS next_broken_down
			FROM breakdown_per_truck_per_ten_minutes
			)
		SELECT t.model AS model, count(*)
		FROM tags t
		INNER JOIN breakdowns_per_truck b ON t.id = b.tags_id
		WHERE t.name IS NOT NULL
		AND broken_down = false AND next_broken_down = true
		GROUP BY model`,
		},

		{
			desc: "use JSON",

			useJSON:            true,
			expectedHumanLabel: "TimescaleDB truck breakdown frequency per model",
			expectedHumanDesc:  "TimescaleDB truck breakdown frequency per model",
			expectedHypertable: iot.DiagnosticsTableName,
			expectedSQLQuery: `WITH breakdown_per_truck_per_ten_minutes
		AS (
			SELECT time_bucket('10 minutes', TIME) AS ten_minutes, tags_id, count(STATUS = 0) / count(*) >= 0.5 AS broken_down
			FROM diagnostics
			GROUP BY ten_minutes, tags_id
			), breakdowns_per_truck
		AS (
			SELECT ten_minutes, tags_id, broken_down, lead(broken_down) OVER (
					PARTITION BY tags_id ORDER BY ten_minutes
					) AS next_broken_down
			FROM breakdown_per_truck_per_ten_minutes
			)
		SELECT t.tagset->>'model' AS model, count(*)
		FROM tags t
		INNER JOIN breakdowns_per_truck b ON t.id = b.tags_id
		WHERE t.tagset->>'name' IS NOT NULL
		AND broken_down = false AND next_broken_down = true
		GROUP BY model`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{
			UseJSON: c.useJSON,
		}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.TruckBreakdownFrequency(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedHypertable, c.expectedSQLQuery)
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
