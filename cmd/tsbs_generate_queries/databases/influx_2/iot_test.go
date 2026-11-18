package influx_2

import (
	"math/rand"
	"net/url"
	"testing"
	"time"

	"github.com/timescale/tsbs/pkg/query"
)

const (
	testScale = 10
)

type IoTTestCase struct {
	desc               string
	input              int
	fail               bool
	failMsg            string
	expectedHumanLabel string
	expectedHumanDesc  string
	expectedQuery      string
}

func TestLastLocByTruck(t *testing.T) {
	cases := []IoTTestCase{
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

			expectedHumanLabel: "Influx 2.x last location by specific truck",
			expectedHumanDesc:  "Influx 2.x last location by specific truck: random    1 trucks",
			// 	expectedQuery: `SELECT "name", "driver", "latitude", "longitude"
			// FROM "readings"
			// WHERE ("name" = 'truck_5')
			// ORDER BY "time"
			// LIMIT 1`,
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 0)
	|> filter(fn: (r) => r._measurement == "readings" and (r.name == 'truck_5'))
	|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	|> group(columns: ["name", "driver"])
	|> sort(columns: ["_time"], desc: true)
	|> first(column: "latitude")
	|> keep(columns: ["name", "driver", "latitude", "longitude", "_time"])`,
		},
		{
			desc:  "three truck",
			input: 3,

			expectedHumanLabel: "Influx 2.x last location by specific truck",
			expectedHumanDesc:  "Influx 2.x last location by specific truck: random    3 trucks",
			// 	expectedQuery: `SELECT "name", "driver", "latitude", "longitude"
			// FROM "readings"
			// WHERE ("name" = 'truck_9' or "name" = 'truck_3' or "name" = 'truck_5')
			// ORDER BY "time"
			// LIMIT 1`,
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 0)
	|> filter(fn: (r) => r._measurement == "readings" and (r.name == 'truck_9' or r.name == 'truck_3' or r.name == 'truck_5'))
	|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	|> group(columns: ["name", "driver"])
	|> sort(columns: ["_time"], desc: true)
	|> first(column: "latitude")
	|> keep(columns: ["name", "driver", "latitude", "longitude", "_time"])`,
		},
	}

	testFunc := func(i *IoT, c IoTTestCase) query.Query {
		q := i.GenerateEmptyQuery()
		i.LastLocByTruck(q, c.input)
		return q
	}

	runIoTTestCases(t, testFunc, time.Now(), time.Now(), cases)
}

func TestLastLocPerTruck(t *testing.T) {
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx 2.x last location per truck",
			expectedHumanDesc:  "Influx 2.x last location per truck",
			// expectedQuery: "/query?q=SELECT+%22latitude%22%2C+%22longitude%22+%0A%09%09" +
			// 	"FROM+%22readings%22+%0A%09%09WHERE+%22fleet%22%3D%27South%27+%0A%09%09" +
			// 	"GROUP+BY+%22name%22%2C%22driver%22+%0A%09%09ORDER+BY+%22time%22+%0A%09%09LIMIT+1",
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 0)
	|> filter(fn: (r) => r._measurement == "readings" and r.fleet == "South")
	|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	|> group(columns: ["name", "driver"])
	|> sort(columns: ["_time"], desc: true)
	|> first(column: "latitude")
	|> keep(columns: ["name", "driver", "latitude", "longitude"])`,
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

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, "/api/v2/query", c.expectedQuery)
	}
}

func TestTrucksWithLowFuel(t *testing.T) {
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx 2.x trucks with low fuel",
			expectedHumanDesc:  "Influx 2.x trucks with low fuel: under 10 percent",
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 0)
	|> filter(fn: (r) => r._measurement == "diagnostics" and r.fleet == "South" and r._field == "fuel_state" and r._value <= 0.1)
	|> group(columns: ["name", "driver"])
	|> last()
	|> keep(columns: ["name", "driver", "_field", "_value"])`,
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

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, "/api/v2/query", c.expectedQuery)
	}
}

func TestTrucksWithHighLoad(t *testing.T) {
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx 2.x trucks with high load",
			expectedHumanDesc:  "Influx 2.x trucks with high load: over 90 percent",
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 0)
	|> filter(fn: (r) => r._measurement == "diagnostics" and r.fleet == "South")
	|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	|> group(columns: ["name", "driver"])
	|> sort(columns: ["_time"], desc: true)
	|> first(column: "current_load")
	|> filter(fn: (r) => r.current_load >= 0.9 * r.load_capacity)
	|> keep(columns: ["name", "driver", "current_load", "load_capacity"])`,
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

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, "/api/v2/query", c.expectedQuery)
	}
}

func TestStationaryTrucks(t *testing.T) {
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx 2.x stationary trucks",
			expectedHumanDesc:  "Influx 2.x stationary trucks: with low avg velocity in last 10 minutes",
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 1970-01-01T00:36:22Z, stop: 1970-01-01T00:46:22Z)
	|> filter(fn: (r) => r._measurement == "readings" and r._field == "velocity" and r.fleet == "West")
	|> group(columns: ["name", "driver", "fleet"])
	|> aggregateWindow(every: 10m, fn: mean, createEmpty: false)
	|> sort(columns: ["_time"], desc: false)
	|> filter(fn: (r) => r._value < 1.0)`,
		},
	}

	for _, c := range cases {
		b := &BaseGenerator{}
		g := NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(time.Hour), 10, b)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.StationaryTrucks(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, "/api/v2/query", c.expectedQuery)
	}
}

func TestTrucksWithLongDrivingSessions(t *testing.T) {
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx 2.x trucks with longer driving sessions",
			expectedHumanDesc:  "Influx 2.x trucks with longer driving sessions: stopped less than 20 mins in 4 hour period",
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 1970-01-01T00:16:22Z, stop: 1970-01-01T04:16:22Z)
	|> filter(fn: (r) => r._measurement == "readings" and r._field == "velocity" and r.fleet == "West")
	|> aggregateWindow(every: 10m, fn: mean, createEmpty: false)
	|> group(columns: ["name", "driver"])
	|> filter(fn: (r) => r._value > 1.0)
	|> aggregateWindow(every: 14400000000000s, fn:count, offset: 0s, createEmpty: false)
	|> filter(fn: (r) => r._value > 22)
	`,
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

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, "/api/v2/query", c.expectedQuery)
	}
}

func TestTrucksWithLongDailySessions(t *testing.T) {
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx 2.x trucks with longer daily sessions",
			expectedHumanDesc:  "Influx 2.x trucks with longer daily sessions: drove more than 10 hours in the last 24 hours",
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 1970-01-01T00:16:22Z, stop: 1970-01-02T00:16:22Z)
	|> filter(fn: (r) => r._measurement == "readings" and r._field == "velocity" and r.fleet == "West")
	|> aggregateWindow(every: 10m, fn: mean, createEmpty: false)
	|> group(columns: ["name", "driver"])
	|> filter(fn: (r) => r._value > 1.0)
	|> aggregateWindow(every: 14400000000000s, fn:count, offset: 0s, createEmpty: false)
	|> filter(fn: (r) => r._value > 60)
	`,
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

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, "/api/v2/query", c.expectedQuery)
	}
}

func TestAvgVsProjectedFuelConsumption(t *testing.T) {
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx 2.x average vs projected fuel consumption per fleet",
			expectedHumanDesc:  "Influx 2.x average vs projected fuel consumption per fleet",
			expectedQuery: `
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
	|> yield(name: "mean_fuel_consumption")`,
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

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, "/api/v2/query", c.expectedQuery)
	}
}

func TestAvgDailyDrivingDuration(t *testing.T) {
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx 2.x average driver driving duration per day",
			expectedHumanDesc:  "Influx 2.x average driver driving duration per day",
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-02T01:00:00Z)
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

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, "/api/v2/query", c.expectedQuery)
	}
}

func TestAvgDailyDrivingSession(t *testing.T) {
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx 2.x average driver driving session without stopping per day",
			expectedHumanDesc:  "Influx 2.x average driver driving session without stopping per day",
			expectedQuery: `
	import "date"
	import "math"

	from(bucket: "benchmark")
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-02T01:00:00Z)
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

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, "/api/v2/query", c.expectedQuery)
	}
}

func TestAvgLoad(t *testing.T) {
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx 2.x average load per truck model per fleet",
			expectedHumanDesc:  "Influx 2.x average load per truck model per fleet",

			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 0)
	|> filter(fn: (r) => r._measurement == "diagnostics")
	|> filter(fn: (r) => r._field == "current_load" or r._field == "load_capacity")
	|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	|> filter(fn: (r) => r.load_capacity > 0.0)
	|> map(fn: (r) => ({r with load_percentage: r.current_load / r.load_capacity}))
	|> group(columns: ["name", "fleet", "model"])
	|> mean(column: "load_percentage")`,
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

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, "/api/v2/query", c.expectedQuery)
	}
}

func TestDailyTruckActivity(t *testing.T) {
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx 2.x daily truck activity per fleet per model",
			expectedHumanDesc:  "Influx 2.x daily truck activity per fleet per model",
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-02T01:00:00Z)
	|> filter(fn: (r) => r._measurement == "diagnostics" and r._field == "status")
	|> filter(fn: (r) => exists r.model and exists r.fleet)
	|> aggregateWindow(every: 10m, fn: mean, createEmpty: false)
	|> group(columns: ["model", "fleet"])
	|> filter(fn: (r) => r._value < 1)
	|> aggregateWindow(every: 1d, fn: count, createEmpty: false)
	|> map(fn: (r) => ({r with _value: float(v:r._value) / 144.0}))`,
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

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, "/api/v2/query", c.expectedQuery)
	}
}

func TestTruckBreakdownFrequency(t *testing.T) {
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx 2.x truck breakdown frequency per model",
			expectedHumanDesc:  "Influx 2.x truck breakdown frequency per model",
			expectedQuery: `
	from(bucket: "benchmark")
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-02T01:00:00Z)
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

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, "/api/v2/query", c.expectedQuery)
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

func runIoTTestCases(t *testing.T, testFunc func(*IoT, IoTTestCase) query.Query, s time.Time, e time.Time, cases []IoTTestCase) {
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

				v := url.Values{}
				v.Set("q", c.expectedQuery)
				expectedPath := "/api/v2/query"

				verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, expectedPath, c.expectedQuery)
			}
		})
	}
}
