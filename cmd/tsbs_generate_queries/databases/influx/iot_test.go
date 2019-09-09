package influx

import (
	"math/rand"
	"testing"
	"time"
)

type IoTTestCase struct {
	desc               string
	expectedHumanLabel string
	expectedHumanDesc  string
	expectedQuery      string
}

func TestLastLocPerTruck(t *testing.T) {
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx last location per truck",
			expectedHumanDesc:  "Influx last location per truck",
			expectedQuery: "/query?q=SELECT+%22latitude%22%2C+%22longitude%22+%0A%09%09" +
				"FROM+%22readings%22+%0A%09%09WHERE+%22fleet%22%3D%27South%27+%0A%09%09" +
				"GROUP+BY+%22name%22%2C%22driver%22+%0A%09%09ORDER+BY+%22time%22+%0A%09%09LIMIT+1",
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
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx trucks with low fuel",
			expectedHumanDesc:  "Influx trucks with low fuel: under 10 percent",
			expectedQuery: "/query?q=SELECT+%22name%22%2C+%22driver%22%2C+%22fuel_state%22+%0A%09%09" +
				"FROM+%22diagnostics%22+%0A%09%09WHERE+%22fuel_state%22+%3C%3D+0.1+AND+%22fleet%22+%3D+%27South%27+%0A%09%09" +
				"GROUP+BY+%22name%22+%0A%09%09ORDER+BY+%22time%22+DESC+%0A%09%09LIMIT+1",
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
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx trucks with high load",
			expectedHumanDesc:  "Influx trucks with high load: over 90 percent",
			expectedQuery: "/query?q=SELECT+%22name%22%2C+%22driver%22%2C+%22current_load%22%2C+%22load_capacity%22+%0A%09%09" +
				"FROM+%28SELECT++%22current_load%22%2C+%22load_capacity%22+%0A%09%09+FROM+%22diagnostics%22+" +
				"WHERE+fleet+%3D+%27South%27+%0A%09%09+GROUP+BY+%22name%22%2C%22driver%22+%0A%09%09+" +
				"ORDER+BY+%22time%22+DESC+%0A%09%09+LIMIT+1%29+%0A%09%09WHERE+%22current_load%22+%3E%3D+0.9+%2A+%22load_capacity%22+%0A%09%09" +
				"GROUP+BY+%22name%22+%0A%09%09ORDER+BY+%22time%22+DESC",
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
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx stationary trucks",
			expectedHumanDesc:  "Influx stationary trucks: with low avg velocity in last 10 minutes",

			expectedQuery: "/query?q=SELECT+%22name%22%2C+%22driver%22+%0A%09%09FROM%28" +
				"SELECT+mean%28%22velocity%22%29+as+mean_velocity+%0A%09%09+FROM+%22readings%22+%0A%09%09+" +
				"WHERE+time+%3E+%271970-01-01T00%3A36%3A22Z%27+AND+time+%3C%3D+%271970-01-01T00%3A46%3A22Z%27+%0A%09%09+" +
				"GROUP+BY+time%2810m%29%2C%22name%22%2C%22driver%22%2C%22fleet%22++%0A%09%09+" +
				"LIMIT+1%29+%0A%09%09WHERE+%22fleet%22+%3D+%27West%27+AND+%22mean_velocity%22+%3C+1+%0A%09%09GROUP+BY+%22name%22",
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
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx trucks with longer driving sessions",
			expectedHumanDesc:  "Influx trucks with longer driving sessions: stopped less than 20 mins in 4 hour period",

			expectedQuery: "/query?q=SELECT+%22name%22%2C%22driver%22+%0A%09%09FROM%28" +
				"SELECT+count%28%2A%29+AS+ten_min+%0A%09%09+FROM%28SELECT+mean%28%22velocity%22%29+AS+mean_velocity+%0A%09%09++" +
				"FROM+readings+%0A%09%09++" +
				"WHERE+%22fleet%22+%3D+%27West%27+AND+time+%3E+%271970-01-01T00%3A16%3A22Z%27+AND+time+%3C%3D+%271970-01-01T04%3A16%3A22Z%27+%0A%09%09++" +
				"GROUP+BY+time%2810m%29%2C%22name%22%2C%22driver%22%29+%0A%09%09+" +
				"WHERE+%22mean_velocity%22+%3E+1+%0A%09%09+GROUP+BY+%22name%22%2C%22driver%22%29+%0A%09%09" +
				"WHERE+ten_min_mean_velocity+%3E+22",
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
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx trucks with longer daily sessions",
			expectedHumanDesc:  "Influx trucks with longer daily sessions: drove more than 10 hours in the last 24 hours",

			expectedQuery: "/query?q=SELECT+%22name%22%2C%22driver%22+%0A%09%09" +
				"FROM%28SELECT+count%28%2A%29+AS+ten_min+%0A%09%09+FROM%28" +
				"SELECT+mean%28%22velocity%22%29+AS+mean_velocity+%0A%09%09++FROM+readings+%0A%09%09++" +
				"WHERE+%22fleet%22+%3D+%27West%27+AND+time+%3E+%271970-01-01T00%3A16%3A22Z%27+AND+time+%3C%3D+%271970-01-02T00%3A16%3A22Z%27+%0A%09%09++" +
				"GROUP+BY+time%2810m%29%2C%22name%22%2C%22driver%22%29+%0A%09%09+" +
				"WHERE+%22mean_velocity%22+%3E+1+%0A%09%09+" +
				"GROUP+BY+%22name%22%2C%22driver%22%29+%0A%09%09WHERE+ten_min_mean_velocity+%3E+60",
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
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx average vs projected fuel consumption per fleet",
			expectedHumanDesc:  "Influx average vs projected fuel consumption per fleet",

			expectedQuery: "/query?q=SELECT+mean%28%22fuel_consumption%22%29+AS+%22mean_fuel_consumption%22%2C+mean%28%22nominal_fuel_consumption%22%29+AS+%22nominal_fuel_consumption%22+%0A%09%09" +
				"FROM+%22readings%22+%0A%09%09WHERE+%22velocity%22+%3E+1+%0A%09%09GROUP+BY+%22fleet%22",
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
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx average driver driving duration per day",
			expectedHumanDesc:  "Influx average driver driving duration per day",

			expectedQuery: "/query?q=SELECT+count%28%22mv%22%29%2F6+as+%22hours+driven%22+%0A%09%09" +
				"FROM+%28SELECT+mean%28%22velocity%22%29+as+%22mv%22+%0A%09%09+" +
				"FROM+%22readings%22+%0A%09%09+WHERE+time+%3E+%271970-01-01T00%3A00%3A00Z%27+AND+time+%3C+%271970-01-02T01%3A00%3A00Z%27+%0A%09%09+" +
				"GROUP+BY+time%2810m%29%2C%22fleet%22%2C+%22name%22%2C+%22driver%22%29+%0A%09%09" +
				"WHERE+time+%3E+%271970-01-01T00%3A00%3A00Z%27+AND+time+%3C+%271970-01-02T01%3A00%3A00Z%27+%0A%09%09" +
				"GROUP+BY+time%281d%29%2C%22fleet%22%2C+%22name%22%2C+%22driver%22",
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
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx average driver driving session without stopping per day",
			expectedHumanDesc:  "Influx average driver driving session without stopping per day",

			expectedQuery: "/query?q=SELECT+%22elapsed%22+%0A%09%09INTO+%22random_measure2_1%22+%0A%09%09FROM+%28" +
				"SELECT+difference%28%22difka%22%29%2C+elapsed%28%22difka%22%2C+1m%29+%0A%09%09+" +
				"FROM+%28SELECT+%22difka%22+%0A%09%09++FROM+%28SELECT+difference%28%22mv%22%29+AS+difka+%0A%09%09+++" +
				"FROM+%28SELECT+floor%28mean%28%22velocity%22%29%2F10%29%2Ffloor%28mean%28%22velocity%22%29%2F10%29+AS+%22mv%22+%0A%09%09++++" +
				"FROM+%22readings%22+%0A%09%09++++" +
				"WHERE+%22name%22%21%3D%27%27+AND+time+%3E+%271970-01-01T00%3A00%3A00Z%27+AND+time+%3C+%271970-01-02T01%3A00%3A00Z%27+%0A%09%09++++" +
				"GROUP+BY+time%2810m%29%2C+%22name%22+fill%280%29%29+%0A%09%09+++" +
				"GROUP+BY+%22name%22%29+%0A%09%09++WHERE+%22difka%22%21%3D0+%0A%09%09++" +
				"GROUP+BY+%22name%22%29+%0A%09%09+GROUP+BY+%22name%22%29+%0A%09%09" +
				"WHERE+%22difference%22+%3D+-2+%0A%09%09GROUP+BY+%22name%22%3B+%0A%09%09" +
				"SELECT+mean%28%22elapsed%22%29+%0A%09%09FROM+%22random_measure2_1%22+%0A%09%09" +
				"WHERE+time+%3E+%271970-01-01T00%3A00%3A00Z%27+AND+time+%3C+%271970-01-02T01%3A00%3A00Z%27+%0A%09%09" +
				"GROUP+BY+time%281d%29%2C%22name%22",
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
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx average load per truck model per fleet",
			expectedHumanDesc:  "Influx average load per truck model per fleet",

			expectedQuery: "/query?q=SELECT+mean%28%22ml%22%29+AS+mean_load_percentage+%0A%09%09" +
				"FROM+%28SELECT+%22current_load%22%2F%22load_capacity%22+AS+%22ml%22+%0A%09%09+" +
				"FROM+%22diagnostics%22+%0A%09%09+GROUP+BY+%22name%22%2C+%22fleet%22%2C+%22model%22%29+%0A%09%09" +
				"GROUP+BY+%22fleet%22%2C+%22model%22",
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
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx daily truck activity per fleet per model",
			expectedHumanDesc:  "Influx daily truck activity per fleet per model",

			expectedQuery: "/query?q=SELECT+count%28%22ms%22%29%2F144+%0A%09%09FROM+%28" +
				"SELECT+mean%28%22status%22%29+AS+ms+%0A%09%09+FROM+%22diagnostics%22+%0A%09%09+" +
				"WHERE+time+%3E%3D+%271970-01-01T00%3A00%3A00Z%27+AND+time+%3C+%271970-01-02T01%3A00%3A00Z%27+%0A%09%09+" +
				"GROUP+BY+time%2810m%29%2C+%22model%22%2C+%22fleet%22%29+%0A%09%09" +
				"WHERE+time+%3E%3D+%271970-01-01T00%3A00%3A00Z%27+AND+time+%3C+%271970-01-02T01%3A00%3A00Z%27+AND+%22ms%22%3C1+%0A%09%09" +
				"GROUP+BY+time%281d%29%2C+%22model%22%2C+%22fleet%22",
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
	cases := []IoTTestCase{
		{
			desc: "default",

			expectedHumanLabel: "Influx truck breakdown frequency per model",
			expectedHumanDesc:  "Influx truck breakdown frequency per model",

			expectedQuery: "/query?q=SELECT+count%28%22state_changed%22%29+%0A%09%09" +
				"FROM+%28SELECT+difference%28%22broken_down%22%29+AS+%22state_changed%22+%0A%09%09+" +
				"FROM+%28SELECT+floor%282%2A%28sum%28%22nzs%22%29%2Fcount%28%22nzs%22%29%29%29%2Ffloor%282%2A%28sum%28%22nzs%22%29%2Fcount%28%22nzs%22%29%29%29+AS+%22broken_down%22+%0A%09%09++" +
				"FROM+%28SELECT+%22model%22%2C+%22status%22%2F%22status%22+AS+nzs+%0A%09%09+++" +
				"FROM+%22diagnostics%22+%0A%09%09+++" +
				"WHERE+time+%3E%3D+%271970-01-01T00%3A00%3A00Z%27+AND+time+%3C+%271970-01-02T01%3A00%3A00Z%27%29+%0A%09%09++" +
				"WHERE+time+%3E%3D+%271970-01-01T00%3A00%3A00Z%27+AND+time+%3C+%271970-01-02T01%3A00%3A00Z%27+%0A%09%09++" +
				"GROUP+BY+time%2810m%29%2C%22model%22%29+%0A%09%09+GROUP+BY+%22model%22%29+%0A%09%09" +
				"WHERE+%22state_changed%22+%3D+1+%0A%09%09GROUP+BY+%22model%22",
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
