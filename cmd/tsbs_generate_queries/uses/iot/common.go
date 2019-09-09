package iot

import (
	"math/rand"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/internal/usecase"
	"github.com/timescale/tsbs/query"
)

const (
	// ReadingsTableName is the name of the table where all the readings
	// time series data is stored.
	ReadingsTableName = "readings"
	// DiagnosticsTableName is the name of the table where all the diagnostics
	// time series data is stored.
	DiagnosticsTableName = "diagnostics"

	// StationaryDuration is the time duration to evaluate stationary trucks.
	StationaryDuration = 10 * time.Minute
	// LongDrivingSessionDuration is the the time duration which is considered a
	// long driving session without stopping.
	LongDrivingSessionDuration = 4 * time.Hour
	// DailyDrivingDuration is time duration of one day of driving.
	DailyDrivingDuration = 24 * time.Hour

	// LabelLastLoc is the label for the last location query.
	LabelLastLoc = "last-loc"
	// LabelLowFuel is the label for the low fuel query.
	LabelLowFuel = "low-fuel"
	// LabelHighLoad is the label for the high load query.
	LabelHighLoad = "high-load"
	// LabelStationaryTrucks is the label for the stationary trucks query.
	LabelStationaryTrucks = "stationary-trucks"
	// LabelLongDrivingSessions is the label for the long driving sessions query.
	LabelLongDrivingSessions = "long-driving-sessions"
	// LabelLongDailySessions is the label for the long daily sessions query.
	LabelLongDailySessions = "long-daily-sessions"
	// LabelAvgVsProjectedFuelConsumption is the label for the avg vs projected fuel consumption query.
	LabelAvgVsProjectedFuelConsumption = "avg-vs-projected-fuel-consumption"
	// LabelAvgDailyDrivingDuration is the label for the avg daily driving durationquery.
	LabelAvgDailyDrivingDuration = "avg-daily-driving-duration"
	// LabelAvgDailyDrivingSession is the label for the avg daily driving session query.
	LabelAvgDailyDrivingSession = "avg-daily-driving-session"
	// LabelAvgLoad is the label for the avg load query.
	LabelAvgLoad = "avg-load"
	// LabelDailyActivity is the label for the daily activity query.
	LabelDailyActivity = "daily-activity"
	// LabelBreakdownFrequency is the label for the breakdown frequency query.
	LabelBreakdownFrequency = "breakdown-frequency"
)

// Core is the common component of all generators for all systems.
type Core struct {
	*common.Core
}

// GetRandomFleet returns one of the fleet choices by random.
func (c Core) GetRandomFleet() string {
	return usecase.FleetChoices[rand.Intn(len(usecase.FleetChoices))]
}

// NewCore returns a new Core for the given time range and cardinality
func NewCore(start, end time.Time, scale int) (*Core, error) {
	c, err := common.NewCore(start, end, scale)
	return &Core{Core: c}, err

}

// LastLocFiller is a type that can fill in a last location query.
type LastLocFiller interface {
	LastLocPerTruck(query.Query)
}

// TruckLowFuelFiller is a type that can fill in a trucks with low fuel query.
type TruckLowFuelFiller interface {
	TrucksWithLowFuel(query.Query)
}

// TruckHighLoadFiller is a type that can fill in a trucks with high load query.
type TruckHighLoadFiller interface {
	TrucksWithHighLoad(query.Query)
}

// StationaryTrucksFiller is a type that can fill in the stationary trucks query.
type StationaryTrucksFiller interface {
	StationaryTrucks(query.Query)
}

// TruckLongDrivingSessionFiller is a type that can fill in a trucks with longer driving sessions query.
type TruckLongDrivingSessionFiller interface {
	TrucksWithLongDrivingSessions(query.Query)
}

// TruckLongDailySessionFiller is a type that can fill in a trucks with longer daily driving sessions query.
type TruckLongDailySessionFiller interface {
	TrucksWithLongDailySessions(query.Query)
}

// AvgVsProjectedFuelConsumptionFiller is a type that can fill in an avg vs projected fuel consumption query.
type AvgVsProjectedFuelConsumptionFiller interface {
	AvgVsProjectedFuelConsumption(query.Query)
}

// AvgDailyDrivingDurationFiller is a type that can fill in an avg daily driving duration per driver query.
type AvgDailyDrivingDurationFiller interface {
	AvgDailyDrivingDuration(query.Query)
}

// AvgDailyDrivingSessionFiller is a type that can fill in an avg daily driving session query.
type AvgDailyDrivingSessionFiller interface {
	AvgDailyDrivingSession(query.Query)
}

// AvgLoadFiller is a type that can fill in an avg load query.
type AvgLoadFiller interface {
	AvgLoad(query.Query)
}

// DailyTruckActivityFiller is a type that can fill in the daily truck activity query.
type DailyTruckActivityFiller interface {
	DailyTruckActivity(query.Query)
}

// TruckBreakdownFrequencyFiller is a type that can fill in the truck breakdown frequency query.
type TruckBreakdownFrequencyFiller interface {
	TruckBreakdownFrequency(query.Query)
}
