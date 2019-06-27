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

	// LabelLastLoc is the label for the last location query.
	LabelLastLoc = "lastloc"
)

// Core is the common component of all generators for all systems.
type Core struct {
	*common.Core
}

// GetRandomFleet returns one of the fleet choices by random.
func (c Core) GetRandomFleet() []byte {
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
