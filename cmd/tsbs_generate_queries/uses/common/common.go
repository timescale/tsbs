package common

import (
	"fmt"
	"reflect"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	internalutils "github.com/timescale/tsbs/internal/utils"
)

// Core is the common component of all generators for all systems
type Core struct {
	// Interval is the entire time range of the dataset
	Interval *internalutils.TimeInterval

	// Scale is the cardinality of the dataset in terms of devices/hosts
	Scale int
}

// NewCore returns a new Core for the given time range and cardinality
func NewCore(start, end time.Time, scale int) (*Core, error) {
	ti, err := internalutils.NewTimeInterval(start, end)
	if err != nil {
		return nil, err
	}

	return &Core{Interval: ti, Scale: scale}, nil
}

// PanicUnimplementedQuery generates a panic for the provided query generator.
func PanicUnimplementedQuery(dg utils.QueryGenerator) {
	panic(fmt.Sprintf("database (%v) does not implement query", reflect.TypeOf(dg)))
}
