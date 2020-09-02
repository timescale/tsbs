package cassandra

import (
	"fmt"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/query"
)

// IoT produces Influx-specific queries for all the iot query types.
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

// LastLocByTruck finds the truck location for nTrucks.
func (i *IoT) LastLocByTruck(qi query.Query, nTrucks int) {
	tagSet, _ := i.GetRandomTrucks(nTrucks)

	tagSets := [][]string{}
	if len(tagSet) > 0 {
		tagSets = append(tagSets, tagSet)
	}

	humanLabel := "Cassandra last location by specific truck"
	humanDesc := fmt.Sprintf("%s: random %4d trucks", humanLabel, nTrucks)
	i.fillInQuery(qi, humanLabel, humanDesc, "", []string{"name", "driver", "latitude", "longitude"}, i.Interval, tagSets)
	q := qi.(*query.Cassandra)
	q.ForEveryN = []byte("driver,1")
}

// LastLocPerTruck finds all the truck locations along with truck and driver names.
func (i *IoT) LastLocPerTruck(qi query.Query) {
	humanLabel := "Cassandra last location per truck"
	humanDesc := fmt.Sprintf("%s: %s\n", humanLabel, i.Interval.StartString())
	i.fillInQuery(qi, humanLabel, humanDesc, "", []string{"name", "driver"}, i.Interval, nil)
	q := qi.(*query.Cassandra)
	q.ForEveryN = []byte("driver,1")
	q.OrderBy = []byte("time DESC")
	q.WhereClause = []byte(fmt.Sprintf("fleet,=,'%s'",i.GetRandomFleet()))
	q.MeasurementName = []byte("diagnostics")
}

// TrucksWithHighLoad finds all trucks that have load over 90%.
func (i *IoT) TrucksWithHighLoad(qi query.Query) {
	humanLabel := "Cassandra trucks with high load"
	humanDesc := fmt.Sprintf("%s: over 90 percent", humanLabel)
	i.fillInQuery(qi, humanLabel, humanDesc, "", []string{"name", "current_load", "load_capacity"}, i.Interval, nil)
	q := qi.(*query.Cassandra)
	q.ForEveryN = []byte("name,1")
	q.WhereClause = []byte(fmt.Sprintf("current_load,>=,0.9,*,load_capacity,AND,fleet,=,'%s'", i.GetRandomFleet()))
	q.MeasurementName = []byte("diagnostics")
}

// TrucksWithLowFuel finds all trucks with low fuel (less than 10%).
func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	humanLabel := "Cassandra trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)
	i.fillInQuery(qi, humanLabel, humanDesc, "", []string{"name", "driver", "fuel_state"}, i.Interval, nil)
	q := qi.(*query.Cassandra)
	q.ForEveryN = []byte("name,1")
	q.OrderBy = []byte("time DESC")
	q.WhereClause = []byte(fmt.Sprintf("fleet,=,'%s',AND,fuel_state,<=,0.1", i.GetRandomFleet()))
	q.MeasurementName = []byte("diagnostics")
}

// StationaryTrucks finds all trucks that have low average velocity in last 10 minutes
func (i *IoT) StationaryTrucks(qi query.Query) {
	humanLabel := "Cassandra stationary trucks"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, "avg", []string{"velocity"}, i.Interval, nil)
	q := qi.(*query.Cassandra)
	q.WhereClause = []byte(fmt.Sprintf("fleet,=,'%s',AND,velocity,<,1", i.GetRandomFleet()))
	q.MeasurementName = []byte("readings")
	q.GroupByDuration = iot.StationaryDuration
}

// AvgVsProjectedFuelConsumption calculates average and projected fuel consumption per fleet.
func (i *IoT) AvgVsProjectedFuelConsumption(qi query.Query) {
	humanLabel := "Cassandra average vs projected fuel consumption per fleet"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)
	i.fillInQuery(qi, humanLabel, humanDesc, "avg", []string{"fuel_consumption", "nominal_fuel_consumption"}, i.Interval, nil)
	q := qi.(*query.Cassandra)
	q.ForEveryN = []byte("fleet,1")
	q.WhereClause = []byte(fmt.Sprintf("velocity,>,1"))
	q.GroupByDuration = iot.DailyDrivingDuration
	q.MeasurementName = []byte("readings")
}

// AvgLoad finds the average load per truck model per fleet.
func (i *IoT) AvgLoad(qi query.Query) {
	humanLabel := "Cassandra average load per truck model per fleet"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, "avg", []string{"current_load"}, i.Interval, nil)
	q := qi.(*query.Cassandra)
	q.ForEveryN = []byte("model,1")
	q.MeasurementName = []byte("diagnostics")
	q.WhereClause = []byte(fmt.Sprintf("fleet,=,'%s'", i.GetRandomFleet()))
}

// DailyTruckActivity returns the number of hours trucks has been active (not out-of-commission) per day per fleet per model.
func (i *IoT) DailyTruckActivity(qi query.Query) {
	humanLabel := "Cassandra daily truck activity per fleet per model"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, "count", []string{"model,fleet"}, i.Interval, nil)
	q := qi.(*query.Cassandra)
	q.MeasurementName = []byte("diagnostics")
	q.GroupByDuration = iot.StationaryDuration
	q.ForEveryN = []byte("model,1")
	q.WhereClause = []byte(fmt.Sprintf("fleet,=,'%s',AND,avg(status),<,1", i.GetRandomFleet()))
}

