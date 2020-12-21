package query

import (
	"fmt"
	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
	"reflect"
	"runtime"
	"sort"
	"strings"
)

type ResponseFunctor func(interface{}) (interface{}, error)
type void struct{}

var member void

type MultiDataPoint struct {
	Timestamp        int64
	HumanReadbleTime *string
	Values           []*float64
}

type MultiRange struct {
	Names      []string
	Labels     []map[string]string
	DataPoints map[int64]MultiDataPoint
}

func GetFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func SingleGroupByTime(res interface{}) (result interface{}, err error) {
	parsedRes, err := redistimeseries.ParseRanges(res)
	if err != nil {
		return
	}
	result = MergeSeriesOnTimestamp(parsedRes)
	return
}

func GroupByTimeAndMax(res interface{}) (result interface{}, err error) {
	parsedRes, err := redistimeseries.ParseRanges(res)
	if err != nil {
		return
	}
	result, err = ReduceSeriesOnTimestampBy(parsedRes, MaxReducerSeriesDatapoints)
	return
}

func GroupByTimeAndTagMax(res interface{}) (result interface{}, err error) {
	parsedRes, err := redistimeseries.ParseRanges(res)
	if err != nil {
		return
	}
	labels, err := GetUniqueLabelValue(parsedRes, "fieldname")
	if err != nil {
		return
	}
	var outseries = make([]redistimeseries.Range, 0, 0)
	for _, label := range labels {
		filteredSeries, err := FilterRangesByLabelValue(parsedRes, "fieldname", label, true)
		if err != nil {
			return result, err
		}
		reducedSerie, err := ReduceSeriesOnTimestampBy(filteredSeries, MaxReducerSeriesDatapoints)
		if err != nil {
			return result, err
		}
		outseries = append(outseries, reducedSerie)
	}
	result = MergeSeriesOnTimestamp(outseries)
	return
}

func GroupByTimeAndTagHostname(res interface{}) (result interface{}, err error) {
	parsedRes, err := redistimeseries.ParseRanges(res)
	if err != nil {
		return
	}
	labels, err := GetUniqueLabelValue(parsedRes, "hostname")
	if err != nil {
		return
	}
	var outseries = make([]MultiRange, 0, 0)
	for _, label := range labels {
		filteredSeries, err := FilterRangesByLabelValue(parsedRes, "hostname", label, true)
		if err != nil {
			return result, err
		}
		merged := MergeSeriesOnTimestamp(filteredSeries)
		outseries = append(outseries, merged)
	}
	result = outseries
	return
}

func HighCpu(res interface{}) (result interface{}, err error) {
	parsedRes, err := redistimeseries.ParseRanges(res)
	if err != nil {
		return
	}
	labels, err := GetUniqueLabelValue(parsedRes, "hostname")
	if err != nil {
		return
	}
	var outseries = make([]MultiRange, 0, 0)
	for _, label := range labels {
		filteredSeries, err := FilterRangesByLabelValue(parsedRes, "hostname", label, true)
		if err != nil {
			return result, err
		}
		merged := MergeSeriesOnTimestamp(filteredSeries)
		above, err := FilterRangesByThresholdAbove(merged, "fieldname", "usage_user", 90)
		if err != nil {
			return result, err
		}
		if len(above.DataPoints) > 0 {
			outseries = append(outseries, above)
		}
	}
	result = outseries
	return
}

func FilterRangesByLabelValue(series []redistimeseries.Range, labelname, labelvalue string, keepMatches bool) (result []redistimeseries.Range, err error) {
	result = make([]redistimeseries.Range, 0, 1)
	for _, serie := range series {
		flagged := false
		value, labelExists := serie.Labels[labelname]
		if labelExists == true && value == labelvalue {
			flagged = true
		}
		if flagged == true && keepMatches == true {
			result = append(result, serie)
		}
		if flagged == false && keepMatches == false {
			result = append(result, serie)

		}
	}
	return
}

func FilterRangesByThresholdAbove(serie MultiRange, labelname, labelvalue string, above float64) (result MultiRange, err error) {
	datapoints := make(map[int64]MultiDataPoint)
	thresholdIdx := -1
	for idx, labels := range serie.Labels {
		v, found := labels[labelname]
		if found {
			if labelvalue == v {
				thresholdIdx = idx
			}
		}
	}

	for ts, datapoint := range serie.DataPoints {
		vp := datapoint.Values[thresholdIdx]
		if vp != nil && *vp > above {
			datapoints[ts] = datapoint
		}
	}
	result = MultiRange{serie.Names, serie.Labels, datapoints}
	return
}

func GetUniqueLabelValue(series []redistimeseries.Range, label string) (result []string, err error) {
	set := make(map[string]void) // New empty set
	result = make([]string, 0, 0)
	for _, serie := range series {
		value, found := serie.Labels[label]
		if found == true {
			set[value] = member
		}
	}
	for k := range set {
		result = append(result, k)
	}
	return
}

func MergeSeriesOnTimestamp(series []redistimeseries.Range) MultiRange {
	names := make([]string, len(series), len(series))
	labels := make([]map[string]string, len(series), len(series))
	datapoints := make(map[int64]MultiDataPoint)
	for idx, serie := range series {
		names[idx] = serie.Name
		labels[idx] = serie.Labels
		for _, datapoint := range serie.DataPoints {
			_, found := datapoints[datapoint.Timestamp]
			if found == true {
				var v = datapoint.Value
				datapoints[datapoint.Timestamp].Values[idx] = &v
			} else {
				multipointValues := make([]*float64, len(series), len(series))
				for ii := range multipointValues {
					multipointValues[ii] = nil
				}
				var v = datapoint.Value
				multipointValues[idx] = &v
				datapoints[datapoint.Timestamp] = MultiDataPoint{datapoint.Timestamp, nil, multipointValues}
			}
		}
	}
	return MultiRange{names, labels, datapoints}
}

func AvgReducerSeriesDatapoints(series []redistimeseries.Range) (c redistimeseries.Range, err error) {
	allNames := make([]string, 0, len(series))
	for _, serie := range series {
		allNames = append(allNames, serie.Name)
	}
	var vPoints = make(map[int64]float64)
	var fPoints = make(map[int64]float64)
	var cPoints = make(map[int64]int64)
	pos := 0
	for pos < len(series) {
		serie := series[pos]
		for _, v := range serie.DataPoints {
			_, found := cPoints[v.Timestamp]
			if found == true {
				cPoints[v.Timestamp] = cPoints[v.Timestamp] + 1
				vPoints[v.Timestamp] = vPoints[v.Timestamp] + v.Value
				fPoints[v.Timestamp] = vPoints[v.Timestamp] / float64(cPoints[v.Timestamp])
			} else {
				cPoints[v.Timestamp] = 1
				vPoints[v.Timestamp] = v.Value
				fPoints[v.Timestamp] = v.Value
			}
		}
		pos = pos + 1
	}
	var keys []int
	for k := range cPoints {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	datapoints := make([]redistimeseries.DataPoint, 0, len(keys))
	for _, k := range keys {
		dp := fPoints[int64(k)]
		datapoints = append(datapoints, redistimeseries.DataPoint{int64(k), dp})
	}
	name := fmt.Sprintf("avg reduction over %s", strings.Join(allNames, " "))
	c = redistimeseries.Range{name, nil, datapoints}
	return
}

func MaxReducerSeriesDatapoints(series []redistimeseries.Range) (c redistimeseries.Range, err error) {
	allNames := make([]string, 0, len(series))
	for _, serie := range series {
		allNames = append(allNames, serie.Name)
	}
	var cPoints = make(map[int64]float64)
	pos := 0
	for pos < len(series) {
		serie := series[pos]
		for _, v := range serie.DataPoints {
			_, found := cPoints[v.Timestamp]
			if found == true {
				if cPoints[v.Timestamp] < v.Value {
					cPoints[v.Timestamp] = v.Value
				}
			} else {
				cPoints[v.Timestamp] = v.Value
			}
		}
		pos = pos + 1
	}
	var keys []int
	for k := range cPoints {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	datapoints := make([]redistimeseries.DataPoint, 0, len(keys))
	for _, k := range keys {
		dp := cPoints[int64(k)]
		datapoints = append(datapoints, redistimeseries.DataPoint{int64(k), dp})
	}
	name := fmt.Sprintf("max reduction over %s", strings.Join(allNames, " "))
	c = redistimeseries.Range{name, nil, datapoints}
	return
}

func ReduceSeriesOnTimestampBy(series []redistimeseries.Range, reducer func(series []redistimeseries.Range) (redistimeseries.Range, error)) (outserie redistimeseries.Range, err error) {
	allNames := make([]string, 0, len(series))
	for _, serie := range series {
		allNames = append(allNames, serie.Name)
	}
	if len(series) == 0 {
		return
	}
	if len(series) == 1 {
		outserie = series[0]
		return
	}
	return reducer(series)
}
