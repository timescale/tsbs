package query

import (
	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
	"reflect"
	"runtime"
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
