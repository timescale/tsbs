package query

import (
	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
	"reflect"
	"sort"
	"testing"
)

func TestMergeSeriesOnTimestamp(t *testing.T) {
	type args struct {
		series []redistimeseries.Range
	}
	ts := []int64{1, 2, 3, 4, 5}
	vals := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	tests := []struct {
		name string
		args args
		want MultiRange
	}{
		//Name       string
		//Labels     map[string]string
		//DataPoints []DataPoint
		{"test 1 series empty labels and datapoints",
			args{
				[]redistimeseries.Range{
					{"serie1", map[string]string{},
						[]redistimeseries.DataPoint{},
					}}},
			MultiRange{[]string{"serie1"}, []map[string]string{{}}, map[int64]MultiDataPoint{}},
		},
		{"test 2 series empty labels and datapoints",
			args{
				[]redistimeseries.Range{
					{"serie1", map[string]string{}, []redistimeseries.DataPoint{}},
					{"serie2", map[string]string{}, []redistimeseries.DataPoint{}},
				}},
			MultiRange{[]string{"serie1", "serie2"}, []map[string]string{{}, {}}, map[int64]MultiDataPoint{}},
		},
		{"test 2 series with labels and empty datapoints",
			args{
				[]redistimeseries.Range{
					{"serie1", map[string]string{"host": "1"}, []redistimeseries.DataPoint{}},
					{"serie2", map[string]string{"host": "2"}, []redistimeseries.DataPoint{}},
				}},
			MultiRange{[]string{"serie1", "serie2"}, []map[string]string{{"host": "1"}, {"host": "2"}}, map[int64]MultiDataPoint{}},
		},
		{"test 2 series with labels and datapoints",
			args{
				[]redistimeseries.Range{
					{"serie1", map[string]string{"host": "1"}, []redistimeseries.DataPoint{{ts[0], vals[0]}}},
					{"serie2", map[string]string{"host": "2"}, []redistimeseries.DataPoint{{ts[0], vals[0]}}}},
			},
			MultiRange{
				[]string{"serie1", "serie2"},
				[]map[string]string{{"host": "1"}, {"host": "2"}},
				map[int64]MultiDataPoint{ts[0]: {
					Timestamp: ts[0],
					Values:    []*float64{&vals[0], &vals[0]},
				}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MergeSeriesOnTimestamp(tt.args.series)
			if !reflect.DeepEqual(got.Names, tt.want.Names) {
				t.Errorf("MergeSeriesOnTimestamp() Error on Names got %v, want %v", got.Names, tt.want.Names)
			}
			if !reflect.DeepEqual(got.Labels, tt.want.Labels) {
				t.Errorf("MergeSeriesOnTimestamp() Error on Labels got %v, want %v", got.Labels, tt.want.Labels)
			}
			if !reflect.DeepEqual(got.DataPoints, tt.want.DataPoints) {
				t.Errorf("MergeSeriesOnTimestamp() Error on DataPoints got %v, want %v", got.DataPoints, tt.want.DataPoints)
			}
		})
	}
}

func TestReduceSeriesOnTimestampBy(t *testing.T) {
	type args struct {
		series  []redistimeseries.Range
		reducer func(series []redistimeseries.Range) (redistimeseries.Range, error)
	}
	ts := []int64{1, 2, 3, 4, 5}
	vals := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	tests := []struct {
		name         string
		args         args
		wantOutserie redistimeseries.Range
		wantErr      bool
	}{
		{"test 1 series with labels and datapoints",
			args{
				[]redistimeseries.Range{
					{"serie1", map[string]string{"host": "1"}, []redistimeseries.DataPoint{{ts[0], vals[0]}, {ts[1], vals[0]}}},
				},
				MaxReducerSeriesDatapoints,
			},
			redistimeseries.Range{"serie1", map[string]string{"host": "1"}, []redistimeseries.DataPoint{{ts[0], vals[0]}, {ts[1], vals[0]}}},
			false,
		},
		{"test 2 series with labels and datapoints",
			args{
				[]redistimeseries.Range{
					{"serie1", map[string]string{"host": "1"}, []redistimeseries.DataPoint{{ts[0], vals[1]}, {ts[1], vals[0]}}},
					{"serie2", map[string]string{"host": "2"}, []redistimeseries.DataPoint{{ts[0], vals[0]}}},
				},
				MaxReducerSeriesDatapoints,
			},
			redistimeseries.Range{"max reduction over serie1 serie2", nil, []redistimeseries.DataPoint{{ts[0], vals[1]}, {ts[1], vals[0]}}},
			false,
		},
		{"test 3 series with labels and datapoints",
			args{
				[]redistimeseries.Range{
					{"serie1", map[string]string{"host": "1"}, []redistimeseries.DataPoint{{ts[0], vals[1]}, {ts[1], vals[0]}}},
					{"serie2", map[string]string{"host": "2"}, []redistimeseries.DataPoint{{ts[0], vals[0]}}},
					{"serie3", map[string]string{"host": "3"}, []redistimeseries.DataPoint{{ts[0], vals[2]}}},
				},
				MaxReducerSeriesDatapoints,
			},
			redistimeseries.Range{"max reduction over serie1 serie2 serie3", nil, []redistimeseries.DataPoint{{ts[0], vals[2]}, {ts[1], vals[0]}}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOutserie, err := ReduceSeriesOnTimestampBy(tt.args.series, tt.args.reducer)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReduceSeriesOnTimestampBy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotOutserie.Name, tt.wantOutserie.Name) {
				t.Errorf("MergeSeriesOnTimestamp() Error on Names got %v, want %v", gotOutserie.Name, tt.wantOutserie.Name)
			}
			if !reflect.DeepEqual(gotOutserie.Labels, tt.wantOutserie.Labels) {
				t.Errorf("MergeSeriesOnTimestamp() Error on Labels got %v, want %v", gotOutserie.Labels, tt.wantOutserie.Labels)
			}
			if !reflect.DeepEqual(gotOutserie.DataPoints, tt.wantOutserie.DataPoints) {
				t.Errorf("MergeSeriesOnTimestamp() Error on DataPoints got %v, want %v", gotOutserie.DataPoints, tt.wantOutserie.DataPoints)
			}
		})
	}
}

func TestGetUniqueLabelValue(t *testing.T) {
	type args struct {
		series []redistimeseries.Range
		label  string
	}
	ts := []int64{1, 2, 3, 4, 5}
	vals := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	tests := []struct {
		name       string
		args       args
		wantResult []string
		wantErr    bool
	}{
		{"test empty label series with distinct labels and datapoints",
			args{
				[]redistimeseries.Range{
					{"serie1", nil, []redistimeseries.DataPoint{{ts[0], vals[1]}, {ts[1], vals[0]}}},
					{"serie2", nil, []redistimeseries.DataPoint{{ts[0], vals[0]}}},
					{"serie3", nil, []redistimeseries.DataPoint{{ts[0], vals[2]}}},
				},
				"host",
			},
			[]string{},
			false,
		},
		{"test 3 series with equal labels and datapoints",
			args{
				[]redistimeseries.Range{
					{"serie1", map[string]string{"host": "1"}, []redistimeseries.DataPoint{{ts[0], vals[1]}, {ts[1], vals[0]}}},
					{"serie2", map[string]string{"host": "1"}, []redistimeseries.DataPoint{{ts[0], vals[0]}}},
					{"serie3", map[string]string{"host": "1"}, []redistimeseries.DataPoint{{ts[0], vals[2]}}},
				},
				"host",
			},
			[]string{"1"},
			false,
		},
		{"test 3 series with distinct labels and datapoints",
			args{
				[]redistimeseries.Range{
					{"serie1", map[string]string{"host": "1"}, []redistimeseries.DataPoint{{ts[0], vals[1]}, {ts[1], vals[0]}}},
					{"serie2", map[string]string{"host": "2"}, []redistimeseries.DataPoint{{ts[0], vals[0]}}},
					{"serie3", map[string]string{"host": "3"}, []redistimeseries.DataPoint{{ts[0], vals[2]}}},
				},
				"host",
			},
			[]string{"1", "2", "3"},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotResult, err := GetUniqueLabelValue(tt.args.series, tt.args.label)
			sort.Strings(gotResult)
			sort.Strings(tt.wantResult)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetUniqueLabelValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotResult, tt.wantResult) {
				t.Errorf("GetUniqueLabelValue() gotResult = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}
