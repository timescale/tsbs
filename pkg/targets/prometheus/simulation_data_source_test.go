package prometheus

import (
	"github.com/prometheus/prometheus/prompb"
	"github.com/timescale/tsbs/pkg/data"
	"testing"
	"time"
)

func TestTimeSeriesIterator(t *testing.T) {
	now := time.Now()
	emptyPoint := data.NewPoint()
	emptyPoint.SetTimestamp(&now)

	oneField := data.NewPoint()
	oneField.AppendTag([]byte("tag"), "tag")
	oneField.AppendField([]byte("m1"), float64(1))
	oneField.SetTimestamp(&now)

	twoFields := data.NewPoint()
	twoFields.AppendTag([]byte("tag"), "tag")
	twoFields.AppendField([]byte("m1"), float64(1))
	twoFields.AppendField([]byte("m2"), float64(2))
	twoFields.SetTimestamp(&now)

	cases := []struct {
		desc   string
		set    *data.Point
		expect []*prompb.TimeSeries
	}{
		{
			desc:   "No fields -> no time series",
			set:    emptyPoint,
			expect: []*prompb.TimeSeries{},
		}, {
			desc: "One field one time series",
			set:  oneField,
		}, {
			desc: "Two fields two time series",
			set:  twoFields,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			iter := timeSeriesIterator{}
			if iter.generatedSeries != nil || iter.currentInd != 0 {
				t.Errorf("unexpected initial iterator state; expected series: nil, currentInd: 0;"+
					" got: %v and %d", iter.generatedSeries, iter.currentInd)
				return
			}
			inputPoint := tc.set
			iter.Set(inputPoint)
			var generated []*prompb.TimeSeries
			for iter.HasNext() {
				generated = append(generated, iter.Next())
			}
			fieldKeys := inputPoint.FieldKeys()
			numExpectedTimeSeries := len(fieldKeys)
			if len(generated) != numExpectedTimeSeries {
				t.Errorf("expected time series for each field (total %d) got: %d",
					numExpectedTimeSeries, len(generated),
				)
			}

			for i, ts := range generated {
				if ts.Samples[0].Value != inputPoint.GetFieldValue(fieldKeys[i]) {
					t.Errorf("unexpected value in time-series; exp: %f; got: %f",
						inputPoint.GetFieldValue(fieldKeys[i]), ts.Samples[0].Value,
					)
				}
				for _, label := range ts.Labels {
					if label.Name == "__name__" {
						if label.Value != string(fieldKeys[i]) {
							t.Errorf("unexpected name for time-series: exp: %s; got %s",
								string(fieldKeys[i]), label.Value,
							)
						}
						continue
					}
					tagValue := inputPoint.GetTagValue([]byte(label.Name))
					if label.Value != tagValue.(string) {
						t.Errorf("unexpected tag in time-series; exp %s; got: %s",
							label.Value, tagValue,
						)
					}
				}
			}
			// check if properly reset after some generating has been done
			iter.Set(inputPoint)
			if len(iter.generatedSeries) != numExpectedTimeSeries {
				t.Errorf("after Set, iterator had unexpected generatedSeries len."+
					"Expected %d; got %d", len(iter.generatedSeries), numExpectedTimeSeries)
			}
			if iter.currentInd != 0 {
				t.Errorf("after Set, iterator currentInd not reset to 0; got: %d", iter.currentInd)
			}
		})
	}
}