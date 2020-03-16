package prometheus

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/prometheus/prometheus/prompb"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"testing"
)

func TestPrometheusSerializer(t *testing.T) {
	var buffer bytes.Buffer
	ser := Serializer{}
	err := ser.Serialize(serialize.TestPointDefault(), &buffer)
	if err != nil {
		t.Errorf("error while serializing point: %v", err)
	}
	promIter, err := NewPrometheusIterator(bufio.NewReader(&buffer))
	if err != nil {
		t.Errorf("error while creating iterator: %v", err)
	}

	series := make([]*prompb.TimeSeries, 0)

	for promIter.HasNext() {
		ts, err := promIter.Next()
		if err != nil {
			t.Errorf("error getting next: %v", err)
		}
		series = append(series, ts)
	}

	assertEqual(1, len(series), t, "Wrong number of series")

	ts := series[0]
	assertEqual(serialize.TestFloat, ts.Samples[0].GetValue(), t, "bad sample value")
	assertEqual(serialize.TestPointDefault().Timestamp().UnixNano()/1000000, ts.Samples[0].GetTimestamp(), t, "wrong timestamp")

	// We add metric name as label into TimeSeries
	assertEqual(len(serialize.TestPointDefault().TagKeys())+1, len(ts.Labels), t, "wrong number of tags")

	// Lets test point with multiple fields. We generate new TimeSeries for each field
	buffer.Reset()
	series = nil
	ser = Serializer{}
	err = ser.Serialize(serialize.TestPointMultiField(), &buffer)
	if err != nil {
		t.Errorf("error while serializing: %v", err)
	}

	promIter, err = NewPrometheusIterator(bufio.NewReader(&buffer))
	if err != nil {
		t.Errorf("error creating deserializer: %v", err)
	}

	for promIter.HasNext() {
		ts, err := promIter.Next()
		if err != nil {
			t.Errorf("error getting next protobuf: %v", err)
		}
		series = append(series, ts)
	}

	assertEqual(len(serialize.TestPointMultiField().FieldKeys()), len(series), t, "wrong number of series")
	assertEqual(len(serialize.TestPointMultiField().FieldKeys()), int(promIter.processed), t, "wrong iterator state")
	if series == nil {
		t.Errorf("expected series to != nil")
	} else {
		assertEqual(float64(serialize.TestInt64), series[0].Samples[0].GetValue(), t, "wrong sample value")
	}
}

func assertEqual(expected, got interface{}, t *testing.T, msg string) {
	if expected != got {
		t.Error(fmt.Sprintf("%s. Expected: %v, Got: %v", msg, expected, got))
	}
}
