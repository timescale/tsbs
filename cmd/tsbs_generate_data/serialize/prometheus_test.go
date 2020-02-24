package serialize

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/prometheus/prometheus/prompb"
	"testing"
)

func TestPrometheusSerializer(t *testing.T) {
	var buffer bytes.Buffer
	ser, err := NewPrometheusSerializer(&buffer)
	if err != nil {
		t.Errorf("failed to create prometheus serializer: %v", err)
	}
	err = ser.Serialize(testPointDefault, &buffer)
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
	assertEqual(testFloat, ts.Samples[0].GetValue(), t, "bad sample value")
	assertEqual(testPointDefault.Timestamp().UnixNano()/1000000, ts.Samples[0].GetTimestamp(), t, "wrong timestamp")

	// We add metric name as label into TimeSeries
	assertEqual(len(testPointDefault.TagKeys())+1, len(ts.Labels), t, "wrong number of tags")

	// Lets test point with multiple fields. We generate new TimeSeries for each field
	buffer.Reset()
	series = nil
	ser, err = NewPrometheusSerializer(&buffer)
	err = ser.Serialize(testPointMultiField, &buffer)
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

	assertEqual(len(testPointMultiField.FieldKeys()), len(series), t, "wrong number of series")
	assertEqual(len(testPointMultiField.FieldKeys()), int(promIter.processed), t, "wrong iterator state")
	assertEqual(float64(testInt64), series[0].Samples[0].GetValue(), t, "wrong sample value")
}

func assertEqual(expected, got interface{}, t *testing.T, msg string) {
	if expected != got {
		t.Error(fmt.Sprintf("%s. Expected: %v, Got: %v", msg, expected, got))
	}
}
