package serialize

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

var (
	testNow         = time.Unix(1451606400, 0)
	testMeasurement = []byte("cpu")
	testTagKeys     = [][]byte{[]byte("hostname"), []byte("region"), []byte("datacenter")}
	testTagVals     = [][]byte{[]byte("host_0"), []byte("eu-west-1"), []byte("eu-west-1b")}
	testColFloat    = []byte("usage_guest_nice")
	testColInt      = []byte("usage_guest")
	testColInt64    = []byte("big_usage_guest")
)

const (
	testFloat             = float64(38.24311829)
	testInt               = 38
	testInt64             = int64(5000000000)
	errWriterAlwaysErr    = "bad write: I always error"
	errWriterSometimesErr = "bad write: I sometimes error"
)

type errWriter struct {
	skipOne bool
	cnt     int
}

func (w *errWriter) Write(p []byte) (n int, err error) {
	if !w.skipOne {
		return 0, fmt.Errorf(errWriterAlwaysErr)
	} else if w.cnt < 1 {
		w.cnt++
		return len(p), nil
	} else {
		return 0, fmt.Errorf(errWriterSometimesErr)
	}
}

var testPointDefault = &Point{
	measurementName: testMeasurement,
	tagKeys:         testTagKeys,
	tagValues:       testTagVals,
	timestamp:       &testNow,
	fieldKeys:       [][]byte{testColFloat},
	fieldValues:     []interface{}{testFloat},
}

var testPointMultiField = &Point{
	measurementName: testMeasurement,
	tagKeys:         testTagKeys,
	tagValues:       testTagVals,
	timestamp:       &testNow,
	fieldKeys:       [][]byte{testColInt64, testColInt, testColFloat},
	fieldValues:     []interface{}{testInt64, testInt, testFloat},
}

var testPointInt = &Point{
	measurementName: testMeasurement,
	tagKeys:         testTagKeys,
	tagValues:       testTagVals,
	timestamp:       &testNow,
	fieldKeys:       [][]byte{testColInt},
	fieldValues:     []interface{}{testInt},
}

var testPointNoTags = &Point{
	measurementName: testMeasurement,
	tagKeys:         [][]byte{},
	tagValues:       [][]byte{},
	timestamp:       &testNow,
	fieldKeys:       [][]byte{testColFloat},
	fieldValues:     []interface{}{testFloat},
}

type serializeCase struct {
	desc       string
	inputPoint *Point
	output     string
}

func testSerializer(t *testing.T, cases []serializeCase, ps PointSerializer) {
	for _, c := range cases {
		b := new(bytes.Buffer)
		ps.Serialize(c.inputPoint, b)
		got := b.String()
		if got != c.output {
			t.Errorf("%s \nOutput incorrect: \nWant: '%s' \nGot:  '%s'", c.desc, c.output, got)
		}
	}
}

func testEmptyPoint(t *testing.T, p *Point, desc string) {
	if p.measurementName != nil {
		t.Errorf("%s has a non-nil measurement name: %s", desc, p.measurementName)
	}
	if got := len(p.tagKeys); got != 0 {
		t.Errorf("%s has a non-0 len for tag keys: %d", desc, got)
	}
	if got := len(p.tagValues); got != 0 {
		t.Errorf("%s has a non-0 len for tag values: %d", desc, got)
	}
	if got := len(p.fieldKeys); got != 0 {
		t.Errorf("%s has a non-0 len for field keys: %d", desc, got)
	}
	if got := len(p.fieldValues); got != 0 {
		t.Errorf("%s has a non-0 len for field values: %d", desc, got)
	}
	if p.timestamp != nil {
		t.Errorf("%s has a non-nil timestamp: %v", desc, p.timestamp)
	}
}

func TestNewPoint(t *testing.T) {
	p := NewPoint()
	testEmptyPoint(t, p, "NewPoint")
}

func TestReset(t *testing.T) {
	p := NewPoint()
	now := time.Now()
	p.timestamp = &now
	p.measurementName = []byte("test")
	p.Reset()
	testEmptyPoint(t, p, "Reset")
}

func TestSetTimestamp(t *testing.T) {
	p := NewPoint()
	now := time.Now()
	p.SetTimestamp(&now)
	if p.timestamp != &now {
		t.Errorf("incorrect timestamp: got %v want %v", p.timestamp, now)
	}
}

func TestSetMeasurementName(t *testing.T) {
	p := NewPoint()
	name := []byte("foo")
	p.SetMeasurementName(name)
	if got := string(p.MeasurementName()); got != string(name) {
		t.Errorf("incorrect name: got %s want %s", got, name)
	}
}

func TestFields(t *testing.T) {
	p := NewPoint()
	if got := len(p.FieldKeys()); got != 0 {
		t.Errorf("empty point has field keys of non-0 len: %d", got)
	}
	if got := len(p.fieldValues); got != 0 {
		t.Errorf("empty point has field values of non-0 len: %d", got)
	}

	k := []byte("foo")
	v := []byte("foo_value")
	p.AppendField(k, v)
	if got := len(p.FieldKeys()); got != 1 {
		t.Errorf("incorrect len: got %d want %d", got, 1)
	}
	if got := string(p.fieldKeys[0]); got != string(k) {
		t.Errorf("incorrect first field key: got %s want %s", got, k)
	}
	if got := string(p.fieldValues[0].([]byte)); got != string(v) {
		t.Errorf("incorrect first field val: got %s want %s", got, v)
	}

	if got := string(p.GetFieldValue([]byte(k)).([]byte)); got != string(v) {
		t.Errorf("incorrect value returned for key: got %s want %s", got, v)
	}
	if got := p.GetFieldValue([]byte("bar")); got != nil {
		t.Errorf("unexpected non-nil return for get field value: %v", got)
	}
}

func TestFieldsPanic(t *testing.T) {
	testPanic := func(p *Point) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("did not panic when should")
			}
		}()
		_ = p.GetFieldValue([]byte{})
	}
	p := NewPoint()
	p.AppendField([]byte("foo"), []byte("bar"))
	p.fieldKeys = p.fieldKeys[:0]
	testPanic(p)
}

func TestTags(t *testing.T) {
	p := NewPoint()
	if got := len(p.tagKeys); got != 0 {
		t.Errorf("empty point has tag keys of non-0 len: %d", got)
	}
	if got := len(p.tagValues); got != 0 {
		t.Errorf("empty point has tag values of non-0 len: %d", got)
	}

	k := []byte("foo")
	v := []byte("foo_value")
	p.AppendTag(k, v)
	if got := len(p.tagKeys); got != 1 {
		t.Errorf("incorrect len: got %d want %d", got, 1)
	}
	if got := len(p.tagValues); got != 1 {
		t.Errorf("incorrect tag val len: got %d want %d", got, 1)
	}
	if got := string(p.tagKeys[0]); got != string(k) {
		t.Errorf("incorrect first field key: got %s want %s", got, k)
	}
	if got := string(p.tagValues[0]); got != string(v) {
		t.Errorf("incorrect first field val: got %s want %s", got, v)
	}

	if got := string(p.GetTagValue([]byte(k))); got != string(v) {
		t.Errorf("incorrect value returned for key: got %s want %s", got, v)
	}
	if got := p.GetTagValue([]byte("bar")); got != nil {
		t.Errorf("unexpected non-nil return for get field value: %v", got)
	}
}

func TestTagsPanic(t *testing.T) {
	testPanic := func(p *Point) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("did not panic when should")
			}
		}()
		_ = p.GetTagValue([]byte{})
	}
	p := NewPoint()
	p.AppendTag([]byte("foo"), []byte("bar"))
	p.tagKeys = p.tagKeys[:0]
	testPanic(p)
}
