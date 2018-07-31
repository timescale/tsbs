package serialize

import (
	"bytes"
	"testing"
	"time"
)

var testNow = time.Unix(1451606400, 0)

var testPointDefault = &Point{
	measurementName: []byte("cpu"),
	tagKeys:         [][]byte{[]byte("hostname"), []byte("region"), []byte("datacenter")},
	tagValues:       [][]byte{[]byte("host_0"), []byte("eu-west-1"), []byte("eu-west-1b")},
	timestamp:       &testNow,
	fieldKeys:       [][]byte{[]byte("usage_guest_nice")},
	fieldValues:     []interface{}{float64(38.24311829)},
}

var testPointMultiField = &Point{
	measurementName: []byte("cpu"),
	tagKeys:         [][]byte{[]byte("hostname"), []byte("region"), []byte("datacenter")},
	tagValues:       [][]byte{[]byte("host_0"), []byte("eu-west-1"), []byte("eu-west-1b")},
	timestamp:       &testNow,
	fieldKeys:       [][]byte{[]byte("usage_guest"), []byte("usage_guest_nice")},
	fieldValues:     []interface{}{38, float64(38.24311829)},
}

var testPointInt = &Point{
	measurementName: []byte("cpu"),
	tagKeys:         [][]byte{[]byte("hostname"), []byte("region"), []byte("datacenter")},
	tagValues:       [][]byte{[]byte("host_0"), []byte("eu-west-1"), []byte("eu-west-1b")},
	timestamp:       &testNow,
	fieldKeys:       [][]byte{[]byte("usage_guest_nice")},
	fieldValues:     []interface{}{38},
}

var testPointNoTags = &Point{
	measurementName: []byte("cpu"),
	tagKeys:         [][]byte{},
	tagValues:       [][]byte{},
	timestamp:       &testNow,
	fieldKeys:       [][]byte{[]byte("usage_guest_nice")},
	fieldValues:     []interface{}{float64(38.24311829)},
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
