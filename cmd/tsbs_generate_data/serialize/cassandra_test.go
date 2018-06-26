package serialize

import (
	"bytes"
	"testing"
	"time"
)

func TestCassandraSerializerSerialize(t *testing.T) {
	now := time.Unix(1451606400, 0)
	cases := []struct {
		desc       string
		inputPoint *Point
		output     string
	}{
		{
			desc: "A Point object should result in CSV output written by the IO Writer",
			inputPoint: &Point{
				measurementName: []byte("cpu"),
				tagKeys:         [][]byte{[]byte("hostname"), []byte("region"), []byte("datacenter")},
				tagValues:       [][]byte{[]byte("host_0"), []byte("eu-west-1"), []byte("eu-west-1b")},
				timestamp:       &now,
				fieldKeys:       [][]byte{[]byte("usage_guest_nice")},
				fieldValues:     []interface{}{float64(38.24311829)},
			},
			output: "series_double,cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,usage_guest_nice,2016-01-01,1451606400000000000,38.24311829\n",
		},
	}

	for _, c := range cases {
		b := new(bytes.Buffer)
		serializer := &CassandraSerializer{}
		serializer.Serialize(c.inputPoint, b)
		got := b.String()
		if got != c.output {
			t.Errorf("%s \nOutput incorrect: \nWant: %s \nGot: %s", c.desc, c.output, got)
		}
	}
}
