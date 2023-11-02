package openmetrics

import (
	"testing"

	"github.com/timescale/tsbs/pkg/data/serialize"
)

func TestInfluxSerializerSerialize(t *testing.T) {
	cases := []serialize.SerializeCase{
		{
			Desc:       "a regular Point",
			InputPoint: serialize.TestPointDefault(),
			Output:     "cpu_usage_guest_nice{hostname=\"host_0\",region=\"eu-west-1\",datacenter=\"eu-west-1b\"} 38.24311829 1451606400\n",
		},
		{
			Desc:       "a regular Point using int as value",
			InputPoint: serialize.TestPointInt(),
			Output:     "cpu_usage_guest{hostname=\"host_0\",region=\"eu-west-1\",datacenter=\"eu-west-1b\"} 38 1451606400\n",
		},
		{
			Desc:       "a regular Point with multiple fields",
			InputPoint: serialize.TestPointMultiField(),
			Output: `cpu_big_usage_guest{hostname="host_0",region="eu-west-1",datacenter="eu-west-1b"} 5000000000 1451606400
cpu_usage_guest{hostname="host_0",region="eu-west-1",datacenter="eu-west-1b"} 38 1451606400
cpu_usage_guest_nice{hostname="host_0",region="eu-west-1",datacenter="eu-west-1b"} 38.24311829 1451606400
`,
		},
		{
			Desc:       "a Point with no tags",
			InputPoint: serialize.TestPointNoTags(),
			Output:     "cpu_usage_guest_nice 38.24311829 1451606400\n",
		}, {
			Desc:       "a Point with a nil tag",
			InputPoint: serialize.TestPointWithNilTag(),
			Output:     "cpu_usage_guest_nice{hostname=\"\"} 38.24311829 1451606400\n",
		}, {
			Desc:       "a Point with a nil field",
			InputPoint: serialize.TestPointWithNilField(),
			Output:     "cpu_usage_guest_nice 38.24311829 1451606400\n",
		},
	}

	serialize.SerializerTest(t, cases, &Serializer{})
}
