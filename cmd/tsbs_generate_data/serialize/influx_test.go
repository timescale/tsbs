package serialize

import (
	"testing"
)

func TestInfluxSerializerSerialize(t *testing.T) {
	cases := []serializeCase{
		{
			desc:       "a regular Point",
			inputPoint: testPointDefault,
			output:     "cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b usage_guest_nice=38.24311829 1451606400000000000\n",
		},
		{
			desc:       "a regular Point using int as value",
			inputPoint: testPointInt,
			output:     "cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b usage_guest=38i 1451606400000000000\n",
		},
		{
			desc:       "a regular Point with multiple fields",
			inputPoint: testPointMultiField,
			output:     "cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b big_usage_guest=5000000000i,usage_guest=38i,usage_guest_nice=38.24311829 1451606400000000000\n",
		},
		{
			desc:       "a Point with no tags",
			inputPoint: testPointNoTags,
			output:     "cpu usage_guest_nice=38.24311829 1451606400000000000\n",
		}, {
			desc:       "a Point with a nil tag",
			inputPoint: testPointWithNilTag,
			output:     "cpu usage_guest_nice=38.24311829 1451606400000000000\n",
		}, {
			desc:       "a Point with a nil field",
			inputPoint: testPointWithNilField,
			output:     "cpu usage_guest_nice=38.24311829 1451606400000000000\n",
		},
	}

	testSerializer(t, cases, &InfluxSerializer{})
}
