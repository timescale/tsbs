package redistimeseries

import (
	"testing"

	"github.com/timescale/tsbs/pkg/data/serialize"
)

func TestInfluxSerializerSerialize(t *testing.T) {
	cases := []serialize.SerializeCase{
		{
			Desc:       "a regular Point",
			InputPoint: serialize.TestPointDefault(),
			Output:     "7116 TS.CREATE cpu_{host_0}_usage_guest_nice LABELS hostname host_0 region eu-west-1 datacenter eu-west-1b measurement cpu fieldname usage_guest_nice\n7116 TS.MADD cpu_{host_0}_usage_guest_nice 1451606400000 38.24311829\n",
		},
		//{
		//	Desc:       "a regular Point using int as value",
		//	InputPoint: serialize.TestPointInt(),
		//	Output:     "cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b usage_guest=38i 1451606400000000000\n",
		//},
		//{
		//	Desc:       "a regular Point with multiple fields",
		//	InputPoint: serialize.TestPointMultiField(),
		//	Output:     "cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b big_usage_guest=5000000000i,usage_guest=38i,usage_guest_nice=38.24311829 1451606400000000000\n",
		//},
		//{
		//	Desc:       "a Point with no tags",
		//	InputPoint: serialize.TestPointNoTags(),
		//	Output:     "cpu usage_guest_nice=38.24311829 1451606400000000000\n",
		//}, {
		//	Desc:       "a Point with a nil tag",
		//	InputPoint: serialize.TestPointWithNilTag(),
		//	Output:     "cpu usage_guest_nice=38.24311829 1451606400000000000\n",
		//}, {
		//	Desc:       "a Point with a nil field",
		//	InputPoint: serialize.TestPointWithNilField(),
		//	Output:     "cpu usage_guest_nice=38.24311829 1451606400000000000\n",
		//},
	}

	serialize.SerializerTest(t, cases, &Serializer{})
}
