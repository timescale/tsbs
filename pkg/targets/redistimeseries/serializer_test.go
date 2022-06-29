package redistimeseries

import (
	"testing"

	"github.com/timescale/tsbs/pkg/data/serialize"
)

func TestRedisTimeSeriesSerializerSerialize(t *testing.T) {
	cases := []serialize.SerializeCase{
		{
			Desc:       "a regular Point",
			InputPoint: serialize.TestPointDefault(),
			Output:     "7116 TS.CREATE {host_0}_usage_guest_nice LABELS hostname host_0 region eu-west-1 datacenter eu-west-1b measurement cpu fieldname usage_guest_nice\n7116 TS.MADD {host_0}_usage_guest_nice 1451606400000 38.24311829\n",
		},
	}

	serialize.SerializerTest(t, cases, &Serializer{})
}
