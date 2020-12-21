package serialize

import (
	"testing"
)

func TestRedisTimeSeriesSerializer(t *testing.T) {
	cases := []serializeCase{
		{
			desc:       "a regular Point",
			inputPoint: testPointDefault,
			output:     "TS.CREATE cpu_usage_guest_nice{1998426147} LABELS hostname host_0 region eu-west-1 datacenter eu-west-1b measurement cpu fieldname usage_guest_nice\nTS.MADD cpu_usage_guest_nice{1998426147} 1451606400000 38.24311829\n",
		},
		{
			desc:       "a regular Point using int as value",
			inputPoint: testPointInt,
			output:     "TS.CREATE cpu_usage_guest{1998426147} LABELS hostname host_0 region eu-west-1 datacenter eu-west-1b measurement cpu fieldname usage_guest\nTS.MADD cpu_usage_guest{1998426147} 1451606400000 38\n",
		},
		//{
		//	desc:       "a Point with no tags",
		//	inputPoint: testPointNoTags,
		//	output:     "TS.ADD cpu_usage_guest_nice{3558706393} 1451606400000 38.24311829 LABELS measurement cpu fieldname usage_guest_nice\n",
		//},
	}

	testSerializer(t, cases, &RedisTimeSeriesSerializer{})
}

func TestRedisTimeSeriesSerializerErr(t *testing.T) {
	p := testPointMultiField
	s := &RedisTimeSeriesSerializer{}
	err := s.Serialize(p, &errWriter{})
	if err == nil {
		t.Errorf("no error returned when expected")
	} else if err.Error() != errWriterAlwaysErr {
		t.Errorf("unexpected writer error: %v", err)
	}
}
