package serialize

import (
	"testing"
)

func TestCrateDBSerializerSerialize(t *testing.T) {
	cases := []serializeCase{
		{
			desc:       "a regular Point",
			inputPoint: testPointDefault,
			output:     "cpu\t{\"hostname\":\"host_0\",\"region\":\"eu-west-1\",\"datacenter\":\"eu-west-1b\"}\t1451606400000000000\t38.24311829\n",
		},
		{
			desc:       "a regular Point using int as value",
			inputPoint: testPointInt,
			output:     "cpu\t{\"hostname\":\"host_0\",\"region\":\"eu-west-1\",\"datacenter\":\"eu-west-1b\"}\t1451606400000000000\t38\n",
		},
		{
			desc:       "a regular Point with multiple fields",
			inputPoint: testPointMultiField,
			output:     "cpu\t{\"hostname\":\"host_0\",\"region\":\"eu-west-1\",\"datacenter\":\"eu-west-1b\"}\t1451606400000000000\t5000000000\t38\t38.24311829\n",

		},
		{
			desc:       "a Point with no tags",
			inputPoint: testPointNoTags,
			output:     "cpu\tnull\t1451606400000000000\t38.24311829\n",
		},
	}

	testSerializer(t, cases, &CrateDBSerializer{})
}

func TestCrateDBSerializerSerializeErr(t *testing.T) {
	p := testPointMultiField
	s := &CrateDBSerializer{}
	err := s.Serialize(p, &errWriter{})
	if err == nil {
		t.Errorf("no error returned when expected")
	} else if err.Error() != errWriterAlwaysErr {
		t.Errorf("unexpected writer error: %v", err)
	}
}
