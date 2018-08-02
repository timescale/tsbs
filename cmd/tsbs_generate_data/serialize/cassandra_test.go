package serialize

import (
	"testing"
)

func TestCassandraSerializerSerialize(t *testing.T) {
	cases := []serializeCase{
		{
			desc:       "a regular Point",
			inputPoint: testPointDefault,
			output:     "series_double,cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,usage_guest_nice,2016-01-01,1451606400000000000,38.24311829\n",
		},
		{
			desc:       "a regular Point using int as value",
			inputPoint: testPointInt,
			output:     "series_bigint,cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,usage_guest,2016-01-01,1451606400000000000,38\n",
		},
		{
			desc:       "a Point with no tags",
			inputPoint: testPointNoTags,
			output:     "series_double,cpu,usage_guest_nice,2016-01-01,1451606400000000000,38.24311829\n",
		},
	}
	testSerializer(t, cases, &CassandraSerializer{})
}

func TestTypeNameForCassandra(t *testing.T) {
	cases := []struct {
		desc string
		v    interface{}
		want string
	}{
		{
			desc: "type int",
			v:    int(5),
			want: "bigint",
		},
		{
			desc: "type int64",
			v:    int(5000000000),
			want: "bigint",
		},
		{
			desc: "type float32",
			v:    float32(3.2),
			want: "float",
		},
		{
			desc: "type float64",
			v:    float64(3.23234545234),
			want: "double",
		},
		{
			desc: "type bool",
			v:    true,
			want: "boolean",
		},
		{
			desc: "type []byte",
			v:    []byte("test"),
			want: "blob",
		},
		{
			desc: "type string",
			v:    "test",
			want: "blob",
		},
	}

	for _, c := range cases {
		if got := typeNameForCassandra(c.v); got != c.want {
			t.Errorf("%s: incorrect type name: got %s want %s", c.desc, got, c.want)
		}
	}
}
