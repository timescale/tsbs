package zigtsdb

import (
	"bytes"
	"strings"
	"testing"

	"github.com/timescale/tsbs/pkg/data"
)

func TestExpandLine(t *testing.T) {
	cases := []struct {
		desc    string
		in      string
		want    string
		fields  uint64
		wantErr string
	}{
		{
			desc: "cpu row with tags and two float fields",
			in:   "cpu,hostname=host_0,region=eu-west-1 usage_user=58.1,usage_system=2.5 1451606400000000000",
			want: "cpu_usage_user,hostname=host_0,region=eu-west-1 value=58.1 1451606400000000000\n" +
				"cpu_usage_system,hostname=host_0,region=eu-west-1 value=2.5 1451606400000000000\n",
			fields: 2,
		},
		{
			desc:   "integer field loses the influx i suffix",
			in:     "disk,hostname=host_1 total=1000000i,used=5i 100",
			want:   "disk_total,hostname=host_1 value=1000000 100\ndisk_used,hostname=host_1 value=5 100\n",
			fields: 2,
		},
		{
			desc:   "no tags",
			in:     "cpu usage_user=1.5 100",
			want:   "cpu_usage_user value=1.5 100\n",
			fields: 1,
		},
		{
			desc:   "tag order is preserved verbatim",
			in:     "cpu,z=1,a=2,m=3 f=0.5 7",
			want:   "cpu_f,z=1,a=2,m=3 value=0.5 7\n",
			fields: 1,
		},
		{
			desc:    "string field is rejected (iot use case)",
			in:      "readings,name=truck_0 status=\"ok\",velocity=10 100",
			wantErr: "string field",
		},
		{
			desc:    "boolean field is rejected",
			in:      "readings,name=truck_0 alive=true 100",
			wantErr: "boolean field",
		},
		{
			desc:    "malformed line without timestamp",
			in:      "cpu,hostname=host_0 usage_user=1",
			wantErr: "3 tuples",
		},
		{
			desc:    "empty field list",
			in:      "cpu,hostname=host_0  100",
			wantErr: "no fields",
		},
	}
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := expandLine([]byte(c.in), &buf)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("expected error containing %q, got %v", c.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := buf.String(); got != c.want {
				t.Fatalf("expansion mismatch\n got: %q\nwant: %q", got, c.want)
			}
			if n != c.fields {
				t.Fatalf("expected %d fields, got %d", c.fields, n)
			}
		})
	}
}

func TestBatchAppendCounts(t *testing.T) {
	f := newTestFactory()
	b := f.New().(*batch)
	if b.Len() != 0 {
		t.Fatalf("new batch must be empty")
	}
	b.Append(data.NewLoadedPoint([]byte("cpu,hostname=host_0 a=1,b=2,c=3 100")))
	b.Append(data.NewLoadedPoint([]byte("cpu,hostname=host_1 a=1,b=2 200")))
	if b.Len() != 2 || b.rows != 2 {
		t.Fatalf("expected 2 rows, got Len=%d rows=%d", b.Len(), b.rows)
	}
	if b.metrics != 5 {
		t.Fatalf("expected 5 metrics, got %d", b.metrics)
	}
	if lines := bytes.Count(b.buf.Bytes(), newLine); lines != 5 {
		t.Fatalf("expected 5 wire lines, got %d", lines)
	}
}

func TestBatchAppendMalformedIsFatal(t *testing.T) {
	restore := captureFatal(t)
	defer restore()
	f := newTestFactory()
	b := f.New().(*batch)
	b.Append(data.NewLoadedPoint([]byte("garbage")))
	if fatalCalls() != 1 {
		t.Fatalf("expected a fatal call on malformed input")
	}
}
