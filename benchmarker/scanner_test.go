package benchmarker

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"sync"
	"testing"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

type testQuery struct {
	ID uint64
}

func (q *testQuery) Release()                     {}
func (q *testQuery) HumanLabelName() []byte       { return []byte("test") }
func (q *testQuery) HumanDescriptionName() []byte { return q.HumanLabelName() }
func (q *testQuery) GetID() uint64                { return q.ID }
func (q *testQuery) SetID(id uint64)              { q.ID = id }
func (q *testQuery) String() string               { return "test" }

var testQueryPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &testQuery{}
	},
}

func TestScannerLimit(t *testing.T) {
	totalQueries := uint64(7) // why 7? why _not_ 7?
	cases := []struct {
		limit uint64
		want  uint64
	}{
		{
			limit: 0, // should test all
			want:  totalQueries,
		},
		{
			limit: 1,
			want:  1,
		},
		{
			limit: 5,
			want:  5,
		},
	}

	var b bytes.Buffer
	out := bufio.NewWriter(&b)
	enc := gob.NewEncoder(out)
	for i := 0; i < 7; i++ {
		q := testQuery{uint64(i)}
		err := enc.Encode(q)
		if err != nil {
			t.Errorf("encode error: %v\n", err)
		}
	}
	out.Flush()

	for _, c := range cases {
		var wg sync.WaitGroup // TODO: Add a timeout feature?
		queryChan := make(chan query.Query, 1)
		scanner := newQueryScanner(&c.limit)
		got := uint64(0)
		wg.Add(1)
		go func() { // simply count the number of queries we process
			for _ = range queryChan {
				got++
			}
			wg.Done()
		}()
		input := bufio.NewReaderSize(bytes.NewReader(b.Bytes()), 1<<20)
		scanner.SetReader(input).Scan(&testQueryPool, queryChan)
		close(queryChan)
		wg.Wait()
		if c.want != got {
			t.Errorf("got: %v want: %v\n", got, c.want)
		}
	}
}
