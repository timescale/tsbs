package query

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"testing"
)

type testQuery struct {
	ID               uint64
	HumanLabel       []byte
	HumanDescription []byte
}

func (q *testQuery) Release()                     {}
func (q *testQuery) HumanLabelName() []byte       { return q.HumanLabel }
func (q *testQuery) HumanDescriptionName() []byte { return q.HumanDescription }
func (q *testQuery) GetID() uint64                { return q.ID }
func (q *testQuery) SetID(id uint64)              { q.ID = id }
func (q *testQuery) String() string               { return "test" }

var testQueryPool = sync.Pool{
	New: func() interface{} {
		return &testQuery{}
	},
}

type genQueryFn func(uint64) Query
type checkQueryFn func(int, Query) error

func encodeQueries(b *bytes.Buffer, totalQueries uint64, g genQueryFn) error {
	out := bufio.NewWriter(b)
	enc := gob.NewEncoder(out)
	for i := uint64(0); i < totalQueries; i++ {
		q := g(i)
		q.SetID(uint64(i))
		err := enc.Encode(q)
		if err != nil {
			return fmt.Errorf("encode error: %v", err)
		}
		q.Release()
	}
	out.Flush()
	return nil
}

func runScan(t *testing.T, b *bytes.Buffer, limit, numQueries uint64, pool *sync.Pool, chk checkQueryFn) error {
	var wg sync.WaitGroup // TODO: Add a timeout feature?
	queryChan := make(chan Query, 1)
	scanner := newScanner(&limit)
	got := uint64(0)
	wg.Add(1)
	go func() { // simply count the number of queries we process
		i := 0
		for q := range queryChan {
			err := chk(i, q)
			if err != nil {
				t.Fatalf(err.Error())
			}
			i++
			got++
		}
		wg.Done()
	}()
	input := bufio.NewReaderSize(bytes.NewReader(b.Bytes()), 1<<20)
	scanner.setReader(input).scan(pool, queryChan)
	close(queryChan)
	wg.Wait()
	if got != numQueries {
		return fmt.Errorf("incorrect num of queries scanned: got: %v want: %v", got, numQueries)
	}
	return nil
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
	err := encodeQueries(&b, totalQueries, func(i uint64) Query {
		return &testQuery{
			HumanLabel:       []byte("testlabel"),
			HumanDescription: []byte("testDesc"),
		}
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	for _, c := range cases {
		runScan(t, &b, c.limit, c.want, &testQueryPool, func(_ int, _ Query) error {
			return nil
		})
	}
}

func TestScanTimescaleDB(t *testing.T) {
	labelFmt := "tslabel%d"
	descFmt := "tsdesc%d"
	hyperFmt := "tshyper%d"
	totalQueries := uint64(7)
	var b bytes.Buffer
	err := encodeQueries(&b, totalQueries, func(i uint64) Query {
		q := NewTimescaleDB()
		q.HumanLabel = []byte(fmt.Sprintf(labelFmt, i))
		q.HumanDescription = []byte(fmt.Sprintf(descFmt, i))
		q.Hypertable = []byte(fmt.Sprintf(hyperFmt, i))
		return q
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	runScan(t, &b, 0, totalQueries, &TimescaleDBPool, func(i int, q Query) error {
		qt := q.(*TimescaleDB)
		want := fmt.Sprintf(labelFmt, i)
		if got := string(qt.HumanLabel); got != want {
			return fmt.Errorf("wrong label for query %d: got %s want %s", i, got, want)
		}
		want = fmt.Sprintf(descFmt, i)
		if got := string(qt.HumanDescription); got != want {
			return fmt.Errorf("wrong desc for query %d: got %s want %s", i, got, want)
		}
		want = fmt.Sprintf(hyperFmt, i)
		if got := string(qt.Hypertable); got != want {
			return fmt.Errorf("wrong hypertable for query %d: got %s want %s", i, got, want)
		}
		if got := qt.GetID(); got != uint64(i) {
			return fmt.Errorf("wrong ID for query: got %d want %d", got, i)
		}
		return nil
	})
}
