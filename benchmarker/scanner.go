package benchmarker

import (
	"encoding/gob"
	"io"
	"log"
	"sync"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// QueryScanner is used to read in Queries from a Reader where they are
// Go-encoded and then distribute them to workers
type QueryScanner struct {
	r     io.Reader
	limit *uint64
}

// NewQueryScanner returns a new QueryScanner for a given Reader and its limit
func newQueryScanner(limit *uint64) *QueryScanner {
	return &QueryScanner{limit: limit}
}

// SetReader sets the source, an io.Reader, that the QueryScanner reads/decodes from
func (qs *QueryScanner) SetReader(r io.Reader) *QueryScanner {
	qs.r = r
	return qs
}

// Scan reads encoded Queries and places them into a channel
func (qs *QueryScanner) Scan(pool *sync.Pool, c chan query.Query) {
	dec := gob.NewDecoder(qs.r)

	n := uint64(0)
	for {
		if *qs.limit > 0 && n >= *qs.limit {
			break
		}

		q := pool.Get().(query.Query)
		err := dec.Decode(q)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		q.SetID(n)
		c <- q
		n++
	}
}
