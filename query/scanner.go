package query

import (
	"encoding/gob"
	"io"
	"log"
	"sync"
)

// scanner is used to read in Queries from a Reader where they are
// Go-encoded and then distribute them to workers
type scanner struct {
	r     io.Reader
	limit *uint64
}

// newScanner returns a new scanner for a given Reader and its limit
func newScanner(limit *uint64) *scanner {
	return &scanner{limit: limit}
}

// setReader sets the source, an io.Reader, that the scanner reads/decodes from
func (qs *scanner) setReader(r io.Reader) *scanner {
	qs.r = r
	return qs
}

// scan reads encoded Queries and places them into a channel
func (qs *scanner) scan(pool *sync.Pool, c chan Query) {
	dec := gob.NewDecoder(qs.r)

	n := uint64(0)
	for {
		if *qs.limit > 0 && n >= *qs.limit {
			break
		}

		q := pool.Get().(Query)
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
