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
func (s *scanner) setReader(r io.Reader) *scanner {
	s.r = r
	return s
}

// scan reads encoded Queries and places them into a channel
func (s *scanner) scan(pool *sync.Pool, c chan Query) {
	decoder := gob.NewDecoder(s.r)

	n := uint64(0)
	for {
		if *s.limit > 0 && n >= *s.limit {
			// request queries limit reached, time to quit
			break
		}

		q := pool.Get().(Query)
		err := decoder.Decode(q)
		if err == io.EOF {
			// EOF, all done
			break
		}
		if err != nil {
			// Can't read, time to quit
			log.Fatal(err)
		}

		// We have a query, send it to the runner
		q.SetID(n)
		c <- q

		// Queries counter
		n++
	}
}
