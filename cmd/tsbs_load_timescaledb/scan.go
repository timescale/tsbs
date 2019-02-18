package main

import (
	"bufio"
	"hash/fnv"
	"strings"

	"github.com/timescale/tsbs/load"
)

// hostnameIndexer is used to consistently send the same hostnames to the same worker
type hostnameIndexer struct {
	partitions uint
}

func (i *hostnameIndexer) GetIndex(item *load.Point) int {
	p := item.Data.(*point)
	hostname := strings.SplitN(p.row.tags, ",", 2)[0]
	h := fnv.New32a()
	h.Write([]byte(hostname))
	return int(h.Sum32()) % int(i.partitions)
}

// point is a single row of data keyed by which hypertable it belongs
type point struct {
	hypertable string
	row        *insertData
}

type hypertableArr struct {
	m   map[string][]*insertData
	cnt int
}

func (ha *hypertableArr) Len() int {
	return ha.cnt
}

func (ha *hypertableArr) Append(item *load.Point) {
	that := item.Data.(*point)
	k := that.hypertable
	ha.m[k] = append(ha.m[k], that.row)
	ha.cnt++
}

type factory struct{}

func (f *factory) New() load.Batch {
	return &hypertableArr{
		m:   map[string][]*insertData{},
		cnt: 0,
	}
}

type decoder struct {
	scanner *bufio.Scanner
}

const tagsPrefix = tagsKey

func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	data := &insertData{}
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return nil
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return nil
	}

	// The first line is a CSV line of tags with the first element being "tags"
	parts := strings.SplitN(d.scanner.Text(), ",", 2) // prefix & then rest of line
	prefix := parts[0]
	if prefix != tagsPrefix {
		fatal("data file in invalid format; got %s expected %s", prefix, tagsPrefix)
		return nil
	}
	data.tags = parts[1]

	// Scan again to get the data line
	ok = d.scanner.Scan()
	if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return nil
	}
	parts = strings.SplitN(d.scanner.Text(), ",", 2) // prefix & then rest of line
	prefix = parts[0]
	data.fields = parts[1]

	return load.NewPoint(&point{
		hypertable: prefix,
		row:        data,
	})
}
