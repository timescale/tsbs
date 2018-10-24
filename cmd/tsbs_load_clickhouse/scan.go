package main

import (
	"bufio"
	"hash/fnv"
	"strings"

	"github.com/timescale/tsbs/load"
)

// hostnameIndexer is used to consistently send the same hostnames to the same queue
type hostnameIndexer struct {
	partitions uint
}

// scan.PointIndexer interface implementation
func (i *hostnameIndexer) GetIndex(item *load.Point) int {
	p := item.Data.(*point)
	hostname := strings.SplitN(p.row.tags, ",", 2)[0]
	h := fnv.New32a()
	h.Write([]byte(hostname))
	return int(h.Sum32()) % int(i.partitions)
}

// Point is a single row of data keyed by which table it belongs
// Ex.:
// tags,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
// cpu,1451606400000000000,58,2,24,61,22,63,6,44,80,38
type point struct {
	table string
	row   *insertData
}

// scan.Batch interface implementation
type tableArr struct {
	m   map[string][]*insertData
	cnt int
}

// scan.Batch interface implementation
func (ta *tableArr) Len() int {
	return ta.cnt
}

// scan.Batch interface implementation
func (ta *tableArr) Append(item *load.Point) {
	that := item.Data.(*point)
	k := that.table
	ta.m[k] = append(ta.m[k], that.row)
	ta.cnt++
}

// scan.BatchFactory interface implementation
type factory struct{}

// scan.BatchFactory interface implementation
func (f *factory) New() load.Batch {
	return &tableArr{
		m:   map[string][]*insertData{},
		cnt: 0,
	}
}

// scan.PointDecoder interface implementation
type decoder struct {
	scanner *bufio.Scanner
}

const tagsPrefix = "tags"

// scan.PointDecoder interface implementation
func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	// Data Point Example
	// tags,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
	// cpu,1451606400000000000,58,2,24,61,22,63,6,44,80,38

	data := &insertData{}
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil {
		// nothing scanned & no error = EOF
		return nil
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return nil
	}

	// The first line is a CSV line of tags with the first element being "tags"
	// Ex.:
	// tags,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
	parts := strings.SplitN(d.scanner.Text(), ",", 2) // prefix & then rest of line
	prefix := parts[0]
	if prefix != tagsPrefix {
		fatal("data file in invalid format; got %s expected %s", prefix, tagsPrefix)
		return nil
	}
	data.tags = parts[1]

	// Scan again to get the data line
	// cpu,1451606400000000000,58,2,24,61,22,63,6,44,80,38
	ok = d.scanner.Scan()
	if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return nil
	}
	parts = strings.SplitN(d.scanner.Text(), ",", 2) // prefix & then rest of line
	prefix = parts[0]
	data.fields = parts[1]

	return load.NewPoint(&point{
		table: prefix,
		row:   data,
	})
}
