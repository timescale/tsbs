package main

import (
	"bufio"
	//"fmt"
	"log"
	"strings"
	"sync"

	"github.com/gomodule/redigo/redis"
	"github.com/timescale/tsbs/load"
)

type decoder struct {
	scanner *bufio.Scanner
}

// Reads and returns a text line that encodes a data point for a specif field name.
// Since scanning happens in a single thread, we hold off on transforming it
// to an INSERT statement until it's being processed concurrently by a worker.
func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return nil
	} else if !ok {
		log.Fatalf("scan error: %v", d.scanner.Err())
	}
	return load.NewPoint(d.scanner.Text())
}

func sendRedisCommand(conn redis.Conn, cmdName string, s redis.Args) (err error) {
	err = conn.Send(cmdName, s...)
	if err != nil {
		log.Fatalf("sendRedisCommand %s failed: %s\n", cmdName, err)
	}
	return
}

func buildCommand(line string, forceUncompressed bool) (cmdname string, s redis.Args) {
	t := strings.Split(line, " ")
	cmdname = t[0]
	if cmdname == "TS.CREATE" && forceUncompressed {
		t = append(t, "UNCOMPRESSED")
		s = s.Add(t[1])
		s = s.Add("UNCOMPRESSED")
		s = s.AddFlat(t[2:])
	} else {
		s = s.AddFlat(t[1:])
	}
	return
}

func sendRedisFlush(count uint64, conn redis.Conn) (metrics uint64, err error) {
	metrics = uint64(0)
	err = conn.Flush()
	if err != nil {
		return
	}
	for i := uint64(0); i < count; i++ {
		_, err := conn.Receive()
		//fmt.Println(r)
		if err != nil {
			log.Fatalf("Flush failed with %v", err)
		} else {
			metrics += 10 // ts.madd
		}
	}
	return metrics, err
}

type eventsBatch struct {
	rows []string
}

func (eb *eventsBatch) Len() int {
	return len(eb.rows)
}

func (eb *eventsBatch) Append(item *load.Point) {
	that := item.Data.(string)
	eb.rows = append(eb.rows, that)
}

var ePool = &sync.Pool{New: func() interface{} { return &eventsBatch{rows: []string{}} }}

type factory struct{}

func (f *factory) New() load.Batch {
	return ePool.Get().(*eventsBatch)
}
