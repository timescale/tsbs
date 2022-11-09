package main

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

// iotdbPoint is a single record(row) of data
type iotdbPoint struct {
	deviceID      string // the deviceID(path) of this record, e.g. "root.cpu.host_0"
	fieldKeyStr   string // the keys of fields, e.g. "timestamp,value,str"
	fieldValueStr string // the values of fields in string, e.g. "1666281600000,44.0,'host_1'"
	fieldsCnt     uint64
}

func (p *iotdbPoint) generateInsertStatement() string {
	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s)", p.deviceID, p.fieldKeyStr, p.fieldValueStr)
	return sql
}

type fileDataSource struct {
	scanner *bufio.Scanner
}

// read new two line, which store one data point
// e.g.,
// deviceID,timestamp,<fieldName1>,<fieldName2>,<fieldName3>,...
// <deviceID>,<timestamp>,<field1>,<field2>,<field3>,...
//
// deviceID,timestamp,hostname,tag2
// root.cpu.host_1,1666281600000,'host_1',44.0
//
// return : bool -> true means got one point, else reaches EOF or error happens
func (d *fileDataSource) nextTwoLines() (bool, string, string, error) {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return false, "", "", nil
	} else if !ok {
		return false, "", "", fmt.Errorf("scan error: %v", d.scanner.Err())
	}
	line1 := d.scanner.Text()
	line_ok := strings.HasPrefix(line1, "deviceID,timestamp,")
	if !line_ok {
		return false, line1, "", fmt.Errorf("scan error, illegal line: %s", line1)
	}
	ok = d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return false, "", "", nil
	} else if !ok {
		return false, "", "", fmt.Errorf("scan error: %v", d.scanner.Err())
	}
	line2 := d.scanner.Text()
	return true, line1, line2, nil
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	scan_ok, line1, line2, err := d.nextTwoLines()
	if !scan_ok {
		if err == nil { // End of file
			return data.LoadedPoint{}
		} else { // Some error occurred
			fatal("IoTDB scan error: %v", err)
			return data.LoadedPoint{}
		}
	}
	line1_parts := strings.SplitN(line1, ",", 2) // 'deviceID' and rest keys of fields
	line2_parts := strings.SplitN(line2, ",", 2) // deviceID and rest values of fields
	return data.NewLoadedPoint(
		&iotdbPoint{
			deviceID:      line2_parts[0],
			fieldKeyStr:   line1_parts[1],
			fieldValueStr: line2_parts[1],
			fieldsCnt:     uint64(len(strings.Split(line1_parts[1], ","))),
		})
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders { return nil }

// A struct that storages data points
type iotdbBatch struct {
	points  []*iotdbPoint
	rows    uint   // count of records(rows)
	metrics uint64 // total count of all metrics in this batch
}

func (b *iotdbBatch) Len() uint {
	return b.rows
}

func (b *iotdbBatch) Append(item data.LoadedPoint) {
	b.rows++
	b.points = append(b.points, item.Data.(*iotdbPoint))
	b.metrics += item.Data.(*iotdbPoint).fieldsCnt
}

type factory struct{}

func (f *factory) New() targets.Batch {
	return &iotdbBatch{
		rows:    0,
		metrics: 0,
	}
}
