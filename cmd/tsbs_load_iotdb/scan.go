package main

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iotdb-client-go/client"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

// iotdbPoint is a single record(row) of data
type iotdbPoint struct {
	deviceID     string // the deviceID(path) of this record, e.g. "root.cpu.host_0"
	timestamp    int64
	measurements []string
	values       []interface{}
	dataTypes    []client.TSDataType
	tagString    string

	fieldsCnt uint64
}

// CRTODO:使用这个函数来生成创建语句。
func (p *iotdbPoint) generateTagsAttributesSQL() string {
	sql := "CREATE timeseries %s._tags with datatype=INT32, encoding=RLE, compression=SNAPPY attributes(%s)"
	// sql2 := "ALTER timeseries %s._tags UPSERT attributes(%s)"
	return fmt.Sprintf(sql, p.deviceID, p.tagString)
}

// parse datatype and convert string into interface
func parseDataToInterface(datatype client.TSDataType, str string) (interface{}, error) {
	switch client.TSDataType(datatype) {
	case client.BOOLEAN:
		value, err := strconv.ParseBool(str)
		return interface{}(value), err
	case client.INT32:
		value, err := strconv.ParseInt(str, 10, 32)
		return interface{}(int32(value)), err
	case client.INT64:
		value, err := strconv.ParseInt(str, 10, 64)
		return interface{}(int64(value)), err
	case client.FLOAT:
		value, err := strconv.ParseFloat(str, 32)
		return interface{}(float32(value)), err
	case client.DOUBLE:
		value, err := strconv.ParseFloat(str, 64)
		return interface{}(float64(value)), err
	case client.TEXT:
		return interface{}(str), nil
	case client.UNKNOW:
		return interface{}(nil), fmt.Errorf("datatype client.UNKNOW, value:%s", str)
	default:
		return interface{}(nil), fmt.Errorf("unknown datatype, value:%s", str)
	}
}

type fileDataSource struct {
	scanner *bufio.Scanner
}

// read new four line, which store one data point
// e.g.,
// e.g.,
// deviceID,timestamp,<fieldName1>,<fieldName2>,<fieldName3>,...
// <deviceID>,<timestamp>,<field1>,<field2>,<field3>,...
// datatype,<datatype1>,<datatype2>,<datatype3>,...
//
// deviceID,timestamp,hostname,value
// root.cpu.host_1,1451606400000000000,'host_1',44.0
// datatype,5,2
//
// return : bool -> true means got one point, else reaches EOF or error happens
func (d *fileDataSource) nextFourLines() (bool, string, string, string, string, error) {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return false, "", "", "", "", nil
	} else if !ok {
		return false, "", "", "", "", fmt.Errorf("scan error: %v", d.scanner.Err())
	}
	line1 := d.scanner.Text()
	line_ok := strings.HasPrefix(line1, "deviceID,timestamp,")
	if !line_ok {
		return false, line1, "", "", "", fmt.Errorf("scan error, illegal line: %s", line1)
	}
	ok = d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return false, "", "", "", "", nil
	} else if !ok {
		return false, "", "", "", "", fmt.Errorf("scan error: %v", d.scanner.Err())
	}
	line2 := d.scanner.Text()
	ok = d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return false, "", "", "", "", nil
	} else if !ok {
		return false, "", "", "", "", fmt.Errorf("scan error: %v", d.scanner.Err())
	}
	line3 := d.scanner.Text()
	ok = d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return false, "", "", "", "", nil
	} else if !ok {
		return false, "", "", "", "", fmt.Errorf("scan error: %v", d.scanner.Err())
	}
	line4 := d.scanner.Text()
	return true, line1, line2, line3, line4, nil
}

func parseFourLines(line1 string, line2 string, line3 string, line4 string) data.LoadedPoint {
	line1_parts := strings.Split(line1, ",")     // 'deviceID' and rest keys of fields
	line2_parts := strings.Split(line2, ",")     // deviceID and rest values of fields
	line3_parts := strings.Split(line3, ",")     // deviceID and rest values of fields
	line4_parts := strings.SplitN(line4, ",", 2) // 'tags' and string of tags
	timestamp, err := strconv.ParseInt(line2_parts[1], 10, 64)
	if err != nil {
		fatal("timestamp convert err: %v", err)
	}
	timestamp = int64(timestamp / int64(time.Millisecond))
	var measurements []string
	var values []interface{}
	var dataTypes []client.TSDataType
	// handle measurements, datatype and values
	measurements = append(measurements, line1_parts[2:]...)
	for type_index := 1; type_index < len(line3_parts); type_index++ {
		value_index := type_index + 1
		datatype, _ := strconv.ParseInt(line3_parts[type_index], 10, 8)
		dataTypes = append(dataTypes, client.TSDataType(datatype))
		value, err := parseDataToInterface(client.TSDataType(datatype), line2_parts[value_index])
		if err != nil {
			panic(fmt.Errorf("iotdb fileDataSource NextItem Parse error:%v", err))
		}
		values = append(values, value)
	}
	return data.NewLoadedPoint(
		&iotdbPoint{
			deviceID:     line2_parts[0],
			timestamp:    timestamp,
			measurements: measurements,
			values:       values,
			dataTypes:    dataTypes,
			tagString:    line4_parts[1],
			fieldsCnt:    uint64(len(line1_parts) - 2),
		})
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	scan_ok, line1, line2, line3, line4, err := d.nextFourLines()
	if !scan_ok {
		if err == nil { // End of file
			return data.LoadedPoint{}
		} else { // Some error occurred
			fatal("IoTDB scan error: %v", err)
			return data.LoadedPoint{}
		}
	}
	return parseFourLines(line1, line2, line3, line4)
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
