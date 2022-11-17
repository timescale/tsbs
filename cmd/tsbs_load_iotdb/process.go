package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/apache/iotdb-client-go/client"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/iotdb"
)

type processor struct {
	numWorker                int // the worker(like thread) ID of this processor
	session                  client.Session
	recordsMaxRows           int                 // max rows of records in 'InsertRecords'
	ProcessedTagsDeviceIDMap map[string]bool     // already processed device ID
	loadToSCV                bool                // if true, do NOT insert into databases, but generate csv files instead.
	csvFilepathPrefix        string              // Prefix of filepath for csv files. Specific a folder or a folder with filename prefix.
	filePtrMap               map[string]*os.File // file pointer for each deviceID
}

func (p *processor) Init(numWorker int, doLoad, _ bool) {
	p.numWorker = numWorker
	if !doLoad {
		return
	}
	p.session = client.NewSession(&clientConfig)
	if err := p.session.Open(false, timeoutInMs); err != nil {
		errMsg := fmt.Sprintf("IoTDB processor init error, session is not open: %v\n", err)
		errMsg = errMsg + fmt.Sprintf("timeout setting: %d ms", timeoutInMs)
		fatal(errMsg)
	}
	p.ProcessedTagsDeviceIDMap = make(map[string]bool, 1024)
	p.filePtrMap = make(map[string]*os.File)
}

type records struct {
	deviceId     []string
	measurements [][]string
	dataTypes    [][]client.TSDataType
	values       [][]interface{}
	timestamps   []int64
}

func (p *processor) pointsToRecords(points []*iotdbPoint) (records, []string) {
	var rcds records
	var sqlList []string
	for _, row := range points {
		rcds.deviceId = append(rcds.deviceId, row.deviceID)
		rcds.measurements = append(rcds.measurements, row.measurements)
		rcds.dataTypes = append(rcds.dataTypes, row.dataTypes)
		rcds.values = append(rcds.values, row.values)
		rcds.timestamps = append(rcds.timestamps, row.timestamp)
		_, exist := p.ProcessedTagsDeviceIDMap[row.deviceID]
		if !exist {
			sqlList = append(sqlList, row.generateTagsAttributesSQL())
			p.ProcessedTagsDeviceIDMap[row.deviceID] = true
		}
	}
	return rcds, sqlList
}

func minInt(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

func getStringOfDatatype(datatype client.TSDataType) string {
	switch datatype {
	case client.BOOLEAN:
		return "BOOLEAN"
	case client.DOUBLE:
		return "DOUBLE"
	case client.FLOAT:
		return "FLOAT"
	case client.INT32:
		return "INT32"
	case client.INT64:
		return "INT64"
	case client.TEXT:
		return "TEXT"
	case client.UNKNOW:
		return "UNKNOW"
	default:
		return "UNKNOW"
	}
}

func generateCSVHeader(point *iotdbPoint) string {
	header := "Time"
	for index, dataType := range point.dataTypes {
		meta := fmt.Sprintf(",%s.%s(%s)", point.deviceID, point.measurements[index],
			getStringOfDatatype(dataType))
		header = header + meta
	}
	header = header + "\n"
	return header
}

func generateCSVContent(point *iotdbPoint) string {
	var valueList []string
	valueList = append(valueList, strconv.FormatInt(point.timestamp, 10))
	for _, value := range point.values {
		valueInStrByte, _ := iotdb.IotdbFormat(value)
		valueList = append(valueList, string(valueInStrByte))
	}
	content := strings.Join(valueList, ",")
	content += "\n"
	return content
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*iotdbBatch)

	// Write records
	if doLoad {
		if !p.loadToSCV {
			var sqlList []string
			for index := 0; index < len(batch.points); {
				startIndex := index
				var endIndex int
				if p.recordsMaxRows > 0 {
					endIndex = minInt(len(batch.points), index+p.recordsMaxRows)
				} else {
					endIndex = len(batch.points)
				}
				rcds, tempSqlList := p.pointsToRecords(batch.points[startIndex:endIndex])
				sqlList = append(sqlList, tempSqlList...)
				_, err := p.session.InsertRecords(
					rcds.deviceId, rcds.measurements, rcds.dataTypes, rcds.values, rcds.timestamps,
				)
				if err != nil {
					fatal("ProcessBatch error:%v", err)
				}
				index = endIndex
			}
			// handle create timeseries SQL to insert tags
			for _, sql := range sqlList {
				p.session.ExecuteUpdateStatement(sql)
			}
		} else {
			for index := 0; index < len(batch.points); index++ {
				point := batch.points[index]
				_, exist := p.filePtrMap[point.deviceID]
				if !exist {
					// create file pointer
					filepath := fmt.Sprintf("%s%s.csv", p.csvFilepathPrefix, point.deviceID)
					filePtr, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY, 0777)
					if err != nil {
						fatal(fmt.Sprintf("ERROR occurs while creating csv file for deviceID: %s, filepath: %s", point.deviceID, filepath))
						panic(err)
					}
					p.filePtrMap[point.deviceID] = filePtr
					// write header of this csv file
					header := generateCSVHeader(point)
					filePtr.WriteString(header)
				}
				filePtr := p.filePtrMap[point.deviceID]
				pointRowInCSV := generateCSVContent(point)
				filePtr.WriteString(pointRowInCSV)
			}
		}

	}

	metricCount = batch.metrics
	rowCount = uint64(batch.rows)
	return metricCount, rowCount
}
