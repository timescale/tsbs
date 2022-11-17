package main

import (
	"fmt"

	"github.com/apache/iotdb-client-go/client"
	"github.com/timescale/tsbs/pkg/targets"
)

type processor struct {
	numWorker                int // the worker(like thread) ID of this processor
	session                  client.Session
	recordsMaxRows           int             // max rows of records in 'InsertRecords'
	ProcessedTagsDeviceIDMap map[string]bool // already processed device ID
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

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*iotdbBatch)

	// Write records
	if doLoad {
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
	}

	metricCount = batch.metrics
	rowCount = uint64(batch.rows)
	return metricCount, rowCount
}
