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

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*iotdbBatch)

	// Write records
	if doLoad {
		var sqlList []string
		if p.recordsMaxRows > 0 {
			for index := 0; index < len(batch.points); {
				var (
					deviceId     []string
					measurements [][]string
					dataTypes    [][]client.TSDataType
					values       [][]interface{}
					timestamps   []int64
				)
				for thisRecordsCnt := 0; thisRecordsCnt < recordsMaxRows && index < len(batch.points); {
					row := batch.points[index]
					deviceId = append(deviceId, row.deviceID)
					measurements = append(measurements, row.measurements)
					dataTypes = append(dataTypes, row.dataTypes)
					values = append(values, row.values)
					timestamps = append(timestamps, row.timestamp)
					_, exist := p.ProcessedTagsDeviceIDMap[row.deviceID]
					if !exist {
						sqlList = append(sqlList, row.generateTagsAttributesSQL())
						p.ProcessedTagsDeviceIDMap[row.deviceID] = true
					}
					thisRecordsCnt++
					index++
				}
				_, err := p.session.InsertRecords(
					deviceId, measurements, dataTypes, values, timestamps,
				)
				if err != nil {
					fatal("ProcessBatch error:%v", err)
				}
			}
		} else {
			var (
				deviceId     []string
				measurements [][]string
				dataTypes    [][]client.TSDataType
				values       [][]interface{}
				timestamps   []int64
			)
			for _, row := range batch.points {
				deviceId = append(deviceId, row.deviceID)
				measurements = append(measurements, row.measurements)
				dataTypes = append(dataTypes, row.dataTypes)
				values = append(values, row.values)
				timestamps = append(timestamps, row.timestamp)
				_, exist := p.ProcessedTagsDeviceIDMap[row.deviceID]
				if !exist {
					sqlList = append(sqlList, row.generateTagsAttributesSQL())
					p.ProcessedTagsDeviceIDMap[row.deviceID] = true
				}
			}
			_, err := p.session.InsertRecords(
				deviceId, measurements, dataTypes, values, timestamps,
			)
			if err != nil {
				fatal("ProcessBatch error:%v", err)
			}
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
