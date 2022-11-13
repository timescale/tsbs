package main

import (
	"fmt"

	"github.com/apache/iotdb-client-go/client"
	"github.com/timescale/tsbs/pkg/targets"
)

type processor struct {
	session        client.Session
	recordsMaxRows int // max rows of records in 'InsertRecords'
}

func (p *processor) Init(_ int, doLoad, _ bool) {
	if !doLoad {
		return
	}
	p.session = client.NewSession(&clientConfig)
	if err := p.session.Open(false, timeoutInMs); err != nil {
		errMsg := fmt.Sprintf("IoTDB processor init error, session is not open: %v\n", err)
		errMsg = errMsg + fmt.Sprintf("timeout setting: %d ms", timeoutInMs)
		fatal(errMsg)
	}
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*iotdbBatch)

	// Write records
	if doLoad {
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
			}
			_, err := p.session.InsertRecords(
				deviceId, measurements, dataTypes, values, timestamps,
			)
			if err != nil {
				fatal("ProcessBatch error:%v", err)
			}
		}
		// for _, row := range batch.points {
		// 	sql := row.generateInsertStatement()
		// 	p.session.ExecuteUpdateStatement(sql)
		// }
	}

	metricCount = batch.metrics
	rowCount = uint64(batch.rows)
	return metricCount, rowCount
}
