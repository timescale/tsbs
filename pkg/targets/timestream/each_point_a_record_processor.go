package timestream

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"github.com/pkg/errors"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
	"log"
	"sync"
)

const maxRecordsPerWriteRequest = 100

type eachValueARecordProcessor struct {
	dbName       string
	batchPool    *sync.Pool
	headers      *common.GeneratedDataHeaders
	writeService *timestreamwrite.TimestreamWrite
}

func (p *eachValueARecordProcessor) Init(_ int, _, _ bool) {}

func (p *eachValueARecordProcessor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	var timestreamBatch batch
	timestreamBatch = *b.(*batch)
	for table, rows := range timestreamBatch.rows {
		rowCount += uint64(len(rows))
		if doLoad {
			newMetricCount, err := p.writeBatch(table, rows)
			if err != nil {
				log.Fatal("could not write to table: " + err.Error())
			}
			metricCount += newMetricCount
		}
	}
	timestreamBatch.reset()
	p.batchPool.Put(b)
	return metricCount, rowCount
}

func (p *eachValueARecordProcessor) writeBatch(table string, rows []deserializedPoint) (numMetrics uint64, err error) {
	records := make([]*timestreamwrite.Record, 0, maxRecordsPerWriteRequest)
	for _, row := range rows {
		if len(records)+len(row.fields) >= maxRecordsPerWriteRequest {
			writeRecordsInput := &timestreamwrite.WriteRecordsInput{
				DatabaseName: &p.dbName,
				TableName:    &table,
				Records:      records,
			}
			_, err := p.writeService.WriteRecords(writeRecordsInput)
			if err != nil {
				return 0, errors.Wrap(err, "could not write records to db")
			}
			numMetrics += uint64(len(records))
			records = make([]*timestreamwrite.Record, 0, maxRecordsPerWriteRequest)
		}
		records = append(records, p.convertToRecords(table, row)...)
	}

	writeRecordsInput := &timestreamwrite.WriteRecordsInput{
		DatabaseName: &p.dbName,
		TableName:    &table,
		Records:      records,
	}
	_, err = p.writeService.WriteRecords(writeRecordsInput)
	if err != nil {
		return 0, errors.Wrap(err, "could not write records to db")
	}
	numMetrics += uint64(len(records))
	return numMetrics, nil
}

func (p *eachValueARecordProcessor) convertToRecords(table string, row deserializedPoint) []*timestreamwrite.Record {
	dimensions := createDimensions(row.tagKeys, row.tags)
	return createRecords(&row, p.headers.FieldKeys[table], dimensions, row.timeUnixNano)
}

func createRecords(point *deserializedPoint, fieldKeys []string, dimensions []*timestreamwrite.Dimension, ts string) (buffer []*timestreamwrite.Record) {
	buffer = make([]*timestreamwrite.Record, 0, len(fieldKeys))
	for i, fieldVal := range point.fields {
		if fieldVal == nil {
			continue
		}
		newRecord := &timestreamwrite.Record{}
		newRecord.SetDimensions(dimensions)
		newRecord.SetMeasureName(fieldKeys[i])
		newRecord.SetMeasureValueType(timestreamwrite.MeasureValueTypeDouble)
		newRecord.SetMeasureValue(*fieldVal)
		newRecord.SetTime(ts)
		newRecord.SetTimeUnit(timestreamwrite.TimeUnitNanoseconds)
		buffer = append(buffer, newRecord)
	}
	return buffer
}
func createDimensions(tagKeys, tagValues []string) (buffer []*timestreamwrite.Dimension) {
	buffer = make([]*timestreamwrite.Dimension, len(tagKeys))
	for i, value := range tagValues {
		buffer[i] = &timestreamwrite.Dimension{}
		buffer[i].Name = &tagKeys[i]
		valNewPtr := value
		buffer[i].Value = &valNewPtr
		buffer[i].DimensionValueType = aws.String(timestreamwrite.DimensionValueTypeVarchar)
	}
	return buffer
}
