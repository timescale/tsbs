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

type commonDimensionsProcessor struct {
	dbName            string
	batchPool         *sync.Pool
	headers           *common.GeneratedDataHeaders
	_dimensionsBuffer []*timestreamwrite.Dimension
	_recordsBuffer    []*timestreamwrite.Record
	writeService      *timestreamwrite.TimestreamWrite
}

func (c *commonDimensionsProcessor) Init(workerNum int, doLoad, hashWorkers bool) {
	c.expandDimensionBuffer(len(c.headers.TagKeys))
	maxFields := 0
	for _, tableFields := range c.headers.FieldKeys {
		if len(tableFields) > maxFields {
			maxFields = len(tableFields)
		}
	}
	c._recordsBuffer = make([]*timestreamwrite.Record, maxFields)
}

func (c *commonDimensionsProcessor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	var timestreamBatch batch
	timestreamBatch = *b.(*batch)
	for table, rows := range timestreamBatch.rows {
		rowCount += uint64(len(rows))
		if doLoad {
			newMetricCount, err := c.writeToTable(table, rows)
			if err != nil {
				log.Fatal("could not write to table: " + err.Error())
			}
			metricCount += newMetricCount
		}
	}
	timestreamBatch.reset()
	c.batchPool.Put(b)
	return metricCount, rowCount
}

func (c *commonDimensionsProcessor) expandDimensionBuffer(requiredDimensions int) {
	if len(c._dimensionsBuffer) < requiredDimensions {
		c._dimensionsBuffer = make([]*timestreamwrite.Dimension, requiredDimensions)
	}
}
func (c *commonDimensionsProcessor) writeToTable(table string, rows []deserializedPoint) (metricCount uint64, err error) {
	for _, row := range rows {
		c.expandDimensionBuffer(len(row.tagKeys))
		numDimensions := convertTagsToDimensions(row.tagKeys, row.tags, c._dimensionsBuffer)
		numRecords := convertPointToRecords(&row, c.headers.FieldKeys[table], c._recordsBuffer)
		writeRecordsInput := &timestreamwrite.WriteRecordsInput{
			DatabaseName: &c.dbName,
			TableName:    &table,
			CommonAttributes: &timestreamwrite.Record{
				Dimensions: c._dimensionsBuffer[:numDimensions],
				Time:       &row.timeUnixNano,
				TimeUnit:   aws.String(timestreamwrite.TimeUnitNanoseconds),
			},
			Records: c._recordsBuffer[:numRecords],
		}
		_, err := c.writeService.WriteRecords(writeRecordsInput)
		if err != nil {
			return 0, errors.Wrap(err, "could not write records to db")
		}
		metricCount += uint64(len(row.fields))
	}

	return metricCount, nil
}

func convertTagsToDimensions(tagKeys, tagValues []string, buffer []*timestreamwrite.Dimension) (numDimensions int) {
	for i, value := range tagValues {
		if buffer[i] == nil {
			buffer[i] = &timestreamwrite.Dimension{}
		}
		buffer[i].Name = &tagKeys[i]
		valNewPtr := value
		buffer[i].Value = &valNewPtr
		buffer[i].DimensionValueType = aws.String(timestreamwrite.DimensionValueTypeVarchar)
	}
	return len(tagValues)
}

func convertPointToRecords(point *deserializedPoint, fieldKeys []string, buffer []*timestreamwrite.Record) (numFields int) {
	numFields = 0
	for i, fieldVal := range point.fields {
		if fieldVal == nil {
			continue
		}
		if buffer[numFields] == nil {
			buffer[numFields] = &timestreamwrite.Record{}
		}

		buffer[numFields].SetMeasureName(fieldKeys[i])
		buffer[numFields].SetMeasureValueType(timestreamwrite.MeasureValueTypeDouble)
		buffer[numFields].SetMeasureValue(*fieldVal)
		numFields++
	}
	return numFields
}
