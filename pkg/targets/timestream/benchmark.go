package timestream

import (
	"bufio"
	"fmt"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"github.com/pkg/errors"
	"github.com/timescale/tsbs/internal/inputs"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/common"
	"log"
	"time"
)

type benchmark struct {
	config       *SpecificConfig
	ds           targets.DataSource
	targetDb     string
	batchFactory *batchFactory
}

func newBenchmark(targetDb string, config *SpecificConfig, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	ds, err := initDataSource(dataSourceConfig, config.UseCurrentTime)
	if err != nil {
		return nil, errors.Wrap(err, "could not create data source")
	}
	return &benchmark{
		config:       config,
		ds:           ds,
		batchFactory: NewBatchFactory(),
		targetDb:     targetDb,
	}, nil
}

func (b benchmark) GetDataSource() targets.DataSource {
	return b.ds
}

func (b benchmark) GetBatchFactory() targets.BatchFactory {
	return b.batchFactory
}

func (b benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	hashProvider, err := createHashProvider(b.ds, b.config.HashProperty)
	if err != nil {
		log.Fatalf("could not create point indexer: %v", err)
		return nil
	}
	return common.NewGenericPointIndexer(maxPartitions, hashProvider)
}

func (b benchmark) GetProcessor() targets.Processor {
	awsSession, err := OpenAWSSession(&b.config.AwsRegion, time.Minute)
	if err != nil {
		panic("could not open aws session")
	}
	if b.config.UseCommonAttributes {
		return &commonDimensionsProcessor{
			dbName:       b.targetDb,
			batchPool:    b.batchFactory.pool,
			headers:      b.ds.Headers(),
			writeService: timestreamwrite.New(awsSession),
		}
	}

	return &eachValueARecordProcessor{
		batchPool:    b.batchFactory.pool,
		writeService: timestreamwrite.New(awsSession),
		headers:      b.ds.Headers(),
		dbName:       b.targetDb,
	}
}

func (b benchmark) GetDBCreator() targets.DBCreator {
	awsSession, err := OpenAWSSession(&b.config.AwsRegion, time.Minute)
	if err != nil {
		panic("could not open aws session")
	}
	return &dbCreator{
		ds:                                 b.ds,
		writeSvc:                           timestreamwrite.New(awsSession),
		magneticStoreRetentionPeriodInDays: b.config.MagStoreRetentionInDays,
		memoryRetentionPeriodInHours:       b.config.MemStoreRetentionInHours,
	}
}

func initDataSource(config *source.DataSourceConfig, useCurrentTs bool) (targets.DataSource, error) {
	if config.Type == source.FileDataSourceType {
		br := load.GetBufferedReader(config.File.Location)
		return &fileDataSource{
			scanner:      bufio.NewScanner(br),
			useCurrentTs: useCurrentTs,
		}, nil
	} else if config.Type == source.SimulatorDataSourceType {
		dataGenerator := &inputs.DataGenerator{}
		simulator, err := dataGenerator.CreateSimulator(config.Simulator)
		if err != nil {
			return nil, err
		}
		return &simulatorDataSource{
			simulator:    simulator,
			useCurrentTs: useCurrentTs,
		}, nil
	}
	panic("unhandled data source type!!!")
}

// createHashProvider creates the function that will take out the
// value used to calculate the hash depending on which is the
// hashProperty.
func createHashProvider(ds targets.DataSource, hashTag string) (func(point *data.LoadedPoint) []byte, error) {
	headers := ds.Headers()
	tagIndex := -1
	for i, tagKey := range headers.TagKeys {
		if tagKey == hashTag {
			tagIndex = i
			break
		}
	}
	if tagIndex < 0 {
		return nil, fmt.Errorf("no dimension named '%s' found in data points", hashTag)
	}

	return func(point *data.LoadedPoint) []byte {
		var dp deserializedPoint
		dp = *point.Data.(*deserializedPoint)
		return []byte(dp.tags[tagIndex])
	}, nil
}
