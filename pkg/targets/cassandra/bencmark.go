package cassandra

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"log"
)

type benchmark struct {
	dbc                *dbCreator
	dataSourceFileName string
}

func NewBenchmark(dbSpecificConfig *SpecificConfig, dsConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	if dsConfig.Type != source.FileDataSourceType {
		return nil, errors.New("only FILE data source implemented for Cassandra")
	}

	if _, ok := consistencyMapping[dbSpecificConfig.ConsistencyLevel]; !ok {
		return nil, fmt.Errorf(
			"invalid consistency level %s; allowed: %v",
			dbSpecificConfig.ConsistencyLevel,
			consistencyMapping,
		)
	}
	return &benchmark{
		dbc: &dbCreator{
			hosts:             dbSpecificConfig.Hosts,
			consistencyLevel:  dbSpecificConfig.ConsistencyLevel,
			replicationFactor: dbSpecificConfig.ReplicationFactor,
			writeTimeout:      dbSpecificConfig.WriteTimeout,
		},
		dataSourceFileName: dsConfig.File.Location,
	}, nil
}

func (b *benchmark) GetDataSource() targets.DataSource {
	return &fileDataSource{scanner: bufio.NewScanner(load.GetBufferedReader(b.dataSourceFileName))}
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(_ uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{b.dbc}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return b.dbc
}

type processor struct {
	dbc *dbCreator
}

func (p *processor) Init(_ int, _, _ bool) {}

// ProcessBatch reads eventsBatches which contain rows of CQL strings and
// creates a gocql.LoggedBatch to insert
func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	events := b.(*eventsBatch)

	if doLoad {
		batch := p.dbc.clientSession.NewBatch(gocql.LoggedBatch)
		for _, event := range events.rows {
			batch.Query(singleMetricToInsertStatement(event))
		}

		err := p.dbc.clientSession.ExecuteBatch(batch)
		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}
	}
	metricCnt := uint64(len(events.rows))
	events.rows = events.rows[:0]
	ePool.Put(events)
	return metricCnt, 0
}
